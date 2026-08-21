// Package client implements the one-request Unix-socket client boundary.
package client

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/transport"
	"github.com/yoshikihorie/codex-runner/internal/transport/schema"
)

const (
	exitCodeSuccess     = 0
	exitCodeFailure     = 1
	exitCodeUnavailable = 2

	protocolLineMaxBytes = 1048576
)

var (
	randomReader = io.Reader(rand.Reader)
	dialContext  = func(ctx context.Context, timeout time.Duration, socketPath string) (net.Conn, error) {
		return (&net.Dialer{Timeout: timeout}).DialContext(ctx, "unix", socketPath)
	}
	errResponseNotTerminated = errors.New("response line is not newline terminated")
)

// Timeouts controls the connection limit and the complete ping round-trip limit.
type Timeouts struct {
	Connect   time.Duration
	PingTotal time.Duration
}

// DialAndSend sends exactly one request on exactly one Unix-domain socket connection.
func DialAndSend(ctx context.Context, socketPath string, timeouts Timeouts, req transport.Request, out io.Writer) (transport.Response, int, error) {
	if err := validateInputs(socketPath, timeouts, req, out); err != nil {
		return transport.Response{}, exitCodeUnavailable, err
	}
	requestLine, err := json.Marshal(req)
	if err != nil {
		return transport.Response{}, exitCodeUnavailable, fmt.Errorf("encode request: %w", err)
	}
	if len(requestLine) > protocolLineMaxBytes {
		return transport.Response{}, exitCodeUnavailable, fmt.Errorf("request line exceeds %d bytes", protocolLineMaxBytes)
	}
	requestFrame := append(requestLine, '\n')

	started := time.Now()
	dialCtx := ctx
	var cancel context.CancelFunc
	if req.Verb == string(domain.ProtocolVerbPing) {
		dialCtx, cancel = context.WithDeadline(ctx, started.Add(timeouts.PingTotal))
		defer cancel()
	}

	conn, err := dialContext(dialCtx, timeouts.Connect, socketPath)
	if err != nil {
		return pingFailureIfNeeded(req, out, dialErrorReason(err), fmt.Errorf("connect: %w", err))
	}
	defer conn.Close()
	stop := context.AfterFunc(dialCtx, func() { _ = conn.Close() })
	defer stop()

	if req.Verb == string(domain.ProtocolVerbPing) {
		if err := conn.SetDeadline(started.Add(timeouts.PingTotal)); err != nil {
			return pingFailureIfNeeded(req, out, domain.ExecutionRouteReasonPingTimeout, fmt.Errorf("set ping deadline: %w", err))
		}
	}

	if err := writeAll(conn, requestFrame); err != nil {
		return pingFailureIfNeeded(req, out, pingStageReason(req, err), fmt.Errorf("send request: %w", err))
	}

	scanner := bufio.NewScanner(conn)
	// Canonical source: validation-rules.md PROTOCOL_LINE_MAX_BYTES.
	// Reserve one byte so Scanner can inspect the terminating newline.
	scanner.Buffer(make([]byte, 64*1024), protocolLineMaxBytes+1)
	scanner.Split(jsonLineSplit)
	var last transport.Response
	for {
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return pingFailureIfNeeded(req, out, pingStageReason(req, err), fmt.Errorf("read response: %w", err))
			}
			return pingFailureIfNeeded(req, out, pingStageReason(req, io.EOF), errors.New("response ended before completion"))
		}

		line := append([]byte(nil), scanner.Bytes()...)
		resp, err := decodeResponseLine(line, req.RequestID)
		if err != nil {
			return pingFailureIfNeeded(req, out, pingStageReason(req, err), fmt.Errorf("decode response: %w", err))
		}
		if req.Verb == string(domain.ProtocolVerbPing) {
			if err := validatePingVersion(resp); err != nil {
				return pingFailureIfNeeded(req, out, domain.ExecutionRouteReasonVersionUnknown, err)
			}
		}
		if req.Verb == string(domain.ProtocolVerbTail) && resp.OK {
			complete, err := tailComplete(resp.Result)
			if err != nil {
				return transport.Response{}, exitCodeUnavailable, fmt.Errorf("validate tail response: %w", err)
			}
			if err := writeRawLine(out, line); err != nil {
				return transport.Response{}, exitCodeUnavailable, fmt.Errorf("forward response: %w", err)
			}
			last = resp
			if complete {
				return last, ResolveExitCode(true, last), nil
			}
			continue
		}

		if err := writeRawLine(out, line); err != nil {
			return transport.Response{}, exitCodeUnavailable, fmt.Errorf("forward response: %w", err)
		}
		last = resp
		return last, ResolveExitCode(true, last), nil
	}
}

func jsonLineSplit(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for index, value := range data {
		if value == '\n' {
			return index + 1, data[:index], nil
		}
	}
	if atEOF && len(data) > 0 {
		return 0, nil, errResponseNotTerminated
	}
	return 0, nil, nil
}

// ResolveExitCode maps a completed round trip and its protocol response to the client contract.
func ResolveExitCode(reachable bool, resp transport.Response) int {
	if !reachable {
		return exitCodeUnavailable
	}
	if resp.OK {
		return exitCodeSuccess
	}
	return exitCodeFailure
}

// NewRequest creates a protocol request with a cryptographically random request ID.
func NewRequest(verb domain.ProtocolVerb, taskID string, params json.RawMessage) (transport.Request, error) {
	if !knownVerb(verb) {
		return transport.Request{}, fmt.Errorf("unknown protocol verb: %q", verb)
	}
	return newRequest(randomReader, verb, taskID, params)
}

func newRequest(random io.Reader, verb domain.ProtocolVerb, taskID string, params json.RawMessage) (transport.Request, error) {
	raw := make([]byte, 16)
	if _, err := io.ReadFull(random, raw); err != nil {
		return transport.Request{}, fmt.Errorf("generate request ID: %w", err)
	}
	return transport.Request{
		ProtocolVersion: transport.ProtocolVersion,
		Verb:            string(verb),
		TaskID:          taskID,
		Params:          params,
		RequestID:       hex.EncodeToString(raw),
	}, nil
}

func validateInputs(socketPath string, timeouts Timeouts, req transport.Request, out io.Writer) error {
	switch {
	case socketPath == "":
		return errors.New("socket path is required")
	case timeouts.Connect <= 0:
		return errors.New("connect timeout must be positive")
	case timeouts.PingTotal <= 0:
		return errors.New("ping total timeout must be positive")
	case !knownVerb(domain.ProtocolVerb(req.Verb)):
		return fmt.Errorf("unknown protocol verb: %q", req.Verb)
	case req.RequestID == "":
		return errors.New("request ID is required")
	case out == nil:
		return errors.New("output writer is required")
	default:
		return nil
	}
}

func knownVerb(verb domain.ProtocolVerb) bool {
	switch verb {
	case domain.ProtocolVerbSubmit, domain.ProtocolVerbStatus, domain.ProtocolVerbCancel, domain.ProtocolVerbTail, domain.ProtocolVerbPing:
		return true
	default:
		return false
	}
}

func decodeResponseLine(line []byte, requestID string) (transport.Response, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(line, &fields); err != nil {
		return transport.Response{}, err
	}
	for _, key := range []string{"protocol_version", "request_id", "ok"} {
		if _, exists := fields[key]; !exists {
			return transport.Response{}, fmt.Errorf("response field %s is missing", key)
		}
	}
	var resp transport.Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return transport.Response{}, err
	}
	if resp.ProtocolVersion == "" || resp.RequestID != requestID {
		return transport.Response{}, errors.New("invalid response correlation")
	}
	if resp.OK {
		if len(resp.Result) == 0 {
			return transport.Response{}, errors.New("successful response has no result")
		}
		if _, exists := fields["error"]; exists {
			return transport.Response{}, errors.New("successful response has an error body")
		}
	} else {
		if _, exists := fields["result"]; exists {
			return transport.Response{}, errors.New("failed response has a result")
		}
		if resp.Error == nil || resp.Error.Code == "" || resp.Error.MessageKey == "" {
			return transport.Response{}, errors.New("failed response has no error body")
		}
	}
	return resp, nil
}

func tailComplete(result json.RawMessage) (bool, error) {
	var line struct {
		LineType string `json:"line_type"`
	}
	if err := json.Unmarshal(result, &line); err != nil {
		return false, err
	}
	switch line.LineType {
	case schema.LineTypeProgress:
		return false, nil
	case schema.LineTypeComplete:
		return true, nil
	default:
		return false, fmt.Errorf("unknown tail line type: %q", line.LineType)
	}
}

func validatePingVersion(resp transport.Response) error {
	if resp.ProtocolVersion != transport.ProtocolVersion {
		return errors.New("unknown response protocol version")
	}
	if !resp.OK {
		return nil
	}
	var result struct {
		ProtocolVersion string `json:"protocol_version"`
	}
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		return fmt.Errorf("decode ping result: %w", err)
	}
	if result.ProtocolVersion != transport.ProtocolVersion {
		return errors.New("unknown ping protocol version")
	}
	return nil
}

func writeRawLine(out io.Writer, line []byte) error {
	if err := writeAll(out, line); err != nil {
		return err
	}
	return writeAll(out, []byte{'\n'})
}

func writeAll(out io.Writer, data []byte) error {
	n, err := out.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func dialErrorReason(err error) domain.ExecutionRouteReason {
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return domain.ExecutionRouteReasonConnectTimeout
	}
	return domain.ExecutionRouteReasonConnectRefused
}

func pingStageReason(req transport.Request, err error) domain.ExecutionRouteReason {
	if req.Verb != string(domain.ProtocolVerbPing) {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return domain.ExecutionRouteReasonPingTimeout
	}
	var networkErr net.Error
	if errors.As(err, &networkErr) && networkErr.Timeout() {
		return domain.ExecutionRouteReasonPingTimeout
	}
	return domain.ExecutionRouteReasonPingTimeout
}

func pingFailureIfNeeded(req transport.Request, out io.Writer, reason domain.ExecutionRouteReason, err error) (transport.Response, int, error) {
	if req.Verb != string(domain.ProtocolVerbPing) {
		return transport.Response{}, exitCodeUnavailable, err
	}
	if writeErr := writeUnavailable(out, reason); writeErr != nil {
		return transport.Response{}, exitCodeUnavailable, fmt.Errorf("%w; write unavailable response: %v", err, writeErr)
	}
	return transport.Response{}, exitCodeUnavailable, err
}

func writeUnavailable(out io.Writer, reason domain.ExecutionRouteReason) error {
	line, err := json.Marshal(struct {
		OK          bool                        `json:"ok"`
		Unavailable bool                        `json:"unavailable"`
		Reason      domain.ExecutionRouteReason `json:"reason"`
	}{OK: false, Unavailable: true, Reason: reason})
	if err != nil {
		return err
	}
	return writeRawLine(out, line)
}
