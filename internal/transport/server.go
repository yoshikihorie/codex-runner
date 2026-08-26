// Package transport implements the Unix-socket protocol boundary for codexd.
package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const (
	protocolUnknownVerbCode       = "PROTOCOL_UNKNOWN_VERB"
	protocolUnknownVerbMessageKey = "error.protocol.unknownVerb"

	// Canonical source: validation-rules.md PROTOCOL_LINE_MAX_BYTES.
	protocolLineMaxBytes = 1048576

	// Canonical source: 20-codexd-additional-preexisting-bugs.md HIGH Accept retry requirements.
	initialAcceptRetryDelay = 5 * time.Millisecond
	maxAcceptRetryDelay     = time.Second
)

var (
	listenUnix              = net.Listen
	errRequestNotTerminated = errors.New("request line is not newline terminated")
)

// Request is a client protocol envelope.
type Request struct {
	ProtocolVersion string          `json:"protocol_version"`
	Verb            string          `json:"verb"`
	TaskID          string          `json:"task_id,omitempty"`
	Params          json.RawMessage `json:"params,omitempty"`
	RequestID       string          `json:"request_id"`
}

// ErrorBody is the error portion of a failed response.
type ErrorBody struct {
	Code       string         `json:"code"`
	MessageKey string         `json:"message_key"`
	Detail     map[string]any `json:"detail,omitempty"`
}

// Response is a server protocol envelope.
type Response struct {
	ProtocolVersion string          `json:"protocol_version"`
	RequestID       string          `json:"request_id"`
	OK              bool            `json:"ok"`
	Result          json.RawMessage `json:"result,omitempty"`
	Error           *ErrorBody      `json:"error,omitempty"`
}

// TailHandler writes tail responses for a validated tail request.
type TailHandler func(context.Context, Request, io.Writer) error

type envelopeResult int

const (
	envelopeValid envelopeResult = iota
	envelopeRespondError
	envelopeDisconnect
)

type tailConnRegistry struct {
	mu       sync.Mutex
	conns    map[net.Conn]context.CancelFunc
	shutting bool
}

// NewTailConnRegistry creates the registry shared by Serve and shutdown handling.
func NewTailConnRegistry() *tailConnRegistry {
	return &tailConnRegistry{conns: make(map[net.Conn]context.CancelFunc)}
}

func (r *tailConnRegistry) add(conn net.Conn, cancel context.CancelFunc) bool {
	r.mu.Lock()
	if r.shutting {
		r.mu.Unlock()
		cancel()
		_ = conn.Close()
		return false
	}
	if r.conns == nil {
		r.conns = make(map[net.Conn]context.CancelFunc)
	}
	r.conns[conn] = cancel
	r.mu.Unlock()
	return true
}

func (r *tailConnRegistry) remove(conn net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.conns, conn)
}

func (r *tailConnRegistry) closeAll() {
	r.mu.Lock()
	r.shutting = true
	targets := make(map[net.Conn]context.CancelFunc, len(r.conns))
	for conn, cancel := range r.conns {
		targets[conn] = cancel
	}
	clear(r.conns)
	r.mu.Unlock()

	for _, cancel := range targets {
		cancel()
	}
	for conn := range targets {
		_ = conn.Close()
	}
}

// Serve starts a Unix-domain socket listener and stops accepting connections when ctx is cancelled.
func Serve(ctx context.Context, socketPath string, dispatch func(Request) Response, tailHandler TailHandler, wg *sync.WaitGroup, tailConns *tailConnRegistry, acceptLoopDone chan<- struct{}, ready chan<- error) (fs.FileInfo, error) {
	defer close(acceptLoopDone)
	if _, err := os.Lstat(socketPath); err == nil {
		err = fmt.Errorf("socket path already exists; refusing to remove unverified entry: %s; stop its owner, verify the path, remove it manually, and restart codexd", socketPath)
		ready <- err
		return nil, err
	} else if !errors.Is(err, fs.ErrNotExist) {
		err = fmt.Errorf("lstat unix socket path: %w", err)
		ready <- err
		return nil, err
	}
	listener, err := listenUnix("unix", socketPath)
	if err != nil {
		err = fmt.Errorf("listen on unix socket: %w", err)
		ready <- err
		return nil, err
	}
	unixListener, ok := listener.(*net.UnixListener)
	if !ok {
		_ = listener.Close()
		err = fmt.Errorf("unix listener does not support unlink control")
		ready <- err
		return nil, err
	}
	unixListener.SetUnlinkOnClose(false)
	expected, err := os.Lstat(socketPath)
	if err != nil {
		_ = listener.Close()
		err = fmt.Errorf("lstat unix socket: %w", err)
		ready <- err
		return nil, err
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		err = fmt.Errorf("chmod unix socket: %w", err)
		ready <- err
		return expected, err
	}
	ready <- nil
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	defer listener.Close()
	return expected, serveAcceptLoop(ctx, listener, dispatch, tailHandler, wg, tailConns)
}

func serveAcceptLoop(ctx context.Context, listener net.Listener, dispatch func(Request) Response, tailHandler TailHandler, wg *sync.WaitGroup, tailConns *tailConnRegistry) error {
	retryDelay := initialAcceptRetryDelay
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return fmt.Errorf("accept closed listener: %w", err)
			}
			slog.Error("accept failed", "error", err)
			timer := time.NewTimer(retryDelay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return nil
			case <-timer.C:
				retryDelay = nextAcceptRetryDelay(retryDelay)
				continue
			}
		}
		retryDelay = initialAcceptRetryDelay
		wg.Add(1)
		go handleConn(ctx, conn, dispatch, tailHandler, wg, tailConns)
	}
}

func nextAcceptRetryDelay(delay time.Duration) time.Duration {
	if delay >= maxAcceptRetryDelay/2 {
		return maxAcceptRetryDelay
	}
	return delay * 2
}

func handleConn(ctx context.Context, conn net.Conn, dispatch func(Request) Response, tailHandler TailHandler, wg *sync.WaitGroup, tailConns *tailConnRegistry) {
	defer wg.Done()
	defer conn.Close()
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("connection handler panicked", "panic", recovered)
		}
	}()
	stop := context.AfterFunc(ctx, func() { _ = conn.SetReadDeadline(time.Now()) })
	defer stop()
	scanner := bufio.NewScanner(conn)
	// Reserve one byte so Scanner can inspect the terminating newline.
	scanner.Buffer(make([]byte, 64*1024), protocolLineMaxBytes+1)
	scanner.Split(jsonLineSplit)
	for {
		if ctx.Err() != nil {
			return
		}
		if !scanner.Scan() {
			if scanner.Err() != nil {
				return
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		var req Request
		if json.Unmarshal(scanner.Bytes(), &req) != nil {
			return
		}
		if resp, result := validateEnvelope(req); result != envelopeValid {
			if result == envelopeDisconnect {
				return
			}
			if err := writeResponseLine(conn, resp); err != nil {
				logProtocolResponseWriteError(err)
				return
			}
			continue
		}
		if req.Verb == string(domain.ProtocolVerbTail) {
			if err := handleTail(ctx, conn, req, tailHandler, tailConns); err != nil {
				logProtocolResponseWriteError(err)
				return
			}
			continue
		}
		if err := writeResponseLine(conn, dispatch(req)); err != nil {
			logProtocolResponseWriteError(err)
			return
		}
	}
}

func logProtocolResponseWriteError(err error) {
	if errors.Is(err, errProtocolLineTooLong) {
		slog.Error("protocol response line exceeds maximum", "max_bytes", protocolLineMaxBytes)
	}
}

func jsonLineSplit(data []byte, atEOF bool) (advance int, token []byte, err error) {
	for index, value := range data {
		if value == '\n' {
			return index + 1, data[:index], nil
		}
	}
	if atEOF && len(data) > 0 {
		return 0, nil, errRequestNotTerminated
	}
	return 0, nil, nil
}

func validateEnvelope(req Request) (Response, envelopeResult) {
	if req.RequestID == "" {
		return Response{}, envelopeDisconnect
	}
	if !isKnownVerb(req.Verb) {
		return unknownVerbResponse(req.RequestID, req.Verb), envelopeRespondError
	}
	return Response{}, envelopeValid
}
func isKnownVerb(verb string) bool {
	switch verb {
	case string(domain.ProtocolVerbSubmit), string(domain.ProtocolVerbStatus), string(domain.ProtocolVerbCancel), string(domain.ProtocolVerbTail), string(domain.ProtocolVerbPing):
		return true
	default:
		return false
	}
}
func unknownVerbResponse(requestID string, verbs ...string) Response {
	verb := ""
	if len(verbs) > 0 {
		verb = verbs[0]
	}
	return Response{ProtocolVersion: ProtocolVersion, RequestID: requestID, OK: false, Error: &ErrorBody{Code: protocolUnknownVerbCode, MessageKey: protocolUnknownVerbMessageKey, Detail: map[string]any{"verb": verb}}}
}
func handleTail(parentCtx context.Context, conn net.Conn, req Request, tailHandler TailHandler, tailConns *tailConnRegistry) error {
	tailCtx, cancel := context.WithCancel(context.WithoutCancel(parentCtx))
	if !tailConns.add(conn, cancel) {
		return nil
	}
	defer cancel()
	defer tailConns.remove(conn)
	return tailHandler(tailCtx, req, newProtocolLineWriter(conn))
}
