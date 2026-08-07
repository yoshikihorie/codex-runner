// Package transport implements the Unix-socket protocol boundary for codexd.
package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const protocolVersion = "1"
const (
	protocolUnknownVerbCode       = "PROTOCOL_UNKNOWN_VERB"
	protocolUnknownVerbMessageKey = "error.protocol.unknownVerb"
)

var listenUnix = net.Listen

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
type envelopeResult int

const (
	envelopeValid envelopeResult = iota
	envelopeRespondError
	envelopeDisconnect
)

type tailConnRegistry struct {
	mu    sync.Mutex
	conns map[net.Conn]struct{}
}

// NewTailConnRegistry creates the registry shared by Serve and shutdown handling.
func NewTailConnRegistry() *tailConnRegistry {
	return &tailConnRegistry{conns: make(map[net.Conn]struct{})}
}

func (r *tailConnRegistry) add(conn net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.conns == nil {
		r.conns = make(map[net.Conn]struct{})
	}
	r.conns[conn] = struct{}{}
}
func (r *tailConnRegistry) remove(conn net.Conn) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.conns, conn)
}

// Serve starts a Unix-domain socket listener and stops accepting connections when ctx is cancelled.
func Serve(ctx context.Context, socketPath string, dispatch func(Request) Response, wg *sync.WaitGroup, tailConns *tailConnRegistry) error {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove existing socket: %w", err)
	}
	listener, err := listenUnix("unix", socketPath)
	if err != nil {
		return fmt.Errorf("listen on unix socket: %w", err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		return fmt.Errorf("chmod unix socket: %w", err)
	}
	stop := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stop()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("accept failed", "error", err)
			continue
		}
		wg.Add(1)
		go handleConn(conn, dispatch, wg, tailConns)
	}
}
func handleConn(conn net.Conn, dispatch func(Request) Response, wg *sync.WaitGroup, tailConns *tailConnRegistry) {
	defer wg.Done()
	defer conn.Close()
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("connection handler panicked", "panic", recovered)
		}
	}()
	reader := bufio.NewReader(conn)
	encoder := json.NewEncoder(conn)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF && len(line) == 0 {
				return
			}
			return
		}
		var req Request
		if json.Unmarshal(line, &req) != nil {
			return
		}
		if resp, result := validateEnvelope(req); result != envelopeValid {
			if result == envelopeDisconnect {
				return
			}
			if encoder.Encode(resp) != nil {
				return
			}
			continue
		}
		if req.Verb == string(domain.ProtocolVerbTail) {
			handleTail(conn, req, encoder, tailConns)
			continue
		}
		if encoder.Encode(dispatch(req)) != nil {
			return
		}
	}
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
	return Response{ProtocolVersion: protocolVersion, RequestID: requestID, OK: false, Error: &ErrorBody{Code: protocolUnknownVerbCode, MessageKey: protocolUnknownVerbMessageKey, Detail: map[string]any{"verb": verb}}}
}
func handleTail(conn net.Conn, _ Request, _ *json.Encoder, tailConns *tailConnRegistry) {
	tailConns.add(conn)
	defer tailConns.remove(conn)
}
