package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

func testTimeouts() Timeouts {
	return Timeouts{Connect: time.Second, PingTotal: time.Second}
}

func serveOne(t *testing.T, respond func(transport.Request, net.Conn)) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "t1-30-sock-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "codexd.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var req transport.Request
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			return
		}
		respond(req, conn)
	}()
	return path
}

func writeResponse(t *testing.T, conn net.Conn, resp transport.Response) {
	t.Helper()
	if err := json.NewEncoder(conn).Encode(resp); err != nil {
		t.Error(err)
	}
}

func TestDialAndSendForEveryVerb(t *testing.T) {
	for _, verb := range []domain.ProtocolVerb{
		domain.ProtocolVerbSubmit, domain.ProtocolVerbStatus, domain.ProtocolVerbCancel, domain.ProtocolVerbTail, domain.ProtocolVerbPing,
	} {
		verb := verb
		t.Run(string(verb), func(t *testing.T) {
			var received transport.Request
			path := serveOne(t, func(req transport.Request, conn net.Conn) {
				received = req
				result := json.RawMessage(`{"value":true}`)
				if verb == domain.ProtocolVerbTail {
					result = json.RawMessage(`{"line_type":"complete"}`)
				}
				if verb == domain.ProtocolVerbPing {
					result = json.RawMessage(`{"protocol_version":"` + transport.ProtocolVersion + `"}`)
				}
				writeResponse(t, conn, transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: req.RequestID, OK: true, Result: result})
			})
			req, err := NewRequest(verb, "task-1", json.RawMessage(`{"input":true}`))
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			_, code, err := DialAndSend(context.Background(), path, testTimeouts(), req, &out)
			if err != nil || code != exitCodeSuccess {
				t.Fatalf("DialAndSend() code=%d err=%v", code, err)
			}
			if received.ProtocolVersion != transport.ProtocolVersion || received.Verb != string(verb) || received.RequestID == "" {
				t.Fatalf("request = %#v", received)
			}
			if out.Len() == 0 || out.Bytes()[out.Len()-1] != '\n' {
				t.Fatalf("output = %q", out.String())
			}
		})
	}
}

func TestNewRequestGeneratesDistinctCompleteIDs(t *testing.T) {
	first, err := NewRequest(domain.ProtocolVerbPing, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRequest(domain.ProtocolVerbPing, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.RequestID) != 32 || first.RequestID == second.RequestID || first.ProtocolVersion != transport.ProtocolVersion {
		t.Fatalf("requests = %#v, %#v", first, second)
	}
}

func TestNewRequestRejectsShortRandomRead(t *testing.T) {
	original := randomReader
	randomReader = shortErrorReader{}
	t.Cleanup(func() { randomReader = original })
	if _, err := NewRequest(domain.ProtocolVerbPing, "", nil); err == nil {
		t.Fatal("NewRequest() error = nil")
	}
}

func TestDialAndSendTailStreamsUntilComplete(t *testing.T) {
	path := serveOne(t, func(req transport.Request, conn net.Conn) {
		for _, result := range []json.RawMessage{json.RawMessage(`{"line_type":"progress","seq":1}`), json.RawMessage(`{"line_type":"progress","seq":2}`), json.RawMessage(`{"line_type":"complete"}`)} {
			writeResponse(t, conn, transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: req.RequestID, OK: true, Result: result})
		}
	})
	req, err := NewRequest(domain.ProtocolVerbTail, "task-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	_, code, err := DialAndSend(context.Background(), path, testTimeouts(), req, &out)
	if err != nil || code != exitCodeSuccess || strings.Count(out.String(), "\n") != 3 {
		t.Fatalf("code=%d err=%v output=%q", code, err, out.String())
	}
}

func TestDialAndSendProtocolErrorReturnsFailure(t *testing.T) {
	for _, verb := range []domain.ProtocolVerb{domain.ProtocolVerbSubmit, domain.ProtocolVerbStatus, domain.ProtocolVerbCancel, domain.ProtocolVerbTail, domain.ProtocolVerbPing} {
		t.Run(string(verb), func(t *testing.T) {
			path := serveOne(t, func(req transport.Request, conn net.Conn) {
				writeResponse(t, conn, transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: req.RequestID, OK: false, Error: &transport.ErrorBody{Code: "REJECTED", MessageKey: "error.rejected"}})
			})
			req, err := NewRequest(verb, "task-1", nil)
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			_, code, err := DialAndSend(context.Background(), path, testTimeouts(), req, &out)
			if err != nil || code != exitCodeFailure || out.Len() == 0 {
				t.Fatalf("code=%d err=%v output=%q", code, err, out.String())
			}
		})
	}
}

func TestDialAndSendRejectsInvalidResponseBeforeForwarding(t *testing.T) {
	path := serveOne(t, func(_ transport.Request, conn net.Conn) {
		_, _ = io.WriteString(conn, `{"protocol_version":"1","request_id":"wrong","ok":true,"result":{}}`+"\n")
	})
	req, err := NewRequest(domain.ProtocolVerbStatus, "task-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	_, code, err := DialAndSend(context.Background(), path, testTimeouts(), req, &out)
	if err == nil || code != exitCodeUnavailable || out.Len() != 0 {
		t.Fatalf("code=%d err=%v output=%q", code, err, out.String())
	}
}

func TestDialAndSendRejectsTailEOFAndUnknownLineType(t *testing.T) {
	for name, result := range map[string]string{"eof": `{"line_type":"progress"}`, "unknown": `{"line_type":"other"}`} {
		name, result := name, result
		t.Run(name, func(t *testing.T) {
			path := serveOne(t, func(req transport.Request, conn net.Conn) {
				writeResponse(t, conn, transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: req.RequestID, OK: true, Result: json.RawMessage(result)})
			})
			req, err := NewRequest(domain.ProtocolVerbTail, "task-1", nil)
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			_, code, err := DialAndSend(context.Background(), path, testTimeouts(), req, &out)
			if err == nil || code != exitCodeUnavailable {
				t.Fatalf("code=%d err=%v", code, err)
			}
		})
	}
}

func TestDialAndSendPingProducesUnavailableResponses(t *testing.T) {
	t.Run("connect refused", func(t *testing.T) {
		req, err := NewRequest(domain.ProtocolVerbPing, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		_, code, err := DialAndSend(context.Background(), filepath.Join(t.TempDir(), "missing.sock"), testTimeouts(), req, &out)
		if err == nil || code != exitCodeUnavailable || !strings.Contains(out.String(), string(domain.ExecutionRouteReasonConnectRefused)) {
			t.Fatalf("code=%d err=%v output=%q", code, err, out.String())
		}
	})
	t.Run("version unknown", func(t *testing.T) {
		path := serveOne(t, func(req transport.Request, conn net.Conn) {
			writeResponse(t, conn, transport.Response{ProtocolVersion: "999", RequestID: req.RequestID, OK: true, Result: json.RawMessage(`{"protocol_version":"999"}`)})
		})
		req, err := NewRequest(domain.ProtocolVerbPing, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		_, code, err := DialAndSend(context.Background(), path, testTimeouts(), req, &out)
		if err == nil || code != exitCodeUnavailable || !strings.Contains(out.String(), string(domain.ExecutionRouteReasonVersionUnknown)) {
			t.Fatalf("code=%d err=%v output=%q", code, err, out.String())
		}
	})
}

func TestDialAndSendPingClassifiesDialTimeout(t *testing.T) {
	original := dialContext
	dialContext = func(context.Context, time.Duration, string) (net.Conn, error) {
		return nil, timeoutError{}
	}
	t.Cleanup(func() { dialContext = original })
	req, err := NewRequest(domain.ProtocolVerbPing, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	_, code, err := DialAndSend(context.Background(), "ignored", testTimeouts(), req, &out)
	if err == nil || code != exitCodeUnavailable || !strings.Contains(out.String(), string(domain.ExecutionRouteReasonConnectTimeout)) {
		t.Fatalf("code=%d err=%v output=%q", code, err, out.String())
	}
}

func TestDecodeResponseLineRejectsMalformedEnvelopes(t *testing.T) {
	for name, line := range map[string]string{
		"missing protocol version": `{"request_id":"r","ok":true,"result":{}}`,
		"missing request ID":       `{"protocol_version":"1","ok":true,"result":{}}`,
		"missing ok":               `{"protocol_version":"1","request_id":"r","result":{}}`,
		"success error":            `{"protocol_version":"1","request_id":"r","ok":true,"result":{},"error":{}}`,
		"failure result":           `{"protocol_version":"1","request_id":"r","ok":false,"result":{},"error":{"code":"X","message_key":"x"}}`,
		"success no result":        `{"protocol_version":"1","request_id":"r","ok":true}`,
		"failure no error":         `{"protocol_version":"1","request_id":"r","ok":false}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeResponseLine([]byte(line), "r"); err == nil {
				t.Fatal("decodeResponseLine() error = nil")
			}
		})
	}
}

func TestDialAndSendRejectsOversizeLine(t *testing.T) {
	path := serveOne(t, func(_ transport.Request, conn net.Conn) {
		_, _ = io.WriteString(conn, strings.Repeat("x", protocolLineMaxBytes+1)+"\n")
	})
	req, err := NewRequest(domain.ProtocolVerbStatus, "task-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	_, code, err := DialAndSend(context.Background(), path, testTimeouts(), req, &out)
	if err == nil || code != exitCodeUnavailable || out.Len() != 0 {
		t.Fatalf("code=%d err=%v output=%q", code, err, out.String())
	}
}

func TestDialAndSendRejectsOversizeRequest(t *testing.T) {
	path := serveOne(t, func(_ transport.Request, _ net.Conn) {
		t.Error("server received an oversized request")
	})
	req, err := NewRequest(domain.ProtocolVerbStatus, "task-1", json.RawMessage(`"`+strings.Repeat("x", protocolLineMaxBytes)+`"`))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	_, code, err := DialAndSend(context.Background(), path, testTimeouts(), req, &out)
	if err == nil || code != exitCodeUnavailable || !strings.Contains(err.Error(), "request line exceeds") || out.Len() != 0 {
		t.Fatalf("code=%d err=%v output=%q", code, err, out.String())
	}
}

func TestDialAndSendResponseLineBoundary(t *testing.T) {
	for name, size := range map[string]int{
		"at maximum":    protocolLineMaxBytes,
		"above maximum": protocolLineMaxBytes + 1,
	} {
		name, size := name, size
		t.Run(name, func(t *testing.T) {
			path := serveOne(t, func(req transport.Request, conn net.Conn) {
				line := responseLineOfSize(t, req.RequestID, size)
				_, _ = conn.Write(append(line, '\n'))
			})
			req, err := NewRequest(domain.ProtocolVerbStatus, "task-1", nil)
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			_, code, err := DialAndSend(context.Background(), path, testTimeouts(), req, &out)
			if size == protocolLineMaxBytes {
				if err != nil || code != exitCodeSuccess || out.Len() != size+1 {
					t.Fatalf("code=%d err=%v output length=%d", code, err, out.Len())
				}
				return
			}
			if err == nil || code != exitCodeUnavailable || out.Len() != 0 {
				t.Fatalf("code=%d err=%v output length=%d", code, err, out.Len())
			}
		})
	}
}

func responseLineOfSize(t *testing.T, requestID string, size int) []byte {
	t.Helper()
	response := transport.Response{
		ProtocolVersion: transport.ProtocolVersion,
		RequestID:       requestID,
		OK:              true,
		Result:          json.RawMessage(`{"value":""}`),
	}
	emptyLine, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	paddingLength := size - len(emptyLine)
	if paddingLength < 0 {
		t.Fatalf("response size %d is smaller than the envelope", size)
	}
	response.Result = json.RawMessage(`{"value":"` + strings.Repeat("x", paddingLength) + `"}`)
	line, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(line) != size {
		t.Fatalf("response line length=%d, want %d", len(line), size)
	}
	return line
}

func TestResolveExitCode(t *testing.T) {
	if ResolveExitCode(false, transport.Response{}) != exitCodeUnavailable || ResolveExitCode(true, transport.Response{OK: true}) != exitCodeSuccess || ResolveExitCode(true, transport.Response{}) != exitCodeFailure {
		t.Fatal("unexpected exit code")
	}
}

type shortErrorReader struct{}

func (shortErrorReader) Read(p []byte) (int, error) {
	copy(p, []byte{1, 2, 3})
	return 3, errors.New("short random read")
}

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return false }
