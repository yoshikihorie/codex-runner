package transport

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type transientAcceptListener struct {
	accepted chan struct{}
	closed   chan struct{}
	once     sync.Once
}

func (l *transientAcceptListener) Accept() (net.Conn, error) {
	select {
	case <-l.accepted:
		<-l.closed
		return nil, net.ErrClosed
	default:
		close(l.accepted)
		return nil, errors.New("temporary accept error")
	}
}

func (l *transientAcceptListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *transientAcceptListener) Addr() net.Addr { return &net.UnixAddr{Name: "test", Net: "unix"} }

func TestValidateEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  Request
		want envelopeResult
	}{
		{"missing request id", Request{Verb: "submit"}, envelopeDisconnect}, {"empty request id", Request{Verb: "submit"}, envelopeDisconnect},
		{"missing verb", Request{RequestID: "r"}, envelopeRespondError}, {"unknown verb", Request{Verb: "x", RequestID: "r"}, envelopeRespondError},
		{"both invalid", Request{}, envelopeDisconnect}, {"empty task id allowed", Request{Verb: "status", RequestID: "r"}, envelopeValid},
		{"malformed task id allowed", Request{Verb: "cancel", TaskID: "bad", RequestID: "r"}, envelopeValid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, got := validateEnvelope(tc.req)
			if got != tc.want {
				t.Fatalf("result = %v", got)
			}
			if got == envelopeRespondError && (resp.Error == nil || resp.Error.Code != protocolUnknownVerbCode) {
				t.Fatalf("response = %#v", resp)
			}
			if got == envelopeRespondError && resp.Error.Detail["verb"] != tc.req.Verb {
				t.Fatalf("unknown verb detail = %#v", resp.Error.Detail)
			}
		})
	}
}

func TestNewTailConnRegistryInitializesConnectionMap(t *testing.T) {
	registry := NewTailConnRegistry()
	if registry == nil || registry.conns == nil {
		t.Fatalf("registry = %#v", registry)
	}
}

func TestHandleConnRecoversDispatcherPanic(t *testing.T) {
	server, client := net.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	go handleConn(server, func(Request) Response { panic("dispatcher panic") }, &wg, NewTailConnRegistry())
	if _, err := client.Write([]byte("{\"verb\":\"ping\",\"request_id\":\"r\"}\n")); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if n, err := client.Read(b[:]); n != 0 || err == nil {
		t.Fatalf("connection was not closed after panic: n=%d err=%v", n, err)
	}
	client.Close()
	wg.Wait()
}

func TestVerbNumberFailsJSONDecoding(t *testing.T) {
	var req Request
	if err := json.Unmarshal([]byte(`{"verb":123,"request_id":"r"}`), &req); err == nil {
		t.Fatal("invalid verb decoded")
	}
}

func TestHandleConnJSONLinesAndTail(t *testing.T) {
	server, client := net.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	calls := 0
	registry := &tailConnRegistry{}
	go handleConn(server, func(req Request) Response {
		calls++
		return Response{ProtocolVersion: ProtocolVersion, RequestID: req.RequestID, OK: true, Result: json.RawMessage(`{}`)}
	}, &wg, registry)
	if _, err := client.Write([]byte("{\"verb\":\"submit\",\"request_id\":\"one\"}\n{\"verb\":\"tail\",\"request_id\":\"two\"}\n")); err != nil {
		t.Fatal(err)
	}
	var resp Response
	if err := json.NewDecoder(client).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if !resp.OK || resp.RequestID != "one" || calls != 1 {
		t.Fatalf("response=%#v calls=%d", resp, calls)
	}
	client.Close()
	wg.Wait()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.conns) != 0 {
		t.Fatal("tail connection was not removed")
	}
}

func TestHandleConnDisconnectsForBadOrConcatenatedJSON(t *testing.T) {
	for _, line := range []string{"{bad}\n", "{\"verb\":\"ping\",\"request_id\":\"a\"}{\"verb\":\"ping\",\"request_id\":\"b\"}\n"} {
		server, client := net.Pipe()
		var wg sync.WaitGroup
		wg.Add(1)
		go handleConn(server, func(Request) Response { t.Fatal("dispatch called"); return Response{} }, &wg, &tailConnRegistry{})
		if _, err := client.Write([]byte(line)); err != nil {
			t.Fatal(err)
		}
		_ = client.SetReadDeadline(time.Now().Add(time.Second))
		var b [1]byte
		if n, err := client.Read(b[:]); n != 0 || err == nil {
			t.Fatalf("connection was not closed: n=%d err=%v", n, err)
		}
		client.Close()
		wg.Wait()
	}
}

func TestHandleConnWaitsForSplitLine(t *testing.T) {
	server, client := net.Pipe()
	var wg sync.WaitGroup
	wg.Add(1)
	go handleConn(server, func(req Request) Response {
		return Response{ProtocolVersion: ProtocolVersion, RequestID: req.RequestID, OK: true, Result: json.RawMessage(`{}`)}
	}, &wg, &tailConnRegistry{})
	if _, err := client.Write([]byte("{\"verb\":\"ping\",")); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	var b [1]byte
	if _, err := client.Read(b[:]); err == nil {
		t.Fatal("response before complete line")
	}
	_ = client.SetReadDeadline(time.Time{})
	_, _ = client.Write([]byte("\"request_id\":\"r\"}\n"))
	var resp Response
	if err := json.NewDecoder(client).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	client.Close()
	wg.Wait()
}

func TestServeSocketPermissionsAndCancellation(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "t1-03-sock-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "codexd.sock")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, path, func(req Request) Response {
			return Response{ProtocolVersion: ProtocolVersion, RequestID: req.RequestID, OK: true, Result: json.RawMessage(`{}`)}
		}, &wg, &tailConnRegistry{})
	}()
	var conn net.Conn
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var err error
		conn, err = net.Dial("unix", path)
		if err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if conn == nil {
		t.Fatal("server did not start")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
	conn.Close()
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, err := net.Dial("unix", path); err == nil {
		t.Fatal("dial succeeded after cancellation")
	}
	wg.Wait()
}

func TestServeContinuesAfterTransientAcceptError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "codexd.sock")
	listener := &transientAcceptListener{accepted: make(chan struct{}), closed: make(chan struct{})}
	originalListenUnix := listenUnix
	listenUnix = func(_, address string) (net.Listener, error) {
		if err := os.WriteFile(address, nil, 0o600); err != nil {
			return nil, err
		}
		return listener, nil
	}
	t.Cleanup(func() { listenUnix = originalListenUnix })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, path, func(Request) Response { return Response{} }, &wg, &tailConnRegistry{})
	}()
	select {
	case <-listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("Serve did not call Accept")
	}
	select {
	case err := <-done:
		t.Fatalf("Serve returned after transient Accept error: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestServeRejectsBindFailure(t *testing.T) {
	var wg sync.WaitGroup
	if err := Serve(context.Background(), "/dev/null/socket", func(Request) Response { return Response{} }, &wg, &tailConnRegistry{}); err == nil {
		t.Fatal("expected bind error")
	}
}

func TestResponseEnvelope(t *testing.T) {
	resp := unknownVerbResponse("r")
	if resp.ProtocolVersion != ProtocolVersion || resp.RequestID != "r" || resp.OK || resp.Error == nil {
		t.Fatalf("response=%#v", resp)
	}
}
