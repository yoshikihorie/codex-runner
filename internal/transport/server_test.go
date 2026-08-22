package transport

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func noOpTailHandler(context.Context, Request, io.Writer) error { return nil }

const protocolLineMaxBytesForTest = 1048576

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
	go handleConn(context.Background(), server, func(Request) Response { panic("dispatcher panic") }, noOpTailHandler, &wg, NewTailConnRegistry())
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
	go handleConn(context.Background(), server, func(req Request) Response {
		calls++
		return Response{ProtocolVersion: ProtocolVersion, RequestID: req.RequestID, OK: true, Result: json.RawMessage(`{}`)}
	}, noOpTailHandler, &wg, registry)
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
		go handleConn(context.Background(), server, func(Request) Response { t.Fatal("dispatch called"); return Response{} }, noOpTailHandler, &wg, &tailConnRegistry{})
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
	go handleConn(context.Background(), server, func(req Request) Response {
		return Response{ProtocolVersion: ProtocolVersion, RequestID: req.RequestID, OK: true, Result: json.RawMessage(`{}`)}
	}, noOpTailHandler, &wg, &tailConnRegistry{})
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

func TestHandleConnRequestLineBoundary(t *testing.T) {
	for name, size := range map[string]int{
		"at maximum":    protocolLineMaxBytesForTest,
		"above maximum": protocolLineMaxBytesForTest + 1,
	} {
		name, size := name, size
		t.Run(name, func(t *testing.T) {
			server, client := net.Pipe()
			defer client.Close()
			var wg sync.WaitGroup
			calls := 0
			wg.Add(1)
			go handleConn(context.Background(), server, func(req Request) Response {
				calls++
				return Response{ProtocolVersion: ProtocolVersion, RequestID: req.RequestID, OK: true, Result: json.RawMessage(`{}`)}
			}, noOpTailHandler, &wg, NewTailConnRegistry())

			line := append(requestLineOfSize(t, size), '\n')
			writeDone := make(chan error, 1)
			go func() {
				_, err := client.Write(line)
				writeDone <- err
			}()

			if size == protocolLineMaxBytesForTest {
				var response Response
				if err := json.NewDecoder(client).Decode(&response); err != nil {
					t.Fatal(err)
				}
				if !response.OK || response.RequestID != "line-limit" || calls != 1 {
					t.Fatalf("response=%#v calls=%d", response, calls)
				}
				select {
				case err := <-writeDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("maximum-size request write did not finish")
				}
				_ = client.Close()
				waitForHandlers(t, &wg)
				return
			}

			assertConnectionClosed(t, client)
			if calls != 0 {
				t.Fatalf("dispatch calls = %d, want 0", calls)
			}
			select {
			case <-writeDone:
			case <-time.After(time.Second):
				t.Fatal("oversize request write did not finish")
			}
			waitForHandlers(t, &wg)
		})
	}
}

func TestHandleConnRejectsUnterminatedOversizeRequestLine(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	var wg sync.WaitGroup
	calls := 0
	wg.Add(1)
	go handleConn(context.Background(), server, func(Request) Response {
		calls++
		return Response{}
	}, noOpTailHandler, &wg, NewTailConnRegistry())

	line := requestLineOfSize(t, protocolLineMaxBytesForTest+1)
	writeDone := make(chan error, 1)
	go func() {
		_, err := client.Write(line)
		writeDone <- err
	}()

	assertConnectionClosed(t, client)
	if calls != 0 {
		t.Fatalf("dispatch calls = %d, want 0", calls)
	}
	select {
	case <-writeDone:
	case <-time.After(time.Second):
		t.Fatal("unterminated oversize request write did not finish")
	}
	waitForHandlers(t, &wg)
}

func requestLineOfSize(t *testing.T, size int) []byte {
	t.Helper()

	req := Request{
		ProtocolVersion: ProtocolVersion,
		Verb:            string(domain.ProtocolVerbPing),
		RequestID:       "line-limit",
		Params:          json.RawMessage(`{"padding":""}`),
	}
	empty, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	req.Params = json.RawMessage(`{"padding":"` + strings.Repeat("x", size-len(empty)) + `"}`)
	line, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(line) != size {
		t.Fatalf("line length = %d, want %d", len(line), size)
	}
	return line
}

func assertConnectionClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if n, err := conn.Read(b[:]); n != 0 || err == nil {
		t.Fatalf("response bytes=%d err=%v, want closed connection without response", n, err)
	}
}

func waitForHandlers(t *testing.T, wg *sync.WaitGroup) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("connection handler did not exit")
	}
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
		}, noOpTailHandler, &wg, &tailConnRegistry{}, make(chan struct{}))
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
		done <- Serve(ctx, path, func(Request) Response { return Response{} }, noOpTailHandler, &wg, &tailConnRegistry{}, make(chan struct{}))
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
	if err := Serve(context.Background(), "/dev/null/socket", func(Request) Response { return Response{} }, noOpTailHandler, &wg, &tailConnRegistry{}, make(chan struct{})); err == nil {
		t.Fatal("expected bind error")
	}
}

func TestHandleConnTailHandlerWritesLinesAndConnectionRemainsReusable(t *testing.T) {
	server, client := net.Pipe()
	var wg sync.WaitGroup
	registry := NewTailConnRegistry()
	called := make(chan Request, 1)
	tailHandler := func(_ context.Context, req Request, out io.Writer) error {
		called <- req
		encoder := json.NewEncoder(out)
		for _, lineType := range []string{"progress", "progress", "complete"} {
			if err := encoder.Encode(Response{ProtocolVersion: ProtocolVersion, RequestID: req.RequestID, OK: true, Result: json.RawMessage(`{"line_type":"` + lineType + `"}`)}); err != nil {
				return err
			}
		}
		return nil
	}
	wg.Add(1)
	go handleConn(context.Background(), server, func(req Request) Response {
		return Response{ProtocolVersion: ProtocolVersion, RequestID: req.RequestID, OK: true, Result: json.RawMessage(`{}`)}
	}, tailHandler, &wg, registry)

	if _, err := client.Write([]byte(`{"protocol_version":"` + ProtocolVersion + `","verb":"tail","task_id":"task-1","request_id":"tail-1"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case req := <-called:
		if req.Verb != "tail" || req.TaskID != "task-1" || req.RequestID != "tail-1" {
			t.Fatalf("tail request = %#v", req)
		}
	case <-time.After(time.Second):
		t.Fatal("tail handler was not called")
	}
	decoder := json.NewDecoder(client)
	for index, wantLineType := range []string{"progress", "progress", "complete"} {
		var response Response
		if err := decoder.Decode(&response); err != nil {
			t.Fatal(err)
		}
		var result struct {
			LineType string `json:"line_type"`
		}
		if err := json.Unmarshal(response.Result, &result); err != nil {
			t.Fatal(err)
		}
		if response.ProtocolVersion != ProtocolVersion || response.RequestID != "tail-1" || !response.OK || response.Result == nil || result.LineType != wantLineType {
			t.Fatalf("tail response %d = %#v, result = %#v", index, response, result)
		}
	}
	if _, err := client.Write([]byte(`{"protocol_version":"` + ProtocolVersion + `","verb":"ping","request_id":"ping-2"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	var ping Response
	if err := decoder.Decode(&ping); err != nil {
		t.Fatal(err)
	}
	if !ping.OK || ping.RequestID != "ping-2" {
		t.Fatalf("ping response = %#v", ping)
	}
	registry.mu.Lock()
	remaining := len(registry.conns)
	registry.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("tail connection registry entries = %d", remaining)
	}
	_ = client.Close()
	wg.Wait()
}

func TestHandleConnAllowsInFlightRequestButRejectsNextRequestAfterShutdown(t *testing.T) {
	server, client := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	started := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	wg.Add(1)
	go handleConn(ctx, server, func(req Request) Response {
		calls++
		close(started)
		<-release
		return Response{ProtocolVersion: ProtocolVersion, RequestID: req.RequestID, OK: true, Result: json.RawMessage(`{}`)}
	}, noOpTailHandler, &wg, NewTailConnRegistry())

	if _, err := client.Write([]byte(`{"verb":"ping","request_id":"first"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dispatcher did not start")
	}
	cancel()
	close(release)
	var response Response
	if err := json.NewDecoder(client).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.RequestID != "first" {
		t.Fatalf("response = %#v", response)
	}
	_, _ = client.Write([]byte(`{"verb":"ping","request_id":"second"}` + "\n"))
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if n, err := client.Read(b[:]); n != 0 || err == nil {
		t.Fatalf("connection remained open after shutdown: n=%d err=%v", n, err)
	}
	_ = client.Close()
	wg.Wait()
	if calls != 1 {
		t.Fatalf("dispatch calls = %d", calls)
	}
}

func TestHandleConnUnblocksReadWhenShutdownBegins(t *testing.T) {
	server, client := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go handleConn(ctx, server, func(Request) Response { return Response{} }, noOpTailHandler, &wg, NewTailConnRegistry())
	if _, err := client.Write([]byte(`{"verb":"ping"`)); err != nil {
		t.Fatal(err)
	}
	cancel()
	completed := make(chan struct{})
	go func() {
		wg.Wait()
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("connection handler remained blocked reading a line")
	}
	_ = client.Close()
}

func TestServeKeepsTailContextAliveUntilRegistryShutdown(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "t1-23-batch4-sock-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "codexd.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	observed := make(chan error, 1)
	tailHandler := func(handlerCtx context.Context, _ Request, _ io.Writer) error {
		close(started)
		<-handlerCtx.Done()
		observed <- handlerCtx.Err()
		return nil
	}
	registry := NewTailConnRegistry()
	var wg sync.WaitGroup
	acceptLoopDone := make(chan struct{})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- Serve(ctx, path, func(Request) Response { return Response{} }, tailHandler, &wg, registry, acceptLoopDone)
	}()

	var client net.Conn
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		client, _ = net.Dial("unix", path)
		if client != nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if client == nil {
		t.Fatal("server did not start")
	}
	defer client.Close()
	if _, err := client.Write([]byte(`{"verb":"tail","request_id":"tail-1"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("tail handler did not start")
	}
	cancel()
	select {
	case err := <-observed:
		t.Fatalf("tail handler context cancelled before registry shutdown: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
	registry.closeAll()
	select {
	case err := <-observed:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler context error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("tail handler did not receive registry cancellation")
	}
	wg.Wait()
	registry.mu.Lock()
	remaining := len(registry.conns)
	registry.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("tail connection registry entries = %d", remaining)
	}
}

func TestHandleConnClosesAfterTailHandlerError(t *testing.T) {
	server, client := net.Pipe()
	var wg sync.WaitGroup
	registry := NewTailConnRegistry()
	sentinel := errors.New("tail handler failed")
	wg.Add(1)
	go handleConn(context.Background(), server, func(Request) Response { return Response{} }, func(context.Context, Request, io.Writer) error { return sentinel }, &wg, registry)
	if _, err := client.Write([]byte(`{"verb":"tail","request_id":"tail-1"}` + "\n")); err != nil {
		t.Fatal(err)
	}
	_ = client.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if n, err := client.Read(b[:]); n != 0 || err == nil {
		t.Fatalf("connection was not closed after tail handler error: n=%d err=%v", n, err)
	}
	_ = client.Close()
	wg.Wait()
	registry.mu.Lock()
	remaining := len(registry.conns)
	registry.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("tail connection registry entries = %d", remaining)
	}
}

func TestHandleConnKeepsConcurrentTailConnectionsIndependent(t *testing.T) {
	type tailWrite struct {
		seq      int
		complete bool
	}
	type tailStart struct {
		requestID string
		taskID    string
		fromSeq   int
	}
	type tailResponse struct {
		requestID string
		seq       int
		complete  bool
	}

	serverA, clientA := net.Pipe()
	serverB, clientB := net.Pipe()
	registry := NewTailConnRegistry()
	var wg sync.WaitGroup
	wg.Add(2)
	handleConnsDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(handleConnsDone)
	}()

	controlA := make(chan tailWrite, 1)
	controlB := make(chan tailWrite, 1)
	started := make(chan tailStart, 2)
	responsesA := make(chan tailResponse, 4)
	responsesB := make(chan tailResponse, 4)
	readerDoneA := make(chan struct{})
	readerDoneB := make(chan struct{})
	waitRegistryEntries := func(want int, message string) {
		deadline := time.After(time.Second)
		for {
			registry.mu.Lock()
			remaining := len(registry.conns)
			registry.mu.Unlock()
			if remaining == want {
				return
			}
			select {
			case <-deadline:
				t.Fatalf("%s; remaining=%d", message, remaining)
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}
	readResponses := func(conn net.Conn, responses chan<- tailResponse, done chan<- struct{}) {
		defer close(done)
		decoder := json.NewDecoder(conn)
		for {
			var response Response
			if err := decoder.Decode(&response); err != nil {
				return
			}
			var result struct {
				Seq      int  `json:"seq"`
				Complete bool `json:"complete"`
			}
			if err := json.Unmarshal(response.Result, &result); err != nil {
				return
			}
			responses <- tailResponse{requestID: response.RequestID, seq: result.Seq, complete: result.Complete}
		}
	}
	go readResponses(clientA, responsesA, readerDoneA)
	go readResponses(clientB, responsesB, readerDoneB)

	tailHandler := func(handlerCtx context.Context, req Request, out io.Writer) error {
		var params struct {
			FromSeq int `json:"from_seq"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return err
		}
		started <- tailStart{requestID: req.RequestID, taskID: req.TaskID, fromSeq: params.FromSeq}
		control := controlA
		if req.RequestID == "tail-b" {
			control = controlB
		}
		encoder := json.NewEncoder(out)
		for {
			select {
			case write := <-control:
				if err := encoder.Encode(Response{
					ProtocolVersion: ProtocolVersion,
					RequestID:       req.RequestID,
					OK:              true,
					Result:          json.RawMessage(fmt.Sprintf(`{"seq":%d,"complete":%t}`, write.seq, write.complete)),
				}); err != nil {
					return err
				}
				if write.complete {
					return nil
				}
			case <-handlerCtx.Done():
				return handlerCtx.Err()
			}
		}
	}

	t.Cleanup(func() {
		registry.closeAll()
		_ = clientA.Close()
		_ = clientB.Close()
		_ = serverA.Close()
		_ = serverB.Close()
		waitTail := func(done <-chan struct{}, message string) {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Error(message)
			}
		}
		waitTail(handleConnsDone, "connection handlers did not exit")
		waitTail(readerDoneA, "A response reader did not exit")
		waitTail(readerDoneB, "B response reader did not exit")
	})

	go handleConn(context.Background(), serverA, func(Request) Response { return Response{} }, tailHandler, &wg, registry)
	go handleConn(context.Background(), serverB, func(Request) Response { return Response{} }, tailHandler, &wg, registry)
	requestA := `{"protocol_version":"` + ProtocolVersion + `","verb":"tail","task_id":"task-concurrent","params":{"from_seq":1},"request_id":"tail-a"}` + "\n"
	requestB := `{"protocol_version":"` + ProtocolVersion + `","verb":"tail","task_id":"task-concurrent","params":{"from_seq":3},"request_id":"tail-b"}` + "\n"
	if _, err := clientA.Write([]byte(requestA)); err != nil {
		t.Fatal(err)
	}
	if _, err := clientB.Write([]byte(requestB)); err != nil {
		t.Fatal(err)
	}

	observed := make(map[string]tailStart, 2)
	for len(observed) < 2 {
		select {
		case start := <-started:
			observed[start.requestID] = start
		case <-time.After(time.Second):
			t.Fatalf("tail handlers did not both start: %#v", observed)
		}
	}
	if got := observed["tail-a"]; got.taskID != "task-concurrent" || got.fromSeq != 1 {
		t.Fatalf("A handler request=%#v", got)
	}
	if got := observed["tail-b"]; got.taskID != "task-concurrent" || got.fromSeq != 3 {
		t.Fatalf("B handler request=%#v", got)
	}
	registry.mu.Lock()
	registered := len(registry.conns)
	registry.mu.Unlock()
	if registered != 2 {
		t.Fatalf("registered tail connections=%d", registered)
	}

	controlA <- tailWrite{seq: 1}
	controlB <- tailWrite{seq: 3}
	for _, check := range []struct {
		responses <-chan tailResponse
		requestID string
		seq       int
	}{
		{responsesA, "tail-a", 1},
		{responsesB, "tail-b", 3},
	} {
		select {
		case response := <-check.responses:
			if response.requestID != check.requestID || response.seq != check.seq || response.complete {
				t.Fatalf("tail response=%#v", response)
			}
		case <-time.After(time.Second):
			t.Fatalf("missing response for %s", check.requestID)
		}
	}

	_ = clientA.Close()
	controlA <- tailWrite{seq: 2}
	waitRegistryEntries(1, "A was not removed from registry")

	controlB <- tailWrite{seq: 4}
	select {
	case response := <-responsesB:
		if response.requestID != "tail-b" || response.seq != 4 || response.complete {
			t.Fatalf("B response after A disconnect=%#v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("B did not receive response after A disconnect")
	}
	controlB <- tailWrite{seq: 5, complete: true}
	select {
	case response := <-responsesB:
		if response.requestID != "tail-b" || response.seq != 5 || !response.complete {
			t.Fatalf("B completion=%#v", response)
		}
	case <-time.After(time.Second):
		t.Fatal("B did not receive completion")
	}
	waitRegistryEntries(0, "registry entries after B completion")
	_ = clientB.Close()
	select {
	case <-handleConnsDone:
	case <-time.After(time.Second):
		t.Fatal("connection handlers did not finish")
	}
}

func TestResponseEnvelope(t *testing.T) {
	resp := unknownVerbResponse("r")
	if resp.ProtocolVersion != ProtocolVersion || resp.RequestID != "r" || resp.OK || resp.Error == nil {
		t.Fatalf("response=%#v", resp)
	}
}
