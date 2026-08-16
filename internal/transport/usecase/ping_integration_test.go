package usecase

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/transport"
)

func TestDispatcherDispatchesPingUseCase(t *testing.T) {
	ignored := func(transport.Request) transport.Response { return transport.Response{} }
	dispatcher, err := transport.NewDispatcher(ignored, ignored, ignored, (&PingUseCase{}).Handle)
	if err != nil {
		t.Fatal(err)
	}

	resp := dispatcher.Dispatch(transport.Request{Verb: "ping", RequestID: "r-3"})
	var result PingResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatal(err)
	}
	if !resp.OK || resp.RequestID != "r-3" || result.ProtocolVersion != transport.ProtocolVersion {
		t.Fatalf("response = %#v, result = %#v", resp, result)
	}
}

func TestServePingConcurrentConnections(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "t1-24-sock-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove socket directory: %v", err)
		}
	})
	socketPath := filepath.Join(dir, "codexd.sock")
	ignored := func(transport.Request) transport.Response { return transport.Response{} }
	dispatcher, err := transport.NewDispatcher(ignored, ignored, ignored, (&PingUseCase{}).Handle)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	acceptLoopDone := make(chan struct{})
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- transport.Serve(ctx, socketPath, dispatcher.Dispatch, func(context.Context, transport.Request, io.Writer) error { return nil }, &wg, transport.NewTailConnRegistry(), acceptLoopDone)
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-serveDone; err != nil {
			t.Errorf("Serve() error = %v", err)
		}
		wg.Wait()
	})

	type responseResult struct {
		requestID string
		response  transport.Response
		result    PingResult
		err       error
	}
	results := make(chan responseResult, 3)
	for _, requestID := range []string{"r-1", "r-2", "r-3"} {
		go func(requestID string) {
			conn, err := dialUnix(socketPath)
			if err != nil {
				results <- responseResult{requestID: requestID, err: err}
				return
			}
			defer conn.Close()
			request := transport.Request{ProtocolVersion: transport.ProtocolVersion, Verb: "ping", RequestID: requestID}
			if err := json.NewEncoder(conn).Encode(request); err != nil {
				results <- responseResult{requestID: requestID, err: err}
				return
			}
			var response transport.Response
			if err := json.NewDecoder(conn).Decode(&response); err != nil {
				results <- responseResult{requestID: requestID, err: err}
				return
			}
			var result PingResult
			if err := json.Unmarshal(response.Result, &result); err != nil {
				results <- responseResult{requestID: requestID, err: err}
				return
			}
			results <- responseResult{requestID: requestID, response: response, result: result}
		}(requestID)
	}

	for range 3 {
		got := <-results
		if got.err != nil {
			t.Fatal(got.err)
		}
		if !got.response.OK || got.response.RequestID != got.requestID || got.response.ProtocolVersion != transport.ProtocolVersion || got.result.ProtocolVersion != transport.ProtocolVersion {
			t.Fatalf("request %q: response = %#v, result = %#v", got.requestID, got.response, got.result)
		}
	}
}

func dialUnix(socketPath string) (net.Conn, error) {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("unix", socketPath)
		if err == nil {
			return conn, nil
		}
		time.Sleep(time.Millisecond)
	}
	return nil, fmt.Errorf("dial Unix socket %q: timed out", socketPath)
}
