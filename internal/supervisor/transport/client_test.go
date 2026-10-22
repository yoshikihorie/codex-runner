package transport

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestClientRejectsOverlongUnterminatedResponse(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sup-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	path := filepath.Join(root, "s.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, e := listener.Accept()
		if e != nil {
			return
		}
		defer conn.Close()
		buffer := make([]byte, 1024)
		_, _ = conn.Read(buffer)
		_, _ = conn.Write(bytes.Repeat([]byte("x"), SupervisorProtocolLineMaxBytes+1))
	}()
	_, err = (Client{SocketPath: path}).Call(context.Background(), Request{Verb: "status"})
	if !errors.Is(err, ErrInvalidResponse) {
		t.Fatalf("error=%v", err)
	}
}

// Test-only timing budgets, independent of production timeout values.
const transportTimingWatchdog = 10 * time.Second
const operatorSuccessConfirmTimeout = time.Hour
const clientTestDialTimeout = 2 * time.Second
const clientTestResponseTimeout = time.Second

type timingClientResult struct {
	response   Response
	err        error
	contextErr error
}

func awaitTransportTiming[T any](t *testing.T, ch <-chan T, operation string) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(transportTimingWatchdog):
		t.Fatalf("watchdog waiting for %s", operation)
		var zero T
		return zero
	}
}

// Accept and complete newline-delimited request receipt are separate synchronization points.
// Cleanup releases the response gate, closes the connection, and joins the server goroutine.
func timingClientPeer(t *testing.T) (string, <-chan error, <-chan error, chan<- string, func()) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "sup-timing-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	path := filepath.Join(root, "s.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	accepted, received := make(chan error, 1), make(chan error, 1)
	reply := make(chan string)
	peerCtx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := listener.Accept()
		accepted <- err
		if err != nil {
			return
		}
		defer conn.Close()
		interrupt := context.AfterFunc(peerCtx, func() { _ = conn.Close() })
		defer interrupt()
		_ = conn.SetDeadline(time.Now().Add(transportTimingWatchdog))
		_, err = bufio.NewReader(conn).ReadBytes('\n')
		received <- err
		if err != nil {
			return
		}
		select {
		case data := <-reply:
			_, err = conn.Write([]byte(data))
		case <-peerCtx.Done():
		}
	}()
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			stop()
			_ = listener.Close()
			awaitTransportTiming(t, done, "client peer cleanup")
		})
	}
	t.Cleanup(cleanup)
	return path, accepted, received, reply, cleanup
}

func startTimingClient(t *testing.T, client Client, verb string, stopPeer func()) <-chan timingClientResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), transportTimingWatchdog)
	result := make(chan timingClientResult, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		response, err := client.Call(ctx, Request{Verb: verb})
		result <- timingClientResult{response, err, ctx.Err()}
	}()
	t.Cleanup(func() { cancel(); stopPeer(); awaitTransportTiming(t, exited, "client cleanup") })
	return result
}

func receiveTimingRequest(t *testing.T, accepted, received <-chan error) {
	t.Helper()
	if err := awaitTransportTiming(t, accepted, "connection acceptance"); err != nil {
		t.Fatal(err)
	}
	if err := awaitTransportTiming(t, received, "newline-delimited request"); err != nil {
		t.Fatal(err)
	}
}

func TestClientClassifiesResponseTimeout(t *testing.T) {
	original := supervisorResponseWaitTimeout
	supervisorResponseWaitTimeout = clientTestResponseTimeout
	t.Cleanup(func() { supervisorResponseWaitTimeout = original })
	path, accepted, received, _, stopPeer := timingClientPeer(t)
	result := startTimingClient(t, Client{SocketPath: path, DialTimeout: transportTimingWatchdog}, "status", stopPeer)
	receiveTimingRequest(t, accepted, received)
	got := awaitTransportTiming(t, result, "response timeout")
	if got.contextErr != nil {
		t.Fatalf("watchdog context ended: %v", got.contextErr)
	}
	if !errors.Is(got.err, ErrClientTimeout) {
		t.Fatalf("error=%v", got.err)
	}
}

func TestClient_SCNsup0528_DedicatedSocketUnavailable(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	client := Client{SocketPath: filepath.Join(home, "supervisor.sock")}
	_, err = client.Call(context.Background(), Request{Verb: "status"})
	if !errors.Is(err, ErrClientUnavailable) {
		t.Fatalf("error=%v", err)
	}
}

func TestClientResponseWaitOutlivesDialTimeout(t *testing.T) {
	original := supervisorResponseWaitTimeout
	supervisorResponseWaitTimeout = time.Hour
	t.Cleanup(func() { supervisorResponseWaitTimeout = original })
	path, accepted, received, reply, stopPeer := timingClientPeer(t)
	result := startTimingClient(t, Client{SocketPath: path, DialTimeout: clientTestDialTimeout}, "stop-session", stopPeer)
	receiveTimingRequest(t, accepted, received)
	// Keep the real timer: a response must survive expiry of the connection/write deadline.
	// Dial and request transmission still have to complete within clientTestDialTimeout.
	start := time.Now()
	timer := time.NewTimer(clientTestDialTimeout)
	defer timer.Stop()
	<-timer.C
	if elapsed := time.Since(start); elapsed < clientTestDialTimeout {
		t.Fatalf("dial separation timer fired early: %s", elapsed)
	}
	select {
	case reply <- "{\"ok\":true,\"result\":{}}\n":
	case <-time.After(transportTimingWatchdog):
		t.Fatal("watchdog releasing response")
	}
	got := awaitTransportTiming(t, result, "response beyond dial deadline")
	if got.contextErr != nil {
		t.Fatalf("watchdog context ended: %v", got.contextErr)
	}
	if got.err != nil || !got.response.OK {
		t.Fatalf("response=%+v err=%v", got.response, got.err)
	}
}

func TestClientClosedAndMalformedResponseDiffer(t *testing.T) {
	for _, tc := range []struct {
		name, reply string
		want        error
	}{{"closed", "", ErrClientClosed}, {"malformed", "not json\n", ErrInvalidResponse}} {
		t.Run(tc.name, func(t *testing.T) {
			root, err := os.MkdirTemp("/tmp", "sup-sock-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(root) })
			path := filepath.Join(root, "s.sock")
			listener, err := net.Listen("unix", path)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			go func() {
				conn, e := listener.Accept()
				if e != nil {
					return
				}
				defer conn.Close()
				buffer := make([]byte, 1024)
				_, _ = conn.Read(buffer)
				_, _ = conn.Write([]byte(tc.reply))
			}()
			_, err = (Client{SocketPath: path}).Call(context.Background(), Request{Verb: "status"})
			if !errors.Is(err, tc.want) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
