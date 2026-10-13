package transport

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type testHandler struct{ calls int }

func waitForServer(t *testing.T, path string) net.Conn {
	t.Helper()
	deadline := time.Now().Add(SupervisorSocketConnectTimeout)
	for {
		conn, err := net.Dial("unix", path)
		if err == nil {
			return conn
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not ready at %s: %v", path, err)
		}
		time.Sleep(time.Millisecond)
	}
}

func (h *testHandler) Handle(context.Context, Request, int) Response {
	h.calls++
	return Response{OK: true, Result: json.RawMessage(`{"ok":true}`)}
}

func TestServer_SCNsup0533_PeerRejectedBeforeRead(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	path := filepath.Join(home, "s.sock")
	h := &testHandler{}
	s := &Server{SocketPath: path, Handler: h, PeerCredentials: func(*net.UnixConn) (int, int, error) { return os.Geteuid() + 1, 123, nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan error, 1)
	go func() { ready <- s.Serve(ctx) }()
	defer func() { cancel(); <-ready }()
	conn := waitForServer(t, path)
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err == nil || len(line) != 0 || h.calls != 0 {
		t.Fatalf("line=%q err=%v calls=%d", line, err, h.calls)
	}
}

func TestServer_SCNsup0529_ProtocolAndLineLimit(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	path := filepath.Join(home, "s.sock")
	h := &testHandler{}
	s := &Server{SocketPath: path, Handler: h, PeerCredentials: func(*net.UnixConn) (int, int, error) { return os.Geteuid(), os.Getpid(), nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan error, 1)
	go func() { ready <- s.Serve(ctx) }()
	defer func() { cancel(); <-ready }()
	_ = waitForServer(t, path).Close()
	for _, line := range []string{`{"supervisor_protocol_version":9,"verb":"status"}` + "\n", strings.Repeat("a", SupervisorProtocolLineMaxBytes+1) + "\n"} {
		conn, err := net.Dial("unix", path)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = conn.Write([]byte(line))
		var response Response
		if err := json.NewDecoder(conn).Decode(&response); err != nil {
			t.Fatal(err)
		}
		_ = conn.Close()
		if response.Error == nil || response.Error.Code != "SUP_REQUEST_MALFORMED" {
			t.Fatalf("response=%+v", response)
		}
	}
	if h.calls != 0 {
		t.Fatalf("handler calls=%d", h.calls)
	}
}

func TestServer_SCNsup0530_ReadTimeoutAndSocketPermissions(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	path := filepath.Join(home, "s.sock")
	h := &testHandler{}
	s := &Server{SocketPath: path, Handler: h, ReadTimeout: 10 * time.Millisecond, PeerCredentials: func(*net.UnixConn) (int, int, error) { return os.Geteuid(), os.Getpid(), nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan error, 1)
	go func() { ready <- s.Serve(ctx) }()
	defer func() { cancel(); <-ready }()
	_ = waitForServer(t, path).Close()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100 && err == nil && info.Mode().Perm() != 0600; i++ {
		time.Sleep(time.Millisecond)
		info, err = os.Lstat(path)
	}
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("socket mode=%v", info.Mode())
	}
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	line, err := bufio.NewReader(conn).ReadBytes('\n')
	if err == nil || len(line) != 0 || h.calls != 0 {
		t.Fatalf("line=%q err=%v calls=%d", line, err, h.calls)
	}
}

func TestServer_SCNsup0534_StaleSocketRequiresDefaultPathAndLock(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	path := filepath.Join(home, "supervisor.sock")
	stale, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	stale.SetUnlinkOnClose(false)
	if err := stale.Close(); err != nil {
		t.Fatal(err)
	}
	h := &testHandler{}
	for _, tc := range []struct {
		locked      bool
		defaultPath string
	}{{false, path}, {true, filepath.Join(home, "another.sock")}} {
		s := &Server{SocketPath: path, DefaultSocketPath: tc.defaultPath, LockHeld: tc.locked, Handler: h}
		if err := s.Serve(context.Background()); err == nil {
			t.Fatal("stale socket accepted without lock/default path")
		}
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("stale socket removed: %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{SocketPath: path, DefaultSocketPath: path, LockHeld: true, Handler: h}
	ready := make(chan error, 1)
	go func() { ready <- s.Serve(ctx) }()
	_ = waitForServer(t, path).Close()
	cancel()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
}
