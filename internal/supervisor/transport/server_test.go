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

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/usecase"
)

type testHandler struct{ calls int }

func TestServer_SCNsup0560_ScreenParams(t *testing.T) {
	recorded := 0
	id := supervisor.SessionID("alpha-1")
	screens := &usecase.ObservedScreenStore{}
	screens.SetSnapshot(id, usecase.ScreenSnapshot{Rows: []string{"private"}, ObservedAt: time.Now()})
	d := &Dispatch{ScreenRead: &usecase.ScreenReadUseCase{Sessions: dispatchTestSessions{values: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionIdle}}}, Screens: screens, RecordStructured: func(store.Event) error { recorded++; return nil }}}
	for _, params := range []string{"", "null", `{}`, `{"session_id":null}`, `{"session_id":7}`, `{"session_id":"alpha-1","project_id":"alpha"}`, `{"session_id":"alpha-1","since_seq":1}`, `{"session_id":"alpha-1","limit":1}`} {
		r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "screen", Params: json.RawMessage(params)}, 0)
		if r.Error == nil || r.Error.Code != "SUP_REQUEST_MALFORMED" || recorded != 0 {
			t.Fatalf("params=%q response=%+v recorded=%d", params, r, recorded)
		}
	}
}

func TestServer_SCNsup0563_64_65_ScreenPrivateAndAudited(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sup-screen-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	id := supervisor.SessionID("alpha-1")
	sessions := usecase.SessionStoreAdapter{Store: store.NewSessionStore(root)}
	if err := sessions.Save(supervisor.ManagedSession{SessionID: id, ProjectID: "alpha", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StateSince: time.Now().UTC(), RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "sessions", "alpha-1.json")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	screens := &usecase.ObservedScreenStore{}
	const privateMarker = "private-screen-全角"
	const private = privateMarker + "-\U0001F600-\x1b[31m"
	screens.SetSnapshot(id, usecase.ScreenSnapshot{Rows: []string{private + "  "}, ObservedAt: time.Now().UTC()})
	events := store.NewEventStore(root)
	d := &Dispatch{ScreenRead: &usecase.ScreenReadUseCase{Sessions: sessions, Screens: screens, RecordStructured: func(e store.Event) error { _, err := events.Record(e); return err }}}
	stop := usecase.NewStopState(root)
	stop.SetGlobal(true)
	d.GlobalStopped = stop.IsGlobalStopped
	t.Setenv("CODEXD_SUPERVISED", "1")
	readEnd, writeEnd, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	previousStdin := os.Stdin
	os.Stdin = readEnd
	defer func() { os.Stdin = previousStdin; _ = readEnd.Close(); _ = writeEnd.Close() }()
	socket := filepath.Join(root, "screen.sock")
	server := &Server{SocketPath: socket, Handler: d, PeerCredentials: func(*net.UnixConn) (int, int, error) { return os.Geteuid(), os.Getpid(), nil }}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx) }()
	defer func() { cancel(); <-served }()
	_ = waitForServer(t, socket).Close()
	for i := 0; i < 2; i++ {
		response, err := (Client{SocketPath: socket}).Call(ctx, Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "screen", Params: json.RawMessage(`{"session_id":"alpha-1"}`)})
		var result usecase.ScreenReadResult
		if err == nil {
			err = json.Unmarshal(response.Result, &result)
		}
		if err != nil || !response.OK || len(result.Rows) != 1 || result.Rows[0] != private {
			t.Fatalf("screen response=%+v err=%v", response, err)
		}
	}
	if !stop.IsGlobalStopped() {
		t.Fatal("screen cleared global STOP")
	}
	current, err := sessions.Load(id)
	if err != nil || current.State != supervisor.SessionStopped {
		t.Fatalf("screen changed stopped session: %+v err=%v", current, err)
	}
	stored, _, err := events.ReadAll()
	if err != nil || len(stored) != 2 {
		t.Fatalf("events=%+v err=%v", stored, err)
	}
	for _, event := range stored {
		if event.Kind != supervisor.EventKindScreenRead || event.RowCount == nil || *event.RowCount != 1 {
			t.Fatalf("event=%+v", event)
		}
	}
	lines, err := os.ReadFile(filepath.Join(root, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(strings.TrimSpace(string(lines)), "\n") {
		var fields map[string]any
		if err := json.Unmarshal([]byte(line), &fields); err != nil {
			t.Fatal(err)
		}
		if len(fields) != 5 || fields["session_id"] != string(id) || fields["row_count"] != float64(1) {
			t.Fatalf("audit fields=%v", fields)
		}
	}
	after, err := os.Stat(path)
	if err != nil || !after.ModTime().Equal(before.ModTime()) {
		t.Fatalf("session file updated: %v", err)
	}
	err = filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() || entry.Type()&os.ModeSocket != 0 {
			return walkErr
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), privateMarker) {
			t.Errorf("screen text persisted at %s", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestServer_SCNsup0565_PeerRejectedScreen(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sup-screen-peer-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socket := filepath.Join(root, "screen.sock")
	handler := &testHandler{}
	server := &Server{SocketPath: socket, Handler: handler, PeerCredentials: func(*net.UnixConn) (int, int, error) { return os.Geteuid() + 1, os.Getpid(), nil }}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx) }()
	defer func() { cancel(); <-served }()
	_ = waitForServer(t, socket).Close()
	_, err = (Client{SocketPath: socket}).Call(ctx, Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "screen", Params: json.RawMessage(`{"session_id":"alpha-1"}`)})
	if err == nil || handler.calls != 0 {
		t.Fatalf("rejected peer: err=%v calls=%d", err, handler.calls)
	}
}

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
