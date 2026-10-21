package transport

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

type operatorServerPTY struct {
	id          supervisor.SessionID
	screens     *usecase.ObservedScreenStore
	send        *usecase.SendInputUseCase
	writes      int
	text        string
	abort       bool
	unconfirmed bool
	writeErr    error
}

func (p *operatorServerPTY) WriteLine(_ context.Context, value string, _ time.Duration, before func() bool) error {
	p.writes++
	p.text = value
	if p.writeErr != nil {
		return p.writeErr
	}
	shot := p.screens.Snapshot(p.id)
	if p.abort {
		shot.Rows[1] = "❯\u00a0other"
	} else {
		shot.Rows[1] = "❯\u00a0" + value
	}
	shot.Cells[1] = []supervisor.Cell{{Text: "❯"}, {Text: "\u00a0"}}
	for _, r := range value {
		shot.Cells[1] = append(shot.Cells[1], supervisor.Cell{Text: string(r)})
	}
	p.screens.SetSnapshot(p.id, shot)
	if !before() {
		return errors.New("before enter refused")
	}
	if p.unconfirmed {
		return nil
	}
	shot.Rows[1] = "❯\u00a0"
	shot.Cells[1] = []supervisor.Cell{{Text: "❯"}, {Text: "\u00a0"}}
	shot.ChangedAt = time.Now().Add(time.Second)
	p.screens.SetSnapshot(p.id, shot)
	// Model two consecutive post-CR snapshots, as required by SCN-sup-02-41.
	for observation := uint64(1); observation <= 2; observation++ {
		shot.Observation = observation
		p.send.NotifyConfirmation(usecase.SendConfirmation{SessionID: p.id, SendID: "alpha-1-s1", Screen: shot})
	}
	return nil
}

func operatorServerFixture(t *testing.T) (*Dispatch, *operatorServerPTY, *store.EventStore, string) {
	t.Helper()
	root := t.TempDir()
	id := supervisor.SessionID("alpha-1")
	sessions := usecase.SessionStoreAdapter{Store: store.NewSessionStore(root)}
	if err := sessions.Save(supervisor.ManagedSession{SessionID: id, ProjectID: "alpha", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionIdle, StateSince: time.Now(), RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}); err != nil {
		t.Fatal(err)
	}
	screens := &usecase.ObservedScreenStore{}
	screens.SetSnapshot(id, usecase.ScreenSnapshot{State: "idle", Rows: []string{"────", "❯\u00a0", "────"}, Cells: [][]supervisor.Cell{{}, {{Text: "❯"}, {Text: "\u00a0"}}, {}}, ObservedAt: time.Now()})
	events := store.NewEventStore(root)
	record := func(e store.Event) error { _, err := events.Record(e); return err }
	stop := usecase.NewStopState(root)
	pty := &operatorServerPTY{id: id, screens: screens}
	send := &usecase.SendInputUseCase{Sessions: sessions, Screens: screens, PTY: pty, IsStopped: stop.IsStopped, StopState: stop, RecordStructured: record, ConfirmTimeout: time.Millisecond}
	pty.send = send
	d := &Dispatch{Sessions: sessions, Events: events, OperatorInput: &usecase.SendOperatorInputUseCase{Sessions: sessions, Send: send, RecordStructured: record}, PeerResume: func(int, map[int]bool) (bool, bool, error) { return false, false, nil }}
	return d, pty, events, root
}

func TestServer_SCNSup0570_SendInputParams(t *testing.T) {
	d, pty, events, _ := operatorServerFixture(t)
	for _, params := range []string{`{}`, `null`, `{"text":"hello"}`, `{"session_id":"alpha-1"}`, `{"session_id":"alpha-1","text":null}`, `{"session_id":"alpha-1","text":3}`, `{"session_id":"alpha-1","text":"hello","name":"x"}`, `{"session_id":"alpha-1","text":"hello","project_id":"alpha"}`, `{"session_id":"alpha-1","text":"a\nb"}`} {
		r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "send-input", Params: json.RawMessage(params)}, os.Getpid())
		if r.Error == nil || r.Error.Code != "SUP_REQUEST_MALFORMED" || pty.writes != 0 {
			t.Fatalf("params=%s response=%+v writes=%d", params, r, pty.writes)
		}
	}
	all, _, err := events.ReadAll()
	if err != nil || len(all) != 0 {
		t.Fatalf("events=%+v err=%v", all, err)
	}
}

func TestServer_SCNSup0569_BodyBoundaryAndInvalidUTF8(t *testing.T) {
	d, pty, events, _ := operatorServerFixture(t)
	invalid := append([]byte(`{"session_id":"alpha-1","text":"`), 0xff)
	invalid = append(invalid, []byte(`"}`)...)
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "send-input", Params: invalid}, os.Getpid())
	if r.Error == nil || r.Error.Code != "SUP_REQUEST_MALFORMED" || len(r.Error.Details) != 0 || pty.writes != 0 {
		t.Fatalf("invalid UTF-8 response=%+v writes=%d", r, pty.writes)
	}
	all, _, err := events.ReadAll()
	if err != nil || len(all) != 0 {
		t.Fatalf("invalid input events=%+v err=%v", all, err)
	}
	const limit = supervisor.SupervisorOperatorInputMaxLength
	text := strings.Repeat("a", limit)
	params, err := json.Marshal(map[string]string{"session_id": "alpha-1", "text": text})
	if err != nil {
		t.Fatal(err)
	}
	r = d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "send-input", Params: params}, os.Getpid())
	if !r.OK || pty.writes != 1 || pty.text != text {
		t.Fatalf("boundary response=%+v writes=%d", r, pty.writes)
	}
}

func TestServer_SCNSup0569_LoneSurrogateMalformedWithoutEvent(t *testing.T) {
	d, pty, events, _ := operatorServerFixture(t)
	for _, params := range []string{`{"session_id":"alpha-1","text":"\ud800"}`, `{"session_id":"alpha-1","text":"\udc00"}`} {
		r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "send-input", Params: json.RawMessage(params)}, os.Getpid())
		if r.Error == nil || r.Error.Code != "SUP_REQUEST_MALFORMED" || pty.writes != 0 {
			t.Fatalf("response=%+v writes=%d", r, pty.writes)
		}
	}
	all, _, err := events.ReadAll()
	if err != nil || len(all) != 0 {
		t.Fatalf("events=%+v err=%v", all, err)
	}
}

func TestServer_SCNSup0577_PeerDescendantsAndTTYless(t *testing.T) {
	for _, probe := range []struct {
		name       string
		supervised bool
		err        error
	}{{"ancestor", true, nil}, {"marker", true, nil}, {"probe failure", false, errors.New("probe failed")}} {
		t.Run(probe.name, func(t *testing.T) {
			d, pty, events, _ := operatorServerFixture(t)
			d.PeerResume = func(int, map[int]bool) (bool, bool, error) { return false, probe.supervised, probe.err }
			r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "send-input", Params: json.RawMessage(`{"session_id":"alpha-1","text":"hello"}`)}, os.Getpid())
			all, _, err := events.ReadAll()
			if r.Error == nil || r.Error.Code != "SUP_OPERATOR_INPUT_FORBIDDEN" || r.Error.Details["reason"] != "supervised_caller" || pty.writes != 0 || err != nil || len(all) != 1 || all[0].Kind != supervisor.EventKindOperatorInputRefused {
				t.Fatalf("response=%+v events=%+v err=%v", r, all, err)
			}
		})
	}
	d, pty, _, _ := operatorServerFixture(t)
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "send-input", Params: json.RawMessage(`{"session_id":"alpha-1","text":"hello"}`)}, os.Getpid())
	if !r.OK || pty.writes != 1 {
		t.Fatalf("ttyless response=%+v writes=%d", r, pty.writes)
	}
}

func TestServer_SCNSup0577_OtherUIDRejectedBeforeRead(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sup-op-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	socket := filepath.Join(root, "operator.sock")
	handler := &testHandler{}
	server := &Server{SocketPath: socket, Handler: handler, PeerCredentials: func(*net.UnixConn) (int, int, error) { return os.Geteuid() + 1, os.Getpid(), nil }}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- server.Serve(ctx) }()
	defer func() { cancel(); <-served }()
	connection := waitForServer(t, socket)
	defer connection.Close()
	_ = connection.SetReadDeadline(time.Now().Add(time.Second))
	line, err := bufio.NewReader(connection).ReadBytes('\n')
	if err == nil || len(line) != 0 || handler.calls != 0 {
		t.Fatalf("line=%q err=%v handler calls=%d", line, err, handler.calls)
	}
}

func TestServer_SCNSup0578_OperatorBodyNeverPersisted(t *testing.T) {
	d, pty, events, root := operatorServerFixture(t)
	const body = "固有の日本語😀 test"
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "send-input", Params: json.RawMessage(`{"session_id":"alpha-1","text":"` + body + `"}`)}, os.Getpid())
	if !r.OK || pty.text != body || strings.Contains(string(r.Result), body) {
		t.Fatalf("response=%+v text matched=%v", r, pty.text == body)
	}
	all, _, err := events.ReadAll()
	if err != nil || len(all) != 2 || all[0].TextLength == nil || *all[0].TextLength != len([]rune(body)) {
		t.Fatalf("events=%+v err=%v", all, err)
	}
	hash := sha256.Sum256([]byte(body))
	markers := []string{body, hex.EncodeToString(hash[:])}
	for _, verb := range []string{"status", "events"} {
		response := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: verb}, os.Getpid())
		encoded, err := json.Marshal(response)
		if err != nil || !response.OK {
			t.Fatalf("verb=%s response=%+v err=%v", verb, response, err)
		}
		for _, marker := range markers {
			if strings.Contains(string(encoded), marker) {
				t.Fatalf("verb=%s leaked marker", verb)
			}
		}
	}
	const invalidBody = "固有の制御文字😀\t日本語"
	params, err := json.Marshal(map[string]string{"session_id": "alpha-1", "text": invalidBody})
	if err != nil {
		t.Fatal(err)
	}
	invalid := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "send-input", Params: params}, os.Getpid())
	if invalid.Error == nil || invalid.Error.Code != "SUP_REQUEST_MALFORMED" || len(invalid.Error.Details) != 0 {
		t.Fatalf("invalid response=%+v", invalid)
	}
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil || entry.IsDir() {
			return walkErr
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(data), body) || strings.Contains(string(data), invalidBody) || strings.Contains(string(data), "固有の制御文字") || strings.Contains(string(data), "固有の日本語") || strings.Contains(string(data), markers[1]) {
			t.Errorf("operator body persisted in %s", path)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestServer_SCNSup0578_WriteFailureLogExcludesBody(t *testing.T) {
	d, pty, _, root := operatorServerFixture(t)
	log, err := store.NewSupervisorLog(filepath.Join(root, "logs"), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	d.OperatorInput.Send.OperationalLog = log
	pty.writeErr = errors.New("simulated PTY failure")
	const body = "unique private operator body"
	response := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "send-input", Params: json.RawMessage(`{"session_id":"alpha-1","text":"` + body + `"}`)}, os.Getpid())
	if response.Error == nil {
		t.Fatalf("write failure succeeded: %+v", response)
	}
	data, err := os.ReadFile(filepath.Join(root, "logs", "codexd-supervisor.log"))
	if err != nil || len(data) == 0 || strings.Contains(string(data), body) || !strings.Contains(string(data), "alpha-1-s1") {
		t.Fatalf("operational log missing or leaked body: err=%v length=%d", err, len(data))
	}
}

func TestServer_SCNSup0579_ConfirmationFailureResponse(t *testing.T) {
	for _, tc := range []struct {
		name  string
		abort bool
		code  string
	}{{"abort", true, "SUP_SEND_ABORTED"}, {"unconfirmed", false, "SUP_SEND_NOT_CONFIRMED"}} {
		t.Run(tc.name, func(t *testing.T) {
			d, pty, events, _ := operatorServerFixture(t)
			pty.abort, pty.unconfirmed = tc.abort, !tc.abort
			r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "send-input", Params: json.RawMessage(`{"session_id":"alpha-1","text":"hello"}`)}, os.Getpid())
			all, _, err := events.ReadAll()
			if err != nil || r.Error == nil || r.Error.Code != tc.code || len(r.Error.Details) != 2 || r.Error.Details["send_id"] != "alpha-1-s1" || pty.writes != 1 || len(all) != 2 {
				t.Fatalf("response=%+v events=%+v err=%v", r, all, err)
			}
		})
	}
}

func TestServer_SCNSup0578_RefusalNeverReturnsBody(t *testing.T) {
	d, pty, events, root := operatorServerFixture(t)
	const body = "force push を実行しないで"
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "send-input", Params: json.RawMessage(`{"session_id":"alpha-1","text":"` + body + `"}`)}, os.Getpid())
	encoded, err := json.Marshal(r)
	if err != nil || r.Error == nil || r.Error.Code != "SUP_OPERATOR_INPUT_FORBIDDEN" || strings.Contains(string(encoded), body) || pty.writes != 0 {
		t.Fatalf("response=%s err=%v writes=%d", encoded, err, pty.writes)
	}
	all, _, err := events.ReadAll()
	if err != nil || len(all) != 1 || all[0].Kind != supervisor.EventKindOperatorInputRefused || all[0].TextLength != nil {
		t.Fatalf("events=%+v err=%v", all, err)
	}
	data, err := os.ReadFile(filepath.Join(root, "events.jsonl"))
	if err != nil || strings.Contains(string(data), body) {
		t.Fatalf("audit data leaked: err=%v", err)
	}
}

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
