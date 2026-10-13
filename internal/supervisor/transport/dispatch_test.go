package transport

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/launch"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/usecase"
)

type dispatchTestControl struct {
	tty, supervised bool
	calls           int
}

type dispatchObservedPTY struct {
	once   sync.Once
	closed chan struct{}
}

func (*dispatchObservedPTY) Pid() int                   { return 61991 }
func (p *dispatchObservedPTY) Read([]byte) (int, error) { <-p.closed; return 0, io.EOF }
func (p *dispatchObservedPTY) Close() error             { p.once.Do(func() { close(p.closed) }); return nil }
func (*dispatchObservedPTY) Kill(syscall.Signal) error  { return nil }
func (p *dispatchObservedPTY) Wait() (int, error)       { <-p.closed; return 0, nil }

type dispatchStarter struct{ pty *dispatchObservedPTY }

func (s dispatchStarter) Start(string, []string, []string, string, uint16, uint16) (usecase.ManagedPTY, error) {
	return s.pty, nil
}

func TestDispatchStartObservedUsesSupervisorContext(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-observe-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(`{"auth_mode":"chatgpt","OPENAI_API_KEY":null}`), 0600); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(home, "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(home, "claude")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	stored := usecase.SessionStoreAdapter{Store: store.NewSessionStore(state)}
	registry := &usecase.PTYRegistry{}
	pty := &dispatchObservedPTY{closed: make(chan struct{})}
	start := &usecase.StartSessionUseCase{Config: usecase.StartSessionConfig{StateDir: state, CodexdPath: bin, MaxSessions: 1, Columns: 80, Rows: 24}, Projects: map[string]usecase.StartProject{"alpha": {ID: "alpha", CLI: supervisor.CLIClaude, CWD: home, BinaryPath: bin}}, Store: stored, Stop: usecase.NewStopState(state), Billing: launch.BillingGuard{}, Starter: dispatchStarter{pty}, PTYs: registry, ParentEnv: func() []string { return nil }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer := &usecase.ObserveSessionUseCase{Sessions: stored, SendInput: &usecase.SendInputUseCase{PTYs: registry}, Interval: time.Hour}
	d := &Dispatch{Sessions: stored, StartSession: start, ObserveFor: func(string) *usecase.ObserveSessionUseCase { return observer }, NewSessionID: func(string) (supervisor.SessionID, error) { return supervisor.NewSessionID("alpha-1") }, SupervisorContext: ctx}
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "start-session", Params: json.RawMessage(`{"project_id":"alpha"}`)}, 0)
	if !r.OK || registry.GetManaged("alpha-1") != pty {
		t.Fatalf("response=%+v managed=%v", r, registry.GetManaged("alpha-1"))
	}
	if string(r.Result) != `{"session_id":"alpha-1"}` {
		t.Fatalf("result=%s", r.Result)
	}
	cancel()
	_ = pty.Close()
	deadline := time.After(time.Second)
	for registry.GetManaged("alpha-1") != nil {
		select {
		case <-deadline:
			t.Fatal("observation did not stop")
		case <-time.After(time.Millisecond):
		}
	}
}

func (c *dispatchTestControl) Stop(context.Context, *supervisor.SessionID) (usecase.StopOutput, error) {
	c.calls++
	return usecase.StopOutput{}, nil
}
func (c *dispatchTestControl) Resume(_ context.Context, _ *supervisor.SessionID, tty, supervised bool) (usecase.ResumeOutput, error) {
	c.tty = tty
	c.supervised = supervised
	c.calls++
	if !tty || supervised {
		return usecase.ResumeOutput{}, usecase.ErrResumeForbidden
	}
	return usecase.ResumeOutput{}, nil
}

func TestDispatch_SCNsup0544_ServerSideResumeConditions(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	for _, tc := range []struct {
		name                        string
		tty, supervised             bool
		probeErr                    error
		wantTTY, wantSupervised, ok bool
	}{
		{"interactive", true, false, nil, true, false, true},
		{"no_terminal", false, false, nil, false, false, false},
		{"owned_ancestor", true, true, nil, true, true, false},
		{"marker", true, true, nil, true, true, false},
		{"probe_failed", true, false, errors.New("probe failed"), false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control := &dispatchTestControl{}
			d := &Dispatch{Control: control, PeerResume: func(int, map[int]bool) (bool, bool, error) { return tc.tty, tc.supervised, tc.probeErr }}
			r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "resume"}, 123)
			if r.OK != tc.ok || control.calls != 1 || control.tty != tc.wantTTY || control.supervised != tc.wantSupervised {
				t.Fatalf("response=%+v control=%+v", r, control)
			}
		})
	}
}

func TestResumeResultJSONKeys(t *testing.T) {
	data, err := json.Marshal(usecase.ResumeOutput{Resumed: []supervisor.SessionID{"alpha-1"}, StillStopped: []usecase.StillStopped{{SessionID: "alpha-2", Reason: "orphan_child_alive"}}})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"resumed":["alpha-1"],"still_stopped":[{"session_id":"alpha-2","reason":"orphan_child_alive"}]}` {
		t.Fatalf("response=%s", data)
	}
}

func TestResumeEmptyResultUsesArrays(t *testing.T) {
	data, err := json.Marshal(usecase.ResumeOutput{})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"resumed":[],"still_stopped":[]}` {
		t.Fatalf("response=%s", data)
	}
}

func TestDispatchErrorCanonicalKeys(t *testing.T) {
	for code, key := range map[string]string{
		"SUP_CONFIG_INVALID":              "error.supervisor.configInvalid",
		"SUP_CONFIG_PATH_CONFLICT":        "error.supervisor.configPathConflict",
		"SUP_PROJECT_NOT_FOUND":           "error.supervisor.projectNotFound",
		"SUP_PROJECT_CWD_UNAVAILABLE":     "error.supervisor.projectCwdUnavailable",
		"SUP_SESSION_ALREADY_RUNNING":     "error.supervisor.sessionAlreadyRunning",
		"SUP_SESSION_LIMIT_REACHED":       "error.supervisor.sessionLimitReached",
		"SUP_SESSION_NOT_FOUND":           "error.supervisor.sessionNotFound",
		"SUP_SPAWN_FAILED":                "error.supervisor.spawnFailed",
		"SUP_ORPHAN_CHILD_ALIVE":          "error.supervisor.orphanChildAlive",
		"SUP_STATE_FILE_CORRUPT":          "error.supervisor.stateFileCorrupt",
		"SUP_ILLEGAL_TRANSITION":          "error.supervisor.illegalTransition",
		"SUP_BILLING_GUARD_FAILED":        "error.supervisor.billingGuardFailed",
		"SUP_RESUME_FORBIDDEN":            "error.supervisor.resumeForbidden",
		"SUP_STOP_ACTIVE":                 "error.supervisor.stopActive",
		"SUP_REQUEST_MALFORMED":           "error.supervisor.requestMalformed",
		"SUP_SEND_BLOCKED":                "error.supervisor.sendBlocked",
		"SUP_RECOMMENDATION_FORBIDDEN":    "error.supervisor.recommendationForbidden",
		"SUP_RECOMMENDATION_UNDETERMINED": "error.supervisor.recommendationUndetermined",
		"SUP_PERMISSION_PENDING":          "info.supervisor.permissionPending",
		"SUP_PERMISSION_MODE_UNSAFE":      "info.supervisor.permissionModeUnsafe",
		"SUP_SEND_ABORTED":                "error.supervisor.sendAborted",
		"SUP_RECOMMENDATION_REPEATED":     "error.supervisor.recommendationRepeated",
		"SUP_AUTO_CONTINUE_LIMIT_REACHED": "error.supervisor.autoContinueLimitReached",
		"SUP_VERIFY_FAILED":               "error.supervisor.verifyFailed",
		"SUP_SENSITIVE_PATH_BLOCKED":      "error.supervisor.sensitivePathBlocked",
		"SUP_COMMIT_FAILED":               "error.supervisor.commitFailed",
		"SUP_PUSH_REJECTED_POLICY":        "error.supervisor.pushRejectedPolicy",
		"SUP_PUSH_FAILED":                 "error.supervisor.pushFailed",
		"SUP_NOTIFY_FAILED":               "error.supervisor.notifyFailed",
		"SUP_VERB_UNKNOWN":                "error.supervisor.verbUnknown",
		"SUP_INSTANCE_LOCK_HELD":          "error.supervisor.instanceLockHeld",
		"SUP_ROTATE_STEP_FAILED":          "error.supervisor.rotateStepFailed",
		"SUP_RESTART_LIMIT_EXCEEDED":      "error.supervisor.restartLimitExceeded",
		"SUP_CLIENT_UNAVAILABLE":          "error.supervisor.clientUnavailable",
		"SUP_SEND_NOT_CONFIRMED":          "error.supervisor.sendNotConfirmed",
		"SUP_PEER_REJECTED":               "error.supervisor.peerRejected",
	} {
		got := dispatchError(errors.New(code))
		if got.Error == nil || got.Error.MessageKey != key {
			t.Errorf("%s: %+v", code, got.Error)
		}
	}
}

func TestDispatchStartRejectedDuringShutdown(t *testing.T) {
	d := &Dispatch{ShuttingDown: func() bool { return true }}
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "start-session", Params: json.RawMessage(`{"project_id":"alpha"}`)}, 0)
	if r.Error == nil || r.Error.Code != "SUP_CONFIG_INVALID" {
		t.Fatalf("response=%+v", r)
	}
}

func TestDispatchSixVerbsReachDependencies(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sup-disp-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	control := &dispatchTestControl{}
	started := 0
	stored := usecase.SessionStoreAdapter{Store: store.NewSessionStore(root)}
	d := &Dispatch{Sessions: dispatchTestSessions{}, Control: control, Events: dispatchTestEvents{}, PeerResume: func(int, map[int]bool) (bool, bool, error) { return true, false, nil }, NewSessionID: func(string) (supervisor.SessionID, error) { return supervisor.NewSessionID("alpha-1") }, SupervisorContext: context.Background(), ObserveFor: func(string) *usecase.ObserveSessionUseCase { return &usecase.ObserveSessionUseCase{} }, StartSession: &usecase.StartSessionUseCase{Store: stored, Lock: func() (func(), error) { started++; return func() {}, nil }}, StopSession: &usecase.StopSessionUseCase{Sessions: stored, PTYs: &usecase.PTYRegistry{}}}
	for _, tc := range []struct {
		verb, params string
		wantOK       bool
		wantCode     string
	}{
		{"status", `{}`, true, ""}, {"stop", `{}`, true, ""}, {"resume", `{}`, true, ""}, {"start-session", `{"project_id":"alpha"}`, false, "SUP_PROJECT_NOT_FOUND"}, {"stop-session", `{"session_id":"alpha-1"}`, false, "SUP_SESSION_NOT_FOUND"}, {"events", `{}`, true, ""},
	} {
		r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: tc.verb, Params: json.RawMessage(tc.params)}, 0)
		if r.OK != tc.wantOK || !tc.wantOK && (r.Error == nil || r.Error.Code != tc.wantCode) {
			t.Errorf("verb=%s response=%+v", tc.verb, r)
		}
		if r.OK {
			var result map[string]json.RawMessage
			if err := json.Unmarshal(r.Result, &result); err != nil {
				t.Fatal(err)
			}
			for _, key := range map[string][]string{"status": {"supervisor", "sessions"}, "stop": {"scope", "stop_file"}, "resume": {"resumed", "still_stopped"}, "events": {"events", "next_seq"}}[tc.verb] {
				if _, ok := result[key]; !ok {
					t.Errorf("verb=%s missing %s: %s", tc.verb, key, r.Result)
				}
			}
		}
	}
	if control.calls != 2 || started != 0 {
		t.Fatalf("control=%d start=%d", control.calls, started)
	}
}

func TestDispatchStartStopActiveAndMissingProject(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "sup-start-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	stops := usecase.NewStopState(root)
	stored := usecase.SessionStoreAdapter{Store: store.NewSessionStore(root)}
	d := &Dispatch{Sessions: dispatchTestSessions{}, StartSession: &usecase.StartSessionUseCase{Store: stored, Stop: stops}, NewSessionID: func(string) (supervisor.SessionID, error) { return supervisor.NewSessionID("alpha-1") }, SupervisorContext: context.Background(), ObserveFor: func(string) *usecase.ObserveSessionUseCase { return &usecase.ObserveSessionUseCase{} }}
	request := Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "start-session", Params: json.RawMessage(`{"project_id":"alpha"}`)}
	if r := d.Handle(context.Background(), request, 0); r.Error == nil || r.Error.Code != "SUP_PROJECT_NOT_FOUND" {
		t.Fatalf("missing project: %+v", r)
	}
	if err := stops.WriteGlobal(); err != nil {
		t.Fatal(err)
	}
	if r := d.Handle(context.Background(), request, 0); r.Error == nil || r.Error.Code != "SUP_STOP_ACTIVE" {
		t.Fatalf("STOP active: %+v", r)
	}
}

func TestDispatchMissingProjectDoesNotReserveID(t *testing.T) {
	called := false
	d := &Dispatch{Sessions: dispatchTestSessions{}, StartSession: &usecase.StartSessionUseCase{Projects: map[string]usecase.StartProject{}}, NewSessionID: func(string) (supervisor.SessionID, error) { called = true; return "alpha-1", nil }, SupervisorContext: context.Background(), ObserveFor: func(string) *usecase.ObserveSessionUseCase { return &usecase.ObserveSessionUseCase{} }}
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "start-session", Params: json.RawMessage(`{"project_id":"alpha"}`)}, 0)
	if called || r.Error == nil || r.Error.Code != "SUP_PROJECT_NOT_FOUND" {
		t.Fatalf("called=%v response=%+v", called, r)
	}
}

func TestDispatch_SCNsup0529_UnknownVerbSanitized(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	d := &Dispatch{}
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "bad\n" + string(make([]byte, 70))}, 0)
	if r.Error == nil || r.Error.Code != "SUP_VERB_UNKNOWN" {
		t.Fatalf("response=%+v", r)
	}
	verb, ok := r.Error.Details["verb"].(string)
	if !ok || len(verb) > 64 {
		t.Fatalf("sanitized verb=%q", verb)
	}
}

func TestDispatch_SCNsup0531_EventsPagination(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	events := store.NewEventStore(t.TempDir())
	for i := 0; i < 3; i++ {
		if _, err := events.Record(store.Event{Kind: supervisor.EventKindSupervisorStarted}); err != nil {
			t.Fatal(err)
		}
	}
	d := &Dispatch{Events: events}
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "events", Params: json.RawMessage(`{"since_seq":1,"limit":1}`)}, 0)
	if !r.OK {
		t.Fatalf("response=%+v", r)
	}
	var body struct {
		Events  []store.Event `json:"events"`
		NextSeq uint64        `json:"next_seq"`
	}
	if err := json.Unmarshal(r.Result, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Events) != 1 || body.Events[0].Seq != 2 || body.NextSeq != 2 {
		t.Fatalf("body=%+v", body)
	}
	for _, tc := range []struct {
		since     uint64
		limit     int
		wantCount int
		wantNext  uint64
	}{{0, eventsPageMax + 1, 3, 3}, {3, eventsPageMax + 1, 0, 3}} {
		params, _ := json.Marshal(map[string]any{"since_seq": tc.since, "limit": tc.limit})
		r = d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "events", Params: params}, 0)
		if !r.OK {
			t.Fatalf("response=%+v", r)
		}
		if err := json.Unmarshal(r.Result, &body); err != nil {
			t.Fatal(err)
		}
		if len(body.Events) != tc.wantCount || body.NextSeq != tc.wantNext {
			t.Fatalf("page=%+v", body)
		}
	}
}

func TestDispatch_SCNsup0529_SixVerbsAndMalformedParams(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	d := &Dispatch{}
	for _, tc := range []struct {
		verb   string
		params string
	}{
		{"status", `{}`}, {"stop", `{}`}, {"resume", `{}`}, {"start-session", `{"project_id":"alpha"}`}, {"stop-session", `{"session_id":"alpha-1"}`}, {"events", `{}`},
	} {
		r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: tc.verb, Params: json.RawMessage(tc.params)}, 0)
		if r.Error == nil || r.Error.Code == "SUP_VERB_UNKNOWN" || r.Error.Code == "SUP_REQUEST_MALFORMED" {
			t.Fatalf("verb=%s response=%+v", tc.verb, r)
		}
	}
	for _, params := range []string{`{"session_id":"../etc"}`, `{"limit":0}`, `{"supervised":false}`, `{"since_seq":-1}`} {
		r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "events", Params: json.RawMessage(params)}, 0)
		if r.Error == nil || r.Error.Code != "SUP_REQUEST_MALFORMED" {
			t.Fatalf("params=%s response=%+v", params, r)
		}
	}
}

type dispatchTestSessions struct{ values []supervisor.ManagedSession }

type dispatchTestEvents struct{ values []store.Event }

func (e dispatchTestEvents) ReadAll() ([]store.Event, int, error) { return e.values, 0, nil }

func TestDispatchEventsOmitsStoredPattern(t *testing.T) {
	d := &Dispatch{Events: dispatchTestEvents{values: []store.Event{{Seq: 1, Kind: supervisor.EventKindSensitivePathBlocked, Pattern: ".env*"}}}}
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "events"}, 0)
	if !r.OK || strings.Contains(string(r.Result), "pattern") {
		t.Fatalf("response=%+v", r)
	}
}

func (s dispatchTestSessions) List() ([]supervisor.ManagedSession, error) { return s.values, nil }
func (s dispatchTestSessions) Load(supervisor.SessionID) (supervisor.ManagedSession, error) {
	return s.values[0], nil
}
func (s dispatchTestSessions) Save(supervisor.ManagedSession) error { return nil }

func TestDispatchStopSessionResultJSONKeys(t *testing.T) {
	id := supervisor.SessionID("alpha-1")
	d := &Dispatch{StopSession: &usecase.StopSessionUseCase{Sessions: dispatchTestSessions{values: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionExited}}}, PTYs: &usecase.PTYRegistry{}}}
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "stop-session", Params: json.RawMessage(`{"session_id":"alpha-1"}`)}, 0)
	if !r.OK || string(r.Result) != `{"session_id":"alpha-1","state":"exited"}` {
		t.Fatalf("response=%+v", r)
	}
}

func TestDispatch_SCNsup0532_StatusOmitsPrivateContent(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	id, _ := supervisor.NewSessionID("alpha-1")
	d := &Dispatch{Sessions: dispatchTestSessions{values: []supervisor.ManagedSession{{SessionID: id, ProjectID: "alpha", CLI: supervisor.CLIClaude, State: supervisor.SessionWorking, LastVerifiedFingerprint: "private-sentinel"}}}}
	r := d.Handle(context.Background(), Request{SupervisorProtocolVersion: SupervisorProtocolVersion, Verb: "status"}, 0)
	if !r.OK || len(r.Result) == 0 || string(r.Result) == "null" {
		t.Fatalf("response=%+v", r)
	}
	if strings.Contains(string(r.Result), "private-sentinel") {
		t.Fatalf("private field leaked: %s", r.Result)
	}
}
