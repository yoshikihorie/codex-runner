package usecase

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/launch"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type registryChildPTY struct {
	line sendTestPTY
	done chan struct{}
}

func (p *registryChildPTY) Pid() int                  { return 461 }
func (p *registryChildPTY) Close() error              { return nil }
func (p *registryChildPTY) Kill(syscall.Signal) error { return nil }
func (p *registryChildPTY) Wait() (int, error)        { <-p.done; return 0, nil }
func (p *registryChildPTY) WriteLine(ctx context.Context, text string, delay time.Duration, before func() bool) error {
	return p.line.WriteLine(ctx, text, delay, before)
}

type registryStarter struct{ pty *registryChildPTY }

func (s registryStarter) Start(string, []string, []string, string, uint16, uint16) (ManagedPTY, error) {
	return s.pty, nil
}

func TestFix3RespawnRegistersPTYForFirstRemoteControl(t *testing.T) {
	u, sessions, _, id := respawnFixture(t)
	child := &registryChildPTY{done: make(chan struct{})}
	registry := &PTYRegistry{}
	u.Starter = registryStarter{pty: child}
	u.PTYs = registry
	observing := make(chan struct{})
	u.Observe = func(context.Context, supervisor.ManagedSession, ManagedPTY) { <-observing }
	defer func() { close(observing); close(child.done) }()
	if err := u.Execute(context.Background(), id, RespawnResumeRecovery); err != nil {
		t.Fatal(err)
	}
	if registry.Get(id) != child {
		t.Fatal("new child PTY not registered")
	}
	sessions.session.State = supervisor.SessionIdle
	screen := &sendTestScreen{shot: blankScreen()}
	child.line.onWrite = func() { echoScreen(screen, "/rc") }
	send := SendInputUseCase{Sessions: sessions, Screens: screen, PTYs: registry, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
	_, _ = send.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if child.line.calls != 1 {
		t.Fatalf("first /rc was not sent to new child: %d", child.line.calls)
	}
}

type respawnSpy struct {
	argv  []string
	err   error
	calls int
}

type respawnStateFlipStore struct {
	*regressionSessionStore
	loads int
}

type respawnWorkingFlipStore struct {
	*regressionSessionStore
	loads int
}

func (s *respawnWorkingFlipStore) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	s.loads++
	if s.loads == 2 {
		s.session.State = supervisor.SessionWorking
		s.session.StopReason = ""
	}
	return s.regressionSessionStore.Load(id)
}

func TestFix3RespawnStateChangeWithoutSTOPDoesNotStop(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	u.Store = &respawnWorkingFlipStore{regressionSessionStore: sessions}
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	if spy.calls != 0 || sessions.session.State != supervisor.SessionWorking || sessions.session.StopReason != "" {
		t.Fatalf("session=%+v calls=%d", sessions.session, spy.calls)
	}
}

type failObservationLoadStore struct {
	*regressionSessionStore
	loads int
}

type ownershipInspectStore struct {
	*regressionSessionStore
	registered bool
}

func (s *ownershipInspectStore) Save(v supervisor.ManagedSession) error {
	if v.ChildPID > 0 {
		s.registered = ownedChildren.Owns(v.SessionID, v.ChildPID)
	}
	return s.regressionSessionStore.Save(v)
}

func TestFix3RespawnRegistersOwnershipBeforeSavingPID(t *testing.T) {
	u, sessions, _, id := respawnFixture(t)
	inspecting := &ownershipInspectStore{regressionSessionStore: sessions}
	u.Store = inspecting
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	if !inspecting.registered {
		t.Fatal("child PID persisted before ownership registration")
	}
}

func (s *failObservationLoadStore) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	s.loads++
	if s.loads == 4 {
		return supervisor.ManagedSession{}, errors.New("observation load failed")
	}
	return s.regressionSessionStore.Load(id)
}

func TestFix3RespawnObservesAfterPostSpawnLoadFailure(t *testing.T) {
	u, sessions, _, id := respawnFixture(t)
	store := &failObservationLoadStore{regressionSessionStore: sessions}
	u.Store = store
	observed := make(chan supervisor.ManagedSession, 1)
	u.Observe = func(_ context.Context, s supervisor.ManagedSession, _ ManagedPTY) { observed <- s }
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	select {
	case s := <-observed:
		if s.ChildPID == 0 || s.Generation != 2 {
			t.Fatalf("fallback record=%+v", s)
		}
	case <-time.After(time.Second):
		t.Fatal("spawned child was not observed")
	}
}

func (s *respawnStateFlipStore) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	s.loads++
	if s.loads == 2 {
		s.session.State = supervisor.SessionStopped
		s.session.StopReason = supervisor.StopSession
	}
	return s.regressionSessionStore.Load(id)
}

func (s *respawnSpy) Start(_ string, argv, _ []string, _ string, _, _ uint16) (ManagedPTY, error) {
	s.calls++
	s.argv = argv
	if s.err != nil {
		return nil, s.err
	}
	return fakeManagedPTY{}, nil
}

func respawnFixture(t *testing.T) (RespawnSessionUseCase, *regressionSessionStore, *respawnSpy, supervisor.SessionID) {
	t.Helper()
	id, _ := supervisor.NewSessionID("alpha-1")
	root := t.TempDir()
	bin := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	sessions := &regressionSessionStore{session: supervisor.ManagedSession{SessionID: id, ProjectID: "alpha", CLI: supervisor.CLIClaude, State: supervisor.SessionStarting, StateSince: now, Generation: 1, ClaudeSessionID: "00000000-0000-0000-0000-000000000001", RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	spy := &respawnSpy{}
	u := RespawnSessionUseCase{Config: StartSessionConfig{StateDir: root, CodexdPath: bin, Columns: 80, Rows: 24}, Projects: map[string]StartProject{"alpha": {ID: "alpha", CLI: supervisor.CLIClaude, CWD: t.TempDir(), BinaryPath: bin}}, Store: sessions, Starter: spy, Billing: launch.BillingGuard{}, ParentEnv: func() []string { return nil }, Now: func() time.Time { return now }}
	return u, sessions, spy, id
}

func TestSCNsup01038RespawnKinds(t *testing.T) {
	for _, tc := range []struct {
		kind   RespawnKind
		state  supervisor.SupervisorSessionState
		count  int
		resume bool
	}{{RespawnRecovery, supervisor.SessionStarting, 1, true}, {RespawnResumeRecovery, supervisor.SessionStarting, 0, true}, {RespawnHandoffStage5, supervisor.SessionRotating, 1, false}} {
		t.Run(string(tc.kind), func(t *testing.T) {
			u, sessions, spy, id := respawnFixture(t)
			sessions.session.State = tc.state
			old := filepath.Join(u.Config.StateDir, "statusline", id.String()+".json")
			if err := os.MkdirAll(filepath.Dir(old), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(old, []byte(`{}`), 0600); err != nil {
				t.Fatal(err)
			}
			if err := u.Execute(context.Background(), id, tc.kind); err != nil {
				t.Fatal(err)
			}
			got := sessions.session
			if got.State != tc.state || got.Generation != 2 || got.RestartCount != tc.count || len(got.RestartTimes) != tc.count {
				t.Fatalf("record: %+v", got)
			}
			if got.ClaudeSessionID != "" || got.PreviousClaudeSessionID != "00000000-0000-0000-0000-000000000001" {
				t.Fatalf("id transfer: %+v", got)
			}
			if slices.Contains(spy.argv, "--resume") != tc.resume {
				t.Fatalf("argv: %v", spy.argv)
			}
			if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("old status: %v", err)
			}
		})
	}
}

func TestRespawnSession_SCN0439_HandoffResume(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	sessions.session.State = supervisor.SessionRotating
	sessions.session.RestartCount = 2
	sessions.session.RestartTimes = []time.Time{u.Now()}
	if err := u.Execute(context.Background(), id, RespawnHandoffResume); err != nil {
		t.Fatal(err)
	}
	if sessions.session.State != supervisor.SessionRotating || sessions.session.Generation != 2 || sessions.session.RestartCount != 2 || len(sessions.session.RestartTimes) != 1 || !slices.Contains(spy.argv, "--resume") || spy.argv[len(spy.argv)-1] != sessions.session.PreviousClaudeSessionID {
		t.Fatalf("session=%+v argv=%v", sessions.session, spy.argv)
	}
}

func TestRespawnSession_SCN0412_RotatingSpawnFailureReturned(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	sessions.session.State = supervisor.SessionRotating
	spy.err = errors.New("spawn failed")
	if err := u.Execute(context.Background(), id, RespawnHandoffStage5); err == nil || sessions.session.State != supervisor.SessionRotating {
		t.Fatalf("err=%v state=%s", err, sessions.session.State)
	}
}

func TestSCNsup01039RespawnFailureKeepsRecord(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	spy.err = errors.New("pty failed")
	var reasons []store.SupervisorStartFailReason
	u.RecordStartFailure = func(project string, reason store.SupervisorStartFailReason) {
		if project != "alpha" {
			t.Fatal(project)
		}
		reasons = append(reasons, reason)
	}
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	if sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopRespawnFailed || spy.calls != 1 {
		t.Fatalf("record: %+v, calls: %d", sessions.session, spy.calls)
	}
	if !slices.Equal(reasons, []store.SupervisorStartFailReason{store.StartFailSpawnFailed}) {
		t.Fatal(reasons)
	}
}

func TestSCNsup01032RespawnUsesPreviousID(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	sessions.session.PreviousClaudeSessionID = sessions.session.ClaudeSessionID
	sessions.session.ClaudeSessionID = ""
	if err := u.Execute(context.Background(), id, RespawnResumeRecovery); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(spy.argv, "--resume") || spy.argv[len(spy.argv)-1] != sessions.session.PreviousClaudeSessionID {
		t.Fatal(spy.argv)
	}
}

func TestSCNsup0522RespawnSkipsSessionStoppedAfterWait(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	sessions.session.State = supervisor.SessionStopped
	sessions.session.StopReason = supervisor.StopSession
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	if spy.calls != 0 || sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopSession {
		t.Fatalf("session=%+v calls=%d", sessions.session, spy.calls)
	}
}

func TestRespawnRechecksStateInsideUpdate(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	flipped := &respawnStateFlipStore{regressionSessionStore: sessions}
	u.Store = flipped
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	if spy.calls != 0 || sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopSession {
		t.Fatalf("session=%+v calls=%d", sessions.session, spy.calls)
	}
}

func TestRespawnDoesNotRejectMissingProjectRegistration(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	u.Projects = nil
	if err := u.Execute(context.Background(), id, RespawnRecovery); errors.Is(err, ErrProjectNotFound) {
		t.Fatal("registration was rechecked")
	}
	if spy.calls != 0 || sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopRespawnFailed {
		t.Fatalf("session=%+v calls=%d", sessions.session, spy.calls)
	}
}

func TestRespawnMissingIDsStopsBeforeSpawn(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	sessions.session.ClaudeSessionID = ""
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	if spy.calls != 0 || sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopClaudeSessionIDMissing {
		t.Fatalf("session=%+v calls=%d", sessions.session, spy.calls)
	}
}

func TestRespawnPrunesOldRestartTimes(t *testing.T) {
	u, sessions, _, id := respawnFixture(t)
	sessions.session.RestartTimes = []time.Time{u.Now().Add(-supervisor.RestartWindow - time.Second), u.Now().Add(-time.Hour)}
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	if len(sessions.session.RestartTimes) != 2 {
		t.Fatalf("restart times=%v", sessions.session.RestartTimes)
	}
}

func TestRespawnOnlyRecoveryNotifiesRestart(t *testing.T) {
	for _, kind := range []RespawnKind{RespawnRecovery, RespawnResumeRecovery, RespawnHandoffStage5} {
		u, sessions, _, id := respawnFixture(t)
		if kind == RespawnHandoffStage5 {
			sessions.session.State = supervisor.SessionRotating
		}
		var notified int
		var recorded int
		u.RecordStructured = func(e store.Event) error {
			if e.Kind == supervisor.EventKindSessionRestarted {
				recorded++
			}
			return nil
		}
		u.Notify = func(k supervisor.SupervisorEventKind, _ string) {
			if k == supervisor.EventKindSessionRestarted {
				notified++
			}
		}
		if err := u.Execute(context.Background(), id, kind); err != nil {
			t.Fatal(err)
		}
		want := 0
		if kind == RespawnRecovery {
			want = 1
		}
		if notified != want || recorded != want {
			t.Fatalf("kind=%s notified=%d recorded=%d", kind, notified, recorded)
		}
	}
}

func TestRespawnHandsPTYToObserver(t *testing.T) {
	u, _, _, id := respawnFixture(t)
	called := make(chan struct{})
	u.Observe = func(_ context.Context, s supervisor.ManagedSession, pty ManagedPTY) {
		defer close(called)
		if s.SessionID != id || pty == nil {
			t.Fatalf("observer input: %+v %v", s, pty)
		}
		_, _ = pty.Wait()
		ownedChildren.Remove(id, pty.Pid())
	}
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("observer not started")
	}
}

func TestRespawnStartFailureUsesNoSessionNotification(t *testing.T) {
	u, _, spy, id := respawnFixture(t)
	spy.err = errors.New("spawn failed")
	var target string
	count := 0
	u.Notify = func(k supervisor.SupervisorEventKind, session string) {
		if k == supervisor.EventKindSessionStartFailed {
			count++
			target = session
		}
	}
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	if count != 1 || target != "" {
		t.Fatalf("notification count=%d target=%q", count, target)
	}
}
