package usecase

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/launch"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

func TestRecoveryTimeoutKeepsWatchingExit(t *testing.T) {
	id, _ := supervisor.NewSessionID("timeout-watch-1")
	started := time.Now().Add(-time.Hour)
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "timeout-watch", CLI: supervisor.CLIClaude, Generation: 2, State: supervisor.SessionStarting, StateSince: started, ChildPID: 123, ChildStartedAt: &started, StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	sessions := &regressionSessionStore{session: s}
	pty := &waitingPTY{closed: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	exitWhileStopped := 0
	go func() {
		result <- (ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateUndetermined}}}, Sessions: sessions, Interval: time.Millisecond, StartupTimeout: time.Nanosecond, RecordStructured: func(event testEvent) error {
			kind := event.Kind
			if kind == supervisor.EventKindExitWhileStopped {
				exitWhileStopped++
			}

			return nil
		}}).Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24))
	}()
	deadline := time.After(time.Second / 2)
	for {
		if current, _ := sessions.Load(id); current.State == supervisor.SessionStopped {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timeout did not stop session")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	_ = pty.Close()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	current, _ := sessions.Load(id)
	if current.ChildPID != 0 || current.ChildStartedAt != nil || exitWhileStopped != 1 {
		t.Fatalf("exit not observed: pid=%d started=%v events=%d", current.ChildPID, current.ChildStartedAt, exitWhileStopped)
	}
}

type timeoutPTY struct {
	done    chan struct{}
	signals []syscall.Signal
	closed  bool
}

func (p *timeoutPTY) Read([]byte) (int, error) { <-p.done; return 0, io.EOF }
func (p *timeoutPTY) Wait() (int, error)       { <-p.done; return 1, nil }
func (p *timeoutPTY) Close() error             { p.closed = true; return nil }
func (p *timeoutPTY) Kill(signal syscall.Signal) error {
	p.signals = append(p.signals, signal)
	if signal == syscall.SIGTERM {
		close(p.done)
	}
	return nil
}

func TestInitialStartupTimeoutTerminatesChildBeforeClearingPID(t *testing.T) {
	id, _ := supervisor.NewSessionID("timeout-first-1")
	started := time.Now().Add(-time.Hour)
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "timeout-first", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStarting, StateSince: started, ChildPID: 123, ChildStartedAt: &started, StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	sessions := &regressionSessionStore{session: s}
	pty := &timeoutPTY{done: make(chan struct{})}
	if err := (ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateUndetermined}}}, Sessions: sessions, Interval: time.Millisecond, StartupTimeout: time.Nanosecond}).Run(context.Background(), s, pty, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	current, _ := sessions.Load(id)
	if current.State != supervisor.SessionFailed || current.ChildPID != 0 || current.ChildStartedAt != nil || !pty.closed || len(pty.signals) != 1 || pty.signals[0] != syscall.SIGTERM {
		t.Fatalf("cleanup state=%#v signals=%v closed=%t", current, pty.signals, pty.closed)
	}
}

func TestGuardResumeCallsTransitionWithoutStateMutex(t *testing.T) {
	stop := NewStopState(t.TempDir())
	id, _ := supervisor.NewSessionID("resume-lock-1")
	done := make(chan error, 1)
	go func() {
		done <- stop.GuardResume(supervisor.StopScopeSession, id, func() error { stop.IsGlobalStopped(); return nil })
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("GuardResume held StopState.mu during callback")
	}
}

func TestModelRejectionRecordsTypedStartReason(t *testing.T) {
	id, _ := supervisor.NewSessionID("model-1")
	binary := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\n"), 0700); err != nil {
		t.Fatal(err)
	}
	model := "forbidden"
	var got store.SupervisorStartFailReason
	u := StartSessionUseCase{Config: StartSessionConfig{MaxSessions: 1}, Projects: map[string]StartProject{"model": {ID: "model", CLI: supervisor.CLIClaude, CWD: t.TempDir(), BinaryPath: binary, Model: &model}}, Store: &startMemoryStore{}, RecordStartFailure: func(project string, reason store.SupervisorStartFailReason) {
		if project != "model" {
			t.Errorf("project=%s", project)
		}
		got = reason
	}}
	if _, err := u.Execute("model", id); err == nil {
		t.Fatal("model accepted")
	}
	if got != store.StartFailLaunchArgvInvalid {
		t.Fatalf("reason=%s", got)
	}
}

func TestSettingsWriteFailureRecordsTypedStartReason(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(`{"auth_mode":"chatgpt"}`), 0600); err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	id, _ := supervisor.NewSessionID("settings-1")
	settingsPath := filepath.Join(stateDir, "statusline", id.String()+".settings.json")
	if err := os.MkdirAll(settingsPath, 0700); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\n"), 0700); err != nil {
		t.Fatal(err)
	}
	var got store.SupervisorStartFailReason
	u := StartSessionUseCase{Config: StartSessionConfig{StateDir: stateDir, CodexdPath: "/bin/true", MaxSessions: 1, Columns: 80, Rows: 24}, Projects: map[string]StartProject{"settings": {ID: "settings", CLI: supervisor.CLIClaude, CWD: t.TempDir(), BinaryPath: binary}}, Store: &startMemoryStore{}, Billing: launch.BillingGuard{ManagedSettingsPath: filepath.Join(home, "managed.json")}, ParentEnv: func() []string { return nil }, RecordStartFailure: func(_ string, reason store.SupervisorStartFailReason) { got = reason }}
	if _, err := u.Execute("settings", id); err == nil {
		t.Fatal("settings write unexpectedly succeeded")
	}
	if got != store.StartFailSettingsGenerationFailed {
		t.Fatalf("reason=%s", got)
	}
}

func TestBillingReasonMappingRejectsUnknown(t *testing.T) {
	for _, reason := range []store.SupervisorStartFailReason{store.StartFailUserSettings, store.StartFailProjectSettings, store.StartFailProjectLocalSettings, store.StartFailClaudeJSON, store.StartFailManaged, store.StartFailCodexAuth, store.StartFailChildEnv} {
		if got := billingStartFailureReason(fmt.Errorf("SUP_BILLING_GUARD_FAILED: %s", reason)); got != reason {
			t.Errorf("reason %s mapped to %s", reason, got)
		}
	}
	if got := billingStartFailureReason(fmt.Errorf("SUP_BILLING_GUARD_FAILED: unknown")); got.Valid() {
		t.Fatalf("unknown reason accepted: %s", got)
	}
}

type changedBeforeUpdateStore struct {
	session supervisor.ManagedSession
	loads   int
}

func (s *changedBeforeUpdateStore) Load(supervisor.SessionID) (supervisor.ManagedSession, error) {
	s.loads++
	if s.loads == 2 {
		s.session.State = supervisor.SessionWorking
	}
	return s.session, nil
}
func (s *changedBeforeUpdateStore) Save(session supervisor.ManagedSession) error {
	s.session = session
	return nil
}

func TestStateChangeFromStateUsesUpdateSnapshot(t *testing.T) {
	id, _ := supervisor.NewSessionID("from-state-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "from-state", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStarting, StateSince: time.Now(), StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	sessions := &changedBeforeUpdateStore{session: s}
	pty := &waitingPTY{closed: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	events := make(chan store.Event, 1)
	done := make(chan error, 1)
	go func() {
		done <- (ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle}}}, Sessions: sessions, Interval: time.Millisecond, RecordStructured: func(event store.Event) error {
			if event.Kind == supervisor.EventKindStateChanged {
				events <- event
			}
			return nil
		}}).Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24))
	}()
	select {
	case event := <-events:
		if event.FromState != supervisor.SessionWorking || event.ToState != supervisor.SessionIdle {
			t.Errorf("transition=%s -> %s", event.FromState, event.ToState)
		}
	case <-ctx.Done():
		t.Fatal("state change was not recorded")
	}
	cancel()
	_ = pty.Close()
	<-done
}

func TestMaxSessionsErrorIsPureCode(t *testing.T) {
	if got := ErrMaxSessionsInvalid.Error(); got != "SUP_CONFIG_INVALID" {
		t.Fatalf("error=%q", got)
	}
}
