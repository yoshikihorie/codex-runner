package usecase

import (
	"context"
	"io"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type stopSessionPTY struct {
	pid    int
	done   chan struct{}
	once   sync.Once
	closed bool
}

func (p *stopSessionPTY) Pid() int                 { return p.pid }
func (p *stopSessionPTY) Read([]byte) (int, error) { <-p.done; return 0, io.EOF }
func (p *stopSessionPTY) Close() error {
	p.once.Do(func() { p.closed = true; close(p.done) })
	return nil
}
func (p *stopSessionPTY) Kill(syscall.Signal) error { panic("stop-session signaled child") }
func (p *stopSessionPTY) Wait() (int, error)        { <-p.done; return 0, nil }

func TestStopSession_SCNsup0535_ObservedExitOnce(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	id, _ := supervisor.NewSessionID("alpha-35")
	now := time.Now().UTC()
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "alpha", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: now, ChildPID: 135, ChildStartedAt: &now, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	sessions := &regressionSessionStore{session: s}
	pty := &stopSessionPTY{pid: 135, done: make(chan struct{})}
	registry := &PTYRegistry{}
	events := make(chan store.Event, 8)
	observer := ObserveSessionUseCase{Sessions: sessions, Detect: DetectSessionStateUseCase{Detector: exitAwareDetector{}}, SendInput: &SendInputUseCase{PTYs: registry}, Interval: time.Millisecond, RecordStructured: func(e store.Event) error { events <- e; return nil }}
	done := observer.StartObserved(context.Background(), s, pty, 80, 24)
	u := StopSessionUseCase{Sessions: sessions, PTYs: registry, ConfirmTimeout: time.Second, PollInterval: time.Millisecond}
	got, err := u.Execute(context.Background(), id)
	if err != nil || got.State != supervisor.SessionExited {
		t.Fatalf("result=%+v err=%v", got, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !pty.closed {
		t.Fatal("PTY not closed")
	}
	if registry.IsStopping(id, pty) {
		t.Fatal("stop mark remained after exit")
	}
	current, _ := sessions.Load(id)
	if current.ExitReason != supervisor.ExitStopSessionRequested || current.ChildPID != 0 {
		t.Fatalf("session=%+v", current)
	}
	count := 0
	for len(events) > 0 {
		e := <-events
		if e.Kind == supervisor.EventKindSessionExited {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("session_exited count=%d", count)
	}
}

func TestStopSession_SCNsup0542_TimeoutAndRetry(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	id, _ := supervisor.NewSessionID("alpha-42")
	s := supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StopReason: supervisor.StopSendNotConfirmed, ChildPID: 142, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	sessions := &regressionSessionStore{session: s}
	pty := &stopSessionPTY{pid: 142, done: make(chan struct{})}
	r := &PTYRegistry{}
	r.RegisterManaged(id, pty)
	ownedChildren.Register(id, 142)
	u := StopSessionUseCase{Sessions: sessions, PTYs: r, ConfirmTimeout: time.Millisecond, PollInterval: time.Millisecond, ChildAlive: func(int) bool { return true }}
	got, err := u.Execute(context.Background(), id)
	if err != nil || got.State != supervisor.SessionStopped || got.Reason != supervisor.StopOrphanChildAlive {
		t.Fatalf("result=%+v err=%v", got, err)
	}
	current, _ := sessions.Load(id)
	if current.StopReason != supervisor.StopOrphanChildAlive || r.GetManaged(id) != nil || !ownedChildren.Owns(id, 142) {
		t.Fatalf("session=%+v", current)
	}
	if r.IsStopping(id, pty) {
		t.Fatal("stop mark remained after timeout")
	}
	u.ChildAlive = func(int) bool { return false }
	got, err = u.Execute(context.Background(), id)
	if err != nil || got.State != supervisor.SessionExited {
		t.Fatalf("retry=%+v err=%v", got, err)
	}
}

func TestStopSessionExitAtDeadlineIsNotOrphaned(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-44")
	s := supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StopReason: supervisor.StopSendNotConfirmed, ChildPID: 144, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	sessions := &regressionSessionStore{session: s}
	checks := 0
	u := StopSessionUseCase{Sessions: sessions, PTYs: &PTYRegistry{}, ConfirmTimeout: time.Millisecond, PollInterval: time.Hour, ChildAlive: func(int) bool { checks++; return checks == 1 }}
	got, err := u.Execute(context.Background(), id)
	if err != nil || got.State != supervisor.SessionExited {
		t.Fatalf("result=%+v err=%v checks=%d", got, err, checks)
	}
}

func TestStopSessionTimeoutRejectsResumeWhileChildAlive(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-45")
	sessions := &regressionSessionStore{session: supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StopReason: supervisor.StopSendNotConfirmed, ChildPID: 145, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	u := StopSessionUseCase{Sessions: sessions, PTYs: &PTYRegistry{}, ConfirmTimeout: time.Millisecond, PollInterval: time.Hour, ChildAlive: func(int) bool { return true }}
	got, err := u.Execute(context.Background(), id)
	if err != nil || got.Reason != supervisor.StopOrphanChildAlive {
		t.Fatalf("stop=%+v err=%v", got, err)
	}
	control := NewStopControlUseCase(NewStopState(t.TempDir()), regressionStopRepository{sessions}, nil, nil)
	control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{ChildAlive: true, OrphanChildAlive: true}, nil
	})
	resumed, err := control.Resume(context.Background(), &id, true, false)
	if err != nil || len(resumed.StillStopped) != 1 || resumed.StillStopped[0].Reason != "orphan_child_alive" {
		t.Fatalf("resume=%+v err=%v", resumed, err)
	}
}

func TestStopSessionFailedTerminalRemainsUnchanged(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-43")
	sessions := &regressionSessionStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionFailed, ChildPID: 143, StopReason: supervisor.StopRecoveryStartTimeout}}
	u := StopSessionUseCase{Sessions: sessions, PTYs: &PTYRegistry{}, ConfirmTimeout: time.Millisecond, PollInterval: time.Millisecond, ChildAlive: func(int) bool { return true }}
	got, err := u.Execute(context.Background(), id)
	if err != nil || got.State != supervisor.SessionFailed || got.Reason != supervisor.StopRecoveryStartTimeout {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestObserve_SCNsup0535_ShutdownSkipsExitTransition(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	id, _ := supervisor.NewSessionID("alpha-36")
	now := time.Now().UTC()
	s := supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionIdle, StateSince: now, ChildPID: 136, ChildStartedAt: &now, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	sessions := &regressionSessionStore{session: s}
	pty := &stopSessionPTY{pid: 136, done: make(chan struct{})}
	r := &PTYRegistry{}
	events := make(chan store.Event, 8)
	u := ObserveSessionUseCase{Sessions: sessions, Detect: DetectSessionStateUseCase{Detector: exitAwareDetector{}}, SendInput: &SendInputUseCase{PTYs: r}, Interval: time.Millisecond, RecordStructured: func(e store.Event) error { events <- e; return nil }}
	done := u.StartObserved(context.Background(), s, pty, 80, 24)
	r.BeginShutdown()
	_ = pty.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	current, _ := sessions.Load(id)
	if current.State != supervisor.SessionIdle || len(events) != 0 {
		t.Fatalf("state=%s events=%d", current.State, len(events))
	}
}

func TestStopSession_SCNsup0535_UnknownIDDoesNotClosePTY(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	known, _ := supervisor.NewSessionID("alpha-37")
	unknown, _ := supervisor.NewSessionID("alpha-38")
	sessions := &regressionSessionStore{session: supervisor.ManagedSession{SessionID: known}}
	pty := &stopSessionPTY{pid: 137, done: make(chan struct{})}
	r := &PTYRegistry{}
	r.RegisterManaged(known, pty)
	u := StopSessionUseCase{Sessions: sessions, PTYs: r}
	if _, err := u.Execute(context.Background(), unknown); err != ErrSessionNotFound {
		t.Fatalf("error=%v", err)
	}
	if pty.closed || r.IsStopping(unknown, nil) {
		t.Fatal("unknown ID touched PTY or stopping mark")
	}
}
