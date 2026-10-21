package usecase

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

func TestPTYRegistryRejectsLaunchAfterShutdown(t *testing.T) {
	r := &PTYRegistry{}
	r.BeginShutdown()
	called := false
	_, err := r.StartChild(func() (ManagedPTY, error) { called = true; return &recoveryPTY{pid: 1}, nil })
	if err == nil || called {
		t.Fatalf("launch called=%v err=%v", called, err)
	}
	if !errors.Is(err, ErrStopActive) {
		t.Fatalf("shutdown error=%v", err)
	}
}

func TestPTYRegistry_SCNsup0535_ManagedAndStoppingIdentity(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	id, _ := supervisor.NewSessionID("alpha-1")
	r := &PTYRegistry{}
	old, next := &recoveryPTY{pid: 1}, &recoveryPTY{pid: 2}
	r.RegisterManaged(id, old)
	r.MarkStopping(id)
	r.RegisterManaged(id, next)
	if r.GetManaged(id) != next || r.IsStopping(id, next) {
		t.Fatal("stale stop mark reached new child")
	}
	r.RemoveManaged(id, old)
	if r.GetManaged(id) != next {
		t.Fatal("old child removed replacement")
	}
	r.MarkStopping(id)
	if !r.IsStopping(id, next) {
		t.Fatal("stop mark missing")
	}
	r.ClearStopping(id, old)
	if !r.IsStopping(id, next) {
		t.Fatal("stale clear removed mark")
	}
	r.ClearStopping(id, next)
	if r.IsStopping(id, next) {
		t.Fatal("mark remained")
	}
	r.BeginShutdown()
	if !r.IsShuttingDown() {
		t.Fatal("shutdown mark missing")
	}
}

func TestFix3PTYRegistryReplacesChildWithoutOldRemoval(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	r := &PTYRegistry{}
	old, next := &sendTestPTY{}, &sendTestPTY{}
	r.Register(id, old)
	r.Register(id, next)
	r.Remove(id, old)
	if r.Get(id) != next {
		t.Fatal("old child removed the current PTY")
	}
	r.Remove(id, next)
	if r.Get(id) != nil {
		t.Fatal("PTY remained after exit")
	}
}

func TestFix3SendUsesRegisteredChildAndBlocksWhenMissing(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	old, next := &sendTestPTY{}, &sendTestPTY{}
	r := &PTYRegistry{}
	r.Register(id, next)
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: old, PTYs: r, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
	next.onWrite = func() { echoScreen(screen, "/rc") }
	_, _ = u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if old.calls != 0 || next.calls != 1 {
		t.Fatalf("old=%d next=%d", old.calls, next.calls)
	}
	r.Remove(id, next)
	s.session.State = supervisor.SessionIdle
	s.session.SendGate.Pending = nil
	s.session.SendGate.LastSendConfirmed = true
	screen.shot = blankScreen()
	got, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err != nil || got.Blocked != supervisor.SendBlockedState || next.calls != 1 {
		t.Fatalf("result=%+v err=%v calls=%d", got, err, next.calls)
	}
}
