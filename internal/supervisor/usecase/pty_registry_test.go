package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

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
