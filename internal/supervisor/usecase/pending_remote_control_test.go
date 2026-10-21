package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

func pendingRemoteControlFixture(t *testing.T) (*PendingRemoteControlUseCase, *sendTestStore, *sendTestPTY, *[]store.Event) {
	t.Helper()
	id, err := supervisor.NewSessionID("alpha-1")
	if err != nil {
		t.Fatal(err)
	}
	sessions := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, Generation: 2, State: supervisor.SessionWorking, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	pty := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
	send := &SendInputUseCase{Sessions: sessions, Screens: screen, PTY: pty, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Second}
	pty.afterEnter = func() {
		shot := blankScreen()
		shot.ChangedAt = time.Now().Add(time.Second)
		notifySendTestPair(send, SendConfirmation{SessionID: id, SendID: sessions.session.SendGate.Pending.SendID, Screen: shot})
		screen.shot = blankScreen()
	}
	events := &[]store.Event{}
	u := &PendingRemoteControlUseCase{Sessions: sessions, Send: send, RecordStructured: func(e store.Event) error { *events = append(*events, e); return nil }}
	return u, sessions, pty, events
}

func registerPendingRemoteControl(t *testing.T, u *PendingRemoteControlUseCase, sessions *sendTestStore) {
	t.Helper()
	if err := u.Register(sessions.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if !sessions.session.PendingRemoteControl || sessions.session.PendingRemoteControlSince == nil {
		t.Fatalf("pending was not saved: %+v", sessions.session)
	}
}

func TestPendingRemoteControl_SCN0417_GenerationChangeExpires(t *testing.T) {
	u, sessions, pty, events := pendingRemoteControlFixture(t)
	registerPendingRemoteControl(t, u, sessions)
	sessions.session.Generation++
	sessions.session.State = supervisor.SessionIdle
	if err := u.OnObserved(context.Background(), sessions.session.SessionID, detect.StateIdle); err != nil {
		t.Fatal(err)
	}
	if sessions.session.PendingRemoteControl || pty.calls != 0 || len(*events) != 2 || (*events)[1].Kind != supervisor.EventKindRemoteControlExpired || sessions.session.State != supervisor.SessionIdle {
		t.Fatalf("session=%+v calls=%d events=%+v", sessions.session, pty.calls, *events)
	}
}

func TestPendingRemoteControl_SCN0417_SessionEndExpires(t *testing.T) {
	for _, state := range []supervisor.SupervisorSessionState{supervisor.SessionExited, supervisor.SessionFailed} {
		t.Run(string(state), func(t *testing.T) {
			u, sessions, pty, events := pendingRemoteControlFixture(t)
			registerPendingRemoteControl(t, u, sessions)
			sessions.session.State = state
			if err := u.OnObserved(context.Background(), sessions.session.SessionID, detect.StateIdle); err != nil {
				t.Fatal(err)
			}
			if sessions.session.PendingRemoteControl || pty.calls != 0 || len(*events) != 2 || (*events)[1].Kind != supervisor.EventKindRemoteControlExpired || sessions.session.State != state {
				t.Fatalf("session=%+v calls=%d events=%+v", sessions.session, pty.calls, *events)
			}
		})
	}
}

func TestPendingRemoteControl_SCN0417_NewHandoffExpires(t *testing.T) {
	u, sessions, pty, events := pendingRemoteControlFixture(t)
	registerPendingRemoteControl(t, u, sessions)
	sessions.session.State = supervisor.SessionRotating
	sessions.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendEnd}
	if err := u.OnObserved(context.Background(), sessions.session.SessionID, detect.StateIdle); err != nil {
		t.Fatal(err)
	}
	if sessions.session.PendingRemoteControl || pty.calls != 0 || len(*events) != 2 || (*events)[1].Kind != supervisor.EventKindRemoteControlExpired || sessions.session.State != supervisor.SessionRotating {
		t.Fatalf("session=%+v calls=%d events=%+v", sessions.session, pty.calls, *events)
	}
}

func TestPendingRemoteControl_SCN0417_SendsOnceAtNextIdle(t *testing.T) {
	u, sessions, pty, events := pendingRemoteControlFixture(t)
	registerPendingRemoteControl(t, u, sessions)
	if err := u.OnObserved(context.Background(), sessions.session.SessionID, detect.StateWorking); err != nil {
		t.Fatal(err)
	}
	if pty.calls != 0 || !sessions.session.PendingRemoteControl {
		t.Fatalf("working: calls=%d pending=%v", pty.calls, sessions.session.PendingRemoteControl)
	}
	sessions.session.State = supervisor.SessionIdle
	for i := 0; i < 2; i++ {
		if err := u.OnObserved(context.Background(), sessions.session.SessionID, detect.StateIdle); err != nil {
			t.Fatal(err)
		}
	}
	if pty.calls != 1 || sessions.session.PendingRemoteControl || sessions.session.PendingRemoteControlSince != nil || len(*events) != 1 || pty.wrote != "/rc" {
		t.Fatalf("session=%+v calls=%d events=%+v", sessions.session, pty.calls, *events)
	}
}

func TestPendingRemoteControl_SCN0417_TimeAloneDoesNotExpire(t *testing.T) {
	u, sessions, pty, events := pendingRemoteControlFixture(t)
	registerPendingRemoteControl(t, u, sessions)
	old := time.Now().Add(-48 * time.Hour)
	sessions.session.PendingRemoteControlSince = &old
	if err := u.OnObserved(context.Background(), sessions.session.SessionID, detect.StateWorking); err != nil {
		t.Fatal(err)
	}
	if !sessions.session.PendingRemoteControl || pty.calls != 0 || len(*events) != 1 {
		t.Fatalf("session=%+v calls=%d events=%+v", sessions.session, pty.calls, *events)
	}
	sessions.session.State = supervisor.SessionIdle
	if err := u.OnObserved(context.Background(), sessions.session.SessionID, detect.StateIdle); err != nil {
		t.Fatal(err)
	}
	if sessions.session.PendingRemoteControl || pty.calls != 1 {
		t.Fatalf("session=%+v calls=%d", sessions.session, pty.calls)
	}
}

func TestPendingRemoteControl_SCN0417_UnconfirmedSendIsNotRetried(t *testing.T) {
	u, sessions, pty, _ := pendingRemoteControlFixture(t)
	registerPendingRemoteControl(t, u, sessions)
	sessions.session.State = supervisor.SessionIdle
	pty.afterEnter = nil
	u.Send.ConfirmTimeout = time.Millisecond
	if err := u.OnObserved(context.Background(), sessions.session.SessionID, detect.StateIdle); err == nil {
		t.Fatal("unconfirmed send must fail")
	}
	if pty.calls != 1 || sessions.session.PendingRemoteControl {
		t.Fatalf("session=%+v calls=%d", sessions.session, pty.calls)
	}
}

func TestPendingRemoteControl_SendDoesNotHoldGlobalMutex(t *testing.T) {
	u, sessions, _, _ := pendingRemoteControlFixture(t)
	registerPendingRemoteControl(t, u, sessions)
	sessions.session.State = supervisor.SessionIdle
	unlocked := false
	u.Send.RecordStructured = func(e store.Event) error {
		if e.Kind == supervisor.EventKindSendIssued && u.mu.TryLock() {
			unlocked = true
			u.mu.Unlock()
		}
		return nil
	}
	if err := u.OnObserved(context.Background(), sessions.session.SessionID, detect.StateIdle); err != nil {
		t.Fatal(err)
	}
	if !unlocked {
		t.Fatal("global mutex held during send")
	}
}

func TestPendingRemoteControl_BlockedGateRecordsOncePerUnchangedObservation(t *testing.T) {
	u, sessions, _, _ := pendingRemoteControlFixture(t)
	registerPendingRemoteControl(t, u, sessions)
	sessions.session.State = supervisor.SessionIdle
	if err := u.Send.StopState.EnableSession(sessions.session.SessionID); err != nil {
		t.Fatal(err)
	}
	blocked := 0
	u.Send.RecordStructured = func(e store.Event) error {
		if e.Kind == supervisor.EventKindSendBlocked {
			blocked++
		}
		return nil
	}
	for range 2 {
		if err := u.OnObserved(context.Background(), sessions.session.SessionID, detect.StateIdle); err != nil {
			t.Fatal(err)
		}
	}
	if blocked != 1 {
		t.Fatalf("send_blocked events=%d", blocked)
	}
}

func TestInitialRemoteControl_SCN0417_RotationDoesNotTriggerInitial(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	requests := 0
	r := &InitialRemoteControl{Request: func(context.Context, SendInput) error { requests++; return nil }}
	s := supervisor.ManagedSession{SessionID: id, Generation: 2, State: supervisor.SessionRotating}
	if err := r.OnStateChange(context.Background(), supervisor.SessionRotating, s); err != nil {
		t.Fatal(err)
	}
	s.State = supervisor.SessionIdle
	if err := r.OnStateChange(context.Background(), supervisor.SessionRotating, s); err != nil {
		t.Fatal(err)
	}
	if requests != 0 {
		t.Fatalf("initial /rc requested %d times", requests)
	}
}
