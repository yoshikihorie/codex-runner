package usecase

import (
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type screenReadSessions struct {
	session supervisor.ManagedSession
	loads   int
}

func (s *screenReadSessions) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	s.loads++
	if id != s.session.SessionID {
		return supervisor.ManagedSession{}, os.ErrNotExist
	}
	return s.session, nil
}

func TestScreenRead_SCNsup0559_61_62_64(t *testing.T) {
	id := supervisor.SessionID("alpha-1")
	at := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	sessions := &screenReadSessions{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionStopped}}
	screens := &ObservedScreenStore{}
	var events []store.Event
	u := ScreenReadUseCase{Sessions: sessions, Screens: screens, RecordStructured: func(e store.Event) error { events = append(events, e); return nil }}
	if _, err := u.Execute("ghost-9"); !errors.Is(err, ErrSessionNotFound) || len(events) != 0 {
		t.Fatalf("unknown: err=%v events=%v", err, events)
	}
	if _, err := u.Execute(id); !errors.Is(err, ErrScreenUnavailable) || len(events) != 0 {
		t.Fatalf("missing snapshot: err=%v events=%v", err, events)
	}
	screens.SetSnapshot(id, ScreenSnapshot{Rows: []string{"全角  ", "second\t "}, ObservedAt: at})
	for i := 0; i < 2; i++ {
		got, err := u.Execute(id)
		if err != nil || got.SessionID != id || got.RowCount != 2 || got.ObservedAt != at || len(got.Rows) != 2 || got.Rows[0] != "全角" || got.Rows[1] != "second" {
			t.Fatalf("result=%+v err=%v", got, err)
		}
	}
	if len(events) != 2 || events[0].Kind != supervisor.EventKindScreenRead || events[0].SessionID != string(id) || events[0].RowCount == nil || *events[0].RowCount != 2 {
		t.Fatalf("events=%+v", events)
	}
	screens.SetSnapshot(id, ScreenSnapshot{Rows: []string{"latest"}, ObservedAt: at.Add(time.Second)})
	latest, err := u.Execute(id)
	if err != nil || latest.RowCount != 1 || latest.Rows[0] != "latest" || latest.ObservedAt != at.Add(time.Second) {
		t.Fatalf("latest=%+v err=%v", latest, err)
	}
	sessions.session.State = supervisor.SessionExited
	if _, err := u.Execute(id); !errors.Is(err, ErrScreenUnavailable) || len(events) != 3 {
		t.Fatalf("exited with stale snapshot: err=%v events=%v", err, events)
	}
	screens.Delete(id)
	if _, err := u.Execute(id); !errors.Is(err, ErrScreenUnavailable) || len(events) != 3 {
		t.Fatalf("after exit: err=%v events=%v", err, events)
	}
}

type screenReaderFunc func(supervisor.SessionID) ScreenSnapshot

func (f screenReaderFunc) Snapshot(id supervisor.SessionID) ScreenSnapshot { return f(id) }

func TestScreenReadRejectsExitDuringSnapshot(t *testing.T) {
	id := supervisor.SessionID("alpha-1")
	for _, state := range []supervisor.SupervisorSessionState{supervisor.SessionExited, supervisor.SessionFailed} {
		sessions := &screenReadSessions{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle}}
		recorded := 0
		u := ScreenReadUseCase{Sessions: sessions, Screens: screenReaderFunc(func(supervisor.SessionID) ScreenSnapshot {
			sessions.session.State = state
			return ScreenSnapshot{Rows: []string{"private screen"}, ObservedAt: time.Now()}
		}), RecordStructured: func(store.Event) error { recorded++; return nil }}
		got, err := u.Execute(id)
		if !errors.Is(err, ErrScreenUnavailable) || len(got.Rows) != 0 || recorded != 0 || sessions.loads != 2 {
			t.Fatalf("state=%s result=%+v err=%v loads=%d recorded=%d", state, got, err, sessions.loads, recorded)
		}
	}
}

func TestScreenReadAuditFailureDoesNotReturnBody(t *testing.T) {
	id := supervisor.SessionID("alpha-1")
	auditErr := errors.New("audit unavailable")
	u := ScreenReadUseCase{
		Sessions: &screenReadSessions{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle}},
		Screens: screenReaderFunc(func(supervisor.SessionID) ScreenSnapshot {
			return ScreenSnapshot{Rows: []string{"private screen"}, ObservedAt: time.Now()}
		}),
		RecordStructured: func(store.Event) error { return auditErr },
	}
	got, err := u.Execute(id)
	if !errors.Is(err, auditErr) || len(got.Rows) != 0 || got.RowCount != 0 {
		t.Fatalf("result=%+v err=%v", got, err)
	}
}

func TestScreenReadSCNsup0559DoesNotWriteOrChangeSession(t *testing.T) {
	session, sessions, events := observeSendFixture(t, supervisor.SessionIdle)
	id := session.SessionID
	pty := newObserveSendPTY()
	observed := &ObservedStateStore{}
	observed.Set(id, detect.StateIdle)
	screens := &ObservedScreenStore{}
	screens.SetSnapshot(id, ScreenSnapshot{Rows: []string{"read only"}, ObservedAt: time.Now()})
	before, err := sessions.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	u := ScreenReadUseCase{Sessions: sessions, Screens: screens, RecordStructured: observeSendRecord(events)}
	if result, err := u.Execute(id); err != nil || result.RowCount != 1 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	after, err := sessions.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	state, ok := observed.Get(id)
	if pty.writes.Load() != 0 || !reflect.DeepEqual(before, after) || !ok || state != detect.StateIdle {
		t.Fatalf("writes=%d before=%+v after=%+v observed=%s", pty.writes.Load(), before, after, state)
	}
}

func TestScreenReadSCNsup0564IsNotNotificationTarget(t *testing.T) {
	if notificationTarget(supervisor.EventKindScreenRead) {
		t.Fatal("screen_read must not trigger notification")
	}
}
