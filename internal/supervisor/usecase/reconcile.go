package usecase

import (
	"errors"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type ReconcileStore interface {
	List() ([]supervisor.ManagedSession, []supervisor.SessionID, []supervisor.SessionID, error)
	Load(supervisor.SessionID) (supervisor.ManagedSession, error)
	Save(supervisor.ManagedSession) error
}
type ReconcileOnStartupUseCase struct {
	Store            ReconcileStore
	Stop             *StopState
	IsAlive          func(int) bool
	ProcessStartedAt func(int) (time.Time, bool)
	Now              func() time.Time
	RecordStructured func(store.Event) error
	Notify           func(supervisor.SupervisorEventKind, string)
}

func (u ReconcileOnStartupUseCase) Execute() ([]supervisor.ManagedSession, error) {
	if u.Store == nil {
		return nil, errors.New("session store is required")
	}
	if u.RecordStructured != nil || u.Notify != nil {
		bindQuarantine(u.Store, func(id supervisor.SessionID) { u.eventNotify(id, supervisor.EventKindStateFileCorrupt, "") })
	}
	now := time.Now().UTC()
	if u.Now != nil {
		now = u.Now().UTC()
	}
	sessions, newlyCorruptIDs, previouslyCorruptIDs, err := u.Store.List()
	if err != nil {
		return nil, err
	}
	for i := range sessions {
		s := &sessions[i]
		hadChildPID := s.ChildPID > 0
		orphanAlive := false
		if s.ChildPID > 0 {
			alive := u.IsAlive
			if alive == nil {
				alive = func(pid int) bool { return syscall.Kill(pid, 0) == nil }
			}
			if alive(s.ChildPID) {
				if started, available := u.processStartedAt(s.ChildPID); !available || s.ChildStartedAt == nil || withinOneSecond(started, *s.ChildStartedAt) {
					orphanAlive = true
					u.eventNotify(s.SessionID, supervisor.EventKindOrphanChildAlive, "")
				}
			}
		}
		if s.ChildPID > 0 && !orphanAlive {
			if err := UpdateSessionWith(sessionLocker, s.SessionID,
				func() (supervisor.ManagedSession, error) { return u.Store.Load(s.SessionID) }, u.Store.Save,
				func(current *supervisor.ManagedSession) error {
					current.ChildPID = 0
					current.ChildStartedAt = nil
					return nil
				}); err != nil {
				return sessions, err
			}
			*s, err = u.Store.Load(s.SessionID)
			if err != nil {
				return sessions, err
			}
		}
		if s.State == supervisor.SessionStopped || s.State == supervisor.SessionExited || s.State == supervisor.SessionFailed {
			continue
		}
		event := supervisor.EventStop
		stopReason := supervisor.StopSupervisorRestarted
		ctx := supervisor.TransitionContext{Generation: s.Generation, StopReason: stopReason}
		if s.State == supervisor.SessionStarting && !hadChildPID && s.Generation == 1 {
			event = supervisor.EventReconcile
		}
		if err := UpdateSessionWith(sessionLocker, s.SessionID,
			func() (supervisor.ManagedSession, error) { return u.Store.Load(s.SessionID) },
			u.Store.Save,
			func(current *supervisor.ManagedSession) error { return current.Apply(event, ctx, now) }); err != nil {
			return sessions, err
		}
		updated, err := u.Store.Load(s.SessionID)
		if err != nil {
			return sessions, err
		}
		previousState := s.State
		*s = updated
		if u.RecordStructured != nil && previousState != s.State {
			_ = u.RecordStructured(store.Event{Kind: supervisor.EventKindStateChanged, SessionID: s.SessionID.String(), FromState: previousState, ToState: s.State})
		}
		if event == supervisor.EventReconcile {
			if u.RecordStructured != nil {
				_ = u.RecordStructured(store.Event{Kind: supervisor.EventKindSessionStartInterrupted, SessionID: s.SessionID.String()})
			}
		} else {
			u.eventNotify(s.SessionID, supervisor.EventKindSessionStopped, store.EventReason(stopReason))
		}
	}
	corruptSet := make(map[supervisor.SessionID]struct{}, len(newlyCorruptIDs)+len(previouslyCorruptIDs))
	for _, id := range append(append([]supervisor.SessionID(nil), newlyCorruptIDs...), previouslyCorruptIDs...) {
		corruptSet[id] = struct{}{}
	}
	for id := range corruptSet {
		found := false
		for _, s := range sessions {
			if s.SessionID == id {
				found = true
				break
			}
		}
		if found {
			continue
		}
		s := supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StateSince: now, StopScope: supervisor.StopScopeNone, StopReason: supervisor.StopStateFileCorrupt, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
		sessions = append(sessions, s)
		_ = newlyCorruptIDs
	}
	if u.Stop != nil {
		if err := u.Stop.RepairFromSessions(sessions); err != nil {
			return sessions, err
		}
	}
	return sessions, nil
}

func containsSessionID(ids []supervisor.SessionID, target supervisor.SessionID) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

func withinOneSecond(a, b time.Time) bool {
	if a.Before(b) {
		return b.Sub(a) <= time.Second
	}
	return a.Sub(b) <= time.Second
}

func (u ReconcileOnStartupUseCase) processStartedAt(pid int) (time.Time, bool) {
	if u.ProcessStartedAt == nil {
		return proc.ProcessStartTime(pid)
	}
	return u.ProcessStartedAt(pid)
}
func (u ReconcileOnStartupUseCase) eventNotify(id supervisor.SessionID, kind supervisor.SupervisorEventKind, reason store.EventReason) {
	if u.RecordStructured != nil {
		event := store.Event{SessionID: id.String(), Kind: kind}
		if kind == supervisor.EventKindSessionStopped {
			event.Reason = reason
		}
		_ = u.RecordStructured(event)
	}
	if u.Notify != nil {
		u.Notify(kind, id.String())
	}
}
