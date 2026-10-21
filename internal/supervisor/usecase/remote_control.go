package usecase

import (
	"context"
	"errors"
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

// InitialRemoteControl receives persisted state transitions from the observer.
// It requests /rc once when a generation first leaves starting for idle.
type InitialRemoteControl struct {
	Request          func(context.Context, SendInput) error
	RecordStructured func(store.Event) error
	OperationalLog   SupervisorOperationalLogger
	mu               sync.Mutex
	requested        map[supervisor.SessionID]int
	started          map[supervisor.SessionID]int
}

func (r *InitialRemoteControl) OnStateChange(ctx context.Context, previous supervisor.SupervisorSessionState, current supervisor.ManagedSession) error {
	if r.Request == nil {
		return nil
	}
	r.mu.Lock()
	if r.started == nil {
		r.started = make(map[supervisor.SessionID]int)
	}
	if previous == supervisor.SessionStarting && current.State != supervisor.SessionStarting {
		r.started[current.SessionID] = current.Generation
	}
	if current.State != supervisor.SessionIdle || r.started[current.SessionID] != current.Generation {
		r.mu.Unlock()
		return nil
	}
	if r.requested == nil {
		r.requested = make(map[supervisor.SessionID]int)
	}
	if r.requested[current.SessionID] == current.Generation {
		r.mu.Unlock()
		return nil
	}
	// Only starting generations after the initial launch use --resume.
	if current.Generation > 1 && current.RemoteControlConfirmed {
		if r.RecordStructured == nil {
			r.mu.Unlock()
			return errors.New("remote control skip event recorder is required")
		}
		if err := r.RecordStructured(store.Event{Kind: supervisor.EventKindRemoteControlSkipped, SessionID: current.SessionID.String(), Reason: store.EventReason(supervisor.RemoteControlSkipResumedAfterConfirmed)}); err != nil {
			r.mu.Unlock()
			return err
		}
		r.requested[current.SessionID] = current.Generation
		r.mu.Unlock()
		if r.OperationalLog != nil {
			_ = r.OperationalLog.Record(store.SupervisorLogEntry{Level: store.LogLevelWarn, Kind: supervisor.EventKindRemoteControlSkipped, SessionID: current.SessionID.String()})
		}
		return nil
	}
	r.requested[current.SessionID] = current.Generation
	r.mu.Unlock()
	return r.Request(ctx, SendInput{SessionID: current.SessionID, Kind: supervisor.SendKindRemoteControl})
}

func (r *InitialRemoteControl) warnFailure(s supervisor.ManagedSession) {
	if r.OperationalLog != nil {
		entry := store.SupervisorLogEntry{Level: store.LogLevelWarn, Kind: supervisor.EventKindRemoteControlSkipped, SessionID: s.SessionID.String()}
		if s.Generation > 1 && s.RemoteControlConfirmed {
			entry.Reason = store.LogReasonEventWriteFailed
		}
		_ = r.OperationalLog.Record(entry)
	}
}

func (r *InitialRemoteControl) EndSession(id supervisor.SessionID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.requested, id)
	delete(r.started, id)
}
