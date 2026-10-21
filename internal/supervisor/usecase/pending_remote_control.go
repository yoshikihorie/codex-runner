package usecase

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

// PendingRemoteControlUseCase owns the post-handoff /rc request. The observer
// calls OnObserved after persisting the current session and observed states.
type PendingRemoteControlUseCase struct {
	Sessions         SessionRecordStore
	Send             *SendInputUseCase
	RecordStructured func(store.Event) error
	Now              func() time.Time
	mu               sync.Mutex
	locks            map[supervisor.SessionID]*sync.Mutex
	registered       map[supervisor.SessionID]int
	blocked          map[supervisor.SessionID]remoteControlObservation
}

type remoteControlObservation struct {
	generation int
	state      supervisor.SupervisorSessionState
	observed   detect.DetectedState
	pending    string
	changedAt  time.Time
	quiet      bool
	stopped    bool
}

func (u *PendingRemoteControlUseCase) sessionLock(id supervisor.SessionID) *sync.Mutex {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.locks == nil {
		u.locks = make(map[supervisor.SessionID]*sync.Mutex)
	}
	if u.locks[id] == nil {
		u.locks[id] = &sync.Mutex{}
	}
	return u.locks[id]
}

func (u *PendingRemoteControlUseCase) registerGeneration(id supervisor.SessionID, generation int) {
	u.mu.Lock()
	if u.registered == nil {
		u.registered = make(map[supervisor.SessionID]int)
	}
	u.registered[id] = generation
	u.mu.Unlock()
}

func (u *PendingRemoteControlUseCase) now() time.Time {
	if u.Now != nil {
		return u.Now().UTC()
	}
	return time.Now().UTC()
}

func (u *PendingRemoteControlUseCase) record(id supervisor.SessionID, kind supervisor.SupervisorEventKind) error {
	if u.RecordStructured == nil {
		return nil
	}
	return u.RecordStructured(store.Event{Kind: kind, SessionID: id.String(), At: u.now()})
}

// Register persists the pending request once stage 7 has completed.
func (u *PendingRemoteControlUseCase) Register(id supervisor.SessionID) error {
	if u.Sessions == nil {
		return errors.New("session store is required")
	}
	lock := u.sessionLock(id)
	lock.Lock()
	defer lock.Unlock()
	created := false
	var generation int
	err := UpdateSession(u.Sessions, id, func(s *supervisor.ManagedSession) error {
		generation = s.Generation
		if s.PendingRemoteControl {
			return nil
		}
		now := u.now()
		s.PendingRemoteControl = true
		s.PendingRemoteControlSince = &now
		created = true
		return nil
	})
	if err != nil {
		return err
	}
	u.registerGeneration(id, generation)
	if created {
		return u.record(id, supervisor.EventKindRemoteControlPending)
	}
	return nil
}

// OnObserved expires a pending request or sends it on the next eligible idle.
// The send gate makes the final decision using a fresh session and screen read.
func (u *PendingRemoteControlUseCase) OnObserved(ctx context.Context, id supervisor.SessionID, observed detect.DetectedState) error {
	if u.Sessions == nil {
		return errors.New("session store is required")
	}
	lock := u.sessionLock(id)
	lock.Lock()
	defer lock.Unlock()
	s, err := u.Sessions.Load(id)
	if err != nil {
		return err
	}
	if !s.PendingRemoteControl {
		u.mu.Lock()
		delete(u.registered, id)
		delete(u.blocked, id)
		u.mu.Unlock()
		return nil
	}
	u.mu.Lock()
	generation, known := u.registered[id]
	u.mu.Unlock()
	if known && generation != s.Generation ||
		s.PendingRemoteControlSince != nil && s.ChildStartedAt != nil && s.ChildStartedAt.After(*s.PendingRemoteControlSince) ||
		s.State == supervisor.SessionExited || s.State == supervisor.SessionFailed ||
		s.State == supervisor.SessionRotating || s.Rotation != nil {
		if err := UpdateSession(u.Sessions, id, func(current *supervisor.ManagedSession) error {
			current.PendingRemoteControl = false
			current.PendingRemoteControlSince = nil
			return nil
		}); err != nil {
			return err
		}
		u.mu.Lock()
		delete(u.registered, id)
		delete(u.blocked, id)
		u.mu.Unlock()
		return u.record(id, supervisor.EventKindRemoteControlExpired)
	}
	if s.State != supervisor.SessionIdle && s.State != supervisor.SessionAwaitingRecommendation {
		return nil
	}
	if observed != detect.StateIdle && observed != detect.StateAwaitingRecommendation {
		return nil
	}
	if u.Send == nil {
		return errors.New("send input use case is required")
	}
	signature := remoteControlObservation{generation: s.Generation, state: s.State, observed: observed, stopped: u.Send.stopped(id)}
	if s.SendGate.Pending != nil {
		signature.pending = s.SendGate.Pending.SendID
	}
	if u.Send.Screens != nil {
		shot := u.Send.Screens.Snapshot(id)
		signature.changedAt = shot.ChangedAt
		signature.quiet = !shot.ChangedAt.IsZero() && u.now().Sub(shot.ChangedAt) >= detect.QuietPeriod
	}
	u.mu.Lock()
	previous, repeated := u.blocked[id]
	u.mu.Unlock()
	if repeated && previous == signature {
		return nil
	}
	result, sendErr := u.Send.Execute(ctx, SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl, Origin: supervisor.SendOriginNormal})
	u.mu.Lock()
	if result.Blocked != "" {
		if u.blocked == nil {
			u.blocked = make(map[supervisor.SessionID]remoteControlObservation)
		}
		u.blocked[id] = signature
	} else {
		delete(u.blocked, id)
	}
	u.mu.Unlock()
	if result.Confirmed || result.SendID != "" && u.Send.CRWritten(id, result.SendID) {
		if err := UpdateSession(u.Sessions, id, func(current *supervisor.ManagedSession) error {
			current.PendingRemoteControl = false
			current.PendingRemoteControlSince = nil
			return nil
		}); err != nil {
			return errors.Join(sendErr, err)
		}
		u.mu.Lock()
		delete(u.registered, id)
		delete(u.blocked, id)
		u.mu.Unlock()
	}
	return sendErr
}
