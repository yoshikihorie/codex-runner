package usecase

import (
	"context"
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// InitialRemoteControl receives persisted state transitions from the observer.
// It requests /rc once when a generation first leaves starting for idle.
type InitialRemoteControl struct {
	Request   func(context.Context, SendInput) error
	mu        sync.Mutex
	requested map[supervisor.SessionID]int
	started   map[supervisor.SessionID]int
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
	r.requested[current.SessionID] = current.Generation
	r.mu.Unlock()
	return r.Request(ctx, SendInput{SessionID: current.SessionID, Kind: supervisor.SendKindRemoteControl})
}

func (r *InitialRemoteControl) EndSession(id supervisor.SessionID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.requested, id)
	delete(r.started, id)
}
