package usecase

import (
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// StartChild serializes a new child launch with BeginShutdown.
func (r *PTYRegistry) StartChild(start func() (ManagedPTY, error)) (ManagedPTY, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.shutdown {
		return nil, ErrStopActive
	}
	return start()
}

// PTYRegistry binds the current child PTY to its session. Remove checks identity
// so an old observer cannot remove a newly respawned child.
type PTYRegistry struct {
	mu       sync.RWMutex
	writers  map[supervisor.SessionID]PTYWriter
	managed  map[supervisor.SessionID]ManagedPTY
	stopping map[supervisor.SessionID]ManagedPTY
	shutdown bool
}

// RegisterManaged binds the child used by the observer and stop-session.
func (r *PTYRegistry) RegisterManaged(id supervisor.SessionID, pty ManagedPTY) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.managed == nil {
		r.managed = make(map[supervisor.SessionID]ManagedPTY)
	}
	r.managed[id] = pty
}

func (r *PTYRegistry) GetManaged(id supervisor.SessionID) ManagedPTY {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.managed[id]
}

func (r *PTYRegistry) RemoveManaged(id supervisor.SessionID, pty ManagedPTY) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.managed[id] == pty {
		delete(r.managed, id)
	}
}

// MarkStopping captures the child identity; nil marks the registration window.
func (r *PTYRegistry) MarkStopping(id supervisor.SessionID) ManagedPTY {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopping == nil {
		r.stopping = make(map[supervisor.SessionID]ManagedPTY)
	}
	r.stopping[id] = r.managed[id]
	return r.managed[id]
}

func (r *PTYRegistry) IsStopping(id supervisor.SessionID, pty ManagedPTY) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	marked, ok := r.stopping[id]
	return ok && (marked == nil || marked == pty)
}

func (r *PTYRegistry) ClearStopping(id supervisor.SessionID, pty ManagedPTY) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if marked, ok := r.stopping[id]; ok && marked == pty {
		delete(r.stopping, id)
	}
}

func (r *PTYRegistry) BeginShutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.shutdown = true
}

func (r *PTYRegistry) IsShuttingDown() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.shutdown
}

func (r *PTYRegistry) Managed() []ManagedPTY {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]ManagedPTY, 0, len(r.managed))
	for _, pty := range r.managed {
		result = append(result, pty)
	}
	return result
}

func (r *PTYRegistry) Register(id supervisor.SessionID, writer PTYWriter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writers == nil {
		r.writers = make(map[supervisor.SessionID]PTYWriter)
	}
	r.writers[id] = writer
}

func (r *PTYRegistry) Get(id supervisor.SessionID) PTYWriter {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.writers[id]
}

func (r *PTYRegistry) Remove(id supervisor.SessionID, writer PTYWriter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.writers[id] == writer {
		delete(r.writers, id)
	}
}
