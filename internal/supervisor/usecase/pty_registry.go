package usecase

import (
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// PTYRegistry binds the current child PTY to its session. Remove checks identity
// so an old observer cannot remove a newly respawned child.
type PTYRegistry struct {
	mu      sync.RWMutex
	writers map[supervisor.SessionID]PTYWriter
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
