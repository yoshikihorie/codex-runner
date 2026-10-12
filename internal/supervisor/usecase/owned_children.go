package usecase

import (
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// ownedChildRegistry tracks children whose PTYs belong to this supervisor process.
type ownedChildRegistry struct {
	mu   sync.Mutex
	pids map[supervisor.SessionID]int
}

var ownedChildren = &ownedChildRegistry{pids: make(map[supervisor.SessionID]int)}

func (r *ownedChildRegistry) Register(id supervisor.SessionID, pid int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pids[id] = pid
}

func (r *ownedChildRegistry) Remove(id supervisor.SessionID, pid int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pids[id] == pid {
		delete(r.pids, id)
	}
}

func (r *ownedChildRegistry) Owns(id supervisor.SessionID, pid int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return pid > 0 && r.pids[id] == pid
}
