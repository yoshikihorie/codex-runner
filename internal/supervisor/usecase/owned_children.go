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

// OwnedChildPIDs returns a detached snapshot for server-side peer checks.
func OwnedChildPIDs() map[int]bool {
	ownedChildren.mu.Lock()
	defer ownedChildren.mu.Unlock()
	result := make(map[int]bool, len(ownedChildren.pids))
	for _, pid := range ownedChildren.pids {
		if pid > 0 {
			result[pid] = true
		}
	}
	return result
}

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
