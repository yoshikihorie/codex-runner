package recovery

import (
	"errors"
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

var ErrRecoveryAlreadyInFlight = errors.New("recovery already in flight")

// RecoveryOwnershipRegistry serializes in-memory recovery ownership per task.
type RecoveryOwnershipRegistry interface {
	Acquire(taskID domain.TaskID) (release func(), acquired bool)
	IsOwned(taskID domain.TaskID) bool
}

type recoveryOwnershipRegistry struct {
	mu         sync.RWMutex
	owners     map[domain.TaskID]uint64
	generation uint64
}

func NewRecoveryOwnershipRegistry() RecoveryOwnershipRegistry {
	return &recoveryOwnershipRegistry{owners: make(map[domain.TaskID]uint64)}
}

func (r *recoveryOwnershipRegistry) Acquire(taskID domain.TaskID) (func(), bool) {
	r.mu.Lock()
	if _, exists := r.owners[taskID]; exists {
		r.mu.Unlock()
		return func() {}, false
	}
	r.generation++
	generation := r.generation
	r.owners[taskID] = generation
	r.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			r.mu.Lock()
			if r.owners[taskID] == generation {
				delete(r.owners, taskID)
			}
			r.mu.Unlock()
		})
	}, true
}

func (r *recoveryOwnershipRegistry) IsOwned(taskID domain.TaskID) bool {
	r.mu.RLock()
	_, owned := r.owners[taskID]
	r.mu.RUnlock()
	return owned
}
