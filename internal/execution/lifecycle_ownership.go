package execution

import (
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// LifecycleOwnershipRegistry serializes the in-memory lifecycle owner for a task.
type LifecycleOwnershipRegistry interface {
	Acquire(taskID domain.TaskID) (generation domain.LifecycleGeneration, release func(), acquired bool)
	Current(taskID domain.TaskID) (generation domain.LifecycleGeneration, owned bool)
	WithCurrent(taskID domain.TaskID, generation domain.LifecycleGeneration, action func() error) (executed bool, err error)
	IsOwned(taskID domain.TaskID) bool
}

type lifecycleOwnershipRegistry struct {
	mu         sync.RWMutex
	owners     map[domain.TaskID]domain.LifecycleGeneration
	generation domain.LifecycleGeneration
}

func NewLifecycleOwnershipRegistry() LifecycleOwnershipRegistry {
	return &lifecycleOwnershipRegistry{owners: make(map[domain.TaskID]domain.LifecycleGeneration)}
}

func (r *lifecycleOwnershipRegistry) Acquire(taskID domain.TaskID) (domain.LifecycleGeneration, func(), bool) {
	r.mu.Lock()
	if _, exists := r.owners[taskID]; exists {
		r.mu.Unlock()
		return 0, func() {}, false
	}
	r.generation++
	generation := r.generation
	r.owners[taskID] = generation
	r.mu.Unlock()

	var once sync.Once
	return generation, func() {
		once.Do(func() {
			r.mu.Lock()
			if r.owners[taskID] == generation {
				delete(r.owners, taskID)
			}
			r.mu.Unlock()
		})
	}, true
}

func (r *lifecycleOwnershipRegistry) Current(taskID domain.TaskID) (domain.LifecycleGeneration, bool) {
	r.mu.RLock()
	generation, owned := r.owners[taskID]
	r.mu.RUnlock()
	return generation, owned
}

func (r *lifecycleOwnershipRegistry) WithCurrent(taskID domain.TaskID, generation domain.LifecycleGeneration, action func() error) (bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	current, owned := r.owners[taskID]
	if !owned || current != generation {
		return false, nil
	}
	err := action()
	return true, err
}

func (r *lifecycleOwnershipRegistry) IsOwned(taskID domain.TaskID) bool {
	r.mu.RLock()
	_, owned := r.owners[taskID]
	r.mu.RUnlock()
	return owned
}
