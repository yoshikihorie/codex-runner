package execution

import (
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// PromotionRegistry tracks queued tasks removed for a lifecycle start whose
// acceptance or compensation has not yet been committed.
type PromotionRegistry struct {
	mu      sync.Mutex
	taskIDs map[domain.TaskID]struct{}
}

// NewPromotionRegistry creates an empty promotion registry.
func NewPromotionRegistry() *PromotionRegistry {
	return &PromotionRegistry{taskIDs: make(map[domain.TaskID]struct{})}
}

// Reserve records an unresolved promotion for taskID.
func (r *PromotionRegistry) Reserve(taskID domain.TaskID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.taskIDs[taskID] = struct{}{}
}

// Resolve removes taskID and reports whether its promotion was unresolved.
func (r *PromotionRegistry) Resolve(taskID domain.TaskID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, found := r.taskIDs[taskID]; !found {
		return false
	}
	delete(r.taskIDs, taskID)
	return true
}

// Len reports the number of unresolved promotions.
func (r *PromotionRegistry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.taskIDs)
}
