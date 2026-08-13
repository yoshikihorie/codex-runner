package execution

import (
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// LaunchingTaskRegistry keeps immutable snapshots for tasks that have reserved a slot but are not yet persisted.
type LaunchingTaskRegistry interface {
	Register(taskID domain.TaskID, snapshot domain.TaskSnapshot)
	Unregister(taskID domain.TaskID)
	Lookup(taskID domain.TaskID) (domain.TaskSnapshot, bool)
}

type launchingTaskRegistry struct {
	mu        sync.RWMutex
	snapshots map[domain.TaskID]domain.TaskSnapshot
}

func NewLaunchingTaskRegistry() LaunchingTaskRegistry {
	return &launchingTaskRegistry{snapshots: make(map[domain.TaskID]domain.TaskSnapshot)}
}

func (r *launchingTaskRegistry) Register(taskID domain.TaskID, snapshot domain.TaskSnapshot) {
	r.mu.Lock()
	r.snapshots[taskID] = copyTaskSnapshot(snapshot)
	r.mu.Unlock()
}

func (r *launchingTaskRegistry) Unregister(taskID domain.TaskID) {
	r.mu.Lock()
	delete(r.snapshots, taskID)
	r.mu.Unlock()
}

func (r *launchingTaskRegistry) Lookup(taskID domain.TaskID) (domain.TaskSnapshot, bool) {
	r.mu.RLock()
	snapshot, found := r.snapshots[taskID]
	r.mu.RUnlock()
	if !found {
		return domain.TaskSnapshot{}, false
	}
	return copyTaskSnapshot(snapshot), true
}

func copyTaskSnapshot(snapshot domain.TaskSnapshot) domain.TaskSnapshot {
	copy := snapshot
	if snapshot.PID != nil {
		value := *snapshot.PID
		copy.PID = &value
	}
	if snapshot.ProcessStartedAt != nil {
		value := *snapshot.ProcessStartedAt
		copy.ProcessStartedAt = &value
	}
	if snapshot.RequestedTimeoutSeconds != nil {
		value := *snapshot.RequestedTimeoutSeconds
		copy.RequestedTimeoutSeconds = &value
	}
	if snapshot.ReasoningEffort != nil {
		value := *snapshot.ReasoningEffort
		copy.ReasoningEffort = &value
	}
	if snapshot.SessionRef != nil {
		value := *snapshot.SessionRef
		copy.SessionRef = &value
	}
	if snapshot.LastEventAt != nil {
		value := *snapshot.LastEventAt
		copy.LastEventAt = &value
	}
	if snapshot.ExitCode != nil {
		value := *snapshot.ExitCode
		copy.ExitCode = &value
	}
	if snapshot.RecoveryOrigin != nil {
		value := *snapshot.RecoveryOrigin
		copy.RecoveryOrigin = &value
	}
	return copy
}
