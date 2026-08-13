package usecase

import (
	"context"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

// AdvanceQueueUseCase releases a completed reservation and selects the next queued task.
type AdvanceQueueUseCase struct {
	queue              execution.TaskQueue
	registry           execution.ActiveTaskRegistry
	launching          execution.LaunchingTaskRegistry
	queueMu            *sync.Mutex
	maxConcurrentTasks int
}

func NewAdvanceQueueUseCase(queue execution.TaskQueue, registry execution.ActiveTaskRegistry, launching execution.LaunchingTaskRegistry, queueMu *sync.Mutex, maxConcurrentTasks int) *AdvanceQueueUseCase {
	if queue == nil || registry == nil || launching == nil || queueMu == nil {
		panic("advance queue use case requires non-nil dependencies")
	}
	return &AdvanceQueueUseCase{queue: queue, registry: registry, launching: launching, queueMu: queueMu, maxConcurrentTasks: maxConcurrentTasks}
}

func (u *AdvanceQueueUseCase) Execute(_ context.Context, taskID domain.TaskID, now time.Time) (execution.TaskLaunchPayload, bool, error) {
	u.queueMu.Lock()
	defer u.queueMu.Unlock()
	u.registry.Remove(taskID)
	payload, found := u.queue.Dequeue()
	if !found {
		return execution.TaskLaunchPayload{}, false, nil
	}
	if payload.Task.State() != domain.StateQueued {
		// The invalid payload has been removed; specification requires no Reindex here.
		return execution.TaskLaunchPayload{}, false, domain.ErrInvalidStateTransition
	}
	if u.registry.Size() >= u.maxConcurrentTasks {
		u.queue.Prepend(payload)
		return execution.TaskLaunchPayload{}, false, nil
	}
	snapshot, err := domain.NewTaskSnapshotFromAdmission(payload.Task, payload.ResolvedTimeout, payload.Model, payload.ReasoningEffort, domain.ExecutionRouteDaemon, payload.Task.RequestedAt())
	if err != nil {
		u.queue.Prepend(payload)
		return execution.TaskLaunchPayload{}, false, err
	}
	u.registry.Add(payload.Task.ID())
	u.launching.Register(payload.Task.ID(), snapshot)
	u.queue.Reindex(now)
	return payload, true, nil
}
