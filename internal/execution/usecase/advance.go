package usecase

import (
	"context"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

// AdvanceQueueUseCase releases a completed reservation and selects the next queued task.
type promotionID uint64

type AdvanceQueueUseCase struct {
	queue              execution.TaskQueue
	registry           execution.ActiveTaskRegistry
	launching          execution.LaunchingTaskRegistry
	queueMu            *sync.Mutex
	maxConcurrentTasks int
	nextPromotionID    promotionID
	promotions         map[domain.TaskID]promotionID
}

func NewAdvanceQueueUseCase(queue execution.TaskQueue, registry execution.ActiveTaskRegistry, launching execution.LaunchingTaskRegistry, queueMu *sync.Mutex, maxConcurrentTasks int) *AdvanceQueueUseCase {
	if queue == nil || registry == nil || launching == nil || queueMu == nil {
		panic("advance queue use case requires non-nil dependencies")
	}
	return &AdvanceQueueUseCase{queue: queue, registry: registry, launching: launching, queueMu: queueMu, maxConcurrentTasks: maxConcurrentTasks, promotions: make(map[domain.TaskID]promotionID)}
}

func (u *AdvanceQueueUseCase) Execute(ctx context.Context, taskID domain.TaskID, now time.Time) (execution.TaskLaunchPayload, promotionID, bool, error) {
	if err := ctx.Err(); err != nil {
		return execution.TaskLaunchPayload{}, 0, false, err
	}
	u.queueMu.Lock()
	defer u.queueMu.Unlock()
	if err := ctx.Err(); err != nil {
		return execution.TaskLaunchPayload{}, 0, false, err
	}
	u.registry.Remove(taskID)
	if err := ctx.Err(); err != nil {
		return execution.TaskLaunchPayload{}, 0, false, err
	}
	payload, found := u.queue.Dequeue()
	if !found {
		return execution.TaskLaunchPayload{}, 0, false, nil
	}
	if payload.Task.State() != domain.StateQueued {
		// The invalid payload has been removed; specification requires no Reindex here.
		return execution.TaskLaunchPayload{}, 0, false, domain.ErrInvalidStateTransition
	}
	if u.registry.Size() >= u.maxConcurrentTasks {
		u.queue.Prepend(payload)
		return execution.TaskLaunchPayload{}, 0, false, nil
	}
	if err := ctx.Err(); err != nil {
		u.queue.Prepend(payload)
		return execution.TaskLaunchPayload{}, 0, false, err
	}
	snapshot, err := domain.NewTaskSnapshotFromAdmission(payload.Task, payload.ResolvedTimeout, payload.Model, payload.ReasoningEffort, domain.ExecutionRouteDaemon, payload.Task.RequestedAt())
	if err != nil {
		u.queue.Prepend(payload)
		return execution.TaskLaunchPayload{}, 0, false, err
	}
	u.registry.Add(payload.Task.ID())
	if err := ctx.Err(); err != nil {
		u.registry.Remove(payload.Task.ID())
		u.queue.Prepend(payload)
		u.queue.Reindex(now)
		return execution.TaskLaunchPayload{}, 0, false, err
	}
	u.launching.Register(payload.Task.ID(), snapshot)
	if err := ctx.Err(); err != nil {
		u.launching.Unregister(payload.Task.ID())
		u.registry.Remove(payload.Task.ID())
		u.queue.Prepend(payload)
		u.queue.Reindex(now)
		return execution.TaskLaunchPayload{}, 0, false, err
	}
	u.nextPromotionID++
	id := u.nextPromotionID
	u.promotions[payload.Task.ID()] = id
	u.queue.Reindex(now)
	return payload, id, true, nil
}

// CompensateRejectedStart restores exactly the queue promotion that owns payload.
func (u *AdvanceQueueUseCase) CompensateRejectedStart(payload execution.TaskLaunchPayload, id promotionID, now time.Time) {
	if payload.Task == nil {
		return
	}
	u.queueMu.Lock()
	defer u.queueMu.Unlock()
	currentID, promoted := u.promotions[payload.Task.ID()]
	if !promoted || currentID != id {
		return
	}
	delete(u.promotions, payload.Task.ID())
	u.launching.Unregister(payload.Task.ID())
	u.registry.Remove(payload.Task.ID())
	u.queue.Prepend(payload)
	u.queue.Reindex(now)
}

// CommitStart discards the promotion once its lifecycle launch has been accepted.
func (u *AdvanceQueueUseCase) CommitStart(payload execution.TaskLaunchPayload, id promotionID) {
	if payload.Task == nil {
		return
	}
	u.queueMu.Lock()
	defer u.queueMu.Unlock()
	currentID, promoted := u.promotions[payload.Task.ID()]
	if promoted && currentID == id {
		delete(u.promotions, payload.Task.ID())
	}
}
