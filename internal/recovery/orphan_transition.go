package recovery

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// LifecycleOwnershipChecker reports whether an in-process lifecycle currently
// owns a task. Persistent reconciliation must not take over owned tasks.
type LifecycleOwnershipChecker interface {
	IsOwned(domain.TaskID) bool
}

type unownedLifecycleTasks struct{}

func (unownedLifecycleTasks) IsOwned(domain.TaskID) bool { return false }

// OrphanTransitionResult describes the synchronous part of orphan handling.
type OrphanTransitionResult struct {
	Finalized       bool
	RecoveryStarted bool
	Deferred        bool
}

// OrphanTransitionHandler is the execution-facing boundary for orphan recovery.
type OrphanTransitionHandler interface {
	Handle(context.Context, domain.TaskID, *domain.SessionRef, time.Time) OrphanTransitionResult
}

// OrphanRecoveryCoordinator owns the common post-orphan transition. It does
// not acquire recovery ownership: RecoverViaResumeUseCase owns that boundary.
type OrphanRecoveryCoordinator struct {
	tasks      AdoptionTaskStore
	reader     ContractReader
	finalizer  OrphanFinalizer
	resume     *RecoverViaResumeUseCase
	pending    *PendingReconciliationSet
	taskMu     TaskMutex
	clock      domain.Clock
	logger     *slog.Logger
	dispatcher orphanResumeDispatcher
}

func NewOrphanRecoveryCoordinator(tasks AdoptionTaskStore, reader ContractReader, finalizer OrphanFinalizer, resume *RecoverViaResumeUseCase, pending *PendingReconciliationSet, taskMu TaskMutex, clock domain.Clock, logger *slog.Logger) *OrphanRecoveryCoordinator {
	return newOrphanRecoveryCoordinator(tasks, reader, finalizer, resume, pending, taskMu, clock, logger, asynchronousOrphanResumeDispatcher{})
}

func newOrphanRecoveryCoordinator(tasks AdoptionTaskStore, reader ContractReader, finalizer OrphanFinalizer, resume *RecoverViaResumeUseCase, pending *PendingReconciliationSet, taskMu TaskMutex, clock domain.Clock, logger *slog.Logger, dispatcher orphanResumeDispatcher) *OrphanRecoveryCoordinator {
	if isNilAdoptionDependency(tasks) || isNilAdoptionDependency(reader) || isNilAdoptionDependency(finalizer) || isNilAdoptionDependency(resume) || isNilAdoptionDependency(pending) || isNilAdoptionDependency(taskMu) || isNilAdoptionDependency(clock) || isNilAdoptionDependency(dispatcher) {
		panic("orphan recovery coordinator requires non-nil dependencies")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &OrphanRecoveryCoordinator{tasks: tasks, reader: reader, finalizer: finalizer, resume: resume, pending: pending, taskMu: taskMu, clock: clock, logger: logger, dispatcher: dispatcher}
}

// Handle runs after orphan persistence and after the caller has released taskMu.
func (c *OrphanRecoveryCoordinator) Handle(ctx context.Context, taskID domain.TaskID, sessionRef *domain.SessionRef, occurredAt time.Time) OrphanTransitionResult {
	if ctx.Err() != nil {
		return OrphanTransitionResult{Deferred: true}
	}
	present, err := c.reader.ReadLastMessage(taskID)
	if err != nil {
		c.logger.Error("read last message for adopted orphan failed", "task_id", taskID.String(), "error", err)
		c.reconcileAfterFailure(taskID)
		return OrphanTransitionResult{Deferred: true}
	}
	if present {
		if err := c.finalizer.Finalize(taskID, 0, true, true, occurredAt); err != nil {
			c.logger.Error("finalize adopted orphan failed", "task_id", taskID.String(), "error", err)
			c.reconcileAfterFailure(taskID)
			return OrphanTransitionResult{Deferred: true}
		}
		c.pending.Remove(taskID)
		return OrphanTransitionResult{Finalized: true}
	}

	dispatchAt := c.clock.Now()
	c.dispatcher.dispatch(func() {
		if _, err := c.resume.Execute(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: taskID, SessionRef: sessionRef, Origin: domain.RecoveryOriginOrphan, OccurredAt: dispatchAt}); err != nil {
			if errors.Is(err, ErrRecoveryAlreadyInFlight) {
				return
			}
			c.logger.Error("resume recovery for orphaned task failed", "task_id", taskID.String(), "error", err)
			c.reconcileAfterFailure(taskID)
			return
		}
		c.pending.Remove(taskID)
	})
	return OrphanTransitionResult{RecoveryStarted: true}
}

func (c *OrphanRecoveryCoordinator) reconcileAfterFailure(taskID domain.TaskID) {
	reconcilePendingAfterFailure(context.Background(), c.tasks, c.pending, c.taskMu, c.logger, taskID, pendingFailureAfterConfirmedDeath)
}
