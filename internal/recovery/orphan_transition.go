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

// OrphanTransitionInput identifies a persisted orphan and its provenance.
type OrphanTransitionInput struct {
	TaskID              domain.TaskID
	SessionRef          *domain.SessionRef
	AdoptedAfterRestart bool
	OccurredAt          time.Time
}

// OrphanTransitionHandler is the execution-facing boundary for orphan recovery.
type OrphanTransitionHandler interface {
	Handle(context.Context, OrphanTransitionInput) OrphanTransitionResult
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
func (c *OrphanRecoveryCoordinator) Handle(ctx context.Context, input OrphanTransitionInput) OrphanTransitionResult {
	if ctx.Err() != nil {
		return OrphanTransitionResult{Deferred: true}
	}
	present, err := c.reader.ReadLastMessage(input.TaskID)
	if err != nil {
		c.logger.Error("read last message for adopted orphan failed", "task_id", input.TaskID.String(), "error", err)
		if ctx.Err() == nil {
			c.reconcileAfterFailure(ctx, input.TaskID)
		}
		return OrphanTransitionResult{Deferred: true}
	}
	if ctx.Err() != nil {
		return OrphanTransitionResult{Deferred: true}
	}
	if present {
		if ctx.Err() != nil {
			return OrphanTransitionResult{Deferred: true}
		}
		if err := c.finalizer.Finalize(input.TaskID, 0, true, input.AdoptedAfterRestart, input.OccurredAt); err != nil {
			c.logger.Error("finalize adopted orphan failed", "task_id", input.TaskID.String(), "error", err)
			if ctx.Err() == nil {
				c.reconcileAfterFailure(ctx, input.TaskID)
			}
			return OrphanTransitionResult{Deferred: true}
		}
		if ctx.Err() != nil {
			return OrphanTransitionResult{Deferred: true}
		}
		c.pending.Remove(input.TaskID)
		return OrphanTransitionResult{Finalized: true}
	}

	dispatchAt := c.clock.Now()
	if ctx.Err() != nil {
		return OrphanTransitionResult{Deferred: true}
	}
	c.dispatcher.dispatch(func() {
		if _, err := c.resume.Execute(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: input.TaskID, SessionRef: input.SessionRef, Origin: domain.RecoveryOriginOrphan, OccurredAt: dispatchAt}); err != nil {
			if errors.Is(err, ErrRecoveryAlreadyInFlight) {
				return
			}
			c.logger.Error("resume recovery for orphaned task failed", "task_id", input.TaskID.String(), "error", err)
			if ctx.Err() == nil {
				c.reconcileAfterFailure(ctx, input.TaskID)
			}
			return
		}
		if ctx.Err() == nil {
			c.pending.Remove(input.TaskID)
		}
	})
	return OrphanTransitionResult{RecoveryStarted: true}
}

func (c *OrphanRecoveryCoordinator) reconcileAfterFailure(ctx context.Context, taskID domain.TaskID) {
	reconcilePendingAfterFailure(ctx, c.tasks, c.pending, c.taskMu, c.logger, taskID, pendingFailureAfterConfirmedDeath)
}
