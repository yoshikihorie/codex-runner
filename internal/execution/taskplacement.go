package execution

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

const taskPlacementCleanupWarningDetailLimit = 100

type TaskPlacementStore interface {
	Plan(context.Context, store.TaskPlacementPolicy) (*store.TaskPlacementPlan, error)
	Execute(context.Context, *store.TaskPlacementPlan) ([]domain.TaskID, error)
}

type EvictedTaskIndex interface{ ForgetEvicted(domain.TaskID) }

type TaskPlacementEvictionPolicy struct {
	RetentionDays int
	DiskBudgetMB  int
	Now           func() time.Time
}
type TaskPlacementPlanResult struct{ Plan *store.TaskPlacementPlan }

// EvictTaskPlacementUseCase coordinates a non-mutating plan with FD-relative execution.
type EvictTaskPlacementUseCase struct {
	store  TaskPlacementStore
	index  EvictedTaskIndex
	policy TaskPlacementEvictionPolicy
	logger *slog.Logger
}

func NewEvictTaskPlacementUseCase(placement TaskPlacementStore, index EvictedTaskIndex, policy TaskPlacementEvictionPolicy, loggers ...*slog.Logger) (*EvictTaskPlacementUseCase, error) {
	if placement == nil || index == nil || policy.RetentionDays < 1 || policy.DiskBudgetMB < 1 || policy.Now == nil {
		return nil, fmt.Errorf("task placement eviction dependencies and policy must be set")
	}
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &EvictTaskPlacementUseCase{store: placement, index: index, policy: policy, logger: logger}, nil
}

func (u *EvictTaskPlacementUseCase) Plan(ctx context.Context) (TaskPlacementPlanResult, error) {
	plan, err := u.store.Plan(ctx, store.TaskPlacementPolicy{RetentionDays: u.policy.RetentionDays, DiskBudgetBytes: int64(u.policy.DiskBudgetMB) * 1000 * 1000, Now: u.policy.Now()})
	if err != nil {
		return TaskPlacementPlanResult{}, err
	}
	return TaskPlacementPlanResult{Plan: plan}, nil
}
func (u *EvictTaskPlacementUseCase) Execute(ctx context.Context, result TaskPlacementPlanResult) (retErr error) {
	if result.Plan == nil {
		return nil
	}
	defer func() {
		if closeErr := result.Plan.Close(); closeErr != nil {
			retErr = errors.Join(retErr, closeErr)
		}
	}()
	deleted, err := u.store.Execute(ctx, result.Plan)
	for _, id := range deleted {
		u.index.ForgetEvicted(id)
	}
	u.logCleanupFailures(err)
	return err
}

func (u *EvictTaskPlacementUseCase) logCleanupFailures(err error) {
	if err == nil {
		return
	}
	var failures []*store.TaskPlacementCleanupFailure
	collectTaskPlacementFailures(err, &failures)
	if len(failures) == 0 {
		u.logger.Warn("task placement eviction failed", "error", err)
		return
	}
	summary := make(map[string]int)
	for i, failure := range failures {
		if i < taskPlacementCleanupWarningDetailLimit {
			u.logger.Warn("TASK_PLACEMENT_CLEANUP_FAILED", "message", "error.taskPlacement.cleanupFailed", "task_id", failure.TaskID.String(), "path", failure.Path, "stage", failure.Stage, "reason", failure.Reason, "error", failure.Err)
			continue
		}
		summary[failure.Reason]++
	}
	if len(summary) > 0 {
		u.logger.Warn("TASK_PLACEMENT_CLEANUP_FAILED summary", "message", "error.taskPlacement.cleanupFailed", "reasons", summary)
	}
}

func collectTaskPlacementFailures(err error, failures *[]*store.TaskPlacementCleanupFailure) {
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			collectTaskPlacementFailures(child, failures)
		}
		return
	}
	var failure *store.TaskPlacementCleanupFailure
	if errors.As(err, &failure) {
		*failures = append(*failures, failure)
	}
}
