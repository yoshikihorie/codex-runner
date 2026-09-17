package execution

import (
	"context"
	"errors"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/store"
)

var taskPlacementEvictionTickerFactory logTickerFactory = realLogTickerFactory{}

// Run logs one non-mutating plan on startup, then performs periodic eviction.
func (u *EvictTaskPlacementUseCase) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	if result, err := u.Plan(ctx); err != nil {
		u.logger.Warn("task placement eviction startup plan failed", "error", err)
	} else if result.Plan != nil {
		u.logger.Info("task placement eviction startup plan", "candidates", len(result.Plan.Candidates), "bytes", result.Plan.CurrentBytes)
		u.logCleanupFailures(errors.Join(taskPlacementPlanFailures(result.Plan)...))
		_ = result.Plan.Close()
	}
	ticker := taskPlacementEvictionTickerFactory.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			result, err := u.Plan(ctx)
			if err != nil {
				u.logger.Warn("task placement eviction plan failed", "error", err)
				continue
			}
			_ = u.Execute(ctx, result)
		}
	}
}

func taskPlacementPlanFailures(plan *store.TaskPlacementPlan) []error {
	if plan == nil {
		return nil
	}
	errs := make([]error, len(plan.Failures))
	for i, failure := range plan.Failures {
		errs[i] = failure
	}
	return errs
}
