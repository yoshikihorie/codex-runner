package usecase

import (
	"context"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
)

type taskLifecycleRunner interface {
	Run(ctx context.Context, input TaskLifecycleInput)
}

type slotFinalizer struct {
	advance *AdvanceQueueUseCase
	starter execution.TaskLifecycleStarter
	logger  *slog.Logger
}

func NewSlotReleaser(advance *AdvanceQueueUseCase, starter execution.TaskLifecycleStarter, loggers ...*slog.Logger) recovery.SlotReleaser {
	if advance == nil || isNilValue(starter) {
		panic("slot releaser requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("slot releaser accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &slotFinalizer{advance: advance, starter: starter, logger: logger}
}

func (f *slotFinalizer) ReleaseAndAdvance(ctx context.Context, taskID domain.TaskID, now time.Time) {
	for {
		payload, found, err := f.advance.Execute(ctx, taskID, now)
		if err != nil {
			f.logger.Warn("advance queue after releasing slot", "task_id", taskID.String(), "error", err)
			return
		}
		if !found {
			return
		}
		if !f.starter.Start(payload) {
			f.advance.CompensateRejectedStart(payload, now)
			return
		}
		f.advance.CommitStart(payload)
	}
}

var _ recovery.SlotReleaser = (*slotFinalizer)(nil)
