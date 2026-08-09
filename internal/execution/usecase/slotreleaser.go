package usecase

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
)

type taskLifecycleRunner interface {
	Run(ctx context.Context, input TaskLifecycleInput)
}

type slotFinalizer struct {
	advance   *AdvanceQueueUseCase
	runner    taskLifecycleRunner
	tasksRoot string
	baseCtx   context.Context
	clock     domain.Clock
	logger    *slog.Logger
}

func NewSlotReleaser(advance *AdvanceQueueUseCase, runner taskLifecycleRunner, tasksRoot string, baseCtx context.Context, clock domain.Clock, loggers ...*slog.Logger) recovery.SlotReleaser {
	if advance == nil || runner == nil || baseCtx == nil || clock == nil {
		panic("slot releaser requires non-nil dependencies")
	}
	if tasksRoot == "" || !filepath.IsAbs(tasksRoot) {
		panic("slot releaser tasks root must be an absolute path")
	}
	if len(loggers) > 1 {
		panic("slot releaser accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &slotFinalizer{advance: advance, runner: runner, tasksRoot: tasksRoot, baseCtx: baseCtx, clock: clock, logger: logger}
}

func (f *slotFinalizer) ReleaseAndAdvance(ctx context.Context, taskID domain.TaskID, now time.Time) {
	payload, found, err := f.advance.Execute(ctx, taskID, now)
	if err != nil {
		f.logger.Warn("advance queue after releasing slot", "task_id", taskID.String(), "error", err)
		return
	}
	if !found {
		return
	}
	taskCtx, cancel := context.WithCancel(f.baseCtx)
	input := TaskLifecycleInput{TaskLaunchPayload: payload, TaskDirPath: filepath.Join(f.tasksRoot, payload.Task.ID().String()), Now: f.clock.Now()}
	go func() {
		defer cancel()
		f.runner.Run(taskCtx, input)
	}()
}

var _ recovery.SlotReleaser = (*slotFinalizer)(nil)
