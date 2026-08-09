package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

type taskLifecycleStarter struct {
	runner    taskLifecycleRunner
	tasksRoot string
	baseCtx   context.Context
	clock     domain.Clock
	logger    *slog.Logger
}

func NewTaskLifecycleStarter(runner taskLifecycleRunner, tasksRoot string, baseCtx context.Context, clock domain.Clock, loggers ...*slog.Logger) (*taskLifecycleStarter, error) {
	if runner == nil || baseCtx == nil || clock == nil {
		return nil, fmt.Errorf("task lifecycle starter requires non-nil dependencies")
	}
	if tasksRoot == "" || !filepath.IsAbs(tasksRoot) {
		return nil, fmt.Errorf("task lifecycle starter tasks root must be an absolute path")
	}
	if len(loggers) > 1 {
		return nil, fmt.Errorf("task lifecycle starter accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &taskLifecycleStarter{runner: runner, tasksRoot: tasksRoot, baseCtx: baseCtx, clock: clock, logger: logger}, nil
}

func (s *taskLifecycleStarter) Start(payload execution.TaskLaunchPayload) {
	ctx, cancel := context.WithCancel(s.baseCtx)
	input := TaskLifecycleInput{TaskLaunchPayload: payload, TaskDirPath: filepath.Join(s.tasksRoot, payload.Task.ID().String()), Now: s.clock.Now()}
	go func() { defer cancel(); s.runner.Run(ctx, input) }()
}

var _ execution.TaskLifecycleStarter = (*taskLifecycleStarter)(nil)
