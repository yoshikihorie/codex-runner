package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type ConfirmTaskRunningOutput struct {
	Dead  bool
	State domain.TaskState
}

type ConfirmTaskRunningUseCase struct {
	tasks    store.TaskStore
	taskMu   taskLocker
	liveness *execution.CheckLivenessUseCase
	contract contract.ContractWriter
	logger   *slog.Logger
}

func NewConfirmTaskRunningUseCase(tasks store.TaskStore, taskMu taskLocker, liveness *execution.CheckLivenessUseCase, contractWriter contract.ContractWriter, loggers ...*slog.Logger) *ConfirmTaskRunningUseCase {
	if isNilValue(tasks) || isNilValue(taskMu) || isNilValue(liveness) || isNilValue(contractWriter) {
		panic("confirm task running use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("confirm task running use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &ConfirmTaskRunningUseCase{tasks: tasks, taskMu: taskMu, liveness: liveness, contract: contractWriter, logger: logger}
}

func (uc *ConfirmTaskRunningUseCase) Execute(ctx context.Context, taskID domain.TaskID, occurredAt time.Time) (ConfirmTaskRunningOutput, error) {
	if occurredAt.IsZero() {
		return ConfirmTaskRunningOutput{}, fmt.Errorf("confirm task running occurred at is required")
	}
	uc.taskMu.Lock(taskID)
	defer uc.taskMu.Unlock(taskID)
	snapshot, err := uc.tasks.Load(taskID)
	if err != nil {
		return ConfirmTaskRunningOutput{}, err
	}
	task, err := snapshot.Restore()
	if err != nil {
		return ConfirmTaskRunningOutput{}, err
	}
	if task.State() == domain.StateCancelling {
		return ConfirmTaskRunningOutput{State: domain.StateCancelling}, nil
	}
	dead, err := uc.liveness.Execute(ctx, taskID)
	if err != nil {
		return ConfirmTaskRunningOutput{}, err
	}
	var events []domain.Event
	if dead {
		events, err = task.DetectOrphan("starting", occurredAt)
	} else {
		err = task.ConfirmRunning(occurredAt)
	}
	if err != nil {
		return ConfirmTaskRunningOutput{}, err
	}
	updated, err := snapshot.WithTask(task, occurredAt)
	if err != nil {
		return ConfirmTaskRunningOutput{}, err
	}
	if err := uc.tasks.Save(taskID, updated); err != nil {
		return ConfirmTaskRunningOutput{}, err
	}
	for _, event := range events {
		if err := uc.contract.AppendEvent(taskID, event); err != nil {
			uc.logger.Warn("append orphan event failed (retained state)", "task_id", taskID.String(), "event_type", event.Type(), "error", err)
		}
	}
	return ConfirmTaskRunningOutput{Dead: dead, State: task.State()}, nil
}
