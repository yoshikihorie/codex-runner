package usecase

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

// RecordTaskStartingUseCase records the pre-launch task state and prompt.
type RecordTaskStartingUseCase struct {
	tasks     store.TaskStore
	contractW contract.ContractWriter
	logger    *slog.Logger
}

func NewRecordTaskStartingUseCase(tasks store.TaskStore, contractWriter contract.ContractWriter, loggers ...*slog.Logger) *RecordTaskStartingUseCase {
	if tasks == nil || contractWriter == nil {
		panic("record task starting use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("record task starting use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &RecordTaskStartingUseCase{tasks: tasks, contractW: contractWriter, logger: logger}
}

func (u *RecordTaskStartingUseCase) Execute(_ context.Context, task *domain.Task, resolvedTimeout domain.Timeout, model string, reasoningEffort *string, route domain.ExecutionRoute, promptText string, now time.Time) error {
	if err := validateRecordTaskStartingInput(task, resolvedTimeout, model, route, promptText, now); err != nil {
		return err
	}
	if _, err := task.Start(resolvedTimeout, model, now); err != nil {
		return err
	}
	if err := u.contractW.WritePrompt(task.ID(), []byte(promptText)); err != nil {
		u.logger.Error("contract write failed", "task_id", task.ID().String(), "code", "CONTRACT_WRITE_FAILED", "stage", "prompt", "error", err)
		return err
	}
	snapshot, err := domain.NewInitialTaskSnapshot(route, reasoningEffort).WithTask(task, now)
	if err != nil {
		return err
	}
	if err := u.tasks.Save(task.ID(), snapshot); err != nil {
		u.logger.Error("contract write failed", "task_id", task.ID().String(), "code", "CONTRACT_WRITE_FAILED", "stage", "task.json", "error", err)
		return wrapTaskStoreSaveError(err)
	}
	return nil
}

func validateRecordTaskStartingInput(task *domain.Task, resolvedTimeout domain.Timeout, model string, route domain.ExecutionRoute, promptText string, now time.Time) error {
	switch {
	case task == nil:
		return fmt.Errorf("record task starting task is required")
	case resolvedTimeout.ResolvedSeconds() <= 0:
		return fmt.Errorf("record task starting resolved timeout is required")
	case model == "":
		return fmt.Errorf("record task starting model is required")
	case promptText == "":
		return fmt.Errorf("record task starting prompt text is required")
	case route != domain.ExecutionRouteDaemon:
		return fmt.Errorf("record task starting route must be daemon")
	case now.IsZero():
		return fmt.Errorf("record task starting now is required")
	default:
		return nil
	}
}

func wrapTaskStoreSaveError(err error) error {
	return fmt.Errorf("%w: %v", domain.ErrContractWriteFailed, err)
}
