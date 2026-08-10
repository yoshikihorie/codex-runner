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

// RecordTaskProcessUseCase records the launched process and its TaskStarted event.
type RecordTaskProcessUseCase struct {
	tasks     store.TaskStore
	contractW contract.ContractWriter
	logger    *slog.Logger
}

func NewRecordTaskProcessUseCase(tasks store.TaskStore, contractWriter contract.ContractWriter, loggers ...*slog.Logger) *RecordTaskProcessUseCase {
	if tasks == nil || contractWriter == nil {
		panic("record task process use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("record task process use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &RecordTaskProcessUseCase{tasks: tasks, contractW: contractWriter, logger: logger}
}

func (u *RecordTaskProcessUseCase) Execute(_ context.Context, task *domain.Task, handle *domain.ProcessHandle, now time.Time) error {
	if err := validateRecordTaskProcessInput(task, handle, now); err != nil {
		return err
	}
	snapshot, err := u.tasks.Load(task.ID())
	if err != nil {
		return err
	}
	restored, err := snapshot.Restore()
	if err != nil {
		return err
	}
	events, err := restored.RecordProcessInfo(handle.PID, handle.ProcessStartedAt, now)
	if err != nil {
		return err
	}
	newSnapshot, err := snapshot.WithTask(restored, now)
	if err != nil {
		return err
	}
	if err := u.tasks.Save(task.ID(), newSnapshot); err != nil {
		u.logger.Error("contract write failed", "task_id", task.ID().String(), "code", "CONTRACT_WRITE_FAILED", "stage", "task.json", "error", err)
		return wrapTaskStoreSaveError(err)
	}
	for _, event := range events {
		if err := u.contractW.AppendEvent(task.ID(), event); err != nil {
			u.logger.Warn("append event failed (retained terminal state)", "task_id", task.ID().String(), "event_type", event.Type(), "error", err)
		}
	}
	return nil
}

func validateRecordTaskProcessInput(task *domain.Task, handle *domain.ProcessHandle, now time.Time) error {
	switch {
	case task == nil:
		return fmt.Errorf("record task process task is required")
	case handle == nil:
		return fmt.Errorf("record task process handle is required")
	case now.IsZero():
		return fmt.Errorf("record task process now is required")
	default:
		return nil
	}
}
