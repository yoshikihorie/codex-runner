package execution

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type ConfirmTaskKilledInput struct {
	TaskID      domain.TaskID
	RawExitCode int
	Estimated   bool
	OccurredAt  time.Time
}
type ConfirmTaskKilledOutput struct{ Events []domain.Event }
type LockedKillResult struct {
	Output     ConfirmTaskKilledOutput
	Confirmed  bool
	Subcommand domain.Subcommand
}

type ConfirmTaskKilledUseCase struct {
	tasks            store.TaskStore
	contractW        contract.ContractWriter
	reader           store.ContractReader
	taskMu           *store.TaskMutex
	timeoutDisarmer  TimeoutDisarmer
	pathLockReleaser *ReleasePathLockUseCase
	slotReleaser     recovery.SlotReleaser
	clock            domain.Clock
	logger           *slog.Logger
}

func NewConfirmTaskKilledUseCase(tasks store.TaskStore, writer contract.ContractWriter, reader store.ContractReader, taskMu *store.TaskMutex, disarmer TimeoutDisarmer, pathLocks *ReleasePathLockUseCase, slots recovery.SlotReleaser, clock domain.Clock, loggers ...*slog.Logger) *ConfirmTaskKilledUseCase {
	if tasks == nil || writer == nil || reader == nil || taskMu == nil || disarmer == nil || pathLocks == nil || slots == nil || clock == nil {
		panic("confirm killed use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("confirm killed use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &ConfirmTaskKilledUseCase{tasks: tasks, contractW: writer, reader: reader, taskMu: taskMu, timeoutDisarmer: disarmer, pathLockReleaser: pathLocks, slotReleaser: slots, clock: clock, logger: logger}
}

func (uc *ConfirmTaskKilledUseCase) Execute(ctx context.Context, in ConfirmTaskKilledInput) (ConfirmTaskKilledOutput, error) {
	uc.taskMu.Lock(in.TaskID)
	result, err := uc.ExecuteLocked(ctx, in)
	uc.taskMu.Unlock(in.TaskID)
	if result.Confirmed {
		uc.ReleaseAfterConfirmation(ctx, result, in.TaskID)
	}
	return result.Output, err
}

// ExecuteLocked must be called while taskMu is held.
func (uc *ConfirmTaskKilledUseCase) ExecuteLocked(_ context.Context, in ConfirmTaskKilledInput) (LockedKillResult, error) {
	snapshot, err := uc.tasks.Load(in.TaskID)
	if err != nil {
		return LockedKillResult{}, err
	}
	task, err := snapshot.Restore()
	if err != nil {
		return LockedKillResult{}, err
	}
	exitCode := domain.NewExitCode(in.RawExitCode)
	events, err := task.ConfirmKilled(exitCode, in.Estimated, in.OccurredAt)
	if err != nil {
		return LockedKillResult{}, err
	}
	result := LockedKillResult{Confirmed: true, Subcommand: task.Subcommand(), Output: ConfirmTaskKilledOutput{Events: events}}
	writeErr, fatalErr := writeExitCodeIdempotently(uc.reader, uc.contractW, in.TaskID, exitCode)
	if fatalErr != nil {
		return result, fatalErr
	}
	if writeErr != nil {
		return result, fmt.Errorf("%w: exit-code", domain.ErrContractWriteFailed)
	}
	updated, err := snapshot.WithTask(task, in.OccurredAt)
	if err != nil {
		return result, err
	}
	if err := uc.tasks.Save(in.TaskID, updated); err != nil {
		return result, fmt.Errorf("%w: task.json", domain.ErrContractWriteFailed)
	}
	for _, event := range events {
		if err := uc.contractW.AppendEvent(in.TaskID, event); err != nil {
			uc.logger.Warn("append killed event failed (retained terminal state)", "task_id", in.TaskID.String(), "event_type", event.Type(), "error", err)
		}
	}
	return result, nil
}

func (uc *ConfirmTaskKilledUseCase) ReleaseAfterConfirmation(ctx context.Context, result LockedKillResult, taskID domain.TaskID) {
	uc.timeoutDisarmer.Disarm(taskID)
	if result.Subcommand == domain.SubcommandImpl {
		if err := uc.pathLockReleaser.Execute(ctx, ReleasePathLockInput{TaskID: taskID}); err != nil {
			uc.logger.Error("release path lock after confirmation", "task_id", taskID.String(), "error", err)
		}
	}
	uc.slotReleaser.ReleaseAndAdvance(ctx, taskID, uc.clock.Now())
}
func (uc *ConfirmTaskKilledUseCase) ConfirmKilled(ctx context.Context, taskID domain.TaskID, rawExitCode int, estimated bool, occurredAt time.Time) error {
	_, err := uc.Execute(ctx, ConfirmTaskKilledInput{TaskID: taskID, RawExitCode: rawExitCode, Estimated: estimated, OccurredAt: occurredAt})
	return err
}
