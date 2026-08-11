package execution

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

const (
	// Canonical source: FD-exec-05 §16.
	exitCodeMin = 0
	exitCodeMax = 255

	// Canonical source: shared error-codes.md CONTRACT_WRITE_FAILED.
	machineCodeContractWriteFailed = "CONTRACT_WRITE_FAILED"
)

// FinalizeTaskInput contains the child-process exit observation to persist.
type FinalizeTaskInput struct {
	TaskID              domain.TaskID
	RawExitCode         int
	Estimated           bool
	AdoptedAfterRestart bool
	OccurredAt          time.Time
}

// FinalizeTaskOutput contains the terminal domain result and write-failure detail.
type FinalizeTaskOutput struct {
	ResultState        domain.TaskState
	Events             []domain.Event
	ContractWriteError error
}

// FinalizeTaskUseCase validates terminal artifacts and records a task's final state.
type FinalizeTaskUseCase struct {
	tasks           store.TaskStore
	contractW       contract.ContractWriter
	reader          store.ContractReader
	clock           domain.Clock
	taskMu          *store.TaskMutex
	slotReleaser    recovery.SlotReleaser
	timeoutDisarmer TimeoutDisarmer
	logger          *slog.Logger
}

func NewFinalizeTaskUseCase(tasks store.TaskStore, contractWriter contract.ContractWriter, reader store.ContractReader, clock domain.Clock, taskMu *store.TaskMutex, slotReleaser recovery.SlotReleaser, timeoutDisarmer TimeoutDisarmer, loggers ...*slog.Logger) *FinalizeTaskUseCase {
	if tasks == nil || contractWriter == nil || reader == nil || clock == nil || taskMu == nil || slotReleaser == nil || timeoutDisarmer == nil {
		panic("finalize task use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("finalize task use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &FinalizeTaskUseCase{tasks: tasks, contractW: contractWriter, reader: reader, clock: clock, taskMu: taskMu, slotReleaser: slotReleaser, timeoutDisarmer: timeoutDisarmer, logger: logger}
}

type contractWriteFailure struct {
	stage   string
	attempt string
	cause   error
}

func (f *contractWriteFailure) Error() string {
	return fmt.Sprintf("%s write failed (%s attempt): %v", f.stage, f.attempt, f.cause)
}

func (f *contractWriteFailure) Unwrap() error { return domain.ErrContractWriteFailed }

// Execute owns taskMu and releases the execution slot only after unlocking it.
func (uc *FinalizeTaskUseCase) Execute(ctx context.Context, in FinalizeTaskInput) (FinalizeTaskOutput, error) {
	present, err := uc.reader.ReadLastMessage(in.TaskID)
	if err != nil {
		return FinalizeTaskOutput{}, err
	}

	uc.taskMu.Lock(in.TaskID)
	var recordExited bool
	defer func() {
		uc.taskMu.Unlock(in.TaskID)
		if recordExited {
			uc.timeoutDisarmer.Disarm(in.TaskID)
			uc.slotReleaser.ReleaseAndAdvance(ctx, in.TaskID, uc.clock.Now())
		}
	}()

	output, recordExited, err := uc.executeLocked(ctx, in, present)
	return output, err
}

// executeLocked performs terminal persistence while the caller holds taskMu.
func (uc *FinalizeTaskUseCase) executeLocked(_ context.Context, in FinalizeTaskInput, present bool) (output FinalizeTaskOutput, recordExited bool, err error) {
	snapshot, err := uc.tasks.Load(in.TaskID)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			uc.logger.Warn("finalize: task not found", "task_id", in.TaskID.String(), "error", err)
		}
		return FinalizeTaskOutput{}, false, err
	}
	task, err := snapshot.Restore()
	if err != nil {
		return FinalizeTaskOutput{}, false, err
	}

	if in.RawExitCode < exitCodeMin || in.RawExitCode > exitCodeMax {
		uc.logger.Warn("output contract violation: exit code out of range", "task_id", in.TaskID.String(), "raw_exit_code", in.RawExitCode, "error", domain.ErrOutputContractViolation)
	}
	exitCode := domain.NewExitCode(in.RawExitCode)
	events, err := task.RecordExit(exitCode, present, in.Estimated, in.AdoptedAfterRestart, in.OccurredAt)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidStateTransition) {
			uc.logger.Warn("finalize: invalid state transition", "task_id", in.TaskID.String(), "error", err)
		}
		return FinalizeTaskOutput{}, false, err
	}

	writeFail, nonRetryable := uc.writeTerminalState(in.TaskID, exitCode, task, snapshot, events, in.OccurredAt, "initial")
	if nonRetryable != nil {
		return FinalizeTaskOutput{ResultState: task.State(), Events: events}, true, nonRetryable
	}
	if writeFail == nil {
		return FinalizeTaskOutput{ResultState: task.State(), Events: events}, true, nil
	}

	uc.logStructuredContractFailure(in.TaskID, writeFail)
	retrySnapshot, loadErr := uc.tasks.Load(in.TaskID)
	if loadErr != nil {
		return FinalizeTaskOutput{ResultState: task.State(), Events: events, ContractWriteError: writeFail}, true, fmt.Errorf("reload for failsafe: %w", loadErr)
	}
	retryTask, restoreErr := retrySnapshot.Restore()
	if restoreErr != nil {
		return FinalizeTaskOutput{ResultState: task.State(), Events: events, ContractWriteError: writeFail}, true, fmt.Errorf("restore for failsafe: %w", restoreErr)
	}
	retryEvents, recordErr := retryTask.RecordExit(exitCode, false, in.Estimated, in.AdoptedAfterRestart, in.OccurredAt)
	if recordErr != nil {
		return FinalizeTaskOutput{ResultState: task.State(), Events: events, ContractWriteError: writeFail}, true, fmt.Errorf("record exit for failsafe: %w", recordErr)
	}

	retryWriteFail, retryNonRetryable := uc.writeTerminalState(in.TaskID, exitCode, retryTask, retrySnapshot, retryEvents, in.OccurredAt, "retry")
	if retryNonRetryable != nil {
		return FinalizeTaskOutput{ResultState: retryTask.State(), Events: retryEvents, ContractWriteError: writeFail}, true, retryNonRetryable
	}
	if retryWriteFail != nil {
		uc.logStructuredContractFailure(in.TaskID, retryWriteFail)
		return FinalizeTaskOutput{ResultState: retryTask.State(), Events: retryEvents, ContractWriteError: writeFail}, true, retryWriteFail
	}
	return FinalizeTaskOutput{ResultState: retryTask.State(), Events: retryEvents, ContractWriteError: writeFail}, true, nil
}

func (uc *FinalizeTaskUseCase) writeTerminalState(taskID domain.TaskID, exitCode domain.ExitCode, task *domain.Task, snapshot domain.TaskSnapshot, events []domain.Event, occurredAt time.Time, attempt string) (*contractWriteFailure, error) {
	writeErr, fatalErr := writeExitCodeIdempotently(uc.reader, uc.contractW, taskID, exitCode)
	if fatalErr != nil {
		if existing, attempted, ok := exitCodeMismatch(fatalErr); ok {
			uc.logExitCodeMismatch(taskID, existing, attempted)
			return nil, fatalErr
		}
		uc.logger.Error("contract write failed: exit-code validation", "task_id", taskID.String(), "code", machineCodeContractWriteFailed, "stage", "exit-code", "error", fatalErr)
		return nil, fatalErr
	}
	if writeErr != nil {
		return &contractWriteFailure{stage: "exit-code", attempt: attempt, cause: writeErr}, nil
	}

	newSnapshot, err := snapshot.WithTask(task, occurredAt)
	if err != nil {
		return nil, fmt.Errorf("validate task snapshot: %w", err)
	}
	if err := uc.tasks.Save(taskID, newSnapshot); err != nil {
		return &contractWriteFailure{stage: "task.json", attempt: attempt, cause: err}, nil
	}
	for _, event := range events {
		if err := uc.contractW.AppendEvent(taskID, event); err != nil {
			uc.logger.Warn("append event failed (retained terminal state)", "task_id", taskID.String(), "event_type", event.Type(), "error", err)
		}
	}
	return nil, nil
}

func (uc *FinalizeTaskUseCase) logStructuredContractFailure(taskID domain.TaskID, failure *contractWriteFailure) {
	uc.logger.Error("contract write failed", "task_id", taskID.String(), "code", machineCodeContractWriteFailed, "stage", failure.stage, "attempt", failure.attempt, "error", failure.cause)
}

func (uc *FinalizeTaskUseCase) logExitCodeMismatch(taskID domain.TaskID, existing, attempted int) {
	uc.logger.Error("contract write failed: exit-code mismatch (fail-closed, not retried)", "task_id", taskID.String(), "code", machineCodeContractWriteFailed, "stage", "exit-code-mismatch", "existing_exit_code", existing, "attempted_exit_code", attempted)
}

// Finalize adapts Execute to the recovery orphan-finalizer boundary.
func (uc *FinalizeTaskUseCase) Finalize(taskID domain.TaskID, rawExitCode int, estimated bool, adoptedAfterRestart bool, occurredAt time.Time) error {
	_, err := uc.Execute(context.Background(), FinalizeTaskInput{TaskID: taskID, RawExitCode: rawExitCode, Estimated: estimated, AdoptedAfterRestart: adoptedAfterRestart, OccurredAt: occurredAt})
	return err
}
