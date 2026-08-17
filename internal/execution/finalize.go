package execution

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
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

// PreparedFinalizeTask contains the terminal observation read before taskMu is acquired.
type PreparedFinalizeTask struct {
	in                 FinalizeTaskInput
	lastMessagePresent bool
}

// LockedFinalizeResult contains the terminal result and whether post-lock release is required.
type LockedFinalizeResult struct {
	Output            FinalizeTaskOutput
	RecordExited      bool
	TerminalPersisted bool
	MetricsInput      metrics.RecordTaskMetricsInput
}

type finalizeStalledTimeTracker interface {
	LeaveStalled(domain.TaskID, time.Time) int
	TakeTotal(domain.TaskID) int
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
	metricsRecorder recovery.MetricsRecorder
	stalledTracker  finalizeStalledTimeTracker
	logger          *slog.Logger
}

func NewFinalizeTaskUseCase(tasks store.TaskStore, contractWriter contract.ContractWriter, reader store.ContractReader, clock domain.Clock, taskMu *store.TaskMutex, slotReleaser recovery.SlotReleaser, timeoutDisarmer TimeoutDisarmer, metricsRecorder recovery.MetricsRecorder, stalledTracker finalizeStalledTimeTracker, loggers ...*slog.Logger) *FinalizeTaskUseCase {
	if isNilStatusDependency(tasks) || isNilStatusDependency(contractWriter) || isNilStatusDependency(reader) || isNilStatusDependency(clock) || isNilStatusDependency(taskMu) || isNilStatusDependency(slotReleaser) || isNilStatusDependency(timeoutDisarmer) || isNilStatusDependency(metricsRecorder) || isNilStatusDependency(stalledTracker) {
		panic("finalize task use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("finalize task use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &FinalizeTaskUseCase{tasks: tasks, contractW: contractWriter, reader: reader, clock: clock, taskMu: taskMu, slotReleaser: slotReleaser, timeoutDisarmer: timeoutDisarmer, metricsRecorder: metricsRecorder, stalledTracker: stalledTracker, logger: logger}
}

type contractWriteFailure struct {
	stage   string
	attempt string
	cause   error
}

func (f *contractWriteFailure) Error() string {
	return fmt.Sprintf("%s write failed (%s attempt): %v", f.stage, f.attempt, f.cause)
}

func (f *contractWriteFailure) Unwrap() []error {
	return []error{domain.ErrContractWriteFailed, f.cause}
}

// Prepare reads terminal artifacts before taskMu is acquired.
func (uc *FinalizeTaskUseCase) Prepare(in FinalizeTaskInput) (PreparedFinalizeTask, error) {
	present, err := uc.reader.ReadLastMessage(in.TaskID)
	if err != nil {
		return PreparedFinalizeTask{}, err
	}
	return PreparedFinalizeTask{in: in, lastMessagePresent: present}, nil
}

// Execute owns taskMu and releases the execution slot only after unlocking it.
func (uc *FinalizeTaskUseCase) Execute(ctx context.Context, in FinalizeTaskInput) (FinalizeTaskOutput, error) {
	prepared, err := uc.Prepare(in)
	if err != nil {
		return FinalizeTaskOutput{}, err
	}
	result, err := func() (LockedFinalizeResult, error) {
		uc.taskMu.Lock(in.TaskID)
		defer uc.taskMu.Unlock(in.TaskID)
		return uc.ExecuteLocked(ctx, prepared)
	}()
	if result.RecordExited {
		uc.ReleaseAfterFinalization(ctx, result, in.TaskID)
	}
	return result.Output, err
}

// ExecuteLocked performs terminal persistence while the caller holds taskMu.
func (uc *FinalizeTaskUseCase) ExecuteLocked(_ context.Context, prepared PreparedFinalizeTask) (result LockedFinalizeResult, err error) {
	in := prepared.in
	present := prepared.lastMessagePresent
	return uc.executeLocked(in, present)
}

// executeLocked performs terminal persistence while the caller holds taskMu.
func (uc *FinalizeTaskUseCase) executeLocked(in FinalizeTaskInput, present bool) (result LockedFinalizeResult, err error) {
	snapshot, err := uc.tasks.Load(in.TaskID)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			uc.logger.Warn("finalize: task not found", "task_id", in.TaskID.String(), "error", err)
		}
		return LockedFinalizeResult{}, err
	}
	task, err := snapshot.Restore()
	if err != nil {
		return LockedFinalizeResult{}, err
	}

	if in.RawExitCode < exitCodeMin || in.RawExitCode > exitCodeMax {
		uc.logger.Warn("output contract violation: exit code out of range", "task_id", in.TaskID.String(), "raw_exit_code", in.RawExitCode, "error", domain.ErrOutputContractViolation)
	}
	exitCode := domain.NewExitCode(in.RawExitCode)
	previousState := task.State()
	events, err := task.RecordExit(exitCode, present, in.Estimated, in.AdoptedAfterRestart, in.OccurredAt)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidStateTransition) {
			uc.logger.Warn("finalize: invalid state transition", "task_id", in.TaskID.String(), "error", err)
		}
		return LockedFinalizeResult{}, err
	}

	writeFail, nonRetryable, persisted := uc.writeTerminalState(in.TaskID, exitCode, task, snapshot, events, in.OccurredAt, "initial")
	if nonRetryable != nil {
		return LockedFinalizeResult{Output: FinalizeTaskOutput{ResultState: task.State(), Events: events}, RecordExited: true}, nonRetryable
	}
	if persisted {
		return uc.persistedFinalizeResult(in.TaskID, previousState, task.State(), events, in.OccurredAt, FinalizeTaskOutput{ResultState: task.State(), Events: events}), nil
	}

	uc.logStructuredContractFailure(in.TaskID, writeFail)
	retrySnapshot, loadErr := uc.tasks.Load(in.TaskID)
	if loadErr != nil {
		return LockedFinalizeResult{Output: FinalizeTaskOutput{ResultState: task.State(), Events: events, ContractWriteError: writeFail}, RecordExited: true}, fmt.Errorf("reload for failsafe: %w", errors.Join(writeFail, loadErr))
	}
	retryTask, restoreErr := retrySnapshot.Restore()
	if restoreErr != nil {
		return LockedFinalizeResult{Output: FinalizeTaskOutput{ResultState: task.State(), Events: events, ContractWriteError: writeFail}, RecordExited: true}, fmt.Errorf("restore for failsafe: %w", errors.Join(writeFail, restoreErr))
	}
	retryPreviousState := retryTask.State()
	retryEvents, recordErr := retryTask.RecordExit(exitCode, false, in.Estimated, in.AdoptedAfterRestart, in.OccurredAt)
	if recordErr != nil {
		return LockedFinalizeResult{Output: FinalizeTaskOutput{ResultState: task.State(), Events: events, ContractWriteError: writeFail}, RecordExited: true}, fmt.Errorf("record exit for failsafe: %w", errors.Join(writeFail, recordErr))
	}

	retryWriteFail, retryNonRetryable, retryPersisted := uc.writeTerminalState(in.TaskID, exitCode, retryTask, retrySnapshot, retryEvents, in.OccurredAt, "retry")
	if retryNonRetryable != nil {
		return LockedFinalizeResult{Output: FinalizeTaskOutput{ResultState: retryTask.State(), Events: retryEvents, ContractWriteError: writeFail}, RecordExited: true}, errors.Join(writeFail, retryNonRetryable)
	}
	if retryPersisted {
		return uc.persistedFinalizeResult(in.TaskID, retryPreviousState, retryTask.State(), retryEvents, in.OccurredAt, FinalizeTaskOutput{ResultState: retryTask.State(), Events: retryEvents, ContractWriteError: writeFail}), nil
	}
	if retryWriteFail != nil {
		uc.logStructuredContractFailure(in.TaskID, retryWriteFail)
		return LockedFinalizeResult{Output: FinalizeTaskOutput{ResultState: retryTask.State(), Events: retryEvents, ContractWriteError: writeFail}, RecordExited: true}, errors.Join(writeFail, retryWriteFail)
	}
	return LockedFinalizeResult{Output: FinalizeTaskOutput{ResultState: retryTask.State(), Events: retryEvents, ContractWriteError: writeFail}, RecordExited: true}, nil
}

// ReleaseAfterFinalization releases resources after taskMu has been unlocked.
func (uc *FinalizeTaskUseCase) ReleaseAfterFinalization(ctx context.Context, result LockedFinalizeResult, taskID domain.TaskID) {
	if !result.RecordExited {
		return
	}
	if result.TerminalPersisted {
		uc.metricsRecorder.Execute(ctx, result.MetricsInput)
	}
	uc.timeoutDisarmer.Disarm(taskID)
	uc.slotReleaser.ReleaseAndAdvance(ctx, taskID, uc.clock.Now())
}

func (uc *FinalizeTaskUseCase) writeTerminalState(taskID domain.TaskID, exitCode domain.ExitCode, task *domain.Task, snapshot domain.TaskSnapshot, events []domain.Event, occurredAt time.Time, attempt string) (*contractWriteFailure, error, bool) {
	writeErr, fatalErr := writeExitCodeIdempotently(uc.reader, uc.contractW, taskID, exitCode)
	if fatalErr != nil {
		if existing, attempted, ok := exitCodeMismatch(fatalErr); ok {
			uc.logExitCodeMismatch(taskID, existing, attempted)
			return nil, fatalErr, false
		}
		uc.logger.Error("contract write failed: exit-code validation", "task_id", taskID.String(), "code", machineCodeContractWriteFailed, "stage", "exit-code", "error", fatalErr)
		return nil, fatalErr, false
	}
	if writeErr != nil {
		return &contractWriteFailure{stage: "exit-code", attempt: attempt, cause: writeErr}, nil, false
	}

	newSnapshot, err := snapshot.WithTask(task, occurredAt)
	if err != nil {
		return nil, fmt.Errorf("validate task snapshot: %w", err), false
	}
	if err := uc.tasks.Save(taskID, newSnapshot); err != nil {
		return &contractWriteFailure{stage: "task.json", attempt: attempt, cause: err}, nil, false
	}
	for _, event := range events {
		if err := uc.contractW.AppendEvent(taskID, event); err != nil {
			uc.logger.Warn("append event failed (retained terminal state)", "task_id", taskID.String(), "event_type", event.Type(), "error", err)
		}
	}
	return nil, nil, true
}

func (uc *FinalizeTaskUseCase) persistedFinalizeResult(taskID domain.TaskID, previousState, finalState domain.TaskState, events []domain.Event, occurredAt time.Time, output FinalizeTaskOutput) LockedFinalizeResult {
	if previousState == domain.StateStalled {
		uc.stalledTracker.LeaveStalled(taskID, occurredAt)
	}
	stalledTotal := uc.stalledTracker.TakeTotal(taskID)
	return LockedFinalizeResult{Output: output, RecordExited: true, TerminalPersisted: true, MetricsInput: metrics.RecordTaskMetricsInput{TaskID: taskID, FinalState: finalState, Estimated: taskExitedEstimated(events), OccurredAt: occurredAt, StalledTotalMs: stalledTotal}}
}

func taskExitedEstimated(events []domain.Event) bool {
	for _, event := range events {
		if exited, ok := event.(domain.TaskExited); ok {
			return exited.Estimated
		}
	}
	return false
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
