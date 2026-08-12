package usecase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

// PathLockReleaser is the minimal boundary used after an impl launch fails.
type PathLockReleaser interface {
	Release(context.Context, domain.TaskID) error
}

type releasePathLockAdapter struct {
	useCase *execution.ReleasePathLockUseCase
}

func NewPathLockReleaser(useCase *execution.ReleasePathLockUseCase) PathLockReleaser {
	if useCase == nil {
		panic("path lock releaser requires non-nil use case")
	}
	return releasePathLockAdapter{useCase: useCase}
}
func (a releasePathLockAdapter) Release(ctx context.Context, taskID domain.TaskID) error {
	return a.useCase.Execute(ctx, execution.ReleasePathLockInput{TaskID: taskID})
}

type FailTaskLaunchInput struct {
	Task            *domain.Task
	ResolvedTimeout domain.Timeout
	Model           string
	ReasoningEffort *string
	OccurredAt      time.Time
}

type FailTaskLaunchUseCase struct {
	tasks     store.TaskStore
	taskMu    taskLocker
	contract  contract.ContractWriter
	reader    store.ContractReader
	slots     recovery.SlotReleaser
	pathLocks PathLockReleaser
	clock     domain.Clock
	logger    *slog.Logger
}

// FailTaskLaunchLockedResult reports whether the task reached a terminal state
// while ExecuteLocked held the caller-owned task mutex.
type FailTaskLaunchLockedResult struct {
	Terminal bool
	Impl     bool
}

func NewFailTaskLaunchUseCase(tasks store.TaskStore, taskMu taskLocker, contractWriter contract.ContractWriter, reader store.ContractReader, slots recovery.SlotReleaser, pathLocks PathLockReleaser, clock domain.Clock, loggers ...*slog.Logger) *FailTaskLaunchUseCase {
	if isNilValue(tasks) || isNilValue(taskMu) || isNilValue(contractWriter) || isNilValue(reader) || isNilValue(slots) || isNilValue(pathLocks) || isNilValue(clock) {
		panic("fail task launch use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("fail task launch use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &FailTaskLaunchUseCase{tasks: tasks, taskMu: taskMu, contract: contractWriter, reader: reader, slots: slots, pathLocks: pathLocks, clock: clock, logger: logger}
}

func (uc *FailTaskLaunchUseCase) Execute(ctx context.Context, in FailTaskLaunchInput) error {
	if in.Task == nil || in.OccurredAt.IsZero() || in.ResolvedTimeout.ResolvedSeconds() <= 0 || in.Model == "" {
		return errors.New("fail task launch requires task, timeout, model, and occurred at")
	}
	taskID := in.Task.ID()
	uc.taskMu.Lock(taskID)
	result, err := uc.ExecuteLocked(ctx, in)
	uc.taskMu.Unlock(taskID)
	if result.Terminal {
		uc.ReleaseAfterFailure(ctx, taskID, result.Impl)
	}
	return err
}

// ExecuteLocked performs launch-failure state transition and persistence while
// the caller holds the task mutex. It never releases path locks or slots.
func (uc *FailTaskLaunchUseCase) ExecuteLocked(_ context.Context, in FailTaskLaunchInput) (FailTaskLaunchLockedResult, error) {
	if in.Task == nil || in.OccurredAt.IsZero() || in.ResolvedTimeout.ResolvedSeconds() <= 0 || in.Model == "" {
		return FailTaskLaunchLockedResult{}, errors.New("fail task launch requires task, timeout, model, and occurred at")
	}
	taskID := in.Task.ID()
	task := in.Task
	snapshot, err := uc.tasks.Load(taskID)
	if err == nil {
		task, err = snapshot.Restore()
		if err != nil {
			return FailTaskLaunchLockedResult{}, err
		}
	} else if !errors.Is(err, domain.ErrTaskNotFound) {
		return FailTaskLaunchLockedResult{}, err
	}
	if task.State() == domain.StateQueued {
		if _, err := task.Start(in.ResolvedTimeout, in.Model, in.OccurredAt); err != nil {
			return FailTaskLaunchLockedResult{}, err
		}
	}
	events, err := task.RecordExit(domain.NewExitCode(1), false, true, false, in.OccurredAt)
	if err != nil {
		return FailTaskLaunchLockedResult{}, err
	}
	// The domain transition is terminal at this point. Persistence failures below
	// must still cause the caller to release the execution resources.
	result := FailTaskLaunchLockedResult{Terminal: true, Impl: task.Subcommand() == domain.SubcommandImpl}
	writeErr, fatalErr := execution.WriteExitCodeIdempotently(uc.reader, uc.contract, taskID, domain.NewExitCode(1))
	if fatalErr != nil {
		return result, fatalErr
	}
	if writeErr != nil {
		return result, fmt.Errorf("%w: exit-code: %v", domain.ErrContractWriteFailed, writeErr)
	}
	if snapshot.TaskID.String() == "" {
		snapshot = domain.NewInitialTaskSnapshot(domain.ExecutionRouteDaemon, in.ReasoningEffort)
	}
	updated, err := snapshot.WithTask(task, in.OccurredAt)
	if err != nil {
		return result, err
	}
	if err := uc.tasks.Save(taskID, updated); err != nil {
		return result, err
	}
	for _, event := range events {
		if err := uc.contract.AppendEvent(taskID, event); err != nil {
			uc.logger.Warn("append launch failure event failed (retained state)", "task_id", taskID.String(), "event_type", event.Type(), "error", err)
		}
	}
	return result, nil
}

// ReleaseAfterFailure releases resources after a terminal launch failure. The
// caller must invoke it only after releasing the task mutex.
func (uc *FailTaskLaunchUseCase) ReleaseAfterFailure(ctx context.Context, taskID domain.TaskID, impl bool) {
	if impl {
		if err := uc.pathLocks.Release(ctx, taskID); err != nil {
			uc.logger.Error("release path lock after launch failure", "task_id", taskID.String(), "error", err)
		}
	}
	uc.slots.ReleaseAndAdvance(ctx, taskID, uc.clock.Now())
}
