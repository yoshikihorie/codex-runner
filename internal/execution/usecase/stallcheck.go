package usecase

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

const (
	// STALL_THRESHOLD_SECONDS and STALL_SCAN_INTERVAL_SECONDS: validation-rules.md.
	stallThresholdSeconds    = 1200
	stallScanIntervalSeconds = 60
)

type stallTicker interface {
	C() <-chan time.Time
	Stop()
}
type stallTickerFactory interface {
	NewTicker(time.Duration) stallTicker
}
type realStallTickerFactory struct{}
type realStallTicker struct{ *time.Ticker }

func (realStallTickerFactory) NewTicker(d time.Duration) stallTicker {
	return realStallTicker{time.NewTicker(d)}
}
func (t realStallTicker) C() <-chan time.Time { return t.Ticker.C }

type checkStallUseCase struct {
	tasks             store.TaskStore
	taskMu            taskLocker
	liveness          *execution.CheckLivenessUseCase
	contract          contract.ContractWriter
	ownership         execution.LifecycleOwnershipRegistry
	orphanCoordinator recovery.OrphanTransitionHandler
	clock             domain.Clock
	stalledTime       stalledTimeTracker
	logger            *slog.Logger
	interval          time.Duration
	tickers           stallTickerFactory
}

func NewCheckStallUseCase(tasks store.TaskStore, taskMu taskLocker, liveness *execution.CheckLivenessUseCase, contractWriter contract.ContractWriter, clock domain.Clock, stalledTime stalledTimeTracker, ownership execution.LifecycleOwnershipRegistry, orphanCoordinator recovery.OrphanTransitionHandler, loggers ...*slog.Logger) *checkStallUseCase {
	if isNilValue(orphanCoordinator) {
		panic("check stall use case requires a non-nil orphan recovery coordinator")
	}
	uc := newCheckStallUseCaseWithOwnership(tasks, taskMu, liveness, contractWriter, clock, stalledTime, time.Duration(stallScanIntervalSeconds)*time.Second, realStallTickerFactory{}, ownership, loggers...)
	uc.orphanCoordinator = orphanCoordinator
	return uc
}
func newCheckStallUseCase(tasks store.TaskStore, taskMu taskLocker, liveness *execution.CheckLivenessUseCase, contractWriter contract.ContractWriter, clock domain.Clock, stalledTime stalledTimeTracker, interval time.Duration, tickers stallTickerFactory, ownership execution.LifecycleOwnershipRegistry, loggers ...*slog.Logger) *checkStallUseCase {
	return newCheckStallUseCaseWithOwnership(tasks, taskMu, liveness, contractWriter, clock, stalledTime, interval, tickers, ownership, loggers...)
}
func newCheckStallUseCaseWithOwnership(tasks store.TaskStore, taskMu taskLocker, liveness *execution.CheckLivenessUseCase, contractWriter contract.ContractWriter, clock domain.Clock, stalledTime stalledTimeTracker, interval time.Duration, tickers stallTickerFactory, ownership execution.LifecycleOwnershipRegistry, loggers ...*slog.Logger) *checkStallUseCase {
	if isNilValue(tasks) || isNilValue(taskMu) || isNilValue(liveness) || isNilValue(contractWriter) || isNilValue(clock) || isNilValue(stalledTime) || isNilValue(tickers) || isNilValue(ownership) {
		panic("check stall use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("check stall use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	if interval <= 0 {
		panic("check stall use case requires positive interval")
	}
	return &checkStallUseCase{tasks: tasks, taskMu: taskMu, liveness: liveness, contract: contractWriter, ownership: ownership, clock: clock, stalledTime: stalledTime, logger: logger, interval: interval, tickers: tickers}
}

func (uc *checkStallUseCase) Run(ctx context.Context) {
	ticker := uc.tickers.NewTicker(uc.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C():
			if ctx.Err() != nil {
				return
			}
			uc.scan(ctx)
		}
	}
}
func (uc *checkStallUseCase) scan(ctx context.Context) {
	snapshots, err := uc.tasks.ListByStates([]domain.TaskState{domain.StateRunning, domain.StateStalled})
	if err != nil {
		uc.logStateRead(domain.TaskID{}, "list-by-states", err)
		return
	}
	for _, snapshot := range snapshots {
		if ctx.Err() != nil {
			return
		}
		uc.checkOne(ctx, snapshot.TaskID)
	}
}
func (uc *checkStallUseCase) checkOne(ctx context.Context, taskID domain.TaskID) {
	ownedBeforeLock := uc.ownership.IsOwned(taskID)
	uc.taskMu.Lock(taskID)
	ownedAfterLock := uc.ownership.IsOwned(taskID)
	snapshot, err := uc.tasks.Load(taskID)
	if err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logStateRead(taskID, "load", err)
		return
	}
	task, err := snapshot.Restore()
	if err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logStateRead(taskID, "restore", err)
		return
	}
	if task.State() != domain.StateRunning && task.State() != domain.StateStalled {
		uc.taskMu.Unlock(taskID)
		return
	}
	now := uc.clock.Now()
	dead := false
	if !ownedBeforeLock && !ownedAfterLock {
		var err error
		dead, err = uc.liveness.Execute(ctx, taskID)
		if err != nil {
			uc.taskMu.Unlock(taskID)
			uc.logLiveness(taskID, err)
			return
		}
	}
	if ctx.Err() != nil {
		uc.taskMu.Unlock(taskID)
		return
	}
	if dead && !ownedBeforeLock && !ownedAfterLock {
		persisted := uc.orphan(taskID, snapshot, task, now)
		uc.taskMu.Unlock(taskID)
		if persisted && uc.orphanCoordinator != nil {
			uc.orphanCoordinator.Handle(ctx, recovery.OrphanTransitionInput{TaskID: taskID, SessionRef: snapshot.SessionRef, AdoptedAfterRestart: snapshot.AdoptedAfterRestart, OccurredAt: now})
		}
		return
	}
	start := snapshot.LastEventAt
	if start == nil {
		start = snapshot.ProcessStartedAt
	}
	if start == nil {
		uc.taskMu.Unlock(taskID)
		return
	}
	elapsed := now.Sub(*start)
	threshold := time.Duration(stallThresholdSeconds) * time.Second
	if elapsed <= threshold {
		uc.taskMu.Unlock(taskID)
		return
	}
	gap := int(elapsed / time.Second)
	if elapsed%time.Second != 0 {
		gap++
	}
	uc.stall(taskID, snapshot, task, gap, now)
	uc.taskMu.Unlock(taskID)
}
func (uc *checkStallUseCase) orphan(taskID domain.TaskID, snapshot domain.TaskSnapshot, task *domain.Task, now time.Time) bool {
	events, err := task.DetectOrphan("running", now)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidStateTransition) {
			logInvalidTransition(uc.logger, taskID, "detect-orphan", task.State(), err)
		}
		return false
	}
	return uc.persist(taskID, snapshot, task, events, now, "detect-orphan")
}
func (uc *checkStallUseCase) stall(taskID domain.TaskID, snapshot domain.TaskSnapshot, task *domain.Task, gap int, now time.Time) {
	events, err := task.MarkStalled(gap, now)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidStateTransition) {
			logInvalidTransition(uc.logger, taskID, "mark-stalled", task.State(), err)
		}
		return
	}
	uc.persist(taskID, snapshot, task, events, now, "mark-stalled")
}
func (uc *checkStallUseCase) persist(taskID domain.TaskID, snapshot domain.TaskSnapshot, task *domain.Task, events []domain.Event, now time.Time, transition string) bool {
	updated, err := snapshot.WithTask(task, now)
	if err != nil {
		uc.logger.Warn("snapshot validation failed after task update", "task_id", taskID.String(), "operation", "with-task", "error", err.Error())
		return false
	}
	if err := uc.tasks.Save(taskID, updated); err != nil {
		uc.logger.Warn("contract write failed", "code", contractWriteFailedCode, "task_id", taskID.String(), "operation", "save-task", "transition", transition, "error", err)
		return false
	}
	if snapshot.State == domain.StateRunning && updated.State == domain.StateStalled {
		uc.stalledTime.EnterStalled(taskID, now)
	}
	if snapshot.State == domain.StateStalled && updated.State == domain.StateOrphaned {
		uc.stalledTime.LeaveStalled(taskID, now)
	}
	for _, event := range events {
		if err := uc.contract.AppendEvent(taskID, event); err != nil {
			uc.logger.Warn("contract write failed", "code", contractWriteFailedCode, "task_id", taskID.String(), "operation", "append-event", "event_type", event.Type(), "error", err)
		}
	}
	return true
}
func (uc *checkStallUseCase) logStateRead(taskID domain.TaskID, operation string, err error) {
	uc.logger.Warn("task state read failed", "code", taskStateReadFailedCode, "task_id", taskID.String(), "operation", operation, "error", err)
}
func (uc *checkStallUseCase) logLiveness(taskID domain.TaskID, err error) {
	// error-codes.md: only task.lock open/flock failures are I/O classification.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	if !errors.Is(err, domain.ErrTaskNotFound) {
		uc.logger.Warn("liveness lock I/O error", "code", "LIVENESS_LOCK_IO_ERROR", "task_id", taskID.String(), "error", err)
	}
	uc.logger.Warn("stall liveness check returned an error", "task_id", taskID.String(), "error", err)
}
