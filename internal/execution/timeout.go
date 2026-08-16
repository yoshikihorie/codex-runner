package execution

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

// Canonical source: validation-rules.md TIMEOUT_KILL_GRACE_SECONDS.
const TimeoutKillGrace = 10 * time.Second

// Canonical source: validation-rules.md TIMEOUT_ENFORCEMENT_RETRY_INTERVAL_SECONDS.
const timeoutEnforcementRetryInterval = 10 * time.Second

// Canonical source: validation-rules.md TIMEOUT_ENFORCEMENT_RETRY_MAX_ATTEMPTS.
const timeoutEnforcementRetryMaxAttempts = 6

type TimeoutEnforcementStage string

const (
	TimeoutEnforcementStageLoad    TimeoutEnforcementStage = "load"
	TimeoutEnforcementStageRestore TimeoutEnforcementStage = "restore"
	TimeoutEnforcementStageSave    TimeoutEnforcementStage = "save"
)

type TimeoutEnforcementError struct {
	Stage     TimeoutEnforcementStage
	Retryable bool
	Cause     error
}

func (e *TimeoutEnforcementError) Error() string {
	return fmt.Sprintf("timeout enforcement %s: %v", e.Stage, e.Cause)
}

func (e *TimeoutEnforcementError) Unwrap() error { return e.Cause }

type TimerFactory interface {
	AfterFunc(d time.Duration, f func()) CancelFunc
}

type CancelFunc func() bool

type armedTimer struct {
	cancel                 CancelFunc
	deadline               time.Time
	resolvedTimeoutSeconds int
	generation             uint64
	retryAttempts          int
	executing              bool
}

type TimeoutWatcher struct {
	enforce        *EnforceTaskTimeoutUseCase
	clock          domain.Clock
	timerFactory   TimerFactory
	baseCtx        context.Context
	logger         *slog.Logger
	mu             sync.Mutex
	timers         map[domain.TaskID]armedTimer
	nextGeneration uint64
	closed         bool
	closeDone      chan struct{}
	wg             sync.WaitGroup
}

type realTimerFactory struct{}

func (realTimerFactory) AfterFunc(d time.Duration, f func()) CancelFunc {
	t := time.AfterFunc(d, f)
	return t.Stop
}

func NewTimeoutWatcher(enforce *EnforceTaskTimeoutUseCase, clock domain.Clock, timerFactory TimerFactory, baseCtx context.Context, logger *slog.Logger) *TimeoutWatcher {
	if enforce == nil || clock == nil || timerFactory == nil || baseCtx == nil {
		panic("timeout watcher requires non-nil dependencies")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &TimeoutWatcher{enforce: enforce, clock: clock, timerFactory: timerFactory, baseCtx: baseCtx, logger: logger, timers: make(map[domain.TaskID]armedTimer), closeDone: make(chan struct{})}
}

func (w *TimeoutWatcher) Arm(taskID domain.TaskID, deadline time.Time, resolvedTimeoutSeconds int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	if old, ok := w.timers[taskID]; ok {
		old.cancel()
	}
	w.nextGeneration++
	generation := w.nextGeneration
	delay := deadline.Sub(w.clock.Now())
	if delay < 0 {
		delay = 0
	}
	// A timer with a zero delay may begin its callback before AfterFunc returns.
	// The gate ensures its map entry is installed before it can inspect it.
	ready := make(chan struct{})
	w.wg.Add(1)
	var done sync.Once
	finish := func() { done.Do(w.wg.Done) }
	timerCancel := w.timerFactory.AfterFunc(delay, func() {
		defer finish()
		<-ready
		w.mu.Lock()
		armed, ok := w.timers[taskID]
		if !ok || armed.generation != generation {
			w.mu.Unlock()
			return
		}
		if armed.executing || w.closed {
			w.mu.Unlock()
			return
		}
		armed.executing = true
		w.timers[taskID] = armed
		w.mu.Unlock()

		_, err := w.enforce.Execute(w.baseCtx, EnforceTaskTimeoutInput{
			TaskID:                 taskID,
			ResolvedTimeoutSeconds: armed.resolvedTimeoutSeconds,
			OccurredAt:             w.clock.Now(),
		})
		w.finishExecution(taskID, generation, err)
		if err != nil {
			w.logger.Error("enforce task timeout", "task_id", taskID.String(), "error", err)
		}
	})
	cancel := func() bool {
		stopped := timerCancel()
		if stopped {
			finish()
		}
		return stopped
	}
	w.timers[taskID] = armedTimer{cancel: cancel, deadline: deadline, resolvedTimeoutSeconds: resolvedTimeoutSeconds, generation: generation}
	close(ready)
}

func (w *TimeoutWatcher) finishExecution(taskID domain.TaskID, generation uint64, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	current, ok := w.timers[taskID]
	if !ok || current.generation != generation {
		return
	}
	var timeoutErr *TimeoutEnforcementError
	if w.closed || err == nil || !errors.As(err, &timeoutErr) || !timeoutErr.Retryable {
		delete(w.timers, taskID)
		return
	}
	if current.retryAttempts >= timeoutEnforcementRetryMaxAttempts {
		delete(w.timers, taskID)
		w.logger.Error("timeout enforcement retries exhausted", "task_id", taskID.String(), "stage", timeoutErr.Stage, "attempts", current.retryAttempts)
		return
	}
	current.retryAttempts++
	current.executing = false
	ready := make(chan struct{})
	w.wg.Add(1)
	var done sync.Once
	finish := func() { done.Do(w.wg.Done) }
	timerCancel := w.timerFactory.AfterFunc(timeoutEnforcementRetryInterval, func() {
		defer finish()
		<-ready
		w.mu.Lock()
		entry, found := w.timers[taskID]
		if !found || w.closed || entry.generation != generation || entry.executing {
			w.mu.Unlock()
			return
		}
		entry.executing = true
		w.timers[taskID] = entry
		w.mu.Unlock()
		_, retryErr := w.enforce.Execute(w.baseCtx, EnforceTaskTimeoutInput{TaskID: taskID, ResolvedTimeoutSeconds: entry.resolvedTimeoutSeconds, OccurredAt: w.clock.Now()})
		w.finishExecution(taskID, generation, retryErr)
		if retryErr != nil {
			w.logger.Error("enforce task timeout", "task_id", taskID.String(), "error", retryErr)
		}
	})
	current.cancel = func() bool {
		stopped := timerCancel()
		if stopped {
			finish()
		}
		return stopped
	}
	w.timers[taskID] = current
	close(ready)
}

func (w *TimeoutWatcher) Disarm(taskID domain.TaskID) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if armed, ok := w.timers[taskID]; ok {
		armed.cancel()
		delete(w.timers, taskID)
	}
}

func (w *TimeoutWatcher) Close() error {
	w.mu.Lock()
	if w.closed {
		done := w.closeDone
		w.mu.Unlock()
		if done != nil {
			<-done
		}
		return nil
	}
	w.closed = true
	if w.closeDone == nil {
		w.closeDone = make(chan struct{})
	}
	done := w.closeDone
	for taskID, armed := range w.timers {
		armed.cancel()
		delete(w.timers, taskID)
	}
	w.mu.Unlock()
	w.wg.Wait()
	close(done)
	return nil
}

type TerminationEnsurer struct {
	liveness *CheckLivenessUseCase
	proc     ProcessRunner
	clock    domain.Clock
	wait     func(ctx context.Context, d time.Duration)
}

func NewTerminationEnsurer(liveness *CheckLivenessUseCase, proc ProcessRunner, clock domain.Clock, wait func(ctx context.Context, d time.Duration)) *TerminationEnsurer {
	if liveness == nil || proc == nil || clock == nil || wait == nil {
		panic("termination ensurer requires non-nil dependencies")
	}
	return &TerminationEnsurer{liveness: liveness, proc: proc, clock: clock, wait: wait}
}

func contextWait(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (e *TerminationEnsurer) Confirm(ctx context.Context, taskID domain.TaskID) (dead bool, err error) {
	dead, err = e.liveness.Execute(ctx, taskID)
	if err != nil || dead {
		return dead, err
	}
	e.wait(ctx, TimeoutKillGrace)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return e.liveness.Execute(ctx, taskID)
}

func (e *TerminationEnsurer) SendAndConfirm(ctx context.Context, taskID domain.TaskID, pid int, grace time.Duration) (dead bool, err error) {
	if terminateErr := e.proc.Terminate(pid, grace); terminateErr != nil {
		slog.Default().Warn("terminate task process group", "task_id", taskID.String(), "error", terminateErr)
	}
	return e.Confirm(ctx, taskID)
}

type RecoveryInvoker interface {
	Execute(context.Context, recovery.RecoverViaResumeInput) (recovery.RecoverViaResumeOutput, error)
}

type TimeoutDisarmer interface {
	Disarm(taskID domain.TaskID)
}

type stalledTimeTracker interface {
	LeaveStalled(domain.TaskID, time.Time) int
}

type taskLocker interface {
	Lock(domain.TaskID)
	Unlock(domain.TaskID)
}

var _ taskLocker = (*store.TaskMutex)(nil)

type EnforceTaskTimeoutInput struct {
	TaskID                 domain.TaskID
	ResolvedTimeoutSeconds int
	OccurredAt             time.Time
}

type EnforceTaskTimeoutOutput struct {
	Outcome string
	Events  []domain.Event
}

type EnforceTaskTimeoutUseCase struct {
	tasks            store.TaskStore
	contract         contract.ContractWriter
	proc             ProcessRunner
	recovery         RecoveryInvoker
	termination      *TerminationEnsurer
	pendingRegistrar recovery.PendingRegistrar
	pathLockReleaser *ReleasePathLockUseCase
	taskMu           taskLocker
	clock            domain.Clock
	stalledTracker   stalledTimeTracker
}

func NewEnforceTaskTimeoutUseCase(tasks store.TaskStore, contractWriter contract.ContractWriter, proc ProcessRunner, recoveryInvoker RecoveryInvoker, termination *TerminationEnsurer, pendingRegistrar recovery.PendingRegistrar, pathLockReleaser *ReleasePathLockUseCase, taskMu taskLocker, clock domain.Clock, stalledTracker stalledTimeTracker) *EnforceTaskTimeoutUseCase {
	if isNilStatusDependency(tasks) || isNilStatusDependency(contractWriter) || isNilStatusDependency(proc) || isNilStatusDependency(recoveryInvoker) || isNilStatusDependency(termination) || isNilStatusDependency(pendingRegistrar) || isNilStatusDependency(pathLockReleaser) || isNilStatusDependency(taskMu) || isNilStatusDependency(clock) || isNilStatusDependency(stalledTracker) {
		panic("enforce task timeout use case requires non-nil dependencies")
	}
	return &EnforceTaskTimeoutUseCase{tasks: tasks, contract: contractWriter, proc: proc, recovery: recoveryInvoker, termination: termination, pendingRegistrar: pendingRegistrar, pathLockReleaser: pathLockReleaser, taskMu: taskMu, clock: clock, stalledTracker: stalledTracker}
}

func (uc *EnforceTaskTimeoutUseCase) Execute(ctx context.Context, in EnforceTaskTimeoutInput) (EnforceTaskTimeoutOutput, error) {
	uc.taskMu.Lock(in.TaskID)
	snapshot, events, terminal, contractErr, err := func() (domain.TaskSnapshot, []domain.Event, bool, error, error) {
		defer uc.taskMu.Unlock(in.TaskID)
		return uc.executeLocked(ctx, in)
	}()
	if err != nil {
		return EnforceTaskTimeoutOutput{}, err
	}
	if terminal {
		slog.Default().Info("skip timeout for terminal task", "task_id", in.TaskID.String())
		return EnforceTaskTimeoutOutput{Outcome: "already-terminal"}, nil
	}

	operationErr := contractErr
	disposition := recovery.PendingSendConfirmOnly
	var authority *recovery.ProcessSignalAuthority
	if snapshot.PID != nil && snapshot.ProcessStartedAt != nil {
		candidate := recovery.ProcessSignalAuthority{TaskID: snapshot.TaskID, PID: *snapshot.PID, ProcessStartedAt: *snapshot.ProcessStartedAt}
		authority = &candidate
		if terminateErr := uc.proc.Terminate(*snapshot.PID, TimeoutKillGrace); terminateErr != nil {
			slog.Default().Warn("terminate task process group", "task_id", in.TaskID.String(), "error", terminateErr)
			disposition = recovery.PendingSendUnsent
		} else {
			disposition = recovery.PendingSendSent
			authority = nil
		}
	}
	dead, confirmErr := uc.termination.Confirm(ctx, in.TaskID)
	if confirmErr != nil || !dead {
		if registerErr := uc.pendingRegistrar.Register(in.TaskID, disposition, authority); registerErr != nil {
			operationErr = errors.Join(operationErr, registerErr)
		}
		if !dead {
			slog.Default().Warn("task termination could not be confirmed; registered for reconciliation", "task_id", in.TaskID.String(), "error", confirmErr)
		}
		return EnforceTaskTimeoutOutput{Outcome: "timed-out", Events: events}, errors.Join(operationErr, confirmErr)
	}
	if snapshot.Subcommand == domain.SubcommandImpl {
		if releaseErr := uc.pathLockReleaser.Execute(ctx, ReleasePathLockInput{TaskID: in.TaskID}); releaseErr != nil {
			operationErr = errors.Join(operationErr, releaseErr)
			uc.reconcileAfterConfirmedDeath(in.TaskID, &operationErr)
			return EnforceTaskTimeoutOutput{Outcome: "timed-out", Events: events}, operationErr
		}
	}
	if _, recoveryErr := uc.recovery.Execute(ctx, recovery.RecoverViaResumeInput{TaskID: in.TaskID, SessionRef: snapshot.SessionRef, Origin: domain.RecoveryOriginTimeout, OccurredAt: in.OccurredAt}); recoveryErr != nil {
		operationErr = errors.Join(operationErr, recoveryErr)
		uc.reconcileAfterConfirmedDeath(in.TaskID, &operationErr)
	}
	return EnforceTaskTimeoutOutput{Outcome: "timed-out", Events: events}, operationErr
}

// executeLocked persists the timeout state while the caller holds taskMu.
func (uc *EnforceTaskTimeoutUseCase) executeLocked(_ context.Context, in EnforceTaskTimeoutInput) (domain.TaskSnapshot, []domain.Event, bool, error, error) {
	snapshot, err := uc.tasks.Load(in.TaskID)
	if err != nil {
		return domain.TaskSnapshot{}, nil, false, nil, timeoutEnforcementError(TimeoutEnforcementStageLoad, err)
	}
	task, err := snapshot.Restore()
	if err != nil {
		return domain.TaskSnapshot{}, nil, false, nil, timeoutEnforcementError(TimeoutEnforcementStageRestore, err)
	}
	if task.State() != domain.StateRunning && task.State() != domain.StateStalled {
		return domain.TaskSnapshot{}, nil, true, nil, nil
	}
	events, err := task.MarkTimedOut(snapshot.SessionRef, in.OccurredAt)
	if err != nil {
		return domain.TaskSnapshot{}, nil, false, nil, timeoutEnforcementError(TimeoutEnforcementStageRestore, err)
	}
	updated, err := snapshot.WithTask(task, in.OccurredAt)
	if err != nil {
		return domain.TaskSnapshot{}, nil, false, nil, timeoutEnforcementError(TimeoutEnforcementStageRestore, err)
	}
	if err := uc.tasks.Save(in.TaskID, updated); err != nil {
		return domain.TaskSnapshot{}, nil, false, nil, timeoutEnforcementError(TimeoutEnforcementStageSave, err)
	}
	if snapshot.State == domain.StateStalled {
		uc.stalledTracker.LeaveStalled(in.TaskID, in.OccurredAt)
	}
	var contractErr error
	for _, event := range events {
		if err := uc.contract.AppendEvent(in.TaskID, event); err != nil {
			contractErr = errors.Join(contractErr, err)
		}
	}
	return snapshot, events, false, contractErr, nil
}

func timeoutEnforcementError(stage TimeoutEnforcementStage, cause error) error {
	return &TimeoutEnforcementError{Stage: stage, Retryable: isTimeoutRetryable(cause), Cause: cause}
}

func isTimeoutRetryable(err error) bool {
	if errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var errno syscall.Errno
	if !errors.As(err, &errno) {
		return false
	}
	switch errno {
	case syscall.EINTR, syscall.EAGAIN, syscall.EBUSY, syscall.EMFILE, syscall.ENFILE, syscall.ENOSPC:
		return true
	default:
		return false
	}
}

func (uc *EnforceTaskTimeoutUseCase) reconcileAfterConfirmedDeath(taskID domain.TaskID, operationErr *error) {
	uc.taskMu.Lock(taskID)
	snapshot, loadErr := uc.tasks.Load(taskID)
	uc.taskMu.Unlock(taskID)
	if loadErr != nil {
		*operationErr = errors.Join(*operationErr, loadErr)
		return
	}
	if isTimeoutTerminalState(snapshot.State) {
		return
	}
	if snapshot.State != domain.StateTimeout && snapshot.State != domain.StateCancelling && snapshot.State != domain.StateRecovering && snapshot.State != domain.StateOrphaned {
		return
	}
	if err := uc.pendingRegistrar.Register(taskID, recovery.PendingSendConfirmOnly, nil); err != nil {
		*operationErr = errors.Join(*operationErr, err)
	}
}

func isTimeoutTerminalState(state domain.TaskState) bool {
	switch state {
	case domain.StateCompleted, domain.StateFailed, domain.StateRecovered, domain.StateTimeoutLost, domain.StateKilled, domain.StateLost:
		return true
	default:
		return false
	}
}

var _ TimeoutDisarmer = (*TimeoutWatcher)(nil)
