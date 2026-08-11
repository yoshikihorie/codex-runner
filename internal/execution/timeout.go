package execution

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

// Canonical source: validation-rules.md TIMEOUT_KILL_GRACE_SECONDS.
const TimeoutKillGrace = 10 * time.Second

type TimerFactory interface {
	AfterFunc(d time.Duration, f func()) CancelFunc
}

type CancelFunc func() bool

type armedTimer struct {
	cancel                 CancelFunc
	resolvedTimeoutSeconds int
	generation             uint64
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
		delete(w.timers, taskID)
		w.mu.Unlock()

		_, err := w.enforce.Execute(w.baseCtx, EnforceTaskTimeoutInput{
			TaskID:                 taskID,
			ResolvedTimeoutSeconds: armed.resolvedTimeoutSeconds,
			OccurredAt:             w.clock.Now(),
		})
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
	w.timers[taskID] = armedTimer{cancel: cancel, resolvedTimeoutSeconds: resolvedTimeoutSeconds, generation: generation}
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
	taskMu           *store.TaskMutex
	clock            domain.Clock
}

func NewEnforceTaskTimeoutUseCase(tasks store.TaskStore, contractWriter contract.ContractWriter, proc ProcessRunner, recoveryInvoker RecoveryInvoker, termination *TerminationEnsurer, pendingRegistrar recovery.PendingRegistrar, pathLockReleaser *ReleasePathLockUseCase, taskMu *store.TaskMutex, clock domain.Clock) *EnforceTaskTimeoutUseCase {
	if tasks == nil || contractWriter == nil || proc == nil || recoveryInvoker == nil || termination == nil || pendingRegistrar == nil || pathLockReleaser == nil || taskMu == nil || clock == nil {
		panic("enforce task timeout use case requires non-nil dependencies")
	}
	return &EnforceTaskTimeoutUseCase{tasks: tasks, contract: contractWriter, proc: proc, recovery: recoveryInvoker, termination: termination, pendingRegistrar: pendingRegistrar, pathLockReleaser: pathLockReleaser, taskMu: taskMu, clock: clock}
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
	if terminateErr := uc.proc.Terminate(*snapshot.PID, TimeoutKillGrace); terminateErr != nil {
		slog.Default().Warn("terminate task process group", "task_id", in.TaskID.String(), "error", terminateErr)
	}
	dead, confirmErr := uc.termination.Confirm(ctx, in.TaskID)
	if confirmErr != nil || !dead {
		if registerErr := uc.pendingRegistrar.Register(in.TaskID, true); registerErr != nil {
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
		}
	}
	if _, recoveryErr := uc.recovery.Execute(ctx, recovery.RecoverViaResumeInput{TaskID: in.TaskID, SessionRef: snapshot.SessionRef, Origin: domain.RecoveryOriginTimeout, OccurredAt: in.OccurredAt}); recoveryErr != nil {
		operationErr = errors.Join(operationErr, recoveryErr)
	}
	return EnforceTaskTimeoutOutput{Outcome: "timed-out", Events: events}, operationErr
}

// executeLocked persists the timeout state while the caller holds taskMu.
func (uc *EnforceTaskTimeoutUseCase) executeLocked(_ context.Context, in EnforceTaskTimeoutInput) (domain.TaskSnapshot, []domain.Event, bool, error, error) {
	snapshot, err := uc.tasks.Load(in.TaskID)
	if err != nil {
		return domain.TaskSnapshot{}, nil, false, nil, err
	}
	task, err := snapshot.Restore()
	if err != nil {
		return domain.TaskSnapshot{}, nil, false, nil, err
	}
	if task.State() != domain.StateRunning && task.State() != domain.StateStalled {
		return domain.TaskSnapshot{}, nil, true, nil, nil
	}
	events, err := task.MarkTimedOut(snapshot.SessionRef, in.OccurredAt)
	if err != nil {
		return domain.TaskSnapshot{}, nil, false, nil, err
	}
	updated, err := snapshot.WithTask(task, in.OccurredAt)
	if err != nil {
		return domain.TaskSnapshot{}, nil, false, nil, err
	}
	if err := uc.tasks.Save(in.TaskID, updated); err != nil {
		return domain.TaskSnapshot{}, nil, false, nil, err
	}
	var contractErr error
	for _, event := range events {
		if err := uc.contract.AppendEvent(in.TaskID, event); err != nil {
			contractErr = errors.Join(contractErr, err)
		}
	}
	return snapshot, events, false, contractErr, nil
}

var _ TimeoutDisarmer = (*TimeoutWatcher)(nil)
