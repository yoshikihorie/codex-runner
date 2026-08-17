package usecase

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type TimeoutArmer interface {
	Arm(domain.TaskID, time.Time, int)
}
type stdoutFileOpener interface {
	Open(string) (*os.File, error)
}
type stdoutFileSystem struct{}

func (stdoutFileSystem) Open(path string) (*os.File, error) { return os.Open(path) }

type lifecycleRecordStarting interface {
	Execute(context.Context, *domain.Task, domain.Timeout, string, *string, domain.ExecutionRoute, string, time.Time) error
}
type lifecycleWorktree interface {
	Execute(context.Context, execution.CreateWorktreeInput) (execution.CreateWorktreeOutput, error)
}
type lifecycleLauncher interface {
	Execute(context.Context, execution.LaunchParams) (*execution.LaunchedProcess, error)
}
type lifecycleRecordProcess interface {
	Execute(context.Context, *domain.Task, *domain.ProcessHandle, time.Time) error
}
type lifecycleFinalizer interface {
	Prepare(execution.FinalizeTaskInput) (execution.PreparedFinalizeTask, error)
	ExecuteLocked(context.Context, execution.PreparedFinalizeTask) (execution.LockedFinalizeResult, error)
	ReleaseAfterFinalization(context.Context, execution.LockedFinalizeResult, domain.TaskID)
}
type lifecycleKillConfirmer interface {
	ConfirmKilled(context.Context, domain.TaskID, int, bool, time.Time) error
	ExecuteLocked(context.Context, execution.ConfirmTaskKilledInput) (execution.LockedKillResult, error)
	ReleaseAfterConfirmation(context.Context, execution.LockedKillResult, domain.TaskID)
}
type lifecycleTerminationEnsurer interface {
	Confirm(context.Context, domain.TaskID) (bool, error)
	SendAndConfirm(context.Context, domain.TaskID, int, time.Duration) (bool, error)
}
type processTerminator interface {
	Terminate(int, time.Duration) error
}

type TaskLifecycleDependencies struct {
	AcquireForChild func(string) (*os.File, error)
	RecordStarting  lifecycleRecordStarting
	CreateWorktree  lifecycleWorktree
	Launch          lifecycleLauncher
	RecordProcess   lifecycleRecordProcess
	FailLaunch      *FailTaskLaunchUseCase
	ConfirmRunning  *ConfirmTaskRunningUseCase
	CheckLiveness   *execution.CheckLivenessUseCase
	TimeoutArmer    TimeoutArmer
	Monitor         interface {
		Run(context.Context, domain.TaskID, io.Reader) error
	}
	Finalize      lifecycleFinalizer
	ConfirmKilled lifecycleKillConfirmer
	Tasks         store.TaskStore
	TaskMu        taskLocker
	Termination   lifecycleTerminationEnsurer
	Terminator    processTerminator
	Pending       recovery.PendingRegistrar
	Ownership     execution.LifecycleOwnershipRegistry
	Launching     execution.LaunchingTaskRegistry
	OpenStdout    stdoutFileOpener
	Clock         domain.Clock
}

type TaskLifecycleLaunchConfig struct {
	CodexBinaryPath string
	PTYEnabled      bool
}
type TaskLifecycleOrchestrator struct {
	deps         TaskLifecycleDependencies
	launchConfig TaskLifecycleLaunchConfig
	logger       *slog.Logger
}

func NewTaskLifecycleOrchestrator(deps TaskLifecycleDependencies, config TaskLifecycleLaunchConfig, loggers ...*slog.Logger) (*TaskLifecycleOrchestrator, error) {
	if len(loggers) > 1 {
		return nil, fmt.Errorf("task lifecycle orchestrator accepts at most one logger")
	}
	if config.CodexBinaryPath == "" || !filepath.IsAbs(config.CodexBinaryPath) {
		return nil, fmt.Errorf("task lifecycle codex binary path must be absolute")
	}
	if deps.AcquireForChild == nil || isNilValue(deps.RecordStarting) || isNilValue(deps.Launch) || isNilValue(deps.RecordProcess) || isNilValue(deps.FailLaunch) || isNilValue(deps.ConfirmRunning) || isNilValue(deps.CheckLiveness) || isNilValue(deps.TimeoutArmer) || isNilValue(deps.Monitor) || isNilValue(deps.Finalize) || isNilValue(deps.ConfirmKilled) || isNilValue(deps.Tasks) || isNilValue(deps.TaskMu) || isNilValue(deps.Termination) || isNilValue(deps.Terminator) || isNilValue(deps.Pending) || isNilValue(deps.Ownership) || isNilValue(deps.Launching) || isNilValue(deps.OpenStdout) || isNilValue(deps.Clock) {
		return nil, fmt.Errorf("task lifecycle orchestrator requires non-nil dependencies")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &TaskLifecycleOrchestrator{deps: deps, launchConfig: config, logger: logger}, nil
}

func (o *TaskLifecycleOrchestrator) Run(ctx context.Context, input TaskLifecycleInput) {
	if ctx == nil || input.Task == nil {
		o.logger.Error("task lifecycle input is invalid")
		return
	}
	taskID := input.Task.ID()
	release, acquired := o.deps.Ownership.Acquire(taskID)
	if !acquired {
		o.logger.Error("task lifecycle ownership conflict", "task_id", taskID.String())
		return
	}
	defer release()
	defer o.deps.Launching.Unregister(taskID)

	if o.stopForCancellation(ctx, input) {
		return
	}
	lock, err := o.deps.AcquireForChild(input.TaskDirPath)
	if err != nil {
		o.fail(ctx, input)
		return
	}
	if o.stopForCancellation(ctx, input) {
		_ = lock.Close()
		return
	}
	if err = o.deps.RecordStarting.Execute(ctx, input.Task, input.ResolvedTimeout, input.Model, input.ReasoningEffort, domain.ExecutionRouteDaemon, input.PromptText, input.Now); err != nil {
		_ = lock.Close()
		o.fail(ctx, input)
		return
	}
	workingDir := input.WorkingDir
	if input.Task.Subcommand() == domain.SubcommandImpl {
		if isNilValue(o.deps.CreateWorktree) {
			_ = lock.Close()
			o.fail(ctx, input)
			return
		}
		if o.stopForCancellation(ctx, input) {
			_ = lock.Close()
			return
		}
		out, createErr := o.deps.CreateWorktree.Execute(ctx, execution.CreateWorktreeInput{TaskID: taskID, SourceWorkingDir: input.SourceWorkingDir})
		if createErr != nil {
			_ = lock.Close()
			o.fail(ctx, input)
			return
		}
		workingDir = &out.WorkingDir
	}
	if workingDir == nil {
		_ = lock.Close()
		o.fail(ctx, input)
		return
	}
	if o.confirmUnlaunchedCancellation(ctx, taskID) {
		_ = lock.Close()
		return
	}
	if o.stopForCancellation(ctx, input) {
		_ = lock.Close()
		return
	}
	launched, err := o.deps.Launch.Execute(ctx, execution.LaunchParams{TaskID: taskID, Subcommand: input.Task.Subcommand(), Model: input.Model, SandboxMode: input.SandboxMode, WorkingDir: *workingDir, PromptText: input.PromptText, TaskDirPath: input.TaskDirPath, AllowResume: true, LivenessLockFile: lock, PTYEnabled: o.launchConfig.PTYEnabled, CodexBinaryPath: o.launchConfig.CodexBinaryPath, ReasoningEffort: input.ReasoningEffort})
	if err != nil || launched == nil || launched.Handle == nil || launched.Waiter == nil {
		o.fail(ctx, input)
		return
	}
	if ctx.Err() != nil {
		o.handleStartingCancellation(context.WithoutCancel(ctx), taskID, launched, true)
		return
	}
	cancelling, recordErr := o.recordProcessAtLaunchBoundary(ctx, input, launched)
	if cancelling {
		o.handleStartingCancellation(ctx, taskID, launched, true)
		return
	}
	if recordErr != nil {
		o.handleRecordProcessFailure(ctx, input, launched)
		return
	}
	if ctx.Err() != nil {
		o.handleStartingCancellation(context.WithoutCancel(ctx), taskID, launched, true)
		return
	}
	confirmed, err := o.deps.ConfirmRunning.Execute(ctx, taskID, input.Now)
	if err != nil {
		o.logger.Warn("confirm running failed", "task_id", taskID.String(), "error", err)
		raw, waitErr := o.waitLaunched(taskID, launched)
		o.confirmTerminal(ctx, taskID, raw, waitErr)
		return
	}
	if confirmed.Dead {
		o.waitLaunched(taskID, launched)
		return
	}
	if confirmed.State == domain.StateCancelling {
		o.handleStartingCancellation(ctx, taskID, launched, false)
		return
	}
	deadline := launched.Handle.ProcessStartedAt.Add(time.Duration(input.ResolvedTimeout.ResolvedSeconds()) * time.Second)
	if ctx.Err() != nil {
		o.handleStartingCancellation(context.WithoutCancel(ctx), taskID, launched, true)
		return
	}
	o.deps.TimeoutArmer.Arm(taskID, deadline, input.ResolvedTimeout.ResolvedSeconds())
	if ctx.Err() != nil {
		o.handleStartingCancellation(context.WithoutCancel(ctx), taskID, launched, true)
		return
	}
	o.monitorAndFinalize(ctx, input, launched)
}

func (o *TaskLifecycleOrchestrator) stopForCancellation(ctx context.Context, input TaskLifecycleInput) bool {
	if ctx.Err() == nil {
		return false
	}
	o.fail(context.WithoutCancel(ctx), input)
	return true
}

func (o *TaskLifecycleOrchestrator) recordProcessAtLaunchBoundary(ctx context.Context, input TaskLifecycleInput, launched *execution.LaunchedProcess) (bool, error) {
	taskID := input.Task.ID()
	o.deps.TaskMu.Lock(taskID)
	defer o.deps.TaskMu.Unlock(taskID)
	snapshot, err := o.deps.Tasks.Load(taskID)
	if err != nil {
		return false, err
	}
	task, err := snapshot.Restore()
	if err != nil {
		return false, err
	}
	if task.State() == domain.StateCancelling {
		return true, nil
	}
	return false, o.deps.RecordProcess.Execute(ctx, input.Task, launched.Handle, input.Now)
}

func (o *TaskLifecycleOrchestrator) confirmUnlaunchedCancellation(ctx context.Context, taskID domain.TaskID) bool {
	o.deps.TaskMu.Lock(taskID)
	snapshot, err := o.deps.Tasks.Load(taskID)
	if err != nil {
		o.deps.TaskMu.Unlock(taskID)
		o.logger.Warn("reload task before launch", "task_id", taskID.String(), "error", err)
		return false
	}
	task, err := snapshot.Restore()
	if err != nil || task.State() != domain.StateCancelling {
		o.deps.TaskMu.Unlock(taskID)
		if err != nil {
			o.logger.Warn("restore task before launch", "task_id", taskID.String(), "error", err)
		}
		return false
	}
	result, confirmErr := o.deps.ConfirmKilled.ExecuteLocked(ctx, execution.ConfirmTaskKilledInput{TaskID: taskID, RawExitCode: 130, Estimated: true, OccurredAt: o.deps.Clock.Now()})
	o.deps.TaskMu.Unlock(taskID)
	if confirmErr != nil {
		o.logger.Warn("confirm unlaunched cancellation", "task_id", taskID.String(), "error", confirmErr)
	}
	if result.Confirmed {
		o.deps.ConfirmKilled.ReleaseAfterConfirmation(ctx, result, taskID)
	} else if registerErr := o.deps.Pending.Register(taskID, recovery.PendingSendConfirmOnly, nil); registerErr != nil {
		o.logger.Warn("register pending lifecycle reconciliation", "task_id", taskID.String(), "error", registerErr)
	}
	return true
}

func (o *TaskLifecycleOrchestrator) handleStartingCancellation(ctx context.Context, taskID domain.TaskID, launched *execution.LaunchedProcess, send bool) {
	var dead bool
	var err error
	if send {
		dead, err = o.deps.Termination.SendAndConfirm(ctx, taskID, launched.Handle.PID, execution.TimeoutKillGrace)
	} else {
		dead, err = o.deps.Termination.Confirm(ctx, taskID)
	}
	if err != nil || !dead {
		if err != nil {
			o.logger.Warn("confirm starting cancellation", "task_id", taskID.String(), "error", err)
		}
		disposition, authority := pendingRegistrationAfterSignal(taskID, launched.Handle, send, err)
		if registerErr := o.deps.Pending.Register(taskID, disposition, authority); registerErr != nil {
			o.logger.Warn("register pending lifecycle reconciliation", "task_id", taskID.String(), "error", registerErr)
		}
		o.waitLaunched(taskID, launched)
		return
	}
	o.deps.TaskMu.Lock(taskID)
	result, confirmErr := o.deps.ConfirmKilled.ExecuteLocked(ctx, execution.ConfirmTaskKilledInput{TaskID: taskID, RawExitCode: 130, Estimated: true, OccurredAt: o.deps.Clock.Now()})
	o.deps.TaskMu.Unlock(taskID)
	if confirmErr != nil {
		o.logger.Warn("confirm killed starting cancellation", "task_id", taskID.String(), "error", confirmErr)
	}
	if result.Confirmed {
		o.deps.ConfirmKilled.ReleaseAfterConfirmation(ctx, result, taskID)
	} else {
		disposition, authority := pendingRegistrationAfterSignal(taskID, launched.Handle, send, err)
		if registerErr := o.deps.Pending.Register(taskID, disposition, authority); registerErr != nil {
			o.logger.Warn("register pending lifecycle reconciliation", "task_id", taskID.String(), "error", registerErr)
		}
	}
	o.waitLaunched(taskID, launched)
}

func (o *TaskLifecycleOrchestrator) waitLaunched(taskID domain.TaskID, launched *execution.LaunchedProcess) (int, error) {
	raw, err := launched.Waiter.Wait()
	if err != nil {
		o.logger.Warn("wait task process", "task_id", taskID.String(), "raw_exit_code", raw, "error", err)
	}
	return raw, err
}

func (o *TaskLifecycleOrchestrator) fail(ctx context.Context, input TaskLifecycleInput) {
	taskID := input.Task.ID()
	o.deps.TaskMu.Lock(taskID)
	snapshot, loadErr := o.deps.Tasks.Load(taskID)
	if loadErr != nil {
		o.logger.Warn("reload task before launch failure", "task_id", taskID.String(), "error", loadErr)
	} else if task, restoreErr := snapshot.Restore(); restoreErr != nil {
		o.logger.Warn("restore task before launch failure", "task_id", taskID.String(), "error", restoreErr)
	} else if task.State() == domain.StateCancelling {
		result, confirmErr := o.deps.ConfirmKilled.ExecuteLocked(ctx, execution.ConfirmTaskKilledInput{TaskID: taskID, RawExitCode: 130, Estimated: true, OccurredAt: o.deps.Clock.Now()})
		o.deps.TaskMu.Unlock(taskID)
		if confirmErr != nil {
			o.logger.Warn("confirm killed launch failure", "task_id", taskID.String(), "error", confirmErr)
		}
		if result.Confirmed {
			o.deps.ConfirmKilled.ReleaseAfterConfirmation(ctx, result, taskID)
		} else if registerErr := o.deps.Pending.Register(taskID, recovery.PendingSendConfirmOnly, nil); registerErr != nil {
			o.logger.Warn("register pending lifecycle reconciliation", "task_id", taskID.String(), "error", registerErr)
		}
		return
	}

	result, failErr := o.deps.FailLaunch.ExecuteLocked(ctx, FailTaskLaunchInput{Task: input.Task, ResolvedTimeout: input.ResolvedTimeout, Model: input.Model, ReasoningEffort: input.ReasoningEffort, OccurredAt: input.Now})
	o.deps.TaskMu.Unlock(taskID)
	if failErr != nil {
		o.logger.Warn("fail task launch", "task_id", taskID.String(), "error", failErr)
	}
	if result.Terminal {
		o.deps.FailLaunch.ReleaseAfterFailure(ctx, taskID, result.Impl)
	}
}

func (o *TaskLifecycleOrchestrator) handleRecordProcessFailure(ctx context.Context, input TaskLifecycleInput, launched *execution.LaunchedProcess) {
	taskID := input.Task.ID()
	terminateErr := o.deps.Terminator.Terminate(launched.Handle.PID, execution.TimeoutKillGrace)
	if terminateErr != nil {
		o.logger.Warn("terminate after process record failure", "task_id", taskID.String(), "error", terminateErr)
	}
	dead, err := o.deps.CheckLiveness.Execute(ctx, taskID)
	if err == nil && dead {
		o.fail(ctx, input)
	} else {
		disposition, authority := pendingRegistrationAfterSignal(taskID, launched.Handle, true, terminateErr)
		if registerErr := o.deps.Pending.Register(taskID, disposition, authority); registerErr != nil {
			o.logger.Warn("register pending lifecycle reconciliation", "task_id", taskID.String(), "error", registerErr)
		}
	}
	o.waitLaunched(taskID, launched)
}

func (o *TaskLifecycleOrchestrator) monitorAndFinalize(ctx context.Context, input TaskLifecycleInput, launched *execution.LaunchedProcess) {
	file, err := o.deps.OpenStdout.Open(filepath.Join(input.TaskDirPath, "stdout.log"))
	if err != nil {
		o.logger.Warn("open stdout log", "task_id", input.Task.ID().String(), "error", err)
	} else {
		defer file.Close()
		tail := execution.NewStdoutTailReader(ctx, file, input.Task.ID(), o.deps.CheckLiveness, 0, o.logger)
		if monitorErr := o.deps.Monitor.Run(ctx, input.Task.ID(), tail); monitorErr != nil {
			o.logger.Warn("monitor task events", "task_id", input.Task.ID().String(), "error", monitorErr)
		}
	}
	raw, waitErr := o.waitLaunched(input.Task.ID(), launched)
	o.confirmTerminal(ctx, input.Task.ID(), raw, waitErr)
}

func (o *TaskLifecycleOrchestrator) confirmTerminal(ctx context.Context, taskID domain.TaskID, raw int, waitErr error) {
	estimated := waitErr != nil
	if estimated {
		raw = 1
	}
	occurredAt := o.deps.Clock.Now()
	skipPrepare := false
	if preSnapshot, preErr := o.deps.Tasks.Load(taskID); preErr == nil && preSnapshot.State == domain.StateCancelling {
		skipPrepare = true
	}
	var prepared execution.PreparedFinalizeTask
	var err error
	if !skipPrepare {
		prepared, err = o.deps.Finalize.Prepare(execution.FinalizeTaskInput{TaskID: taskID, RawExitCode: raw, Estimated: estimated, OccurredAt: occurredAt})
	}

	o.deps.TaskMu.Lock(taskID)
	snapshot, loadErr := o.deps.Tasks.Load(taskID)
	if loadErr != nil {
		o.deps.TaskMu.Unlock(taskID)
		o.logger.Error("reload task before terminal confirmation", "task_id", taskID.String(), "error", loadErr)
		return
	}
	if snapshot.State == domain.StateCancelling {
		killOccurredAt := o.deps.Clock.Now()
		killResult, confirmErr := o.deps.ConfirmKilled.ExecuteLocked(ctx, execution.ConfirmTaskKilledInput{TaskID: taskID, RawExitCode: raw, Estimated: estimated, OccurredAt: killOccurredAt})
		o.deps.TaskMu.Unlock(taskID)
		if confirmErr != nil {
			o.logger.Warn("terminal lifecycle confirmation", "task_id", taskID.String(), "error", confirmErr)
		}
		if killResult.Confirmed {
			o.deps.ConfirmKilled.ReleaseAfterConfirmation(ctx, killResult, taskID)
		} else if registerErr := o.deps.Pending.Register(taskID, recovery.PendingSendConfirmOnly, nil); registerErr != nil {
			o.logger.Warn("register pending lifecycle reconciliation", "task_id", taskID.String(), "error", registerErr)
		}
		return
	}
	if skipPrepare {
		o.deps.TaskMu.Unlock(taskID)
		o.logger.Warn("task left cancelling before terminal confirmation", "task_id", taskID.String())
		return
	}
	if err != nil {
		o.deps.TaskMu.Unlock(taskID)
		o.logger.Error("prepare task terminal confirmation", "task_id", taskID.String(), "error", err)
		return
	}
	finalizeResult, finalizeErr := o.deps.Finalize.ExecuteLocked(ctx, prepared)
	o.deps.TaskMu.Unlock(taskID)
	if finalizeErr != nil {
		o.logger.Warn("terminal lifecycle confirmation", "task_id", taskID.String(), "error", finalizeErr)
	}
	if finalizeResult.RecordExited {
		o.deps.Finalize.ReleaseAfterFinalization(ctx, finalizeResult, taskID)
	}
}

func pendingRegistrationAfterSignal(taskID domain.TaskID, handle *domain.ProcessHandle, sent bool, signalErr error) (recovery.PendingSendDisposition, *recovery.ProcessSignalAuthority) {
	if !sent {
		return recovery.PendingSendConfirmOnly, nil
	}
	if signalErr == nil {
		return recovery.PendingSendSent, nil
	}
	if handle == nil || handle.PID <= 0 || handle.ProcessStartedAt.IsZero() {
		return recovery.PendingSendConfirmOnly, nil
	}
	authority := recovery.ProcessSignalAuthority{TaskID: taskID, PID: handle.PID, ProcessStartedAt: handle.ProcessStartedAt}
	return recovery.PendingSendUnsent, &authority
}

var _ taskLifecycleRunner = (*TaskLifecycleOrchestrator)(nil)
