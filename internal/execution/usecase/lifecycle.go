package usecase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type TimeoutArmer interface {
	Arm(domain.TaskID, time.Time, int, domain.LifecycleGeneration)
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
	ResolveWorkingDir(domain.TaskID) (string, error)
	Execute(context.Context, execution.CreateWorktreeInput) (execution.CreateWorktreeOutput, error)
}
type lifecycleLauncher interface {
	Execute(context.Context, execution.LaunchParams) (*execution.LaunchedProcess, error)
}
type lifecycleRecordProcess interface {
	Execute(context.Context, *domain.Task, *domain.ProcessHandle, time.Time) error
}
type lifecycleFinalizer interface {
	Prepare(context.Context, execution.FinalizeTaskInput) (execution.PreparedFinalizeTask, error)
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
	SendAndConfirm(context.Context, domain.TaskID, recovery.ProcessSignalAuthority, time.Duration) execution.TerminationAttemptResult
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
	Changes       execution.TaskChangeNotifier
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
	if deps.AcquireForChild == nil || isNilValue(deps.RecordStarting) || isNilValue(deps.Launch) || isNilValue(deps.RecordProcess) || isNilValue(deps.FailLaunch) || isNilValue(deps.ConfirmRunning) || isNilValue(deps.CheckLiveness) || isNilValue(deps.TimeoutArmer) || isNilValue(deps.Monitor) || isNilValue(deps.Finalize) || isNilValue(deps.ConfirmKilled) || isNilValue(deps.Tasks) || isNilValue(deps.TaskMu) || isNilValue(deps.Termination) || isNilValue(deps.Terminator) || isNilValue(deps.Pending) || isNilValue(deps.Ownership) || isNilValue(deps.Launching) || isNilValue(deps.Changes) || isNilValue(deps.OpenStdout) || isNilValue(deps.Clock) {
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
	generation, release, acquired := o.deps.Ownership.Acquire(taskID)
	if !acquired {
		o.logger.Error("task lifecycle ownership conflict", "task_id", taskID.String())
		return
	}
	defer release()
	defer o.deps.Launching.Unregister(taskID)

	if o.stopForCancellation(ctx) {
		return
	}
	lock, err := o.deps.AcquireForChild(input.TaskDirPath)
	if err != nil {
		o.fail(ctx, input, 130, true)
		return
	}
	if o.stopForCancellation(ctx) {
		_ = lock.Close()
		return
	}
	launchPrompt := input.PromptText
	workingDir := input.WorkingDir
	plannedWorkingDir := ""
	useWorktree := input.Task.Subcommand() == domain.SubcommandImpl && input.WorktreeMode == domain.WorktreeModeAuto
	if useWorktree {
		if isNilValue(o.deps.CreateWorktree) {
			_ = lock.Close()
			o.fail(ctx, input, 130, true)
			return
		}
		plannedWorkingDir, err = o.deps.CreateWorktree.ResolveWorkingDir(taskID)
		if err != nil {
			_ = lock.Close()
			o.fail(ctx, input, 130, true)
			return
		}
		launchPrompt = replaceSourceWorkingDir(input.PromptText, input.SourceWorkingDir, plannedWorkingDir)
		workingDir = &plannedWorkingDir
	}
	if o.stopForCancellation(ctx) {
		_ = lock.Close()
		return
	}
	if err = o.deps.RecordStarting.Execute(ctx, input.Task, input.ResolvedTimeout, input.Model, input.ReasoningEffort, domain.ExecutionRouteDaemon, launchPrompt, input.Now); err != nil {
		_ = lock.Close()
		o.fail(ctx, input, 130, true)
		return
	}
	if useWorktree {
		if o.stopForCancellation(ctx) {
			_ = lock.Close()
			return
		}
		out, createErr := o.deps.CreateWorktree.Execute(ctx, execution.CreateWorktreeInput{TaskID: taskID, SourceWorkingDir: input.SourceWorkingDir})
		if createErr != nil || out.WorkingDir != plannedWorkingDir {
			_ = lock.Close()
			o.fail(ctx, input, 130, true)
			return
		}
	}
	if workingDir == nil {
		_ = lock.Close()
		o.fail(ctx, input, 130, true)
		return
	}
	if o.confirmUnlaunchedCancellation(ctx, taskID) {
		_ = lock.Close()
		return
	}
	if o.stopForCancellation(ctx) {
		_ = lock.Close()
		return
	}
	launchCtx := context.WithoutCancel(ctx)
	launched, err := o.deps.Launch.Execute(launchCtx, execution.LaunchParams{TaskID: taskID, Subcommand: input.Task.Subcommand(), Model: input.Model, SandboxMode: input.SandboxMode, WorkingDir: *workingDir, PromptText: launchPrompt, TaskDirPath: input.TaskDirPath, AllowResume: true, LivenessLockFile: lock, PTYEnabled: o.launchConfig.PTYEnabled, CodexBinaryPath: o.launchConfig.CodexBinaryPath, ReasoningEffort: input.ReasoningEffort})
	if ctx.Err() != nil {
		return
	}
	if err != nil || launched == nil || launched.Handle == nil || launched.Waiter == nil {
		o.fail(ctx, input, 130, true)
		return
	}
	cancelling, recordErr := o.recordProcessAtLaunchBoundary(ctx, input, launched)
	if ctx.Err() != nil {
		return
	}
	if cancelling {
		o.handleStartingCancellation(ctx, taskID, launched, true, generation)
		return
	}
	if recordErr != nil {
		o.handleRecordProcessFailure(ctx, input, launched)
		return
	}
	confirmed, err := o.deps.ConfirmRunning.Execute(ctx, taskID, input.Now)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		o.logger.Warn("confirm running failed", "task_id", taskID.String(), "error", err)
		raw, waitErr := o.waitLaunched(taskID, launched)
		o.confirmTerminal(ctx, taskID, raw, waitErr)
		return
	}
	if confirmed.Dead {
		if registerErr := o.deps.Pending.Register(taskID, recovery.PendingSendConfirmOnly, nil); registerErr != nil {
			o.logger.Warn("register pending lifecycle reconciliation", "task_id", taskID.String(), "error", registerErr)
		}
		o.waitLaunched(taskID, launched)
		return
	}
	if confirmed.State == domain.StateCancelling {
		o.handleStartingCancellation(ctx, taskID, launched, false, generation)
		return
	}
	deadline := launched.Handle.ProcessStartedAt.Add(time.Duration(input.ResolvedTimeout.ResolvedSeconds()) * time.Second)
	o.deps.TimeoutArmer.Arm(taskID, deadline, input.ResolvedTimeout.ResolvedSeconds(), generation)
	if ctx.Err() != nil {
		return
	}
	o.monitorAndFinalize(ctx, input, launched)
}

func replaceSourceWorkingDir(prompt, sourceWorkingDir, plannedWorkingDir string) string {
	if sourceWorkingDir == "" {
		return prompt
	}

	var rewritten strings.Builder
	remaining := prompt
	for {
		match := strings.Index(remaining, sourceWorkingDir)
		if match < 0 {
			rewritten.WriteString(remaining)
			return rewritten.String()
		}

		rewritten.WriteString(remaining[:match])
		afterMatch := match + len(sourceWorkingDir)
		if afterMatch == len(remaining) || remaining[afterMatch] == '/' {
			rewritten.WriteString(plannedWorkingDir)
		} else {
			rewritten.WriteString(sourceWorkingDir)
		}
		remaining = remaining[afterMatch:]
	}
}

func (o *TaskLifecycleOrchestrator) stopForCancellation(ctx context.Context) bool {
	return ctx.Err() != nil
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

// confirmUnlaunchedCancellation reports whether Run must stop before launch.
func (o *TaskLifecycleOrchestrator) confirmUnlaunchedCancellation(ctx context.Context, taskID domain.TaskID) bool {
	o.deps.TaskMu.Lock(taskID)
	snapshot, err := o.deps.Tasks.Load(taskID)
	if err != nil {
		o.deps.TaskMu.Unlock(taskID)
		o.logger.Warn("reload task before launch", "task_id", taskID.String(), "error", err)
		return true
	}
	task, err := snapshot.Restore()
	if err != nil || task.State() != domain.StateCancelling {
		o.deps.TaskMu.Unlock(taskID)
		if err != nil {
			o.logger.Warn("restore task before launch", "task_id", taskID.String(), "error", err)
			return true
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

func (o *TaskLifecycleOrchestrator) handleStartingCancellation(ctx context.Context, taskID domain.TaskID, launched *execution.LaunchedProcess, send bool, generation domain.LifecycleGeneration) {
	var claim recovery.SendClaim
	if send {
		authority := recovery.ProcessSignalAuthority{TaskID: taskID, PID: launched.Handle.PID, ProcessStartedAt: launched.Handle.ProcessStartedAt, ExpectedState: domain.StateCancelling, LifecycleGeneration: &generation}
		outcome := recovery.ClaimNotFound
		claim, outcome = o.deps.Pending.ClaimInitialSend(taskID, authority)
		switch outcome {
		case recovery.ClaimAcquired:
			if !o.prepareStartingClaim(taskID, launched.Handle, generation, claim) {
				o.waitLaunched(taskID, launched)
				return
			}
		case recovery.ClaimSent:
			send = false
		case recovery.ClaimAlreadyClaimed, recovery.ClaimConfirmOnly, recovery.ClaimNotFound:
			o.waitLaunched(taskID, launched)
			return
		default:
			o.waitLaunched(taskID, launched)
			return
		}
	}
	var dead bool
	var err error
	var terminateErr error
	if send {
		authority := recovery.ProcessSignalAuthority{TaskID: taskID, PID: launched.Handle.PID, ProcessStartedAt: launched.Handle.ProcessStartedAt, ExpectedState: domain.StateCancelling, LifecycleGeneration: &generation}
		result := o.deps.Termination.SendAndConfirm(ctx, taskID, authority, execution.TimeoutKillGrace)
		dead, err, terminateErr = result.Dead, result.ConfirmErr, result.TerminateErr
		if terminateErr == nil {
			o.deps.Pending.CompleteSend(claim)
		} else if errors.Is(terminateErr, recovery.ErrProcessSignalAuthorityInvalid) {
			o.deps.Pending.InvalidateSend(claim)
		} else {
			o.deps.Pending.ReleaseSend(claim)
		}
	} else {
		dead, err = o.deps.Termination.Confirm(ctx, taskID)
	}
	if !dead {
		if err != nil {
			o.logger.Warn("confirm starting cancellation", "task_id", taskID.String(), "error", err)
		}
		if terminateErr != nil {
			o.logger.Warn("terminate starting cancellation", "task_id", taskID.String(), "error", terminateErr)
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
	}
	o.waitLaunched(taskID, launched)
}

func (o *TaskLifecycleOrchestrator) prepareStartingClaim(taskID domain.TaskID, handle *domain.ProcessHandle, generation domain.LifecycleGeneration, claim recovery.SendClaim) bool {
	o.deps.TaskMu.Lock(taskID)
	snapshot, err := o.deps.Tasks.Load(taskID)
	if err != nil {
		o.deps.TaskMu.Unlock(taskID)
		o.deps.Pending.RemoveClaim(claim)
		o.logger.Warn("reload task before starting cancellation", "task_id", taskID.String(), "error", err)
		return false
	}
	current, owned := o.deps.Ownership.Current(taskID)
	if snapshot.State != domain.StateCancelling || !owned || current != generation || handle == nil || snapshot.PID != nil && (*snapshot.PID != handle.PID || snapshot.ProcessStartedAt == nil || !snapshot.ProcessStartedAt.Equal(handle.ProcessStartedAt)) {
		o.deps.TaskMu.Unlock(taskID)
		o.deps.Pending.RemoveClaim(claim)
		return false
	}
	if snapshot.PID == nil {
		snapshot.PID = &handle.PID
		startedAt := handle.ProcessStartedAt
		snapshot.ProcessStartedAt = &startedAt
		if err := o.deps.Tasks.Save(taskID, snapshot); err != nil {
			o.deps.TaskMu.Unlock(taskID)
			o.deps.Pending.RemoveClaim(claim)
			o.logger.Warn("record process for starting cancellation", "task_id", taskID.String(), "error", err)
			return false
		}
	}
	o.deps.TaskMu.Unlock(taskID)
	return true
}

func (o *TaskLifecycleOrchestrator) waitLaunched(taskID domain.TaskID, launched *execution.LaunchedProcess) (int, error) {
	raw, err := launched.Waiter.Wait()
	if err != nil {
		o.logger.Warn("wait task process", "task_id", taskID.String(), "raw_exit_code", raw, "error", err)
	}
	return raw, err
}

func (o *TaskLifecycleOrchestrator) fail(ctx context.Context, input TaskLifecycleInput, rawExitCode int, estimated bool) bool {
	taskID := input.Task.ID()
	o.deps.TaskMu.Lock(taskID)
	if ctx.Err() != nil {
		o.deps.TaskMu.Unlock(taskID)
		return false
	}
	snapshot, loadErr := o.deps.Tasks.Load(taskID)
	if loadErr != nil {
		o.deps.TaskMu.Unlock(taskID)
		o.logger.Warn("reload task before launch failure", "task_id", taskID.String(), "error", loadErr)
		return false
	}
	task, restoreErr := snapshot.Restore()
	if restoreErr != nil {
		o.deps.TaskMu.Unlock(taskID)
		o.logger.Warn("restore task before launch failure", "task_id", taskID.String(), "error", restoreErr)
		return false
	}
	if task.State() == domain.StateCancelling {
		result, confirmErr := o.deps.ConfirmKilled.ExecuteLocked(ctx, execution.ConfirmTaskKilledInput{TaskID: taskID, RawExitCode: rawExitCode, Estimated: estimated, OccurredAt: o.deps.Clock.Now()})
		o.deps.TaskMu.Unlock(taskID)
		if confirmErr != nil {
			o.logger.Warn("confirm killed launch failure", "task_id", taskID.String(), "error", confirmErr)
		}
		if result.Confirmed {
			o.deps.ConfirmKilled.ReleaseAfterConfirmation(ctx, result, taskID)
		} else if registerErr := o.deps.Pending.Register(taskID, recovery.PendingSendConfirmOnly, nil); registerErr != nil {
			o.logger.Warn("register pending lifecycle reconciliation", "task_id", taskID.String(), "error", registerErr)
		}
		return true
	}

	result, failErr := o.deps.FailLaunch.ExecuteLocked(ctx, FailTaskLaunchInput{Task: input.Task, ResolvedTimeout: input.ResolvedTimeout, Model: input.Model, ReasoningEffort: input.ReasoningEffort, OccurredAt: input.Now})
	o.deps.TaskMu.Unlock(taskID)
	if failErr != nil {
		o.logger.Warn("fail task launch", "task_id", taskID.String(), "error", failErr)
	}
	if result.Terminal {
		o.deps.FailLaunch.ReleaseAfterFailure(ctx, taskID, result.Impl)
	}
	return true
}

func (o *TaskLifecycleOrchestrator) handleRecordProcessFailure(ctx context.Context, input TaskLifecycleInput, launched *execution.LaunchedProcess) {
	taskID := input.Task.ID()
	terminateErr := o.deps.Terminator.Terminate(launched.Handle.PID, execution.TimeoutKillGrace)
	if terminateErr != nil {
		o.logger.Warn("terminate after process record failure", "task_id", taskID.String(), "error", terminateErr)
	}
	dead, err := o.deps.CheckLiveness.Execute(ctx, taskID)
	if ctx.Err() != nil {
		return
	}

	resolved := false
	if err == nil && dead {
		if ctx.Err() != nil {
			return
		}
		resolved = o.fail(ctx, input, 130, true)
	}
	if ctx.Err() != nil {
		return
	}
	rawExitCode, waitErr := o.waitLaunched(taskID, launched)
	if ctx.Err() != nil || resolved {
		return
	}
	estimated := waitErr != nil
	if estimated {
		rawExitCode = 1
	}
	o.fail(ctx, input, rawExitCode, estimated)
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
	if ctx.Err() != nil {
		return
	}
	estimated := waitErr != nil
	if estimated {
		raw = 1
	}
	occurredAt := o.deps.Clock.Now()
	if ctx.Err() != nil {
		return
	}
	changes, unsubscribe := o.deps.Changes.Subscribe(taskID)
	defer unsubscribe()
	prepareCtx, cancelPrepare := context.WithCancel(ctx)
	defer cancelPrepare()

	if ctx.Err() != nil {
		return
	}
	skipPrepare := false
	if preSnapshot, preErr := o.deps.Tasks.Load(taskID); preErr == nil && preSnapshot.State == domain.StateCancelling {
		skipPrepare = true
	}
	if ctx.Err() != nil {
		return
	}
	var prepared execution.PreparedFinalizeTask
	var err error
	if !skipPrepare {
		type prepareResult struct {
			prepared execution.PreparedFinalizeTask
			err      error
		}
		results := make(chan prepareResult, 1)
		input := execution.FinalizeTaskInput{TaskID: taskID, RawExitCode: raw, Estimated: estimated, OccurredAt: occurredAt}
		if ctx.Err() != nil {
			return
		}
		go func() {
			result := prepareResult{}
			defer func() {
				if recovered := recover(); recovered != nil {
					result.err = fmt.Errorf("prepare task terminal confirmation panic: %v", recovered)
				}
				results <- result
			}()
			if prepareCtx.Err() != nil {
				return
			}
			result.prepared, result.err = o.deps.Finalize.Prepare(prepareCtx, input)
		}()
		for {
			select {
			case <-ctx.Done():
				cancelPrepare()
				return
			case result := <-results:
				prepared, err = result.prepared, result.err
				goto preparedOrCancelled
			case <-changes:
				if ctx.Err() != nil {
					return
				}
				snapshot, loadErr := o.deps.Tasks.Load(taskID)
				if ctx.Err() != nil {
					return
				}
				if loadErr != nil {
					o.logger.Warn("reload task after terminal change", "task_id", taskID.String(), "error", loadErr)
					continue
				}
				if snapshot.State == domain.StateCancelling {
					cancelPrepare()
					goto preparedOrCancelled
				}
			}
		}
	}

preparedOrCancelled:

	if ctx.Err() != nil {
		return
	}
	o.deps.TaskMu.Lock(taskID)
	if ctx.Err() != nil {
		o.deps.TaskMu.Unlock(taskID)
		return
	}
	snapshot, loadErr := o.deps.Tasks.Load(taskID)
	if ctx.Err() != nil {
		o.deps.TaskMu.Unlock(taskID)
		return
	}
	if loadErr != nil {
		o.deps.TaskMu.Unlock(taskID)
		o.logger.Error("reload task before terminal confirmation", "task_id", taskID.String(), "error", loadErr)
		return
	}
	if snapshot.State == domain.StateCancelling {
		if ctx.Err() != nil {
			o.deps.TaskMu.Unlock(taskID)
			return
		}
		killOccurredAt := o.deps.Clock.Now()
		if ctx.Err() != nil {
			o.deps.TaskMu.Unlock(taskID)
			return
		}
		killResult, confirmErr := o.deps.ConfirmKilled.ExecuteLocked(ctx, execution.ConfirmTaskKilledInput{TaskID: taskID, RawExitCode: raw, Estimated: estimated, OccurredAt: killOccurredAt})
		o.deps.TaskMu.Unlock(taskID)
		if ctx.Err() != nil {
			return
		}
		if confirmErr != nil {
			o.logger.Warn("terminal lifecycle confirmation", "task_id", taskID.String(), "error", confirmErr)
		}
		if killResult.Confirmed {
			o.deps.ConfirmKilled.ReleaseAfterConfirmation(ctx, killResult, taskID)
		} else if registerErr := o.deps.Pending.Register(taskID, recovery.PendingSendSent, nil); registerErr != nil {
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
	if ctx.Err() != nil {
		o.deps.TaskMu.Unlock(taskID)
		return
	}
	finalizeResult, finalizeErr := o.deps.Finalize.ExecuteLocked(ctx, prepared)
	o.deps.TaskMu.Unlock(taskID)
	if ctx.Err() != nil {
		return
	}
	if finalizeErr != nil {
		o.logger.Warn("terminal lifecycle confirmation", "task_id", taskID.String(), "error", finalizeErr)
	}
	if finalizeResult.RecordExited {
		o.deps.Finalize.ReleaseAfterFinalization(ctx, finalizeResult, taskID)
	}
}

var _ taskLifecycleRunner = (*TaskLifecycleOrchestrator)(nil)
