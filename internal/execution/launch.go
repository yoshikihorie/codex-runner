package execution

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
)

type LaunchParams struct {
	TaskID           domain.TaskID
	Subcommand       domain.Subcommand
	Model            string
	SandboxMode      string
	WorkingDir       string
	PromptText       string
	TaskDirPath      string
	AllowResume      bool
	LivenessLockFile *os.File
	PTYEnabled       bool
	CodexBinaryPath  string
	ReasoningEffort  *string
}

// LaunchedProcess is the launched child process handle and its one-shot waiter.
type LaunchedProcess struct {
	Handle *domain.ProcessHandle
	Waiter ProcessWaiter
}

// ProcessWaiter wraps cmd.Wait so lifecycle orchestration owns waiting for the process.
type ProcessWaiter interface {
	Wait() (rawExitCode int, err error)
}

type executionLogOpener interface {
	OpenExecutionLogs(taskID domain.TaskID) (*contract.ExecutionLogs, error)
}

type ProcessRunner interface {
	Launch(ctx context.Context, params LaunchParams) (*LaunchedProcess, error)
	SendTerminate(pid int) error
	SendKill(pid int) error
}

var launchNewSession = proc.LaunchNewSession
var sendTerminate = proc.SendTerminate
var sendKill = proc.SendKill
var now = time.Now
var newResumeProcessWaiter = func(cmd *exec.Cmd) ProcessWaiter {
	return &processWaiter{cmd: cmd}
}

const (
	scriptBinaryPath = "/usr/bin/script"
	stdbufBinaryPath = "/usr/bin/stdbuf"
	// Canonical source: validation-rules.md RESUME_STDERR_BUFFER_MAX_BYTES.
	resumeStderrBufferMaxBytes = 4096
	shellSignalExitBase        = 128
)

func buildLaunchArgs(p LaunchParams) (headProcess string, args []string) {
	args = []string{"-oL", p.CodexBinaryPath, "exec", "--json",
		"--sandbox", p.SandboxMode,
		"-C", p.WorkingDir,
		"--model", p.Model}
	if p.ReasoningEffort != nil {
		args = append(args, "-c", "model_reasoning_effort="+*p.ReasoningEffort)
	}
	args = append(args, "--output-last-message", filepath.Join(p.TaskDirPath, "last-message.md"), "--", p.PromptText)
	if p.PTYEnabled {
		return scriptBinaryPath, append([]string{"-q", "/dev/null", stdbufBinaryPath}, args...)
	}
	return stdbufBinaryPath, args
}

type processRunner struct {
	logs executionLogOpener
}

type resumeLauncher struct {
	runner         ProcessRunner
	logger         *slog.Logger
	killGrace      time.Duration
	newExitWatcher func(int) (proc.ExitWatcher, error)
}

// NewResumeLauncher constructs the recovery boundary without exposing execution process types.
func NewResumeLauncher(runner ProcessRunner, loggers ...*slog.Logger) recovery.ResumeLauncher {
	if runner == nil {
		panic("resume launcher requires a process runner")
	}
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &resumeLauncher{runner: runner, logger: logger, killGrace: TimeoutKillGrace, newExitWatcher: proc.WatchExitWithoutReaping}
}

func buildResumeArgs(params recovery.ResumeLaunchParams) []string {
	return []string{"exec", "resume", params.SessionID, "--output-last-message", params.OutputLastMessagePath}
}

func (l *resumeLauncher) LaunchAndWait(ctx context.Context, params recovery.ResumeLaunchParams) (retErr error) {
	taskDirPath := filepath.Join(taskPlacementRoot, params.TaskID.String())
	expectedOutputPath := filepath.Join(taskDirPath, "last-message.md")
	if !filepath.IsAbs(params.CodexBinaryPath) || params.OutputLastMessagePath != expectedOutputPath {
		return fmt.Errorf("resume launch paths are invalid")
	}
	lock, err := AcquireExistingForChild(taskDirPath)
	if err != nil {
		return fmt.Errorf("acquire resume liveness lock: %w", err)
	}
	markerPath := worktreeEvictionMarkerPath(params.TaskID)
	if _, markerErr := os.Lstat(markerPath); markerErr == nil {
		evictionErr := fmt.Errorf("%w: %s", ErrWorktreeEvicted, markerPath)
		_ = lock.Close()
		l.logger.Error("reject resume after worktree eviction", "task_id", params.TaskID.String(), "marker_path", markerPath, "error", evictionErr)
		return evictionErr
	} else if !errors.Is(markerErr, fs.ErrNotExist) {
		err := fmt.Errorf("inspect worktree eviction marker %q: %w", markerPath, markerErr)
		_ = lock.Close()
		l.logger.Error("inspect worktree eviction marker", "task_id", params.TaskID.String(), "marker_path", markerPath, "error", err)
		return err
	}
	if err := ctx.Err(); err != nil {
		_ = lock.Close()
		return err
	}
	defer lock.Close()
	var stderr limitedWriter
	stderr.limit = resumeStderrBufferMaxBytes
	cmd, err := launchNewSession(context.Background(), params.CodexBinaryPath, proc.SafeChildEnv(), lock, nil, &stderr, buildResumeArgs(params)...)
	if err != nil {
		l.logStderr(params, err, &stderr)
		return err
	}
	waiter := newResumeProcessWaiter(cmd)
	watcher, err := l.newExitWatcher(cmd.Process.Pid)
	if err != nil {
		retErr = l.finishWatcherFailure(cmd.Process.Pid, waiter, err, false, nil)
		l.logStderr(params, retErr, &stderr)
		return retErr
	}
	defer func() { retErr = errors.Join(retErr, watcher.Close()) }()

	if watchErr := watcher.Wait(ctx); watchErr == nil {
		retErr = l.finishResumeWait(params, nil, waiter, &stderr)
		return retErr
	} else if ctx.Err() == nil || !errors.Is(watchErr, ctx.Err()) {
		retErr = l.finishWatcherFailure(cmd.Process.Pid, waiter, watchErr, false, nil)
		l.logStderr(params, retErr, &stderr)
		return retErr
	}

	retErr = l.finishContextCancellation(ctx, cmd.Process.Pid, watcher, waiter, &stderr, params)
	return retErr
}

func (l *resumeLauncher) finishContextCancellation(ctx context.Context, pid int, watcher proc.ExitWatcher, waiter ProcessWaiter, stderr *limitedWriter, params recovery.ResumeLaunchParams) error {
	ctxErr := ctx.Err()
	terminateErr := l.runner.SendTerminate(pid)
	graceCtx, cancel := context.WithTimeout(context.Background(), l.killGrace)
	watchErr := watcher.Wait(graceCtx)
	cancel()
	if watchErr == nil {
		return l.finishResumeWait(params, errors.Join(ctxErr, terminateErr), waiter, stderr)
	}
	if !errors.Is(watchErr, context.DeadlineExceeded) {
		return l.finishWatcherFailure(pid, waiter, errors.Join(ctxErr, terminateErr, watchErr), false, nil)
	}

	killErr := l.runner.SendKill(pid)
	graceCtx, cancel = context.WithTimeout(context.Background(), l.killGrace)
	watchErr = watcher.Wait(graceCtx)
	cancel()
	if watchErr == nil {
		return l.finishResumeWait(params, errors.Join(ctxErr, terminateErr, killErr), waiter, stderr)
	}
	if errors.Is(watchErr, context.DeadlineExceeded) {
		err := errors.Join(ctxErr, terminateErr, killErr, errors.New("resume process was not reaped after SIGKILL"))
		l.logStderr(params, err, stderr)
		return err
	}
	return l.finishWatcherFailure(pid, waiter, errors.Join(ctxErr, terminateErr, killErr, watchErr), true, killErr)
}

func (l *resumeLauncher) finishWatcherFailure(pid int, waiter ProcessWaiter, watchErr error, killAttempted bool, killErr error) (retErr error) {
	replacement, rewatchErr := l.newExitWatcher(pid)
	if rewatchErr != nil {
		if !killAttempted {
			killErr = l.runner.SendKill(pid)
		}
		return errors.Join(watchErr, rewatchErr, killErr)
	}
	defer func() { retErr = errors.Join(retErr, replacement.Close()) }()

	if !killAttempted {
		killErr = l.runner.SendKill(pid)
	}
	if killErr != nil {
		return errors.Join(watchErr, killErr)
	}
	graceCtx, cancel := context.WithTimeout(context.Background(), l.killGrace)
	defer cancel()
	if exitErr := replacement.Wait(graceCtx); exitErr != nil {
		return errors.Join(watchErr, exitErr)
	}
	_, waitErr := waiter.Wait()
	return errors.Join(watchErr, waitErr)
}

func (l *resumeLauncher) finishResumeWait(params recovery.ResumeLaunchParams, priorErr error, waiter ProcessWaiter, stderr *limitedWriter) error {
	exitCode, waitErr := waiter.Wait()
	err := errors.Join(priorErr, waitErr)
	if err != nil {
		l.logStderr(params, err, stderr)
		return err
	}
	if exitCode != 0 {
		l.logStderr(params, fmt.Errorf("resume process exited with code %d", exitCode), stderr)
	}
	return nil
}

func (l *resumeLauncher) logStderr(params recovery.ResumeLaunchParams, err error, stderr *limitedWriter) {
	if stderr.Len() == 0 {
		return
	}
	// stderr_bytes is the captured buffer length, not the child process's total stderr output.
	l.logger.Warn("resume process failed", "task_id", params.TaskID.String(), "error", err, "stderr_bytes", stderr.Len())
}

type limitedWriter struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	limit  int
}

// Len returns the number of bytes captured in the bounded buffer.
func (w *limitedWriter) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.buffer.Len()
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if remaining := w.limit - w.buffer.Len(); remaining > 0 {
		if len(p) > remaining {
			_, _ = w.buffer.Write(p[:remaining])
		} else {
			_, _ = w.buffer.Write(p)
		}
	}
	return len(p), nil
}

func NewProcessRunner(logs executionLogOpener) ProcessRunner {
	return &processRunner{logs: logs}
}

func (r *processRunner) Launch(ctx context.Context, p LaunchParams) (*LaunchedProcess, error) {
	if p.LivenessLockFile == nil {
		return nil, fmt.Errorf("liveness lock file is required")
	}
	info, err := os.Stat(p.TaskDirPath)
	if err != nil || !info.IsDir() {
		_ = p.LivenessLockFile.Close()
		if err == nil {
			err = fmt.Errorf("not a directory: %s", p.TaskDirPath)
		}
		return nil, fmt.Errorf("task dir does not exist: %w", err)
	}

	head, args := buildLaunchArgs(p)
	logs, err := r.logs.OpenExecutionLogs(p.TaskID)
	if err != nil {
		_ = p.LivenessLockFile.Close()
		return nil, fmt.Errorf("%w: %v", domain.ErrContractWriteFailed, err)
	}
	defer logs.Close()

	cmd, err := launchNewSession(context.WithoutCancel(ctx), head, proc.SafeChildEnv(), p.LivenessLockFile, logs.Stdout, logs.Stderr, args...)
	if err != nil {
		if p.PTYEnabled {
			return nil, fmt.Errorf("%w: %v", domain.ErrPTYAllocationFailed, err)
		}
		return nil, fmt.Errorf("%w: %v", domain.ErrChildProcessLaunchFailed, err)
	}

	handle := &domain.ProcessHandle{PID: cmd.Process.Pid, ProcessStartedAt: now()}
	return &LaunchedProcess{Handle: handle, Waiter: &processWaiter{cmd: cmd}}, nil
}

func (r *processRunner) SendTerminate(pid int) error {
	return sendTerminate(pid)
}

func (r *processRunner) SendKill(pid int) error {
	return sendKill(pid)
}

type processWaiter struct {
	cmd *exec.Cmd
}

func (w *processWaiter) Wait() (rawExitCode int, err error) {
	waitErr := w.cmd.Wait()
	if waitErr == nil {
		return w.cmd.ProcessState.ExitCode(), nil
	}
	var exitErr *exec.ExitError
	if !errors.As(waitErr, &exitErr) {
		return 1, waitErr
	}
	if code := exitErr.ExitCode(); code >= 0 {
		return code, nil
	}
	if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
		return shellSignalExitBase + int(status.Signal()), nil
	}
	return 1, fmt.Errorf("resolve signaled process exit: %w", waitErr)
}
