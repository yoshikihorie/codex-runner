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
	"regexp"
	"strings"
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
var terminateProcessGroup = proc.TerminateProcessGroup
var sendTerminate = proc.SendTerminate
var sendKill = proc.SendKill
var now = time.Now

const (
	scriptBinaryPath = "/usr/bin/script"
	stdbufBinaryPath = "/usr/bin/stdbuf"
	// Canonical source: validation-rules.md RESUME_STDERR_BUFFER_MAX_BYTES.
	resumeStderrBufferMaxBytes = 4096
)

func buildLaunchArgs(p LaunchParams) (headProcess string, args []string) {
	args = []string{"-oL", p.CodexBinaryPath, "exec", "--json",
		"--sandbox", p.SandboxMode,
		"-C", p.WorkingDir,
		"--model", p.Model}
	if p.ReasoningEffort != nil {
		args = append(args, "-c", "model_reasoning_effort="+*p.ReasoningEffort)
	}
	args = append(args, "--output-last-message", filepath.Join(p.TaskDirPath, "last-message.md"), p.PromptText)
	if p.PTYEnabled {
		return scriptBinaryPath, append([]string{"-q", "/dev/null", stdbufBinaryPath}, args...)
	}
	return stdbufBinaryPath, args
}

type processRunner struct {
	logs executionLogOpener
}

type resumeLauncher struct {
	runner ProcessRunner
	logger *slog.Logger
}

type resumeWaitOutcome struct {
	exitCode int
	err      error
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
	return &resumeLauncher{runner: runner, logger: logger}
}

func buildResumeArgs(params recovery.ResumeLaunchParams) []string {
	return []string{"exec", "resume", params.SessionID, "--output-last-message", params.OutputLastMessagePath}
}

func (l *resumeLauncher) LaunchAndWait(ctx context.Context, params recovery.ResumeLaunchParams) error {
	if !filepath.IsAbs(params.CodexBinaryPath) || !filepath.IsAbs(params.OutputLastMessagePath) {
		return fmt.Errorf("resume launch paths must be absolute")
	}
	taskDirPath := filepath.Join(taskPlacementRoot, params.TaskID.String())
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
	waiter := &processWaiter{cmd: cmd}
	waitResult := make(chan resumeWaitOutcome, 1)
	go func() {
		exitCode, waitErr := waiter.Wait()
		waitResult <- resumeWaitOutcome{exitCode: exitCode, err: waitErr}
	}()
	select {
	case outcome := <-waitResult:
		if outcome.err != nil {
			l.logStderr(params, outcome.err, &stderr)
			return outcome.err
		}
		if outcome.exitCode != 0 {
			l.logStderr(params, fmt.Errorf("resume process exited with code %d", outcome.exitCode), &stderr)
		}
		return nil
	case <-ctx.Done():
		terminateErr := terminateProcessGroup(cmd.Process.Pid, TimeoutKillGrace)
		timer := time.NewTimer(TimeoutKillGrace)
		defer timer.Stop()
		select {
		case outcome := <-waitResult:
			err := errors.Join(ctx.Err(), terminateErr, outcome.err)
			l.logStderr(params, err, &stderr)
			return err
		case <-timer.C:
			err := errors.Join(ctx.Err(), terminateErr, fmt.Errorf("resume process did not exit after termination"))
			l.logStderr(params, err, &stderr)
			return err
		}
	}
}

// ansiEscapePattern follows the canonical ANSI_ESCAPE_PATTERN. It is kept in
// this package because recovery's equivalent is intentionally unexported.
var resumeANSISequencePattern = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)

func sanitizeResumeStderr(raw []byte) string {
	text := resumeANSISequencePattern.ReplaceAllString(string(raw), "")
	text = strings.Map(func(r rune) rune {
		if r < 0x20 && r != '\n' && r != '\t' {
			return -1
		}
		return r
	}, text)
	const maxSummaryBytes = 500
	if len(text) > maxSummaryBytes {
		return text[:maxSummaryBytes] + "...(truncated)"
	}
	return text
}

func (l *resumeLauncher) logStderr(params recovery.ResumeLaunchParams, err error, stderr *limitedWriter) {
	summary := sanitizeResumeStderr(stderr.buffer.Bytes())
	if summary == "" {
		return
	}
	l.logger.Warn("resume process failed", "task_id", params.TaskID.String(), "error", err, "stderr_summary", summary)
}

type limitedWriter struct {
	buffer bytes.Buffer
	limit  int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
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
	if errors.As(waitErr, &exitErr) {
		return exitErr.ExitCode(), nil
	}
	return 1, waitErr
}
