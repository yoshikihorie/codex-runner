package execution

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/proc"
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
	Terminate(pid int, grace time.Duration) error
}

var launchNewSession = proc.LaunchNewSession
var terminateProcessGroup = proc.TerminateProcessGroup
var now = time.Now

const (
	scriptBinaryPath = "/usr/bin/script"
	stdbufBinaryPath = "/usr/bin/stdbuf"
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

	cmd, err := launchNewSession(ctx, head, proc.SafeChildEnv(), p.LivenessLockFile, logs.Stdout, args...)
	if err != nil {
		if p.PTYEnabled {
			return nil, fmt.Errorf("%w: %v", domain.ErrPTYAllocationFailed, err)
		}
		return nil, fmt.Errorf("%w: %v", domain.ErrChildProcessLaunchFailed, err)
	}

	handle := &domain.ProcessHandle{PID: cmd.Process.Pid, ProcessStartedAt: now()}
	return &LaunchedProcess{Handle: handle, Waiter: &processWaiter{cmd: cmd}}, nil
}

func (r *processRunner) Terminate(pid int, grace time.Duration) error {
	return terminateProcessGroup(pid, grace)
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
