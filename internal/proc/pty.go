package proc

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type childStartError struct {
	cause error
}

func (e childStartError) Error() string { return "child process start failed" }
func (e childStartError) Unwrap() error { return e.cause }

// LaunchNewSession starts a child in a new session, redirects standard streams, and closes the transferred liveness lock file.
func LaunchNewSession(ctx context.Context, name string, workingDir string, env []string, livenessLockFile *os.File, stdout io.Writer, stderr io.Writer, args ...string) (*exec.Cmd, error) {
	if livenessLockFile == nil {
		return nil, fmt.Errorf("livenessLockFile is required")
	}
	defer livenessLockFile.Close()
	if !filepath.IsAbs(name) {
		return nil, fmt.Errorf("name must be an absolute path")
	}
	path, err := domain.NewNormalizedPath(workingDir)
	if err != nil {
		return nil, fmt.Errorf("workingDir is invalid: %w", err)
	}
	if err := validateSafeEnv(env); err != nil {
		return nil, fmt.Errorf("env is invalid: %w", err)
	}

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.ExtraFiles = []*os.File{livenessLockFile}
	cmd.Env = env
	cmd.Dir = path.String()
	if stdout != nil {
		cmd.Stdout = stdout
	}
	if stderr != nil {
		cmd.Stderr = stderr
	}
	if err := cmd.Start(); err != nil {
		return nil, childStartError{cause: err}
	}
	return cmd, nil
}
