package proc

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

// LaunchNewSession starts a child in a new session, redirects stdout, and closes the transferred liveness lock file.
func LaunchNewSession(ctx context.Context, name string, env []string, livenessLockFile *os.File, stdout io.Writer, args ...string) (*exec.Cmd, error) {
	if livenessLockFile == nil {
		return nil, fmt.Errorf("livenessLockFile is required")
	}
	defer livenessLockFile.Close()
	if !filepath.IsAbs(name) {
		return nil, fmt.Errorf("name must be an absolute path")
	}
	if err := validateSafeEnv(env); err != nil {
		return nil, fmt.Errorf("env is invalid: %w", err)
	}

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.ExtraFiles = []*os.File{livenessLockFile}
	cmd.Env = env
	if stdout != nil {
		cmd.Stdout = stdout
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("launch %s: %w", name, err)
	}
	return cmd, nil
}
