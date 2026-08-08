package proc

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
)

// LaunchNewSession starts a child in a new session, redirects stdout, and closes the transferred liveness lock file.
func LaunchNewSession(ctx context.Context, name string, livenessLockFile *os.File, stdout io.Writer, args ...string) (*exec.Cmd, error) {
	if livenessLockFile == nil {
		return nil, fmt.Errorf("livenessLockFile is required")
	}
	defer livenessLockFile.Close()

	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.ExtraFiles = []*os.File{livenessLockFile}
	if stdout != nil {
		cmd.Stdout = stdout
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("launch %s: %w", name, err)
	}
	return cmd, nil
}
