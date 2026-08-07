package proc

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"
)

// LaunchNewSession starts a child in a new session and transfers liveness-lock ownership to it.
func LaunchNewSession(ctx context.Context, name string, livenessLockFile *os.File, args ...string) (*exec.Cmd, error) {
	if livenessLockFile == nil {
		return nil, fmt.Errorf("livenessLockFile is required")
	}
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.ExtraFiles = []*os.File{livenessLockFile}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("launch %s: %w", name, err)
	}
	return cmd, nil
}
