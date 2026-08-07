package proc

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"
	"time"
)

// NewShutdownContext cancels its context when the process receives a shutdown signal.
func NewShutdownContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
}

// TerminateProcessGroup sends a termination sequence without task-liveness confirmation.
//
// Known limitation: this function relies only on the numeric process-group ID. If the
// target exits and is reaped during grace, its PID can be reused and SIGKILL may reach an
// unrelated process. ProcessRunner.Terminate accepts only a PID, so this function cannot
// use task-lock liveness to prevent that race. The limitation is accepted by user decision
// on 2026-08-07; eliminating it requires changing that established interface contract.
func TerminateProcessGroup(pid int, grace time.Duration) error {
	if pid <= 0 {
		return fmt.Errorf("invalid pid %d: must be positive", pid)
	}
	if err := sendSignal(-pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("send SIGTERM to process group %d: %w", pid, err)
	}
	time.Sleep(grace)
	if err := sendSignal(-pid, syscall.SIGKILL); err != nil {
		return fmt.Errorf("send SIGKILL to process group %d: %w", pid, err)
	}
	return nil
}

// sendSignal treats ESRCH and EPERM as already gone. It only targets process groups
// started by LaunchNewSession, so EPERM cannot mean another user's process; on macOS,
// a zombie session leader not yet reaped by cmd.Wait returns EPERM instead of ESRCH.
func sendSignal(pid int, signal syscall.Signal) error {
	err := syscall.Kill(pid, signal)
	if errors.Is(err, syscall.ESRCH) || errors.Is(err, syscall.EPERM) {
		return nil
	}
	return err
}
