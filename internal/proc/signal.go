package proc

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"
	"time"
)

var syscallKill = syscall.Kill

// NewShutdownContext cancels its context when the process receives a shutdown signal.
func NewShutdownContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
}

// TerminateProcessGroup sends a termination sequence without task-liveness confirmation.
//
// Known limitation: this function relies only on the numeric process-group ID. If the
// target exits and is reaped during grace, its PID can be reused and SIGKILL may reach an
// unrelated process. Task lifecycle callers use SendTerminate and SendKill separately so
// their upper layer can revalidate authority before each signal. This helper remains for
// directly owned processes that require the existing combined termination behavior.
func TerminateProcessGroup(pid int, grace time.Duration) error {
	if err := SendTerminate(pid); err != nil {
		return err
	}
	time.Sleep(grace)
	return SendKill(pid)
}

// SendTerminate sends SIGTERM to a process group once.
func SendTerminate(pid int) error {
	return sendProcessGroupSignal(pid, syscall.SIGTERM)
}

// SendKill sends SIGKILL to a process group once.
func SendKill(pid int) error {
	return sendProcessGroupSignal(pid, syscall.SIGKILL)
}

func sendProcessGroupSignal(pid int, signal syscall.Signal) error {
	if pid <= 1 {
		return fmt.Errorf("invalid pid %d: must be greater than 1", pid)
	}
	if err := sendSignal(-pid, signal); err != nil {
		return fmt.Errorf("send %s to process group %d: %w", signal, pid, err)
	}
	return nil
}

// sendSignal treats ESRCH and EPERM as already gone. It only targets process groups
// started by LaunchNewSession, so EPERM cannot mean another user's process; on macOS,
// a zombie session leader not yet reaped by cmd.Wait returns EPERM instead of ESRCH.
func sendSignal(pid int, signal syscall.Signal) error {
	err := syscallKill(pid, signal)
	if errors.Is(err, syscall.ESRCH) || errors.Is(err, syscall.EPERM) {
		return nil
	}
	return err
}
