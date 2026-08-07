package proc

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestNewShutdownContextCancelsOnSIGTERM(t *testing.T) {
	ctx, stop := NewShutdownContext(context.Background())
	defer stop()
	if err := syscall.Kill(syscall.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("context was not canceled after SIGTERM")
	}
}

func TestNewShutdownContextDoesNotCancelWithoutSignal(t *testing.T) {
	ctx, stop := NewShutdownContext(context.Background())
	defer stop()
	select {
	case <-ctx.Done():
		t.Fatal("context was canceled without a signal")
	case <-time.After(25 * time.Millisecond):
	}
}

func TestTerminateProcessGroupStopsOnSIGTERM(t *testing.T) {
	cmd := startProcessGroup(t, "/bin/sleep", "10")
	if err := TerminateProcessGroup(cmd.Process.Pid, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("process exited without a signal")
	} else if !exitedBySignal(err, syscall.SIGTERM) {
		t.Fatalf("process did not exit by SIGTERM: %v", err)
	}
}

func TestTerminateProcessGroupFallsBackToSIGKILL(t *testing.T) {
	readyPath := filepath.Join(t.TempDir(), "ready")
	cmd := startProcessGroup(t, "/bin/sh", "-c", "trap '' TERM; : > \"$1\"; while :; do sleep 10; done", "sh", readyPath)
	waitForFile(t, readyPath)
	if err := TerminateProcessGroup(cmd.Process.Pid, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("process exited without a signal")
	} else if !exitedBySignal(err, syscall.SIGKILL) {
		t.Fatalf("process did not exit by SIGKILL: %v", err)
	}
}

func TestTerminateProcessGroupIgnoresMissingProcess(t *testing.T) {
	if err := TerminateProcessGroup(1<<30, 0); err != nil {
		t.Fatalf("TerminateProcessGroup returned an error for a missing process: %v", err)
	}
}

func TestTerminateProcessGroupRejectsNonPositivePID(t *testing.T) {
	for _, pid := range []int{0, -1} {
		if err := TerminateProcessGroup(pid, 0); err == nil {
			t.Errorf("TerminateProcessGroup(%d) returned nil error", pid)
		}
	}
}

func startProcessGroup(t *testing.T, name string, args ...string) *exec.Cmd {
	t.Helper()
	cmd, err := LaunchNewSession(context.Background(), name, newLockFile(t), args...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		_ = cmd.Wait()
	})
	return cmd
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("process did not create ready file: %s", path)
}

func exitedBySignal(err error, signal syscall.Signal) bool {
	exitErr, ok := err.(*exec.ExitError)
	if !ok {
		return false
	}
	waitStatus, ok := exitErr.Sys().(syscall.WaitStatus)
	return ok && waitStatus.Signaled() && waitStatus.Signal() == signal
}
