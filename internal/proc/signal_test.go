package proc

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
	"time"
)

type signalCall struct {
	pid    int
	signal syscall.Signal
}

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

func TestProcessGroupSignalRejectsInvalidPIDWithoutSignal(t *testing.T) {
	original := syscallKill
	t.Cleanup(func() { syscallKill = original })

	var calls []signalCall
	syscallKill = func(gotPID int, gotSignal syscall.Signal) error {
		calls = append(calls, signalCall{pid: gotPID, signal: gotSignal})
		return nil
	}

	for _, tt := range []struct {
		name string
		send func(int) error
	}{
		{name: "terminate process group", send: func(pid int) error { return TerminateProcessGroup(pid, 0) }},
		{name: "send terminate", send: SendTerminate},
		{name: "send kill", send: SendKill},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, pid := range []int{-1, 0, 1} {
				calls = nil
				err := tt.send(pid)
				if err == nil {
					t.Errorf("Send(%d) returned nil error", pid)
					continue
				}
				wantErr := fmt.Sprintf("invalid pid %d: must be greater than 1", pid)
				if err.Error() != wantErr {
					t.Errorf("Send(%d) error = %q, want %q", pid, err, wantErr)
				}
				if len(calls) != 0 {
					t.Errorf("Send(%d) calls = %#v, want no calls", pid, calls)
				}
			}
		})
	}
}

func TestSendTerminateAndSendKillDispatchExactlyOneSignal(t *testing.T) {
	for _, tt := range []struct {
		name string
		send func(int) error
		want syscall.Signal
	}{
		{name: "terminate", send: SendTerminate, want: syscall.SIGTERM},
		{name: "kill", send: SendKill, want: syscall.SIGKILL},
	} {
		t.Run(tt.name, func(t *testing.T) {
			original := syscallKill
			t.Cleanup(func() { syscallKill = original })

			const pid = 12345
			var calls []signalCall
			syscallKill = func(gotPID int, gotSignal syscall.Signal) error {
				calls = append(calls, signalCall{pid: gotPID, signal: gotSignal})
				return nil
			}

			if err := tt.send(pid); err != nil {
				t.Fatal(err)
			}
			wantCalls := []signalCall{{pid: -pid, signal: tt.want}}
			if !slices.Equal(calls, wantCalls) {
				t.Fatalf("calls = %#v, want %#v", calls, wantCalls)
			}
		})
	}
}

func TestSendTerminateAndSendKillIgnoreMissingProcess(t *testing.T) {
	for _, tt := range []struct {
		name string
		send func(int) error
		want syscall.Signal
		err  error
	}{
		{name: "terminate ESRCH", send: SendTerminate, want: syscall.SIGTERM, err: syscall.ESRCH},
		{name: "terminate EPERM", send: SendTerminate, want: syscall.SIGTERM, err: syscall.EPERM},
		{name: "kill ESRCH", send: SendKill, want: syscall.SIGKILL, err: syscall.ESRCH},
		{name: "kill EPERM", send: SendKill, want: syscall.SIGKILL, err: syscall.EPERM},
	} {
		t.Run(tt.name, func(t *testing.T) {
			original := syscallKill
			t.Cleanup(func() { syscallKill = original })

			const pid = 12345
			var calls []signalCall
			syscallKill = func(gotPID int, gotSignal syscall.Signal) error {
				calls = append(calls, signalCall{pid: gotPID, signal: gotSignal})
				return tt.err
			}

			if err := tt.send(pid); err != nil {
				t.Fatalf("error = %v, want nil", err)
			}
			wantCalls := []signalCall{{pid: -pid, signal: tt.want}}
			if !slices.Equal(calls, wantCalls) {
				t.Fatalf("calls = %#v, want %#v", calls, wantCalls)
			}
		})
	}
}

func startProcessGroup(t *testing.T, name string, args ...string) *exec.Cmd {
	t.Helper()
	cmd, err := LaunchNewSession(context.Background(), name, SafeChildEnv(), newLockFile(t), nil, nil, args...)
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
