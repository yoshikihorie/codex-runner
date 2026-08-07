package proc

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestLaunchNewSessionStartsNewSession(t *testing.T) {
	lockFile := newLockFile(t)
	cmd, err := LaunchNewSession(context.Background(), "/bin/sleep", lockFile, "10")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = cmd.Wait()
	})
	childSessionID, err := syscall.Getsid(cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	parentSessionID, err := syscall.Getsid(0)
	if err != nil {
		t.Fatal(err)
	}
	if childSessionID == parentSessionID {
		t.Fatalf("child session ID = parent session ID = %d", childSessionID)
	}
}

func TestLaunchNewSessionPassesLivenessLockFile(t *testing.T) {
	lockFile := newLockFile(t)
	cmd, err := LaunchNewSession(context.Background(), "/bin/sh", lockFile, "-c", "test -e /dev/fd/3")
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestLaunchNewSessionReturnsErrorForCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := LaunchNewSession(ctx, "/bin/sleep", newLockFile(t), "10"); err == nil {
		t.Fatal("LaunchNewSession succeeded with a canceled context")
	}
}

func TestLaunchNewSessionReturnsErrorForMissingCommand(t *testing.T) {
	if _, err := LaunchNewSession(context.Background(), "/missing-command", newLockFile(t)); err == nil {
		t.Fatal("LaunchNewSession succeeded with a missing command")
	}
}

func TestLaunchNewSessionReturnsErrorForNilLivenessLockFile(t *testing.T) {
	cmd, err := LaunchNewSession(context.Background(), "/bin/sleep", nil, "10")
	if err == nil {
		t.Fatal("LaunchNewSession succeeded with a nil liveness lock file")
	}
	if cmd != nil {
		t.Fatal("LaunchNewSession returned a command for a nil liveness lock file")
	}
}

func newLockFile(t *testing.T) *os.File {
	t.Helper()
	file, err := os.Create(filepath.Join(t.TempDir(), "task.lock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Error(err)
		}
	})
	return file
}
