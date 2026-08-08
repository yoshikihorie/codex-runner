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
	cmd, err := LaunchNewSession(context.Background(), "/bin/sleep", lockFile, nil, "10")
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
	cmd, err := LaunchNewSession(context.Background(), "/bin/sh", lockFile, nil, "-c", "test -e /dev/fd/3")
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
	if _, err := LaunchNewSession(ctx, "/bin/sleep", newLockFile(t), nil, "10"); err == nil {
		t.Fatal("LaunchNewSession succeeded with a canceled context")
	}
}

func TestLaunchNewSessionReturnsErrorForMissingCommand(t *testing.T) {
	if _, err := LaunchNewSession(context.Background(), "/missing-command", newLockFile(t), nil); err == nil {
		t.Fatal("LaunchNewSession succeeded with a missing command")
	}
}

func TestLaunchNewSessionReturnsErrorForNilLivenessLockFile(t *testing.T) {
	cmd, err := LaunchNewSession(context.Background(), "/bin/sleep", nil, nil, "10")
	if err == nil {
		t.Fatal("LaunchNewSession succeeded with a nil liveness lock file")
	}
	if cmd != nil {
		t.Fatal("LaunchNewSession returned a command for a nil liveness lock file")
	}
}

func TestLaunchNewSessionRedirectsStdout(t *testing.T) {
	stdout, err := os.Create(filepath.Join(t.TempDir(), "stdout.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stdout.Close()
	})

	cmd, err := LaunchNewSession(context.Background(), "/bin/echo", newLockFile(t), stdout, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := stdout.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(stdout.Name())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(contents), "hello\n"; got != want {
		t.Fatalf("stdout contents = %q, want %q", got, want)
	}
}

func TestLaunchNewSessionClosesLivenessLockFileAfterSuccessfulStart(t *testing.T) {
	lockFile := newLockFile(t)
	cmd, err := LaunchNewSession(context.Background(), "/bin/echo", lockFile, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := lockFile.Stat(); err == nil {
		t.Fatal("liveness lock file remained open after a successful start")
	}
}

func TestLaunchNewSessionClosesLivenessLockFileAfterFailedStart(t *testing.T) {
	lockFile := newLockFile(t)
	if _, err := LaunchNewSession(context.Background(), "/missing-command", lockFile, nil); err == nil {
		t.Fatal("LaunchNewSession succeeded with a missing command")
	}
	if _, err := lockFile.Stat(); err == nil {
		t.Fatal("liveness lock file remained open after a failed start")
	}
}

func newLockFile(t *testing.T) *os.File {
	t.Helper()
	file, err := os.Create(filepath.Join(t.TempDir(), "task.lock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = file.Close()
	})
	return file
}
