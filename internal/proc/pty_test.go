package proc

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestLaunchNewSessionStartsNewSession(t *testing.T) {
	lockFile := newLockFile(t)
	cmd, err := LaunchNewSession(context.Background(), "/bin/sleep", SafeChildEnv(), lockFile, nil, nil, "10")
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
	cmd, err := LaunchNewSession(context.Background(), "/bin/sh", SafeChildEnv(), lockFile, nil, nil, "-c", "test -e /dev/fd/3")
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
	if _, err := LaunchNewSession(ctx, "/bin/sleep", SafeChildEnv(), newLockFile(t), nil, nil, "10"); err == nil {
		t.Fatal("LaunchNewSession succeeded with a canceled context")
	}
}

func TestLaunchNewSessionReturnsErrorForMissingCommand(t *testing.T) {
	if _, err := LaunchNewSession(context.Background(), "/missing-command", SafeChildEnv(), newLockFile(t), nil, nil); err == nil {
		t.Fatal("LaunchNewSession succeeded with a missing command")
	}
}

func TestLaunchNewSessionReturnsErrorForNilLivenessLockFile(t *testing.T) {
	cmd, err := LaunchNewSession(context.Background(), "/bin/sleep", SafeChildEnv(), nil, nil, nil, "10")
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

	cmd, err := LaunchNewSession(context.Background(), "/bin/echo", SafeChildEnv(), newLockFile(t), stdout, nil, "hello")
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
	cmd, err := LaunchNewSession(context.Background(), "/bin/echo", SafeChildEnv(), lockFile, nil, nil)
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
	if _, err := LaunchNewSession(context.Background(), "/missing-command", SafeChildEnv(), lockFile, nil, nil); err == nil {
		t.Fatal("LaunchNewSession succeeded with a missing command")
	}
	if _, err := lockFile.Stat(); err == nil {
		t.Fatal("liveness lock file remained open after a failed start")
	}
}

func TestLaunchNewSessionUsesOnlySafeEnvironment(t *testing.T) {
	t.Setenv("FAKE_API_KEY", "secret")
	var stdout bytes.Buffer
	cmd, err := LaunchNewSession(context.Background(), "/usr/bin/env", SafeChildEnv(), newLockFile(t), &stdout, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if got, want := envKeys(stdout.String()), envKeys(strings.Join(SafeChildEnv(), "\n")); !equalStringSets(got, want) {
		t.Fatalf("child environment keys = %v, want %v", got, want)
	}
	if strings.Contains(stdout.String(), "FAKE_API_KEY=") {
		t.Fatalf("child environment leaked FAKE_API_KEY: %q", stdout.String())
	}
	if !strings.Contains(stdout.String(), "PATH="+FixedPath()) {
		t.Fatalf("child environment did not use fixed PATH: %q", stdout.String())
	}
}

func TestLaunchNewSessionRejectsUnsafeEnvironmentAndClosesLockFile(t *testing.T) {
	lockFile := newLockFile(t)
	if _, err := LaunchNewSession(context.Background(), "/bin/echo", nil, lockFile, nil, nil); err == nil {
		t.Fatal("LaunchNewSession succeeded with nil environment")
	}
	if _, err := lockFile.Stat(); err == nil {
		t.Fatal("liveness lock file remained open after invalid environment")
	}
}

func TestLaunchNewSessionRejectsRelativeCommand(t *testing.T) {
	for _, name := range []string{"git", "./local-bin"} {
		if _, err := LaunchNewSession(context.Background(), name, SafeChildEnv(), newLockFile(t), nil, nil); err == nil {
			t.Errorf("LaunchNewSession(%q) succeeded", name)
		}
	}
}

func TestLaunchNewSessionRedirectsStderr(t *testing.T) {
	var stderr bytes.Buffer
	cmd, err := LaunchNewSession(context.Background(), "/bin/sh", SafeChildEnv(), newLockFile(t), nil, &stderr, "-c", "printf stderr >&2")
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if got, want := stderr.String(), "stderr"; got != want {
		t.Fatalf("stderr = %q, want %q", got, want)
	}
}

func envKeys(output string) map[string]bool {
	keys := make(map[string]bool)
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		key, _, found := strings.Cut(line, "=")
		if found {
			keys[key] = true
		}
	}
	return keys
}

func equalStringSets(got, want map[string]bool) bool {
	if len(got) != len(want) {
		return false
	}
	for key := range got {
		if !want[key] {
			return false
		}
	}
	return true
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
