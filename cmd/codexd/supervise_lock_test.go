package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSuperviseLock_SCNSup0117(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "slock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	runDir := filepath.Join(home, ".claude", "run")
	if err := os.MkdirAll(runDir, 0700); err != nil {
		t.Fatal(err)
	}
	lease, err := acquireDaemonInstanceLease(filepath.Join(runDir, supervisorLockName))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Unlock() })
	var stderr bytes.Buffer
	code := runEntrypoint(context.Background(), []string{"supervise"}, strings.NewReader(""), &bytes.Buffer{}, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "SUP_INSTANCE_LOCK_HELD") {
		t.Fatalf("code=%d err=%q", code, stderr.String())
	}
	if _, err := os.Stat(filepath.Join(home, ".claude", "codexd", "supervisor")); !os.IsNotExist(err) {
		t.Fatalf("state dir created before lock: %v", err)
	}
	if _, err := os.Stat(filepath.Join(runDir, "codexd.lock")); !os.IsNotExist(err) {
		t.Fatalf("daemon lock touched: %v", err)
	}
}
