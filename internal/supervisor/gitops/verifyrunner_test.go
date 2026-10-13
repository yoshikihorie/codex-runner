package gitops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

type testDeadlineContext struct {
	context.Context
	done <-chan struct{}
}

func (c testDeadlineContext) Done() <-chan struct{} { return c.done }

func (c testDeadlineContext) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

func TestVerifyRunnerReasonsAndTimeoutRecheck(t *testing.T) {
	r := VerifyRunner{StateDir: t.TempDir()}
	if _, err := r.Run(context.Background(), nil, "", t.TempDir()); err == nil || err.Error() != "verify_exec_failed" {
		t.Fatalf("invalid command reason: %v", err)
	}
	bin := filepath.Join(t.TempDir(), "verify")
	content := []byte("#!/bin/sh\nprintf changed > \"$0.new\"\n/bin/mv \"$0.new\" \"$0\"\nprintf ready > \"$0.ready\"\n/bin/sleep 30\n")
	if err := os.WriteFile(bin, content, 0700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	if _, err := r.Run(context.Background(), []string{bin}, hash, filepath.Join(t.TempDir(), "missing")); err == nil || err.Error() != "verify_cwd_invalid" {
		t.Fatalf("missing cwd reason: %v", err)
	}
	done := make(chan struct{})
	defer func() {
		select {
		case <-done:
		default:
			close(done)
		}
	}()
	result := make(chan error, 1)
	cwd := t.TempDir()
	go func() {
		_, err := r.Run(testDeadlineContext{Context: context.Background(), done: done}, []string{bin}, hash, cwd)
		result <- err
	}()
	deadline := time.After(10 * time.Second)
	for {
		if _, err := os.Stat(bin + ".ready"); err == nil {
			break
		}
		select {
		case err := <-result:
			t.Fatalf("verify returned before binary changed: %v", err)
		case <-deadline:
			t.Fatal("verify binary did not change")
		case <-time.After(time.Millisecond):
		}
	}
	close(done)
	if err := <-result; !errors.Is(err, ErrVerifyBinaryChanged) {
		t.Fatalf("timeout missed changed binary: %v", err)
	}
}

func TestVerifyRunnerRejectsChangedBinary(t *testing.T) {
	bin := t.TempDir() + "/verify"
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256([]byte("changed"))
	r := VerifyRunner{}
	if _, err := r.Run(context.Background(), []string{bin}, hex.EncodeToString(sum[:]), t.TempDir()); err != ErrVerifyBinaryChanged {
		t.Fatalf("expected binary change error, got %v", err)
	}
}

func TestVerifyRunnerSuccessFailureAndContextTimeout(t *testing.T) {
	bin := t.TempDir() + "/verify"
	content := []byte("#!/bin/sh\nif [ \"$1\" = slow ]; then /bin/sleep 5; exit 0; fi\nif [ -n \"$GH_TOKEN\" ] || [ -n \"$SSH_AUTH_SOCK\" ] || [ -n \"$AWS_ACCESS_KEY_ID\" ]; then exit 77; fi\nexit 0\n")
	if err := os.WriteFile(bin, content, 0700); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	r := VerifyRunner{parentEnv: []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir(), "GH_TOKEN=secret", "SSH_AUTH_SOCK=/tmp/fake", "AWS_ACCESS_KEY_ID=secret"}, StateDir: t.TempDir()}
	if code, err := r.Run(context.Background(), []string{bin}, hash, t.TempDir()); err != nil || code != 0 {
		t.Fatalf("success result %d: %v", code, err)
	}
	if _, err := r.Run(context.Background(), []string{bin}, hash, "relative"); err == nil {
		t.Fatal("accepted relative cwd")
	}
	noisy := filepath.Join(filepath.Dir(bin), "exit-seven")
	bad := []byte("#!/bin/sh\nexit 7\n")
	if err := os.WriteFile(noisy, bad, 0700); err != nil {
		t.Fatal(err)
	}
	badSum := sha256.Sum256(bad)
	if code, err := r.Run(context.Background(), []string{noisy}, hex.EncodeToString(badSum[:]), t.TempDir()); err != nil || code != 7 {
		t.Fatalf("exit code not preserved: %d %v", code, err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if _, err := r.Run(canceled, []string{bin}, hash, t.TempDir()); err != context.Canceled {
		t.Fatalf("parent cancellation misclassified: %v", err)
	}
	if code, err := r.Run(context.Background(), []string{bin}, hash, t.TempDir()); err != nil || code != 0 {
		t.Fatalf("unexpected environment leak %d: %v", code, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := r.Run(ctx, []string{bin, "slow"}, hash, t.TempDir()); err != ErrVerifyTimeout {
		t.Fatalf("want verify timeout, got %v", err)
	}
}
