package domain_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

var _ domain.LivenessLock = domain.LivenessLockFunc(nil)

func TestLivenessLockFuncDelegatesToStore(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "task.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	dead, err := domain.LivenessLockFunc(store.TryAcquireLiveness).TryAcquire(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if !dead {
		t.Fatal("adapter reported a held lock")
	}

	directDead, err := store.TryAcquireLiveness(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if directDead != dead {
		t.Fatalf("adapter result = %t, direct result = %t", dead, directDead)
	}
}
