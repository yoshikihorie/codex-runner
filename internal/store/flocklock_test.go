package store

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestTryAcquireLiveness(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "task.lock")
	createLockFile(t, lockPath)
	dead, err := TryAcquireLiveness(lockPath)
	if err != nil || !dead {
		t.Fatalf("first acquire = (%t, %v)", dead, err)
	}
	f, err := os.OpenFile(lockPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	dead, err = TryAcquireLiveness(lockPath)
	if err != nil || dead {
		t.Fatalf("held acquire = (%t, %v)", dead, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	dead, err = TryAcquireLiveness(lockPath)
	if err != nil || !dead {
		t.Fatalf("reacquire = (%t, %v)", dead, err)
	}
}

func TestTryAcquireLivenessReturnsErrorWhenLockFileMissing(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "task.lock")
	if _, err := TryAcquireLiveness(lockPath); err == nil {
		t.Fatal("TryAcquireLiveness succeeded with a missing lock file")
	}
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Fatalf("lock file exists after liveness check: %v", err)
	}
}

func TestTryAcquireLivenessReturnsIOError(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "missing", "task.lock")
	if _, err := TryAcquireLiveness(lockPath); err == nil {
		t.Fatal("TryAcquireLiveness succeeded with a missing parent directory")
	}
}

func TestTryAcquireLivenessReturnsErrorForSymlink(t *testing.T) {
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.lock")
	createLockFile(t, targetPath)
	lockPath := filepath.Join(dir, "task.lock")
	if err := os.Symlink(targetPath, lockPath); err != nil {
		t.Fatal(err)
	}
	if _, err := TryAcquireLiveness(lockPath); err == nil {
		t.Fatal("TryAcquireLiveness succeeded with a symlink lock file")
	}
}

func TestFileMutexBlocksAndCanBeReused(t *testing.T) {
	mutex := NewFileMutex(filepath.Join(t.TempDir(), "path-locks.lock"))
	if err := mutex.Lock(); err != nil {
		t.Fatal(err)
	}
	locked := make(chan error, 1)
	go func() { locked <- mutex.Lock() }()
	select {
	case err := <-locked:
		t.Fatalf("second Lock returned before Unlock: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if err := mutex.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := <-locked; err != nil {
		t.Fatal(err)
	}
	if err := mutex.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := mutex.Lock(); err != nil {
		t.Fatal(err)
	}
	if err := mutex.Unlock(); err != nil {
		t.Fatal(err)
	}
}

func TestFileMutexSeparateInstancesAreMutuallyExclusive(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "path-locks.lock")
	first := NewFileMutex(lockPath)
	second := NewFileMutex(lockPath)
	if err := first.Lock(); err != nil {
		t.Fatal(err)
	}
	locked := make(chan error, 1)
	go func() { locked <- second.Lock() }()
	select {
	case err := <-locked:
		t.Fatalf("second instance Lock returned before first Unlock: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if err := first.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := <-locked; err != nil {
		t.Fatal(err)
	}
	if err := second.Unlock(); err != nil {
		t.Fatal(err)
	}
}

func TestFileMutexRejectsUnlockWhenNotLocked(t *testing.T) {
	mutex := NewFileMutex(filepath.Join(t.TempDir(), "path-locks.lock"))
	if err := mutex.Unlock(); err == nil {
		t.Fatal("Unlock succeeded before Lock")
	}
	if err := mutex.Lock(); err != nil {
		t.Fatal(err)
	}
	if err := mutex.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := mutex.Unlock(); err == nil {
		t.Fatal("second Unlock succeeded")
	}
}

func TestFileMutexLockReturnsErrorForSymlink(t *testing.T) {
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "target.lock")
	createLockFile(t, targetPath)
	lockPath := filepath.Join(dir, "path-locks.lock")
	if err := os.Symlink(targetPath, lockPath); err != nil {
		t.Fatal(err)
	}
	if err := NewFileMutex(lockPath).Lock(); err == nil {
		t.Fatal("FileMutex.Lock succeeded with a symlink lock file")
	}
}

func TestFileMutexLockCorrectsExistingFilePermissions(t *testing.T) {
	lockPath := filepath.Join(t.TempDir(), "path-locks.lock")
	createLockFile(t, lockPath)
	if err := os.Chmod(lockPath, 0o666); err != nil {
		t.Fatal(err)
	}
	mutex := NewFileMutex(lockPath)
	if err := mutex.Lock(); err != nil {
		t.Fatal(err)
	}
	if err := mutex.Unlock(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != lockFilePerm {
		t.Fatalf("permissions = %o", info.Mode().Perm())
	}
}

func TestFileMutexLockResetsStateWhenChmodFails(t *testing.T) {
	originalChmodLockFile := chmodLockFile
	chmodLockFile = func(*os.File, os.FileMode) error {
		return errors.New("chmod failed")
	}
	t.Cleanup(func() {
		chmodLockFile = originalChmodLockFile
	})

	mutex := NewFileMutex(filepath.Join(t.TempDir(), "path-locks.lock"))
	if err := mutex.Lock(); err == nil {
		t.Fatal("FileMutex.Lock succeeded when chmod failed")
	}
	if mutex.locking {
		t.Fatal("FileMutex remained in locking state after chmod failure")
	}
	if mutex.file != nil {
		t.Fatal("FileMutex retained a file after chmod failure")
	}
	chmodLockFile = originalChmodLockFile
	if err := mutex.Lock(); err != nil {
		t.Fatalf("FileMutex could not be reused after chmod failure: %v", err)
	}
	if err := mutex.Unlock(); err != nil {
		t.Fatal(err)
	}
}

func createLockFile(t *testing.T, lockPath string) {
	t.Helper()
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, lockFilePerm)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
