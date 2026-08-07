package store

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
)

const lockFilePerm = 0o600

var chmodLockFile = func(f *os.File, perm os.FileMode) error {
	return f.Chmod(perm)
}

// TryAcquireLiveness reports dead when lockPath is currently not held.
// It returns an error when the lock file does not exist rather than creating one,
// because liveness checks are side-effect-free queries.
func TryAcquireLiveness(lockPath string) (dead bool, err error) {
	f, err := os.OpenFile(lockPath, os.O_RDWR|syscall.O_NOFOLLOW, lockFilePerm)
	if err != nil {
		return false, err
	}
	defer f.Close()

	flockErr := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	switch {
	case flockErr == nil:
		if err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN); err != nil {
			return false, err
		}
		return true, nil
	case errors.Is(flockErr, syscall.EWOULDBLOCK):
		return false, nil
	default:
		return false, flockErr
	}
}

// FileMutex is a blocking flock-based mutex for one lock path.
type FileMutex struct {
	lockPath string
	mu       sync.Mutex
	cond     *sync.Cond
	file     *os.File
	locking  bool
}

// NewFileMutex returns a FileMutex that serializes holders of lockPath.
func NewFileMutex(lockPath string) *FileMutex {
	m := &FileMutex{lockPath: lockPath}
	m.cond = sync.NewCond(&m.mu)
	return m
}

// Lock blocks until it holds an exclusive flock for the configured path.
func (m *FileMutex) Lock() error {
	m.mu.Lock()
	for m.file != nil || m.locking {
		m.cond.Wait()
	}
	m.locking = true
	m.mu.Unlock()

	f, err := os.OpenFile(m.lockPath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, lockFilePerm)
	if err == nil {
		err = chmodLockFile(f, lockFilePerm)
	}
	if err == nil {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
	}
	if err != nil {
		if f != nil {
			_ = f.Close()
		}
		m.mu.Lock()
		m.locking = false
		m.cond.Broadcast()
		m.mu.Unlock()
		return err
	}

	m.mu.Lock()
	m.file = f
	m.locking = false
	m.cond.Broadcast()
	m.mu.Unlock()
	return nil
}

// Unlock releases the current flock and rejects unlocking an unlocked mutex.
func (m *FileMutex) Unlock() error {
	m.mu.Lock()
	if m.file == nil {
		m.mu.Unlock()
		return fmt.Errorf("file mutex is not locked")
	}
	f := m.file
	m.file = nil
	m.mu.Unlock()

	err := syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	closeErr := f.Close()
	m.mu.Lock()
	m.cond.Broadcast()
	m.mu.Unlock()
	if err != nil {
		return err
	}
	return closeErr
}
