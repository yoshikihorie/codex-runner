package execution

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const (
	existingLivenessLockRetryFirstWait  = 10 * time.Millisecond
	existingLivenessLockRetrySecondWait = 30 * time.Millisecond
	existingLivenessLockRetryThirdWait  = 60 * time.Millisecond
)

var flockFunc = func(f *os.File, how int) error {
	return syscall.Flock(int(f.Fd()), how)
}

// LockPathResolver resolves a task's liveness-lock path.
type LockPathResolver func(taskID domain.TaskID) string

// NewLockPathResolver returns a liveness resolver rooted at a validated task placement path.
func NewLockPathResolver(root string) (LockPathResolver, error) {
	path, err := domain.NewNormalizedPath(root)
	if err != nil {
		return nil, err
	}
	return func(taskID domain.TaskID) string { return filepath.Join(path.String(), taskID.String(), "task.lock") }, nil
}

// AcquireForChild creates and exclusively locks a liveness lock for child inheritance.
func AcquireForChild(taskDirPath string) (*os.File, error) {
	lockPath := filepath.Join(taskDirPath, "task.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}

	if err := flockFunc(f, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("acquire liveness lock for child: %w", err)
	}
	return f, nil
}

// AcquireExistingForChild reopens and exclusively locks an existing liveness lock for child inheritance.
func AcquireExistingForChild(taskDirPath string) (*os.File, error) {
	return acquireExistingForChild(taskDirPath, time.Sleep)
}

func acquireExistingForChild(taskDirPath string, wait func(time.Duration)) (*os.File, error) {
	lockPath := filepath.Join(taskDirPath, "task.lock")
	f, err := os.OpenFile(lockPath, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}

	retryWaits := [...]time.Duration{
		existingLivenessLockRetryFirstWait,
		existingLivenessLockRetrySecondWait,
		existingLivenessLockRetryThirdWait,
	}
	for attempt := range len(retryWaits) + 1 {
		err := flockFunc(f, syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) || attempt == len(retryWaits) {
			_ = f.Close()
			return nil, fmt.Errorf("acquire existing liveness lock for child: %w", err)
		}
		wait(retryWaits[attempt])
	}

	panic("unreachable")
}

// CheckLivenessUseCase checks whether a task liveness lock is currently unheld.
type CheckLivenessUseCase struct {
	lock            domain.LivenessLock
	resolveLockPath LockPathResolver
}

// NewCheckLivenessUseCase constructs a liveness query with injected dependencies.
func NewCheckLivenessUseCase(lock domain.LivenessLock, resolveLockPath LockPathResolver) *CheckLivenessUseCase {
	return &CheckLivenessUseCase{lock: lock, resolveLockPath: resolveLockPath}
}

// Execute reports whether taskID's liveness lock is unheld.
func (u *CheckLivenessUseCase) Execute(ctx context.Context, taskID domain.TaskID) (dead bool, err error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	dead, err = u.lock.TryAcquire(u.resolveLockPath(taskID))
	switch {
	case err == nil:
		return dead, nil
	case errors.Is(err, fs.ErrNotExist):
		return false, domain.ErrTaskNotFound
	default:
		return false, fmt.Errorf("check task liveness: %w", err)
	}
}

// AcquireDeathLease exclusively locks taskID's existing liveness lock until the
// caller closes the returned file. A held lock means the task may still resume.
func (u *CheckLivenessUseCase) AcquireDeathLease(taskID domain.TaskID) (lease *os.File, dead bool, err error) {
	lockPath := u.resolveLockPath(taskID)
	lease, err = os.OpenFile(lockPath, os.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, false, domain.ErrTaskNotFound
		}
		return nil, false, fmt.Errorf("open death lease %q: %w", lockPath, err)
	}
	if err := flockFunc(lease, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lease.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("acquire death lease %q: %w", lockPath, err)
	}
	return lease, true, nil
}
