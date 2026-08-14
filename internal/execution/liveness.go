package execution

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const taskPlacementRoot = "/tmp/codex-tasks"

var flockFunc = func(f *os.File, how int) error {
	return syscall.Flock(int(f.Fd()), how)
}

type lockPathResolver func(taskID domain.TaskID) string

// DefaultLockPathResolver returns the standard liveness lock location for taskID.
func DefaultLockPathResolver(taskID domain.TaskID) string {
	return filepath.Join(taskPlacementRoot, taskID.String(), "task.lock")
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
	lockPath := filepath.Join(taskDirPath, "task.lock")
	f, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	if err := flockFunc(f, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("acquire existing liveness lock for child: %w", err)
	}
	return f, nil
}

// CheckLivenessUseCase checks whether a task liveness lock is currently unheld.
type CheckLivenessUseCase struct {
	lock            domain.LivenessLock
	resolveLockPath lockPathResolver
}

// NewCheckLivenessUseCase constructs a liveness query with injected dependencies.
func NewCheckLivenessUseCase(lock domain.LivenessLock, resolveLockPath lockPathResolver) *CheckLivenessUseCase {
	return &CheckLivenessUseCase{lock: lock, resolveLockPath: resolveLockPath}
}

// Execute reports whether taskID's liveness lock is unheld.
func (u *CheckLivenessUseCase) Execute(_ context.Context, taskID domain.TaskID) (dead bool, err error) {
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
