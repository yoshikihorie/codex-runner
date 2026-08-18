package recovery

import (
	"context"
	"errors"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

var ErrProcessSignalAuthorityInvalid = errors.New("process signal authority invalid")

type lifecycleGenerationVerifier interface {
	WithCurrent(
		taskID domain.TaskID,
		generation domain.LifecycleGeneration,
		action func() error,
	) (executed bool, err error)
}

// ProcessSignalAuthorityValidator proves that a signal still targets the
// current process immediately before the syscall is made.
type ProcessSignalAuthorityValidator struct {
	tasks     store.TaskStore
	taskMu    *store.TaskMutex
	ownership lifecycleGenerationVerifier
}

func NewProcessSignalAuthorityValidator(tasks store.TaskStore, taskMu *store.TaskMutex, ownership lifecycleGenerationVerifier) *ProcessSignalAuthorityValidator {
	if tasks == nil || taskMu == nil || ownership == nil {
		panic("process signal authority validator requires non-nil dependencies")
	}
	return &ProcessSignalAuthorityValidator{tasks: tasks, taskMu: taskMu, ownership: ownership}
}

func (v *ProcessSignalAuthorityValidator) Validate(_ context.Context, authority ProcessSignalAuthority, action func(pid int) error) (executed bool, err error) {
	if !validProcessSignalAuthority(authority) {
		return false, nil
	}

	v.taskMu.Lock(authority.TaskID)
	defer v.taskMu.Unlock(authority.TaskID)

	snapshot, err := v.tasks.Load(authority.TaskID)
	if err != nil {
		return false, err
	}
	if !matchesProcessSignalAuthority(snapshot, authority) {
		return false, nil
	}
	if authority.LifecycleGeneration == nil {
		return true, action(authority.PID)
	}
	return v.ownership.WithCurrent(authority.TaskID, *authority.LifecycleGeneration, func() error {
		return action(authority.PID)
	})
}

func validProcessSignalAuthority(authority ProcessSignalAuthority) bool {
	if authority.TaskID.String() == "" || authority.PID <= 0 || authority.ProcessStartedAt.IsZero() {
		return false
	}
	return authority.ExpectedState == domain.StateTimeout || authority.ExpectedState == domain.StateCancelling
}

func matchesProcessSignalAuthority(snapshot domain.TaskSnapshot, authority ProcessSignalAuthority) bool {
	return snapshot.TaskID == authority.TaskID &&
		snapshot.PID != nil && *snapshot.PID == authority.PID &&
		snapshot.ProcessStartedAt != nil && snapshot.ProcessStartedAt.Equal(authority.ProcessStartedAt) &&
		snapshot.State == authority.ExpectedState
}
