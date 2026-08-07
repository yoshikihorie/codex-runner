package domain

// LivenessLock queries whether a task liveness lock can be acquired.
type LivenessLock interface {
	TryAcquire(lockPath string) (dead bool, err error)
}

// LivenessLockFunc adapts a function to LivenessLock.
type LivenessLockFunc func(lockPath string) (dead bool, err error)

func (f LivenessLockFunc) TryAcquire(lockPath string) (dead bool, err error) { return f(lockPath) }
