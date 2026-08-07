package domain

import (
	"fmt"
	"time"
)

// PathLock records the normalized paths owned by a task.
type PathLock struct {
	TaskID     TaskID
	OwnedPaths []NormalizedPath
	AcquiredAt time.Time
}

// PathLockSnapshot is the raw persisted representation read by the store.
// Its paths are normalized by the execution use case before comparison.
type PathLockSnapshot struct {
	TaskID     TaskID
	OwnedPaths []string
}

// Overlaps reports whether p and other own any identical normalized path.
func (p *PathLock) Overlaps(other *PathLock) bool {
	if p == nil || other == nil {
		return false
	}
	owned := make(map[string]struct{}, len(p.OwnedPaths))
	for _, path := range p.OwnedPaths {
		owned[path.String()] = struct{}{}
	}
	for _, path := range other.OwnedPaths {
		if _, ok := owned[path.String()]; ok {
			return true
		}
	}
	return false
}

// Acquire creates a path lock when requestedPaths do not overlap activeLocks.
func Acquire(taskID TaskID, requestedPaths []NormalizedPath, activeLocks []*PathLock, now time.Time) (*PathLock, error) {
	candidate := &PathLock{TaskID: taskID, OwnedPaths: requestedPaths, AcquiredAt: now}
	for _, active := range activeLocks {
		if candidate.Overlaps(active) {
			return nil, ErrPathLockConflict
		}
	}
	return candidate, nil
}

// Release verifies that taskID owns the path lock. Persistence is handled by the use case.
func (p *PathLock) Release(taskID TaskID) error {
	if p == nil || p.TaskID != taskID {
		return fmt.Errorf("path lock task id mismatch")
	}
	return nil
}
