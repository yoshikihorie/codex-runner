package domain

import (
	"errors"
	"testing"
	"time"
)

func TestPathLockAcquireAndOverlap(t *testing.T) {
	taskID, err := NewTaskID("impl-20260807-120000-a1b2-path-lock")
	if err != nil {
		t.Fatal(err)
	}
	otherID, err := NewTaskID("impl-20260807-120001-a1b2-other-lock")
	if err != nil {
		t.Fatal(err)
	}
	path, err := NewNormalizedPath("/tmp/path-lock-test")
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := NewNormalizedPath("/tmp/path-lock-test/child")
	if err != nil {
		t.Fatal(err)
	}
	lock, err := Acquire(taskID, []NormalizedPath{path}, nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if lock.Overlaps(&PathLock{TaskID: otherID, OwnedPaths: []NormalizedPath{prefix}}) {
		t.Fatal("prefix paths overlap")
	}
	if !lock.Overlaps(&PathLock{TaskID: otherID, OwnedPaths: []NormalizedPath{path}}) {
		t.Fatal("equal paths do not overlap")
	}
	if _, err := Acquire(otherID, []NormalizedPath{path}, []*PathLock{lock}, time.Now()); !errors.Is(err, ErrPathLockConflict) {
		t.Fatalf("conflicting acquire error = %v", err)
	}
	if err := lock.Release(taskID); err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(otherID); err == nil {
		t.Fatal("Release succeeded for another task")
	}
}
