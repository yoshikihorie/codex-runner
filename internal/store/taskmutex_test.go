package store

import (
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func mutexTaskID(t *testing.T, suffix string) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260806-120000-a1b2-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestTaskMutexDifferentTaskIDsLockIndependently(t *testing.T) {
	m := NewTaskMutex()
	m.Lock(mutexTaskID(t, "first"))
	locked := make(chan struct{})
	go func() { m.Lock(mutexTaskID(t, "second")); close(locked) }()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("different task ID was blocked")
	}
	m.Unlock(mutexTaskID(t, "second"))
	m.Unlock(mutexTaskID(t, "first"))
}

func TestTaskMutexSameTaskIDBlocksUntilUnlock(t *testing.T) {
	m, id := NewTaskMutex(), mutexTaskID(t, "same")
	m.Lock(id)
	locked := make(chan struct{})
	go func() { m.Lock(id); close(locked) }()
	select {
	case <-locked:
		t.Fatal("same task ID did not block")
	case <-time.After(25 * time.Millisecond):
	}
	m.Unlock(id)
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("Lock did not proceed after Unlock")
	}
	m.Unlock(id)
}

func TestTaskMutexSecondLockProceedsAfterRelease(t *testing.T) {
	TestTaskMutexSameTaskIDBlocksUntilUnlock(t)
}

func TestTaskMutexDoubleUnlockPanics(t *testing.T) {
	m, id := NewTaskMutex(), mutexTaskID(t, "double-unlock")
	m.Lock(id)
	m.Unlock(id)
	defer func() {
		if recover() == nil {
			t.Fatal("double Unlock did not panic")
		}
	}()
	m.Unlock(id)
}
