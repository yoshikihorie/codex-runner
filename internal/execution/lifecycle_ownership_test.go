package execution

import (
	"sync"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestLifecycleOwnershipRegistryRejectsDuplicateAndReleaseIsIdempotent(t *testing.T) {
	id, err := domain.NewTaskID("impl-20260812-120000-a1b2-ownership")
	if err != nil {
		t.Fatal(err)
	}
	registry := NewLifecycleOwnershipRegistry()
	release, acquired := registry.Acquire(id)
	if !acquired || !registry.IsOwned(id) {
		t.Fatal("first acquisition was not recorded")
	}
	if _, acquired := registry.Acquire(id); acquired {
		t.Fatal("duplicate acquisition succeeded")
	}
	release()
	release()
	if registry.IsOwned(id) {
		t.Fatal("ownership remained after release")
	}
}

func TestLifecycleOwnershipRegistrySeparatesTasksAndRejectsConcurrentOwners(t *testing.T) {
	registry := NewLifecycleOwnershipRegistry()
	first := ownershipTestID(t, "first")
	second := ownershipTestID(t, "second")
	releaseFirst, acquiredFirst := registry.Acquire(first)
	releaseSecond, acquiredSecond := registry.Acquire(second)
	if !acquiredFirst || !acquiredSecond || !registry.IsOwned(first) || !registry.IsOwned(second) {
		t.Fatal("different task IDs must be independently ownable")
	}
	releaseFirst()
	if registry.IsOwned(first) || !registry.IsOwned(second) {
		t.Fatal("releasing one task must not affect another task")
	}
	releaseSecond()

	id := ownershipTestID(t, "concurrent")
	const contenders = 16
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(contenders)
	done.Add(contenders)
	var mu sync.Mutex
	successes := 0
	for range contenders {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			release, acquired := registry.Acquire(id)
			if acquired {
				mu.Lock()
				successes++
				mu.Unlock()
				release()
			}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	if successes == 0 {
		t.Fatal("at least one contender must acquire ownership")
	}
}

func TestLifecycleOwnershipRegistryOldReleaseCannotDeleteNewGeneration(t *testing.T) {
	registry := NewLifecycleOwnershipRegistry()
	id := ownershipTestID(t, "generation")
	oldRelease, acquired := registry.Acquire(id)
	if !acquired {
		t.Fatal("initial acquisition failed")
	}
	oldRelease()
	newRelease, acquired := registry.Acquire(id)
	if !acquired {
		t.Fatal("reacquisition failed")
	}
	oldRelease()
	if !registry.IsOwned(id) {
		t.Fatal("old release deleted newer ownership")
	}
	newRelease()
	if registry.IsOwned(id) {
		t.Fatal("new ownership remained after release")
	}
}

func ownershipTestID(t *testing.T, suffix string) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260812-120000-a1b2-ownership-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
