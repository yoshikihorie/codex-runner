package recovery

import (
	"sync"
	"testing"
)

func TestRecoveryOwnershipRegistryAllowsOneConcurrentOwner(t *testing.T) {
	registry := NewRecoveryOwnershipRegistry()
	id := adoptionID(t, "recovery-owner")
	const contenders = 16
	start := make(chan struct{})
	ready := make(chan struct{}, contenders)
	var wg sync.WaitGroup
	var successes int
	var successesMu sync.Mutex
	var winnerRelease func()

	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			<-start
			release, acquired := registry.Acquire(id)
			if !acquired {
				return
			}
			successesMu.Lock()
			successes++
			winnerRelease = release
			successesMu.Unlock()
		}()
	}
	for range contenders {
		<-ready
	}
	close(start)
	wg.Wait()
	if successes != 1 {
		t.Fatalf("acquired count = %d, want 1", successes)
	}
	winnerRelease()
	if registry.IsOwned(id) {
		t.Fatal("owner remains after release")
	}
}

func TestRecoveryOwnershipRegistryReleaseIsIdempotent(t *testing.T) {
	registry := NewRecoveryOwnershipRegistry()
	id := adoptionID(t, "idempotent-release")
	release, acquired := registry.Acquire(id)
	if !acquired {
		t.Fatal("Acquire did not acquire")
	}
	release()
	release()
	if registry.IsOwned(id) {
		t.Fatal("owner remains after repeated release")
	}
}

func TestRecoveryOwnershipRegistryReleaseDoesNotDeleteNewGeneration(t *testing.T) {
	registry := NewRecoveryOwnershipRegistry().(*recoveryOwnershipRegistry)
	id := adoptionID(t, "generation-safe-release")
	release, acquired := registry.Acquire(id)
	if !acquired {
		t.Fatal("initial Acquire did not acquire")
	}
	registry.mu.Lock()
	oldGeneration := registry.owners[id]
	registry.generation++
	registry.owners[id] = registry.generation
	registry.mu.Unlock()
	release()
	if !registry.IsOwned(id) {
		t.Fatal("old release deleted a newer owner")
	}
	registry.mu.RLock()
	newGeneration := registry.owners[id]
	registry.mu.RUnlock()
	if newGeneration == oldGeneration {
		t.Fatal("test setup did not install a new generation")
	}
}

func TestRecoveryOwnershipRegistryAllowsDifferentTasks(t *testing.T) {
	registry := NewRecoveryOwnershipRegistry()
	firstRelease, first := registry.Acquire(adoptionID(t, "first-task"))
	secondRelease, second := registry.Acquire(adoptionID(t, "second-task"))
	if !first || !second {
		t.Fatalf("acquired = (%t, %t), want both true", first, second)
	}
	firstRelease()
	secondRelease()
}
