package execution

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestLifecycleOwnershipRegistryAcquireCurrentAndRelease(t *testing.T) {
	registry := NewLifecycleOwnershipRegistry()
	id := ownershipTestID(t, "acquire-current")
	generation, release, acquired := registry.Acquire(id)
	if !acquired || generation == 0 || release == nil {
		t.Fatalf("Acquire() = (%d, %t, %t)", generation, release != nil, acquired)
	}
	if current, owned := registry.Current(id); !owned || current != generation {
		t.Fatalf("Current() = (%d, %t), want (%d, true)", current, owned, generation)
	}
	duplicateGeneration, duplicateRelease, duplicateAcquired := registry.Acquire(id)
	if duplicateAcquired || duplicateGeneration != 0 || duplicateRelease == nil {
		t.Fatalf("duplicate Acquire() = (%d, %t, %t)", duplicateGeneration, duplicateRelease != nil, duplicateAcquired)
	}
	duplicateRelease()
	if current, owned := registry.Current(id); !owned || current != generation {
		t.Fatalf("duplicate Acquire changed ownership: Current() = (%d, %t)", current, owned)
	}
	release()
	release()
	if current, owned := registry.Current(id); owned || current != 0 {
		t.Fatalf("Current() after release = (%d, %t)", current, owned)
	}
	nextGeneration, nextRelease, nextAcquired := registry.Acquire(id)
	if !nextAcquired || nextGeneration == 0 || nextGeneration == generation {
		t.Fatalf("reacquire = (%d, %t, %t), previous generation = %d", nextGeneration, nextRelease != nil, nextAcquired, generation)
	}
	release()
	if current, owned := registry.Current(id); !owned || current != nextGeneration {
		t.Fatalf("old release deleted new owner: Current() = (%d, %t)", current, owned)
	}
	nextRelease()
}

func TestLifecycleOwnershipRegistrySeparatesTasksAndRejectsConcurrentOwners(t *testing.T) {
	registry := NewLifecycleOwnershipRegistry()
	first := ownershipTestID(t, "first")
	second := ownershipTestID(t, "second")
	_, releaseFirst, acquiredFirst := registry.Acquire(first)
	_, releaseSecond, acquiredSecond := registry.Acquire(second)
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
			_, release, acquired := registry.Acquire(id)
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

func TestLifecycleOwnershipRegistryWithCurrent(t *testing.T) {
	registry := NewLifecycleOwnershipRegistry()
	id := ownershipTestID(t, "with-current")
	generation, release, acquired := registry.Acquire(id)
	if !acquired {
		t.Fatal("Acquire() failed")
	}
	defer release()

	calls := 0
	executed, err := registry.WithCurrent(id, generation, func() error { calls++; return nil })
	if !executed || err != nil || calls != 1 {
		t.Fatalf("WithCurrent(current) = (%t, %v), calls = %d", executed, err, calls)
	}
	executed, err = registry.WithCurrent(ownershipTestID(t, "missing"), generation, func() error { calls++; return nil })
	if executed || err != nil || calls != 1 {
		t.Fatalf("WithCurrent(missing) = (%t, %v), calls = %d", executed, err, calls)
	}
	executed, err = registry.WithCurrent(id, generation+1, func() error { calls++; return nil })
	if executed || err != nil || calls != 1 {
		t.Fatalf("WithCurrent(stale) = (%t, %v), calls = %d", executed, err, calls)
	}
	wantErr := errors.New("signal failed")
	executed, err = registry.WithCurrent(id, generation, func() error { calls++; return wantErr })
	if !executed || !errors.Is(err, wantErr) || calls != 2 {
		t.Fatalf("WithCurrent(error) = (%t, %v), calls = %d", executed, err, calls)
	}
}

func TestLifecycleOwnershipRegistryWithCurrentSeparatesTasks(t *testing.T) {
	registry := NewLifecycleOwnershipRegistry()
	first := ownershipTestID(t, "with-current-first")
	second := ownershipTestID(t, "with-current-second")
	firstGeneration, releaseFirst, firstAcquired := registry.Acquire(first)
	secondGeneration, releaseSecond, secondAcquired := registry.Acquire(second)
	if !firstAcquired || !secondAcquired {
		t.Fatal("Acquire() failed")
	}
	defer releaseFirst()
	defer releaseSecond()

	calls := 0
	executed, err := registry.WithCurrent(first, secondGeneration, func() error {
		calls++
		return nil
	})
	if executed || err != nil || calls != 0 {
		t.Fatalf("WithCurrent(first, second generation) = (%t, %v), calls = %d", executed, err, calls)
	}
	executed, err = registry.WithCurrent(second, secondGeneration, func() error {
		calls++
		return nil
	})
	if !executed || err != nil || calls != 1 {
		t.Fatalf("WithCurrent(second, current generation) = (%t, %v), calls = %d", executed, err, calls)
	}
	if current, owned := registry.Current(first); !owned || current != firstGeneration {
		t.Fatalf("Current(first) = (%d, %t), want (%d, true)", current, owned, firstGeneration)
	}
}

func TestLifecycleOwnershipRegistryWithCurrentBlocksGenerationReplacement(t *testing.T) {
	registry := NewLifecycleOwnershipRegistry()
	id := ownershipTestID(t, "with-current-replacement")
	generation, release, acquired := registry.Acquire(id)
	if !acquired {
		t.Fatal("Acquire() failed")
	}
	actionStarted := make(chan struct{})
	allowAction := make(chan struct{})
	withCurrentDone := make(chan struct{})
	go func() {
		executed, err := registry.WithCurrent(id, generation, func() error {
			close(actionStarted)
			<-allowAction
			return nil
		})
		if !executed || err != nil {
			t.Errorf("WithCurrent() = (%t, %v)", executed, err)
		}
		close(withCurrentDone)
	}()
	<-actionStarted
	replacementStarted := make(chan struct{})
	replacementDone := make(chan domain.LifecycleGeneration, 1)
	go func() {
		close(replacementStarted)
		release()
		nextGeneration, _, nextAcquired := registry.Acquire(id)
		if !nextAcquired {
			t.Errorf("replacement Acquire() failed")
		}
		replacementDone <- nextGeneration
	}()
	<-replacementStarted
	select {
	case nextGeneration := <-replacementDone:
		t.Fatalf("generation replacement completed during action: %d", nextGeneration)
	default:
	}
	close(allowAction)
	<-withCurrentDone
	nextGeneration := <-replacementDone
	if nextGeneration == generation || nextGeneration == 0 {
		t.Fatalf("replacement generation = %d, old = %d", nextGeneration, generation)
	}
	executed, err := registry.WithCurrent(id, generation, func() error { t.Fatal("stale action ran"); return nil })
	if executed || err != nil {
		t.Fatalf("WithCurrent(stale after replacement) = (%t, %v)", executed, err)
	}
}

func TestLifecycleOwnershipRegistryWithCurrentReleasesReadLockAfterPanic(t *testing.T) {
	registry := NewLifecycleOwnershipRegistry()
	id := ownershipTestID(t, "with-current-panic")
	generation, release, acquired := registry.Acquire(id)
	if !acquired {
		t.Fatal("Acquire() failed")
	}

	panicValue := "signal action panic"
	panicDone := make(chan any, 1)
	go func() {
		defer func() {
			panicDone <- recover()
		}()
		registry.WithCurrent(id, generation, func() error {
			panic(panicValue)
		})
	}()
	if got := <-panicDone; got != panicValue {
		t.Fatalf("panic = %#v, want %#v", got, panicValue)
	}

	releaseDone := make(chan struct{})
	go func() {
		release()
		close(releaseDone)
	}()
	select {
	case <-releaseDone:
	case <-time.After(time.Second):
		t.Fatal("release blocked after WithCurrent panic")
	}

	nextGeneration, nextRelease, nextAcquired := registry.Acquire(id)
	if !nextAcquired || nextGeneration == 0 || nextGeneration == generation {
		t.Fatalf("Acquire() after panic = (%d, %t)", nextGeneration, nextAcquired)
	}
	nextRelease()
}

func ownershipTestID(t *testing.T, suffix string) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260812-120000-a1b2-ownership-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
