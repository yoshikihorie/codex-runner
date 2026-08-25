package execution

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestPromotionRegistryReserveAndResolveAreIdempotent(t *testing.T) {
	registry := NewPromotionRegistry()
	taskID := promotionTaskID(t, 1)

	registry.Reserve(taskID)
	registry.Reserve(taskID)
	if registry.Len() != 1 {
		t.Fatalf("length = %d, want 1", registry.Len())
	}
	if !registry.Resolve(taskID) {
		t.Fatal("first resolve did not find the reservation")
	}
	if registry.Resolve(taskID) {
		t.Fatal("second resolve found an already resolved reservation")
	}
	if registry.Len() != 0 {
		t.Fatalf("length = %d, want 0", registry.Len())
	}
}

func TestPromotionRegistryCountsReservationsByTaskID(t *testing.T) {
	registry := NewPromotionRegistry()
	first := promotionTaskID(t, 1)
	second := promotionTaskID(t, 2)
	registry.Reserve(first)
	registry.Reserve(second)
	if registry.Len() != 2 {
		t.Fatalf("length = %d, want 2", registry.Len())
	}
	if !registry.Resolve(first) || registry.Len() != 1 {
		t.Fatalf("length after first resolve = %d, want 1", registry.Len())
	}
	if !registry.Resolve(second) || registry.Len() != 0 {
		t.Fatalf("length after second resolve = %d, want 0", registry.Len())
	}
}

func TestPromotionRegistrySupportsConcurrentAccess(t *testing.T) {
	const taskCount = 128
	registry := NewPromotionRegistry()
	ids := make([]domain.TaskID, taskCount)
	for index := range ids {
		ids[index] = promotionTaskID(t, index)
	}

	var wait sync.WaitGroup
	for _, taskID := range ids {
		wait.Add(1)
		go func() {
			defer wait.Done()
			registry.Reserve(taskID)
		}()
	}
	wait.Wait()
	if registry.Len() != taskCount {
		t.Fatalf("length after reserve = %d, want %d", registry.Len(), taskCount)
	}

	var resolved atomic.Int32
	for _, taskID := range ids {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if registry.Resolve(taskID) {
				resolved.Add(1)
			}
		}()
	}
	wait.Wait()
	if got := resolved.Load(); got != taskCount {
		t.Fatalf("resolved = %d, want %d", got, taskCount)
	}
	if registry.Len() != 0 {
		t.Fatalf("length after resolve = %d, want 0", registry.Len())
	}
}

func promotionTaskID(t *testing.T, index int) domain.TaskID {
	t.Helper()
	taskID, err := domain.NewTaskID(fmt.Sprintf("review-20260825-120000-a1b2-promotion-%03d", index))
	if err != nil {
		t.Fatal(err)
	}
	return taskID
}
