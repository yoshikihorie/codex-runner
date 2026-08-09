package execution

import (
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
)

func registryTestID(t *testing.T, suffix string) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260809-120000-a1b2-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestActiveTaskRegistrySetOperations(t *testing.T) {
	registry := NewActiveTaskRegistry()
	first, second := registryTestID(t, "first"), registryTestID(t, "second")
	registry.Add(first)
	registry.Add(first)
	if registry.Size() != 1 {
		t.Fatalf("size = %d", registry.Size())
	}
	registry.Remove(second)
	registry.Reset([]domain.TaskID{first, second})
	if registry.Size() != 2 {
		t.Fatalf("size after reset = %d", registry.Size())
	}
	var _ recovery.SlotResetter = registry
}
