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
	registry.Add(first, domain.SubcommandImpl)
	registry.Add(first, domain.SubcommandImpl)
	if registry.Size() != 1 || registry.ImplSize() != 1 {
		t.Fatalf("size=%d impl_size=%d", registry.Size(), registry.ImplSize())
	}
	registry.Remove(second)
	if registry.Size() != 1 || registry.ImplSize() != 1 {
		t.Fatalf("size=%d impl_size=%d after unknown remove", registry.Size(), registry.ImplSize())
	}
	registry.Reset(map[domain.TaskID]domain.Subcommand{first: domain.SubcommandImpl, second: domain.SubcommandReview})
	if registry.Size() != 2 || registry.ImplSize() != 1 {
		t.Fatalf("size=%d impl_size=%d after reset", registry.Size(), registry.ImplSize())
	}
	registry.Remove(first)
	if registry.Size() != 1 || registry.ImplSize() != 0 {
		t.Fatalf("size=%d impl_size=%d after impl remove", registry.Size(), registry.ImplSize())
	}
	var _ recovery.SlotResetter = registry
}

func TestActiveTaskRegistryResetRestoresImplCountForSCNQueue0122(t *testing.T) {
	registry := NewActiveTaskRegistry()
	first := registryTestID(t, "scn22-first")
	second := registryTestID(t, "scn22-second")
	review, err := domain.NewTaskID("review-20260809-120000-a1b2-scn22-review")
	if err != nil {
		t.Fatal(err)
	}

	registry.Reset(map[domain.TaskID]domain.Subcommand{
		first:  domain.SubcommandImpl,
		second: domain.SubcommandImpl,
		review: domain.SubcommandReview,
	})
	if registry.Size() != 3 || registry.ImplSize() != 2 {
		t.Fatalf("size=%d impl_size=%d", registry.Size(), registry.ImplSize())
	}
}
