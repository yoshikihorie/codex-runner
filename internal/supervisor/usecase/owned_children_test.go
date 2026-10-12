package usecase

import (
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

func TestOwnedChildRegistryRejectsStaleRemoval(t *testing.T) {
	r := &ownedChildRegistry{pids: make(map[supervisor.SessionID]int)}
	id := supervisor.SessionID("alpha-1")
	r.Register(id, 101)
	r.Register(id, 102)
	r.Remove(id, 101)
	if !r.Owns(id, 102) || r.Owns(id, 101) {
		t.Fatal("stale removal changed owner")
	}
	r.Remove(id, 102)
	if r.Owns(id, 102) {
		t.Fatal("owner remained after exit")
	}
}
