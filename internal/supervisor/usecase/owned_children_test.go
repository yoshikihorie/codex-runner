package usecase

import (
	"context"
	"os"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

func TestTimedOutOwnedChildStillBlocksResume(t *testing.T) {
	id := supervisor.SessionID("owned-orphan-1")
	ownedChildren.Register(id, 61234)
	t.Cleanup(func() { ownedChildren.Remove(id, 61234) })
	transition, err := NewProcessResumeTransitionContext(func(int) bool { return true }, nil)(context.Background(), supervisor.ManagedSession{SessionID: id, ChildPID: 61234, State: supervisor.SessionStopped, StopReason: supervisor.StopOrphanChildAlive})
	if err != nil || !transition.OrphanChildAlive {
		t.Fatalf("transition=%+v err=%v", transition, err)
	}
}

func TestOwnedChildPIDs_SCNsup0544_Snapshot(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	id := supervisor.SessionID("alpha-1")
	ownedChildren.Register(id, 12345)
	t.Cleanup(func() { ownedChildren.Remove(id, 12345) })
	got := OwnedChildPIDs()
	if !got[12345] {
		t.Fatal("owned child missing")
	}
	delete(got, 12345)
	if !OwnedChildPIDs()[12345] {
		t.Fatal("snapshot changed registry")
	}
}

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
