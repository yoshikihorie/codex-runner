package execution

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestLaunchingTaskRegistryStoresIndependentSnapshots(t *testing.T) {
	registry := NewLaunchingTaskRegistry()
	snapshot := launchingSnapshot(t, "independent")
	registry.Register(snapshot.TaskID, snapshot)

	*snapshot.PID = 99
	*snapshot.RequestedTimeoutSeconds = 3600
	*snapshot.ReasoningEffort = "low"
	stored, found := registry.Lookup(snapshot.TaskID)
	if !found || stored.PID == nil || *stored.PID != 42 || stored.RequestedTimeoutSeconds == nil || *stored.RequestedTimeoutSeconds != 1800 || stored.ReasoningEffort == nil || *stored.ReasoningEffort != "high" {
		t.Fatalf("stored=%#v found=%t", stored, found)
	}

	*stored.PID = 100
	*stored.RequestedTimeoutSeconds = 7200
	*stored.ReasoningEffort = "medium"
	again, found := registry.Lookup(snapshot.TaskID)
	if !found || again.PID == nil || *again.PID != 42 || again.RequestedTimeoutSeconds == nil || *again.RequestedTimeoutSeconds != 1800 || again.ReasoningEffort == nil || *again.ReasoningEffort != "high" {
		t.Fatalf("again=%#v found=%t", again, found)
	}
}

func TestLaunchingTaskRegistryLookupAndUnregister(t *testing.T) {
	registry := NewLaunchingTaskRegistry()
	snapshot := launchingSnapshot(t, "lookup")
	if got, found := registry.Lookup(snapshot.TaskID); found || got != (domain.TaskSnapshot{}) {
		t.Fatalf("got=%#v found=%t", got, found)
	}
	registry.Register(snapshot.TaskID, snapshot)
	registry.Unregister(snapshot.TaskID)
	if got, found := registry.Lookup(snapshot.TaskID); found || got != (domain.TaskSnapshot{}) {
		t.Fatalf("got=%#v found=%t", got, found)
	}
	registry.Unregister(snapshot.TaskID)
}

func TestLaunchingTaskRegistryReplacesSnapshot(t *testing.T) {
	registry := NewLaunchingTaskRegistry()
	first := launchingSnapshot(t, "first")
	second := launchingSnapshot(t, "second")
	second.TaskID = first.TaskID
	second.Model = "replacement"
	registry.Register(first.TaskID, first)
	registry.Register(first.TaskID, second)
	got, found := registry.Lookup(first.TaskID)
	if !found || got.Model != "replacement" {
		t.Fatalf("got=%#v found=%t", got, found)
	}
}

func TestLaunchingTaskRegistryConcurrentAccess(t *testing.T) {
	registry := NewLaunchingTaskRegistry()
	const workers = 32
	var group sync.WaitGroup
	for index := 0; index < workers; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			snapshot := launchingSnapshot(t, fmt.Sprintf("concurrent-%d", index))
			registry.Register(snapshot.TaskID, snapshot)
			got, found := registry.Lookup(snapshot.TaskID)
			if !found || got.TaskID != snapshot.TaskID {
				t.Errorf("got=%#v found=%t", got, found)
			}
			registry.Unregister(snapshot.TaskID)
		}(index)
	}
	group.Wait()
	for index := 0; index < workers; index++ {
		snapshot := launchingSnapshot(t, fmt.Sprintf("concurrent-%d", index))
		if got, found := registry.Lookup(snapshot.TaskID); found || got != (domain.TaskSnapshot{}) {
			t.Fatalf("got=%#v found=%t", got, found)
		}
	}
}

func launchingSnapshot(t *testing.T, suffix string) domain.TaskSnapshot {
	t.Helper()
	id, err := domain.NewTaskID("review-20260813-120000-a1b2-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	pid, timeout, effort := 42, 1800, "high"
	started, lastEvent := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC), time.Date(2026, 8, 13, 12, 1, 0, 0, time.UTC)
	session, err := domain.NewSessionRef("123e4567-e89b-12d3-a456-426614174000", started, false)
	if err != nil {
		t.Fatal(err)
	}
	exitCode, recoveryOrigin := domain.NewExitCode(1), domain.RecoveryOriginTimeout
	return domain.TaskSnapshot{
		TaskID: id, Subcommand: domain.SubcommandReview, PID: &pid, ProcessStartedAt: &started,
		ResolvedTimeoutSeconds: 1800, RequestedTimeoutSeconds: &timeout, Model: "model", ReasoningEffort: &effort,
		RequestedAt: started, Route: domain.ExecutionRouteDaemon, State: domain.StateQueued, StateUpdatedAt: started,
		SessionRef: &session, LastEventAt: &lastEvent, ExitCode: &exitCode, RecoveryOrigin: &recoveryOrigin, SchemaVersion: 1,
	}
}
