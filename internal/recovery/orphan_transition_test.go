package recovery

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// These coordinator tests deliberately use the real resume use case: its
// ownership acquisition must be the first and only recovery reservation.
func TestOrphanRecoveryCoordinatorDelegatesRecoveryOwnershipToResume(t *testing.T) {
	id := recoveryTestTaskID(t)
	resume, resumeStore, writer, recoverer, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, nil, RecoveryResult{})
	registry := NewRecoveryOwnershipRegistry()
	resume.WithRecoveryOwnership(registry)
	pending := &PendingReconciliationSet{}
	tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: resumeStore.snapshot}}
	coordinator := newOrphanRecoveryCoordinator(tasks, &adoptionReaderFake{}, &adoptionFinalizerFake{}, resume, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), slog.Default(), &adoptionOrphanResumeDispatcherFake{execute: true})

	result := coordinator.Handle(context.Background(), id, nil, time.Now())

	if !result.RecoveryStarted || registry.IsOwned(id) {
		t.Fatalf("result=%+v owned=%t", result, registry.IsOwned(id))
	}
	if recoverer.calls != 0 { // nil session reaches lost without launching resume.
		t.Fatalf("recoverer calls=%d, want 0", recoverer.calls)
	}
	if resumeStore.snapshot.State != domain.StateLost || writer.exitCodeCalls != 1 || writer.exitCode.Raw() != 1 {
		t.Fatalf("resume state=%s exit calls=%d exit=%d, want lost/1", resumeStore.snapshot.State, writer.exitCodeCalls, writer.exitCode.Raw())
	}
	if entries := pending.List(); len(entries) != 0 {
		t.Fatalf("pending=%+v, want empty", entries)
	}
}

func TestOrphanRecoveryCoordinatorTreatsConcurrentRecoveryAsBenign(t *testing.T) {
	id := recoveryTestTaskID(t)
	resume, resumeStore, _, _, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, nil, RecoveryResult{})
	registry := NewRecoveryOwnershipRegistry()
	resume.WithRecoveryOwnership(registry)
	release, acquired := registry.Acquire(id)
	if !acquired {
		t.Fatal("competing recovery did not acquire ownership")
	}
	defer release()
	pending := &PendingReconciliationSet{}
	tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: resumeStore.snapshot}}
	coordinator := newOrphanRecoveryCoordinator(tasks, &adoptionReaderFake{}, &adoptionFinalizerFake{}, resume, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), slog.Default(), &adoptionOrphanResumeDispatcherFake{execute: true})

	result := coordinator.Handle(context.Background(), id, nil, time.Now())

	if !result.RecoveryStarted || resumeStore.snapshot.State != domain.StateOrphaned || len(pending.List()) != 0 {
		t.Fatalf("result=%+v state=%s pending=%+v", result, resumeStore.snapshot.State, pending.List())
	}
}

func TestOrphanRecoveryCoordinatorReadFailureDefersWithoutResumeOrFinalize(t *testing.T) {
	id := recoveryTestTaskID(t)
	resume, resumeStore, _, recoverer, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, nil, RecoveryResult{})
	pending := &PendingReconciliationSet{}
	finalizer := &adoptionFinalizerFake{}
	tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: resumeStore.snapshot}}
	coordinator := newOrphanRecoveryCoordinator(tasks, &adoptionReaderFake{err: errors.New("temporary read")}, finalizer, resume, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), slog.Default(), &adoptionOrphanResumeDispatcherFake{execute: true})

	result := coordinator.Handle(context.Background(), id, nil, time.Now())

	if !result.Deferred || finalizer.calls != 0 || recoverer.calls != 0 || resumeStore.snapshot.State != domain.StateOrphaned {
		t.Fatalf("result=%+v finalize=%d resume=%d state=%s", result, finalizer.calls, recoverer.calls, resumeStore.snapshot.State)
	}
	entries := pending.List()
	if len(entries) != 1 || pendingDisposition(entries[0]) != PendingSendConfirmOnly {
		t.Fatalf("pending=%+v, want confirm-only", entries)
	}
}
