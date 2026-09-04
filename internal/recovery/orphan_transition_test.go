package recovery

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type orphanTransitionReader struct {
	present   bool
	err       error
	afterRead func()
}

func (r *orphanTransitionReader) ReadLastMessage(domain.TaskID) (bool, error) {
	if r.afterRead != nil {
		r.afterRead()
	}
	return r.present, r.err
}

func (*orphanTransitionReader) ReadStderrLog(domain.TaskID) ([]byte, error) { return nil, nil }

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

	result := coordinator.Handle(context.Background(), OrphanTransitionInput{TaskID: id, OccurredAt: time.Now()})

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

	result := coordinator.Handle(context.Background(), OrphanTransitionInput{TaskID: id, OccurredAt: time.Now()})

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

	result := coordinator.Handle(context.Background(), OrphanTransitionInput{TaskID: id, OccurredAt: time.Now()})

	if !result.Deferred || finalizer.calls != 0 || recoverer.calls != 0 || resumeStore.snapshot.State != domain.StateOrphaned {
		t.Fatalf("result=%+v finalize=%d resume=%d state=%s", result, finalizer.calls, recoverer.calls, resumeStore.snapshot.State)
	}
	entries := pending.List()
	if len(entries) != 1 || pendingDisposition(entries[0]) != PendingSendConfirmOnly {
		t.Fatalf("pending=%+v, want confirm-only", entries)
	}
}

func TestOrphanRecoveryCoordinatorPassesInputProvenanceToFinalizer(t *testing.T) {
	for _, adoptedAfterRestart := range []bool{false, true} {
		t.Run("adopted="+map[bool]string{false: "false", true: "true"}[adoptedAfterRestart], func(t *testing.T) {
			id := recoveryTestTaskID(t)
			resume, resumeStore, _, _, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, nil, RecoveryResult{})
			finalizer := &adoptionFinalizerFake{}
			coordinator := newOrphanRecoveryCoordinator(
				&adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: resumeStore.snapshot}},
				&adoptionReaderFake{present: true}, finalizer, resume, &PendingReconciliationSet{},
				&adoptionMutexFake{}, domain.ClockFunc(time.Now), slog.Default(), &adoptionOrphanResumeDispatcherFake{execute: true},
			)

			result := coordinator.Handle(context.Background(), OrphanTransitionInput{TaskID: id, AdoptedAfterRestart: adoptedAfterRestart, OccurredAt: time.Now()})
			if !result.Finalized || finalizer.calls != 1 || !finalizer.estimated || finalizer.adopted != adoptedAfterRestart {
				t.Fatalf("result=%+v calls=%d estimated=%t adopted=%t", result, finalizer.calls, finalizer.estimated, finalizer.adopted)
			}
		})
	}
}

func TestOrphanRecoveryCoordinatorCancellationStopsPostIOEffects(t *testing.T) {
	t.Run("after read failure skips failure reconciliation", func(t *testing.T) {
		id := recoveryTestTaskID(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resume, store, _, _, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, nil, RecoveryResult{})
		pending := &PendingReconciliationSet{}
		coordinator := newOrphanRecoveryCoordinator(&adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: store.snapshot}}, &orphanTransitionReader{err: errors.New("read"), afterRead: cancel}, &adoptionFinalizerFake{}, resume, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), slog.Default(), &adoptionOrphanResumeDispatcherFake{execute: true})

		coordinator.Handle(ctx, OrphanTransitionInput{TaskID: id, OccurredAt: time.Now()})

		if len(pending.List()) != 0 {
			t.Fatalf("pending=%+v", pending.List())
		}
	})

	t.Run("after successful read skips resume dispatch", func(t *testing.T) {
		id := recoveryTestTaskID(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resume, store, _, _, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, nil, RecoveryResult{})
		dispatcher := &adoptionOrphanResumeDispatcherFake{execute: true}
		coordinator := newOrphanRecoveryCoordinator(&adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: store.snapshot}}, &orphanTransitionReader{afterRead: cancel}, &adoptionFinalizerFake{}, resume, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), slog.Default(), dispatcher)

		result := coordinator.Handle(ctx, OrphanTransitionInput{TaskID: id, OccurredAt: time.Now()})

		if !result.Deferred || dispatcher.calls != 0 {
			t.Fatalf("result=%+v dispatches=%d", result, dispatcher.calls)
		}
	})

	t.Run("after resume failure skips failure reconciliation", func(t *testing.T) {
		id := recoveryTestTaskID(t)
		session := recoveryTestSession(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resume, store, _, recoverer, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, &session, RecoveryResult{})
		store.saveErr, store.saveErrOn = errors.New("terminal save"), 2
		recoverer.cancel = cancel
		pending := &PendingReconciliationSet{}
		coordinator := newOrphanRecoveryCoordinator(&adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: store.snapshot}}, &orphanTransitionReader{}, &adoptionFinalizerFake{}, resume, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), slog.Default(), &adoptionOrphanResumeDispatcherFake{execute: true})

		coordinator.Handle(ctx, OrphanTransitionInput{TaskID: id, SessionRef: &session, OccurredAt: time.Now()})

		if len(pending.List()) != 0 {
			t.Fatalf("pending=%+v", pending.List())
		}
	})

	t.Run("after successful dispatch keeps pending reconciliation", func(t *testing.T) {
		id := recoveryTestTaskID(t)
		session := recoveryTestSession(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resume, store, _, recoverer, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, &session, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)})
		recoverer.cancel = cancel
		pending := &PendingReconciliationSet{}
		if err := pending.Register(id, PendingSendConfirmOnly, nil); err != nil {
			t.Fatal(err)
		}
		coordinator := newOrphanRecoveryCoordinator(&adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: store.snapshot}}, &orphanTransitionReader{}, &adoptionFinalizerFake{}, resume, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), slog.Default(), &adoptionOrphanResumeDispatcherFake{execute: true})

		coordinator.Handle(ctx, OrphanTransitionInput{TaskID: id, SessionRef: &session, OccurredAt: time.Now()})

		if len(pending.List()) != 1 {
			t.Fatalf("pending=%+v", pending.List())
		}
	})
}
