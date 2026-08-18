package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type reconcileStoreFake struct {
	mu      sync.Mutex
	entries map[domain.TaskID]domain.TaskSnapshot
	loads   int
	saves   int
	order   *[]string
	saveErr error
}

func (f *reconcileStoreFake) Load(id domain.TaskID) (domain.TaskSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	*f.order = append(*f.order, "load")
	snapshot, found := f.entries[id]
	if !found {
		return domain.TaskSnapshot{}, domain.ErrTaskNotFound
	}
	return snapshot, nil
}

func (f *reconcileStoreFake) Save(id domain.TaskID, snapshot domain.TaskSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saves++
	*f.order = append(*f.order, "save")
	if f.saveErr == nil {
		f.entries[id] = snapshot
	}
	return f.saveErr
}

func (*reconcileStoreFake) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	return nil, nil
}

type reconcileLivenessFake struct {
	dead    bool
	err     error
	called  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *reconcileLivenessFake) Execute(ctx context.Context, _ domain.TaskID) (bool, error) {
	f.once.Do(func() { close(f.called) })
	select {
	case <-f.release:
		return f.dead, f.err
	default:
	}
	select {
	case <-f.release:
		return f.dead, f.err
	case <-ctx.Done():
		select {
		case <-f.release:
			return f.dead, f.err
		default:
			return false, ctx.Err()
		}
	}
}

type reconcileReaderResult struct {
	present bool
	err     error
}

type reconcileReaderFake struct {
	present    bool
	err        error
	exitCode   int
	exitExists bool
	exitErr    error
	results    []reconcileReaderResult
	calls      int
	afterRead  func()
}

func (f *reconcileReaderFake) ReadLastMessage(domain.TaskID) (bool, error) {
	f.calls++
	if len(f.results) == 0 {
		present, err := f.present, f.err
		if err == nil && f.afterRead != nil {
			f.afterRead()
		}
		return present, err
	}
	result := f.results[0]
	f.results = f.results[1:]
	if result.err == nil && f.afterRead != nil {
		f.afterRead()
	}
	return result.present, result.err
}
func (*reconcileReaderFake) ReadStderrLog(domain.TaskID) ([]byte, error) { return nil, nil }
func (f *reconcileReaderFake) ReadExitCode(domain.TaskID) (int, bool, error) {
	return f.exitCode, f.exitExists, f.exitErr
}

type reconcileWriterFake struct {
	order            *[]string
	events           int
	exitCode         *domain.ExitCode
	adoptedMarkers   int
	recoveredMarkers int
	exitCodes        int
	appendErr        error
	adoptedErr       error
	recoveredErr     error
	exitErr          error
}

func (*reconcileWriterFake) WritePrompt(domain.TaskID, []byte) error         { return nil }
func (*reconcileWriterFake) WriteReviewInput(domain.TaskID, []byte) error    { return nil }
func (*reconcileWriterFake) WriteCombinedPrompt(domain.TaskID, []byte) error { return nil }
func (*reconcileWriterFake) OpenExecutionLogs(domain.TaskID) (*contract.ExecutionLogs, error) {
	return nil, nil
}
func (f *reconcileWriterFake) WriteExitCode(_ domain.TaskID, exitCode domain.ExitCode) error {
	f.exitCodes++
	f.exitCode = &exitCode
	*f.order = append(*f.order, "exit-code")
	return f.exitErr
}
func (*reconcileWriterFake) WritePartialOutput(domain.TaskID, string) error { return nil }
func (f *reconcileWriterFake) WriteRecoveredMarker(domain.TaskID, time.Time) error {
	f.recoveredMarkers++
	*f.order = append(*f.order, "recovered-marker")
	return f.recoveredErr
}
func (f *reconcileWriterFake) WriteAdoptedMarker(domain.TaskID, time.Time) error {
	f.adoptedMarkers++
	*f.order = append(*f.order, "adopted-marker")
	return f.adoptedErr
}
func (f *reconcileWriterFake) AppendEvent(domain.TaskID, domain.Event) error {
	f.events++
	*f.order = append(*f.order, "event")
	return f.appendErr
}
func (*reconcileWriterFake) AppendRawEvent(domain.TaskID, string, json.RawMessage) error {
	return nil
}

type reconcileTerminationFake struct {
	confirmDead  bool
	sendDead     bool
	confirm      int
	send         int
	grace        time.Duration
	called       chan struct{}
	release      chan struct{}
	once         sync.Once
	finished     chan struct{}
	finishedOnce sync.Once
	terminateErr error
	confirmErr   error
	authority    ProcessSignalAuthority
}

func (f *reconcileTerminationFake) Confirm(context.Context, domain.TaskID) (bool, error) {
	f.confirm++
	f.once.Do(func() { close(f.called) })
	<-f.release
	f.finishedOnce.Do(func() { close(f.finished) })
	return f.confirmDead, nil
}
func (f *reconcileTerminationFake) SendAndConfirm(_ context.Context, _ domain.TaskID, authority ProcessSignalAuthority, grace time.Duration) TerminationAttemptResult {
	f.send++
	f.authority = authority
	f.grace = grace
	f.once.Do(func() { close(f.called) })
	<-f.release
	f.finishedOnce.Do(func() { close(f.finished) })
	return TerminationAttemptResult{Dead: f.sendDead, TerminateErr: f.terminateErr, ConfirmErr: f.confirmErr}
}

func TestReconcileTerminationKeepsClaimAuthority(t *testing.T) {
	id := adoptionID(t, "authority")
	started := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	generation := domain.LifecycleGeneration(2)
	authority := ProcessSignalAuthority{TaskID: id, PID: 4321, ProcessStartedAt: started, ExpectedState: domain.StateCancelling, LifecycleGeneration: &generation}
	termination := &reconcileTerminationFake{called: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	close(termination.release)
	termination.SendAndConfirm(context.Background(), id, authority, time.Second)
	if termination.authority != authority {
		t.Fatalf("authority=%#v want=%#v", termination.authority, authority)
	}
}

type reconcileKilledFake struct {
	calls       int
	rawExitCode int
}

func (f *reconcileKilledFake) ConfirmKilled(_ context.Context, _ domain.TaskID, rawExitCode int, _ bool, _ time.Time) error {
	f.calls++
	f.rawExitCode = rawExitCode
	return nil
}

type reconcilePathLocksFake struct {
	order *[]string
	calls int
}

func (f *reconcilePathLocksFake) Release(context.Context, domain.TaskID) error {
	f.calls++
	*f.order = append(*f.order, "path-lock")
	return nil
}

type reconcileSlotsFake struct {
	order *[]string
	calls int
}

func (f *reconcileSlotsFake) ReleaseAndAdvance(context.Context, domain.TaskID, time.Time) {
	f.calls++
	*f.order = append(*f.order, "slot")
}

type reconcileMutexFake struct {
	order *[]string
	held  bool
}

func (f *reconcileMutexFake) Lock(domain.TaskID) {
	if f.held {
		panic("reconcile task mutex double lock")
	}
	f.held = true
	*f.order = append(*f.order, "lock")
}
func (f *reconcileMutexFake) Unlock(domain.TaskID) {
	if !f.held {
		panic("reconcile task mutex unlock without lock")
	}
	f.held = false
	*f.order = append(*f.order, "unlock")
}

func newReconcileFixture(t *testing.T, state domain.TaskState, dead bool, present bool) (*ReconcilePendingUseCase, *PendingReconciliationSet, *reconcileStoreFake, *reconcileLivenessFake, *reconcileWriterFake, *reconcileTerminationFake, *reconcileKilledFake, *reconcilePathLocksFake, *reconcileSlotsFake, *reconcileMutexFake, *[]string) {
	t.Helper()
	id := adoptionID(t, "reconcile-"+string(state))
	snapshot := adoptionSnapshot(t, id, state)
	order := []string{}
	pending := &PendingReconciliationSet{}
	tasks := &reconcileStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, order: &order}
	liveness := &reconcileLivenessFake{dead: dead, called: make(chan struct{}), release: make(chan struct{}, 1)}
	writer := &reconcileWriterFake{order: &order}
	termination := &reconcileTerminationFake{called: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	killed := &reconcileKilledFake{}
	pathLocks := &reconcilePathLocksFake{order: &order}
	slots := &reconcileSlotsFake{order: &order}
	mutex := &reconcileMutexFake{order: &order}
	uc := NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{present: present}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, newAdoptionResumeUseCase(t), slots, mutex, domain.ClockFunc(time.Now), time.Nanosecond, 17*time.Second, slog.Default())
	return uc, pending, tasks, liveness, writer, termination, killed, pathLocks, slots, mutex, &order
}

type reconciliationRun struct {
	releaseLiveness func()
	stop            func()
}

func registerReconcilePending(t *testing.T, pending *PendingReconciliationSet, snapshot domain.TaskSnapshot, disposition PendingSendDisposition) {
	t.Helper()
	var authority *ProcessSignalAuthority
	if disposition == PendingSendUnsent {
		value, ok := processSignalAuthority(snapshot)
		if !ok {
			t.Fatal("unsent pending test entry requires authority")
		}
		authority = &value
	}
	if err := pending.Register(snapshot.TaskID, disposition, authority); err != nil {
		t.Fatal(err)
	}
}

func startReconciliation(t *testing.T, uc *ReconcilePendingUseCase, liveness *reconcileLivenessFake) reconciliationRun {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		uc.Run(ctx)
		close(done)
	}()
	select {
	case <-liveness.called:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not begin")
	}
	return reconciliationRun{
		releaseLiveness: func() {
			liveness.release <- struct{}{}
		},
		stop: func() {
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("reconciliation did not stop after context cancellation")
			}
		},
	}
}

func runReconciliation(t *testing.T, uc *ReconcilePendingUseCase, liveness *reconcileLivenessFake) func() {
	t.Helper()
	entries := uc.pending.List()
	if len(entries) != 1 {
		t.Fatalf("pending entries=%+v, want one", entries)
	}
	done := make(chan struct{})
	go func() {
		uc.reconcileOne(context.Background(), entries[0])
		close(done)
	}()
	select {
	case <-liveness.called:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not begin")
	}
	return func() {
		liveness.release <- struct{}{}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("reconciliation did not finish")
		}
	}
}

func TestReconcilePendingRetainsLiveRecoveringTask(t *testing.T) {
	uc, pending, tasks, liveness, _, _, _, _, _, _, _ := newReconcileFixture(t, domain.StateRecovering, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
	finish := runReconciliation(t, uc, liveness)
	finish()
	if entries := pending.List(); len(entries) != 1 || entries[0].taskID != id {
		t.Fatalf("pending=%+v, want live recovering task retained", entries)
	}
}

func TestReconcilePendingCompletesDeadRecoveringTaskBeforeReleasingSlot(t *testing.T) {
	uc, pending, tasks, liveness, writer, _, _, _, slots, mutex, order := newReconcileFixture(t, domain.StateRecovering, true, true)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
	finish := runReconciliation(t, uc, liveness)
	finish()
	if got := tasks.entries[id].State; got != domain.StateRecovered {
		t.Fatalf("state=%q, want %q", got, domain.StateRecovered)
	}
	if mutex.held || writer.events == 0 || slots.calls != 1 || len(pending.List()) != 0 {
		t.Fatalf("held=%v events=%d slots=%d pending=%+v", mutex.held, writer.events, slots.calls, pending.List())
	}
	want := []string{"lock", "load", "unlock", "lock", "load", "adopted-marker", "recovered-marker", "exit-code", "save", "event", "unlock", "slot"}
	if len(*order) < len(want) {
		t.Fatalf("order=%v, want prefix %v", *order, want)
	}
	for i, item := range want {
		if (*order)[i] != item {
			t.Fatalf("order=%v, want prefix %v", *order, want)
		}
	}
}

func TestReconcilePendingFailsDeadRecoveringTaskWithoutOutput(t *testing.T) {
	uc, pending, tasks, liveness, writer, _, _, _, slots, mutex, _ := newReconcileFixture(t, domain.StateRecovering, true, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
	finish := runReconciliation(t, uc, liveness)
	finish()
	if got := tasks.entries[id].State; got != domain.StateTimeoutLost {
		t.Fatalf("state=%q, want %q", got, domain.StateTimeoutLost)
	}
	if mutex.held || writer.events == 0 || slots.calls != 1 || len(pending.List()) != 0 {
		t.Fatalf("held=%v events=%d slots=%d pending=%+v", mutex.held, writer.events, slots.calls, pending.List())
	}
	if writer.adoptedMarkers != 1 || writer.recoveredMarkers != 0 || writer.exitCodes != 1 || writer.exitCode == nil || writer.exitCode.Raw() != 1 || writer.exitCode.Class() != domain.ExitCodeClassFailure || !tasks.entries[id].AdoptedAfterRestart {
		t.Fatalf("writer=%+v snapshot=%+v", writer, tasks.entries[id])
	}
}

func TestReconcilePendingOrphanReadErrorRetriesFromReadLastMessage(t *testing.T) {
	id := adoptionID(t, "reconcile-orphan-read-retry")
	snapshot := adoptionSnapshot(t, id, domain.StateOrphaned)
	order := []string{}
	pending := &PendingReconciliationSet{}
	registerReconcilePending(t, pending, snapshot, PendingSendConfirmOnly)
	tasks := &reconcileStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, order: &order}
	reader := &reconcileReaderFake{results: []reconcileReaderResult{{err: errors.New("read")}, {present: true}}}
	finalizer := &adoptionFinalizerFake{}
	uc := NewReconcilePendingUseCase(pending, tasks, &reconcileLivenessFake{called: make(chan struct{}), release: make(chan struct{})}, reader, &reconcileWriterFake{order: &order}, finalizer, &reconcileTerminationFake{called: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}, &reconcileKilledFake{}, &reconcilePathLocksFake{order: &order}, newAdoptionResumeUseCase(t), &reconcileSlotsFake{order: &order}, &reconcileMutexFake{order: &order}, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())

	entries := pending.List()
	uc.reconcileOne(context.Background(), entries[0])
	entries = pending.List()
	if reader.calls != 1 || finalizer.calls != 0 || len(entries) != 1 || pendingDisposition(entries[0]) != PendingSendConfirmOnly {
		t.Fatalf("reader=%d finalizer=%d pending=%+v", reader.calls, finalizer.calls, entries)
	}

	entries = pending.List()
	uc.reconcileOne(context.Background(), entries[0])
	if reader.calls != 2 || finalizer.calls != 1 || len(pending.List()) != 0 {
		t.Fatalf("reader=%d finalizer=%d pending=%+v", reader.calls, finalizer.calls, pending.List())
	}
}

func TestReconcilePendingRecoveringNonSaveWriteFailuresAreFailSoft(t *testing.T) {
	for _, tc := range []struct {
		name      string
		present   bool
		configure func(*reconcileStoreFake, *reconcileWriterFake)
	}{
		{"adopted marker", true, func(_ *reconcileStoreFake, writer *reconcileWriterFake) { writer.adoptedErr = errors.New("adopted") }},
		{"recovered marker", true, func(_ *reconcileStoreFake, writer *reconcileWriterFake) {
			writer.recoveredErr = errors.New("recovered")
		}},
		{"exit code", true, func(_ *reconcileStoreFake, writer *reconcileWriterFake) { writer.exitErr = errors.New("exit") }},
		{"append event", true, func(_ *reconcileStoreFake, writer *reconcileWriterFake) { writer.appendErr = errors.New("event") }},
		{"failure exit code", false, func(_ *reconcileStoreFake, writer *reconcileWriterFake) { writer.exitErr = errors.New("exit") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc, pending, tasks, liveness, writer, _, _, _, slots, mutex, _ := newReconcileFixture(t, domain.StateRecovering, true, tc.present)
			var logs bytes.Buffer
			uc.logger = slog.New(slog.NewTextHandler(&logs, nil))
			tc.configure(tasks, writer)
			id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
			registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
			finish := runReconciliation(t, uc, liveness)
			finish()
			if mutex.held || slots.calls != 1 || len(pending.List()) != 0 || writer.events == 0 || logs.Len() == 0 {
				t.Fatalf("held=%v slots=%d pending=%+v events=%d logs=%q", mutex.held, slots.calls, pending.List(), writer.events, logs.String())
			}
		})
	}
}

func TestReconcilePendingRecoveringSaveFailureRetainsPending(t *testing.T) {
	uc, pending, tasks, liveness, writer, _, _, _, slots, mutex, _ := newReconcileFixture(t, domain.StateRecovering, true, true)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	tasks.saveErr = errors.New("save")
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
	finish := runReconciliation(t, uc, liveness)
	finish()

	entries := pending.List()
	if mutex.held || slots.calls != 0 || tasks.entries[id].State != domain.StateRecovering || len(entries) != 1 || pendingDisposition(entries[0]) != PendingSendConfirmOnly || writer.events != 0 {
		t.Fatalf("held=%v slots=%d state=%s pending=%+v events=%d", mutex.held, slots.calls, tasks.entries[id].State, entries, writer.events)
	}
}

func TestNewReconcilePendingUseCasePanicsForNonPositiveTerminationGrace(t *testing.T) {
	for _, grace := range []time.Duration{0, -time.Second} {
		t.Run(grace.String(), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewReconcilePendingUseCase did not panic")
				}
			}()
			uc, pending, tasks, liveness, writer, termination, killed, pathLocks, slots, mutex, _ := newReconcileFixture(t, domain.StateRecovering, false, false)
			_ = uc
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, newAdoptionResumeUseCase(t), slots, mutex, domain.ClockFunc(time.Now), time.Second, grace, slog.Default())
		})
	}
}

func TestNewReconcilePendingUseCaseRejectsTypedNilDependencies(t *testing.T) {
	uc, pending, tasks, liveness, writer, termination, killed, pathLocks, slots, mutex, _ := newReconcileFixture(t, domain.StateRecovering, false, false)
	_ = uc
	resume := newAdoptionResumeUseCase(t)
	for _, tc := range []struct {
		name string
		make func()
	}{
		{"pending", func() {
			NewReconcilePendingUseCase((*PendingReconciliationSet)(nil), tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"tasks", func() {
			NewReconcilePendingUseCase(pending, (*reconcileStoreFake)(nil), liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"liveness", func() {
			NewReconcilePendingUseCase(pending, tasks, (*reconcileLivenessFake)(nil), &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"reader", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, (*reconcileReaderFake)(nil), writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"writer", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, (*reconcileWriterFake)(nil), &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"finalizer", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, (*adoptionFinalizerFake)(nil), termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"termination", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, (*reconcileTerminationFake)(nil), killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"killed", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, (*reconcileKilledFake)(nil), pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"path-locks", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, (*reconcilePathLocksFake)(nil), resume, slots, mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"resume", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, (*RecoverViaResumeUseCase)(nil), slots, mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"slots", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, (*reconcileSlotsFake)(nil), mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"task-mutex", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, (*reconcileMutexFake)(nil), domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
		}},
		{"clock", func() {
			var clock domain.ClockFunc
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, clock, time.Second, time.Second, slog.Default())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.make()
		})
	}
}

func TestReconcilePendingUsesExistingExitCodeForConfirmedCancellation(t *testing.T) {
	uc, pending, tasks, _, _, _, killed, _, _, _, _ := newReconcileFixture(t, domain.StateCancelling, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateCancelling))
	uc.reader = &reconcileReaderFake{exitCode: 23, exitExists: true}
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)

	uc.completeTerminated(context.Background(), id, tasks.entries[id], false, time.Now())
	if killed.calls != 1 || killed.rawExitCode != 23 || len(pending.List()) != 0 {
		t.Fatalf("calls=%d rawExitCode=%d pending=%+v", killed.calls, killed.rawExitCode, pending.List())
	}
}

func TestReconcilePendingRecoveryWriteOrder(t *testing.T) {
	for _, tc := range []struct {
		name    string
		present bool
		want    []string
	}{
		{"success", true, []string{"lock", "load", "unlock", "lock", "load", "adopted-marker", "recovered-marker", "exit-code", "save", "event", "unlock", "slot"}},
		{"failure", false, []string{"lock", "load", "unlock", "lock", "load", "adopted-marker", "exit-code", "save", "event", "unlock", "slot"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc, pending, tasks, liveness, _, _, _, _, _, _, order := newReconcileFixture(t, domain.StateRecovering, true, tc.present)
			id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
			registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
			finish := runReconciliation(t, uc, liveness)
			finish()
			if !reflect.DeepEqual(*order, tc.want) {
				t.Fatalf("order=%v, want %v", *order, tc.want)
			}
		})
	}
}

func TestReconcilePendingTimeoutClaimsBeforeSendingAndUsesTerminationGrace(t *testing.T) {
	uc, pending, tasks, _, _, termination, _, _, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateTimeout))
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendUnsent)
	entries := pending.List()
	done := make(chan struct{})
	go func() {
		uc.reconcileOne(context.Background(), entries[0])
		close(done)
	}()
	select {
	case <-termination.called:
	case <-time.After(time.Second):
		t.Fatal("termination send did not begin")
	}
	if termination.send != 1 || termination.confirm != 0 || termination.grace != 17*time.Second {
		t.Fatalf("send=%d confirm=%d grace=%s", termination.send, termination.confirm, termination.grace)
	}
	close(termination.release)
	select {
	case <-termination.finished:
	case <-time.After(time.Second):
		t.Fatal("termination send did not finish")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not return after termination send")
	}
}

func TestReconcilePendingTimeoutOnlyConfirmsAfterSignalWasSent(t *testing.T) {
	uc, pending, tasks, _, _, termination, _, _, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateTimeout))
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendSent)
	entries := pending.List()
	done := make(chan struct{})
	go func() {
		uc.reconcileOne(context.Background(), entries[0])
		close(done)
	}()
	select {
	case <-termination.called:
	case <-time.After(time.Second):
		t.Fatal("termination confirmation did not begin")
	}
	if termination.send != 0 || termination.confirm != 1 {
		t.Fatalf("send=%d confirm=%d", termination.send, termination.confirm)
	}
	close(termination.release)
	select {
	case <-termination.finished:
	case <-time.After(time.Second):
		t.Fatal("termination confirmation did not finish")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not return after termination confirmation")
	}
}

func TestReconcilePendingCancellingConfirmsKilledAfterDeath(t *testing.T) {
	uc, pending, tasks, _, _, termination, killed, _, _, _, _ := newReconcileFixture(t, domain.StateCancelling, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateCancelling))
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendSent)
	termination.confirmDead = true
	entries := pending.List()
	done := make(chan struct{})
	go func() {
		uc.reconcileOne(context.Background(), entries[0])
		close(done)
	}()
	select {
	case <-termination.called:
	case <-time.After(time.Second):
		t.Fatal("termination confirmation did not begin")
	}
	close(termination.release)
	select {
	case <-termination.finished:
	case <-time.After(time.Second):
		t.Fatal("termination confirmation did not finish")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not return after cancellation confirmation")
	}
	if killed.calls != 1 || len(pending.List()) != 0 {
		t.Fatalf("killed=%d pending=%+v", killed.calls, pending.List())
	}
}

func TestReconcilePendingAfterConfirmedDeathDoesNotRestoreSendAuthority(t *testing.T) {
	uc, pending, tasks, _, _, termination, _, _, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateTimeout))
	snapshot := tasks.entries[id]
	registerReconcilePending(t, pending, snapshot, PendingSendUnsent)

	uc.reconcilePendingAfterConfirmedDeath(id)

	entries := pending.List()
	if len(entries) != 1 || pendingDisposition(entries[0]) != PendingSendConfirmOnly || entries[0].authority != (ProcessSignalAuthority{}) {
		t.Fatalf("pending=%+v, want confirm-only without authority", entries)
	}
	authority, ok := processSignalAuthority(snapshot)
	if !ok {
		t.Fatal("fixture requires signal authority")
	}
	claim, outcome := pending.ClaimForSend(id, authority)
	if outcome != ClaimConfirmOnly || claim.Token != 0 || termination.send != 0 {
		t.Fatalf("claim=%+v outcome=%v sends=%d, want no resend claim", claim, outcome, termination.send)
	}
}

func TestReconcilePendingTimeoutDispatchRemovesPendingAfterResumeSucceeds(t *testing.T) {
	uc, pending, tasks, _, _, _, _, pathLocks, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateTimeout))
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)

	uc.completeTerminated(context.Background(), id, tasks.entries[id], true, time.Now())
	waitForPendingRemoval(t, pending)

	if pathLocks.calls != 1 || len(pending.List()) != 0 {
		t.Fatalf("pathLocks=%d pending=%+v", pathLocks.calls, pending.List())
	}
}

func TestTimeoutRecoveryResolvesClaimAfterResumeSucceedsOrFails(t *testing.T) {
	_, testFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve test file path")
	}

	for _, tc := range []struct {
		name     string
		source   string
		function string
	}{
		{name: "adoption", source: "adopt.go", function: "confirmTimeoutTermination"},
		{name: "reconciliation", source: "reconcile.go", function: "completeTerminated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sourcePath := filepath.Join(filepath.Dir(testFile), tc.source)
			assertNoUnconditionalPendingRemovalBeforeDispatch(t, sourcePath, tc.function)
			assertClaimResolutionAfterExecuteInResumeRecovery(t, sourcePath)
		})
	}
}

func assertNoUnconditionalPendingRemovalBeforeDispatch(t *testing.T, sourcePath, functionName string) {
	t.Helper()
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, sourcePath, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	var dispatchPos token.Pos
	var functionBody *ast.BlockStmt
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != functionName {
			continue
		}
		functionBody = function.Body
		ast.Inspect(function.Body, func(node ast.Node) bool {
			dispatch, ok := node.(*ast.GoStmt)
			if ok && isResumeRecoveryCall(dispatch.Call) {
				dispatchPos = dispatch.Pos()
			}
			return true
		})
	}
	if functionBody == nil || !dispatchPos.IsValid() {
		t.Fatalf("resume dispatch not found in %s", sourcePath)
	}
	ast.Inspect(functionBody, func(node ast.Node) bool {
		expression, ok := node.(*ast.ExprStmt)
		if ok && expression.Pos() < dispatchPos && isPendingRemoveCall(expression.X) {
			t.Fatalf("pending removal before resume dispatch in %s", sourcePath)
		}
		return true
	})
}

func assertClaimResolutionAfterExecuteInResumeRecovery(t *testing.T, sourcePath string) {
	t.Helper()
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, sourcePath, nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	var executePos token.Pos
	var resolutionPositions []token.Pos
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || function.Name.Name != "resumeRecovery" {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if isResumeExecuteCall(call) {
				executePos = call.Pos()
			}
			if isClaimResolutionCall(call) {
				resolutionPositions = append(resolutionPositions, call.Pos())
			}
			return true
		})
	}
	if !executePos.IsValid() {
		t.Fatalf("resume Execute not found in %s", sourcePath)
	}
	for _, resolutionPos := range resolutionPositions {
		if resolutionPos > executePos {
			return
		}
	}
	t.Fatalf("claim resolution after resume Execute not found in %s", sourcePath)
}

func isPendingRemoveCall(expression ast.Expr) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Remove" {
		return false
	}
	pending, ok := selector.X.(*ast.SelectorExpr)
	return ok && pending.Sel.Name == "pending"
}

func isResumeExecuteCall(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Execute" {
		return false
	}
	resume, ok := selector.X.(*ast.SelectorExpr)
	return ok && resume.Sel.Name == "resume"
}

func isClaimResolutionCall(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && (selector.Sel.Name == "RemoveClaim" || selector.Sel.Name == "InvalidateSend")
}

func isResumeRecoveryCall(call *ast.CallExpr) bool {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	return ok && selector.Sel.Name == "resumeRecovery"
}

func TestReconcilePendingWithoutPIDConfirmsInsteadOfSending(t *testing.T) {
	uc, pending, tasks, _, _, termination, _, _, _, _, _ := newReconcileFixture(t, domain.StateCancelling, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateCancelling))
	snapshot := tasks.entries[id]
	snapshot.PID = nil
	snapshot.ProcessStartedAt = nil
	snapshot.AdoptedAfterRestart = true
	tasks.entries[id] = snapshot
	registerReconcilePending(t, pending, snapshot, PendingSendConfirmOnly)
	entries := pending.List()
	done := make(chan struct{})
	go func() {
		uc.reconcileOne(context.Background(), entries[0])
		close(done)
	}()
	select {
	case <-termination.called:
	case <-time.After(time.Second):
		t.Fatal("termination confirmation did not begin")
	}
	close(termination.release)
	select {
	case <-termination.finished:
	case <-time.After(time.Second):
		t.Fatal("termination confirmation did not finish")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not return without a process ID")
	}
	if termination.send != 0 || termination.confirm != 1 {
		t.Fatalf("send=%d confirm=%d", termination.send, termination.confirm)
	}
}

func TestReconcilePendingStopsWhenContextIsCancelled(t *testing.T) {
	uc, pending, tasks, liveness, _, _, _, _, _, _, _ := newReconcileFixture(t, domain.StateRecovering, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
	run := startReconciliation(t, uc, liveness)
	run.stop()
}

func TestReconcilePendingCancellationStopsBeforeNextEntryLoad(t *testing.T) {
	first := adoptionID(t, "reconcile-cancel-first")
	second := adoptionID(t, "reconcile-cancel-second")
	firstSnapshot := adoptionSnapshot(t, first, domain.StateRecovering)
	secondSnapshot := adoptionSnapshot(t, second, domain.StateRecovering)
	order := []string{}
	pending := &PendingReconciliationSet{}
	registerReconcilePending(t, pending, firstSnapshot, PendingSendConfirmOnly)
	registerReconcilePending(t, pending, secondSnapshot, PendingSendConfirmOnly)
	tasks := &reconcileStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{first: firstSnapshot, second: secondSnapshot}, order: &order}
	liveness := &reconcileLivenessFake{called: make(chan struct{}), release: make(chan struct{}, 1)}
	termination := &reconcileTerminationFake{called: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}
	uc := NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, &reconcileWriterFake{order: &order}, &adoptionFinalizerFake{}, termination, &reconcileKilledFake{}, &reconcilePathLocksFake{order: &order}, newAdoptionResumeUseCase(t), &reconcileSlotsFake{order: &order}, &reconcileMutexFake{order: &order}, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		uc.reconcileTick(ctx)
		close(done)
	}()
	<-liveness.called
	cancel()
	liveness.release <- struct{}{}
	<-done
	if tasks.loads != 1 || termination.confirm != 0 || termination.send != 0 {
		t.Fatalf("loads=%d confirm=%d send=%d", tasks.loads, termination.confirm, termination.send)
	}
}

func TestReconcilePendingRecoveringCancellationAfterReadStopsBeforeLock(t *testing.T) {
	id := adoptionID(t, "reconcile-cancel-after-read")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	order := []string{}
	pending := &PendingReconciliationSet{}
	registerReconcilePending(t, pending, snapshot, PendingSendConfirmOnly)
	tasks := &reconcileStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, order: &order}
	ctx, cancel := context.WithCancel(context.Background())
	reader := &reconcileReaderFake{present: true, afterRead: cancel}
	writer := &reconcileWriterFake{order: &order}
	slots := &reconcileSlotsFake{order: &order}
	mutex := &reconcileMutexFake{order: &order}
	liveness := &reconcileLivenessFake{dead: true, called: make(chan struct{}), release: make(chan struct{}, 1)}
	liveness.release <- struct{}{}
	uc := NewReconcilePendingUseCase(pending, tasks, liveness, reader, writer, &adoptionFinalizerFake{}, &reconcileTerminationFake{called: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}, &reconcileKilledFake{}, &reconcilePathLocksFake{order: &order}, newAdoptionResumeUseCase(t), slots, mutex, domain.ClockFunc(time.Now), time.Second, time.Second, slog.Default())

	uc.reconcileRecovering(ctx, id)

	if reader.calls != 1 || tasks.loads != 0 || tasks.saves != 0 || mutex.held || len(order) != 0 || slots.calls != 0 || len(pending.List()) != 1 {
		t.Fatalf("reader=%d loads=%d saves=%d held=%t order=%v slots=%d pending=%+v", reader.calls, tasks.loads, tasks.saves, mutex.held, order, slots.calls, pending.List())
	}
	if writer.adoptedMarkers != 0 || writer.recoveredMarkers != 0 || writer.exitCodes != 0 || writer.events != 0 {
		t.Fatalf("writer=%+v", writer)
	}
}

var _ ContractReader = (*reconcileReaderFake)(nil)
var _ contract.ContractWriter = (*reconcileWriterFake)(nil)
