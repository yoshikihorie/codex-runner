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
	"github.com/yoshikihorie/codex-runner/internal/metrics"
)

type reconcileStoreFake struct {
	mu          sync.Mutex
	entries     map[domain.TaskID]domain.TaskSnapshot
	loads       int
	saves       int
	order       *[]string
	saveErr     error
	loadResults map[domain.TaskID][]reconcileLoadResult
}

type reconcileLoadResult struct {
	snapshot domain.TaskSnapshot
	err      error
}

func (f *reconcileStoreFake) Load(id domain.TaskID) (domain.TaskSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	*f.order = append(*f.order, "load")
	if results := f.loadResults[id]; len(results) > 0 {
		result := results[0]
		f.loadResults[id] = results[1:]
		return result.snapshot, result.err
	}
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

type reconcileOwnershipProbe struct {
	registry     RecoveryOwnershipRegistry
	mu           sync.Mutex
	acquireCalls int
	acquired     chan struct{}
	once         sync.Once
}

func (p *reconcileOwnershipProbe) Acquire(taskID domain.TaskID) (func(), bool) {
	p.mu.Lock()
	p.acquireCalls++
	p.mu.Unlock()
	release, acquired := p.registry.Acquire(taskID)
	if acquired {
		p.once.Do(func() { close(p.acquired) })
	}
	return release, acquired
}

func (p *reconcileOwnershipProbe) IsOwned(taskID domain.TaskID) bool {
	return p.registry.IsOwned(taskID)
}

func (p *reconcileOwnershipProbe) calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acquireCalls
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
	confirmDead    bool
	sendDead       bool
	confirm        int
	send           int
	grace          time.Duration
	called         chan struct{}
	release        chan struct{}
	once           sync.Once
	finished       chan struct{}
	finishedOnce   sync.Once
	terminateErr   error
	confirmErr     error
	confirmOnlyErr error
	authority      ProcessSignalAuthority
}

func (f *reconcileTerminationFake) Confirm(ctx context.Context, _ domain.TaskID) (bool, error) {
	f.confirm++
	f.once.Do(func() { close(f.called) })
	select {
	case <-f.release:
		f.finishedOnce.Do(func() { close(f.finished) })
		return f.confirmDead, f.confirmOnlyErr
	case <-ctx.Done():
		return false, ctx.Err()
	}
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

type reconcileKilledFake struct {
	calls       int
	rawExitCode int
	err         error
}

func (f *reconcileKilledFake) ConfirmKilled(_ context.Context, _ domain.TaskID, rawExitCode int, _ bool, _ time.Time) error {
	f.calls++
	f.rawExitCode = rawExitCode
	return f.err
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

type reconcileTrackerProbe struct {
	*adoptionStalledTrackerFake
	mutex             *reconcileMutexFake
	calledWhileLocked bool
	order             *[]string
}

func (f *reconcileTrackerProbe) TakeTotal(id domain.TaskID) int {
	f.calledWhileLocked = f.calledWhileLocked || f.mutex.held
	if f.order != nil {
		*f.order = append(*f.order, "tracker")
	}
	return f.adoptionStalledTrackerFake.TakeTotal(id)
}

type reconcileMetricsProbe struct {
	inputs []metrics.RecordTaskMetricsInput
	record metrics.RecordTaskMetricsOutput
	order  *[]string
}

func (f *reconcileMetricsProbe) Execute(_ context.Context, in metrics.RecordTaskMetricsInput) metrics.RecordTaskMetricsOutput {
	if f.order != nil {
		*f.order = append(*f.order, "metrics")
	}
	f.inputs = append(f.inputs, in)
	return f.record
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
	uc := NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{present: present}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, newAdoptionResumeUseCase(t), slots, mutex, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, time.Nanosecond, 17*time.Second, slog.Default())
	return uc, pending, tasks, liveness, writer, termination, killed, pathLocks, slots, mutex, &order
}

func TestReconcilePendingSkipsOwnedRecoveringTask(t *testing.T) {
	uc, pending, tasks, liveness, _, _, _, _, _, _, _ := newReconcileFixture(t, domain.StateRecovering, true, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	registry := NewRecoveryOwnershipRegistry()
	release, acquired := registry.Acquire(id)
	if !acquired {
		t.Fatal("Acquire did not acquire")
	}
	t.Cleanup(release)
	uc.WithRecoveryOwnership(registry)
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
	liveness.release <- struct{}{}

	uc.reconcileOne(context.Background(), pending.List()[0])

	select {
	case <-liveness.called:
		t.Fatal("liveness was called for an owned recovery")
	default:
	}
	if len(pending.List()) != 1 {
		t.Fatalf("pending entries = %+v, want one retained entry", pending.List())
	}
	if tasks.entries[id].State != domain.StateRecovering {
		t.Fatalf("state = %q, want recovering", tasks.entries[id].State)
	}
}

func TestReconcilePendingRecoveringAcquiresOwnershipBeforeConcurrentRecovery(t *testing.T) {
	uc, pending, tasks, liveness, _, _, _, _, _, _, _ := newReconcileFixture(t, domain.StateRecovering, false, false)
	id := adoptionID(t, "reconcile-ownership-race")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	tasks.entries[id] = snapshot
	registerReconcilePending(t, pending, snapshot, PendingSendConfirmOnly)
	registry := NewRecoveryOwnershipRegistry()
	ownership := &reconcileOwnershipProbe{registry: registry, acquired: make(chan struct{})}
	uc.WithRecoveryOwnership(ownership)

	done := make(chan struct{})
	go func() {
		uc.reconcileOne(context.Background(), pending.List()[0])
		close(done)
	}()
	select {
	case <-ownership.acquired:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not acquire recovery ownership")
	}
	otherRelease, otherAcquired := registry.Acquire(id)
	if otherAcquired {
		otherRelease()
		t.Fatal("concurrent recovery acquired ownership while reconciliation was active")
	}
	select {
	case <-liveness.called:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not enter liveness check")
	}
	liveness.release <- struct{}{}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not complete")
	}
}

func TestReconcilePendingRecoveringSkipsProcessingWhenOwnershipAcquireFails(t *testing.T) {
	uc, pending, tasks, liveness, _, _, _, _, _, _, _ := newReconcileFixture(t, domain.StateRecovering, true, false)
	id := adoptionID(t, "reconcile-ownership-denied")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	tasks.entries[id] = snapshot
	registerReconcilePending(t, pending, snapshot, PendingSendConfirmOnly)
	registry := NewRecoveryOwnershipRegistry()
	release, acquired := registry.Acquire(id)
	if !acquired {
		t.Fatal("competing recovery did not acquire ownership")
	}
	t.Cleanup(release)
	ownership := &reconcileOwnershipProbe{registry: registry, acquired: make(chan struct{})}
	uc.WithRecoveryOwnership(ownership)

	uc.reconcileOne(context.Background(), pending.List()[0])

	if ownership.calls() != 1 {
		t.Fatalf("Acquire calls = %d, want 1", ownership.calls())
	}
	select {
	case <-liveness.called:
		t.Fatal("liveness was called after ownership acquisition failed")
	default:
	}
}

func TestReconcilePendingRecoveringReleasesOwnershipAfterCompletion(t *testing.T) {
	uc, pending, tasks, liveness, _, _, _, _, _, _, _ := newReconcileFixture(t, domain.StateRecovering, true, true)
	id := adoptionID(t, "reconcile-ownership-release")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	tasks.entries[id] = snapshot
	registerReconcilePending(t, pending, snapshot, PendingSendConfirmOnly)
	registry := NewRecoveryOwnershipRegistry()
	uc.WithRecoveryOwnership(registry)
	liveness.release <- struct{}{}

	uc.reconcileOne(context.Background(), pending.List()[0])

	if registry.IsOwned(id) {
		t.Fatal("reconciliation retained recovery ownership after completion")
	}
}

func TestRecoveryUseCasesRequireSharedOwnershipRegistry(t *testing.T) {
	resume, _, _, _, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateTimeout, nil, RecoveryResult{})
	reconcile, _, _, _, _, _, _, _, _, _, _ := newReconcileFixture(t, domain.StateRecovering, true, false)
	id := adoptionID(t, "shared-ownership")
	release, acquired := resume.ownership.Acquire(id)
	if !acquired {
		t.Fatal("resume registry did not acquire")
	}
	defer release()
	if reconcile.ownership.IsOwned(id) {
		t.Fatal("independent registries unexpectedly share recovery ownership")
	}

	shared := NewRecoveryOwnershipRegistry()
	resume.WithRecoveryOwnership(shared)
	reconcile.WithRecoveryOwnership(shared)
	sharedRelease, sharedAcquired := shared.Acquire(id)
	if !sharedAcquired {
		t.Fatal("shared registry did not acquire")
	}
	defer sharedRelease()
	if !reconcile.ownership.IsOwned(id) {
		t.Fatal("injected shared registry did not protect reconcile")
	}
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

func TestReconcilePendingRecoveringRecordsTerminalMetrics(t *testing.T) {
	at := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		present   bool
		origin    domain.RecoveryOrigin
		wantState domain.TaskState
		recorded  bool
	}{
		{"recovered", true, domain.RecoveryOriginTimeout, domain.StateRecovered, true},
		{"timeout lost", false, domain.RecoveryOriginTimeout, domain.StateTimeoutLost, true},
		{"lost", false, domain.RecoveryOriginOrphan, domain.StateLost, true},
		{"metrics fail soft", false, domain.RecoveryOriginOrphan, domain.StateLost, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc, pending, tasks, liveness, _, _, _, _, slots, mutex, order := newReconcileFixture(t, domain.StateRecovering, true, tc.present)
			id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
			snapshot := adoptionRecoveringSnapshot(t, id, tc.origin)
			tasks.entries[id] = snapshot
			tracker := &reconcileTrackerProbe{adoptionStalledTrackerFake: &adoptionStalledTrackerFake{total: 29}, mutex: mutex, order: order}
			recorder := &reconcileMetricsProbe{record: metrics.RecordTaskMetricsOutput{Recorded: tc.recorded}, order: order}
			uc.stalledTracker = tracker
			uc.metricsRecorder = recorder
			uc.clock = domain.ClockFunc(func() time.Time { return at })
			registerReconcilePending(t, pending, snapshot, PendingSendConfirmOnly)

			finish := runReconciliation(t, uc, liveness)
			finish()

			if got := tasks.entries[id].State; got != tc.wantState || !tasks.entries[id].StateUpdatedAt.Equal(at) {
				t.Fatalf("state=%q occurredAt=%s, want state=%q occurredAt=%s", got, tasks.entries[id].StateUpdatedAt, tc.wantState, at)
			}
			if tracker.calledWhileLocked || tracker.takes != 1 || tracker.takeID != id || len(recorder.inputs) != 1 || slots.calls != 1 || len(pending.List()) != 0 {
				t.Fatalf("tracker=%+v metrics=%+v slots=%d pending=%+v", tracker, recorder.inputs, slots.calls, pending.List())
			}
			in := recorder.inputs[0]
			if in.TaskID != id || in.FinalState != tc.wantState || !in.Estimated || !in.OccurredAt.Equal(at) || in.StalledTotalMs != tracker.total {
				t.Fatalf("metrics=%+v", in)
			}
			wantSuffix := []string{"tracker", "metrics", "slot"}
			if got := (*order)[len(*order)-len(wantSuffix):]; !reflect.DeepEqual(got, wantSuffix) {
				t.Fatalf("order suffix=%v, want %v", got, wantSuffix)
			}
		})
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
	uc := NewReconcilePendingUseCase(pending, tasks, &reconcileLivenessFake{called: make(chan struct{}), release: make(chan struct{})}, reader, &reconcileWriterFake{order: &order}, finalizer, &reconcileTerminationFake{called: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}, &reconcileKilledFake{}, &reconcilePathLocksFake{order: &order}, newAdoptionResumeUseCase(t), &reconcileSlotsFake{order: &order}, &reconcileMutexFake{order: &order}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, time.Second, time.Second, slog.Default())

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
	tracker := &adoptionStalledTrackerFake{}
	recorder := &adoptionMetricsFake{}
	uc.stalledTracker = tracker
	uc.metricsRecorder = recorder
	tasks.saveErr = errors.New("save")
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
	finish := runReconciliation(t, uc, liveness)
	finish()

	entries := pending.List()
	if mutex.held || slots.calls != 0 || tracker.takes != 0 || len(recorder.inputs) != 0 || tasks.entries[id].State != domain.StateRecovering || len(entries) != 1 || pendingDisposition(entries[0]) != PendingSendConfirmOnly || writer.events != 0 {
		t.Fatalf("held=%v slots=%d takes=%d metrics=%+v state=%s pending=%+v events=%d", mutex.held, slots.calls, tracker.takes, recorder.inputs, tasks.entries[id].State, entries, writer.events)
	}
}

func TestReconcilePendingRecoveringStateConflictDoesNotRecordMetrics(t *testing.T) {
	uc, pending, tasks, liveness, _, _, _, _, slots, mutex, _ := newReconcileFixture(t, domain.StateRecovering, true, true)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	terminal := tasks.entries[id]
	terminal.State = domain.StateRecovered
	tasks.loadResults = map[domain.TaskID][]reconcileLoadResult{id: {{snapshot: terminal}}}
	tracker := &adoptionStalledTrackerFake{}
	recorder := &adoptionMetricsFake{}
	uc.stalledTracker = tracker
	uc.metricsRecorder = recorder
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
	liveness.release <- struct{}{}

	uc.reconcileRecovering(context.Background(), id)

	if mutex.held || tracker.takes != 0 || len(recorder.inputs) != 0 || slots.calls != 0 || len(pending.List()) != 0 {
		t.Fatalf("held=%v takes=%d metrics=%+v slots=%d pending=%+v", mutex.held, tracker.takes, recorder.inputs, slots.calls, pending.List())
	}
}

func TestReconcilePendingRecoveringExitCodeMismatchFailsClosed(t *testing.T) {
	uc, pending, tasks, liveness, writer, _, _, _, slots, _, _ := newReconcileFixture(t, domain.StateRecovering, true, true)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	uc.reader = &reconcileReaderFake{present: true, exitCode: 1, exitExists: true}
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
	finish := runReconciliation(t, uc, liveness)
	finish()
	if tasks.saves != 0 || writer.adoptedMarkers != 0 || writer.recoveredMarkers != 0 || writer.exitCodes != 0 || writer.events != 0 || slots.calls != 0 || len(pending.List()) != 1 {
		t.Fatal("recovering reconciliation did not fail closed")
	}
}

func TestReconcilePendingRecoveringExitCodeReadFailureFailsClosed(t *testing.T) {
	uc, pending, tasks, liveness, writer, _, _, _, slots, _, _ := newReconcileFixture(t, domain.StateRecovering, true, true)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	uc.reader = &reconcileReaderFake{present: true, exitErr: errors.New("read exit")}
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendConfirmOnly)
	finish := runReconciliation(t, uc, liveness)
	finish()
	if tasks.saves != 0 || writer.adoptedMarkers != 0 || writer.recoveredMarkers != 0 || writer.exitCodes != 0 || writer.events != 0 || slots.calls != 0 || len(pending.List()) != 1 {
		t.Fatal("recovering reconciliation did not fail closed")
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
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, newAdoptionResumeUseCase(t), slots, mutex, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, time.Second, grace, slog.Default())
		})
	}
}

func TestNewReconcilePendingUseCaseRejectsTypedNilDependencies(t *testing.T) {
	uc, pending, tasks, liveness, writer, termination, killed, pathLocks, slots, mutex, _ := newReconcileFixture(t, domain.StateRecovering, false, false)
	_ = uc
	resume := newAdoptionResumeUseCase(t)
	tracker := &adoptionStalledTrackerFake{}
	recorder := &adoptionMetricsFake{}
	for _, tc := range []struct {
		name string
		make func()
	}{
		{"pending", func() {
			NewReconcilePendingUseCase((*PendingReconciliationSet)(nil), tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"tasks", func() {
			NewReconcilePendingUseCase(pending, (*reconcileStoreFake)(nil), liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"liveness", func() {
			NewReconcilePendingUseCase(pending, tasks, (*reconcileLivenessFake)(nil), &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"reader", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, (*reconcileReaderFake)(nil), writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"writer", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, (*reconcileWriterFake)(nil), &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"finalizer", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, (*adoptionFinalizerFake)(nil), termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"termination", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, (*reconcileTerminationFake)(nil), killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"killed", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, (*reconcileKilledFake)(nil), pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"path-locks", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, (*reconcilePathLocksFake)(nil), resume, slots, mutex, domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"resume", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, (*RecoverViaResumeUseCase)(nil), slots, mutex, domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"slots", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, (*reconcileSlotsFake)(nil), mutex, domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"task-mutex", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, (*reconcileMutexFake)(nil), domain.ClockFunc(time.Now), tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"clock", func() {
			var clock domain.ClockFunc
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, clock, tracker, recorder, time.Second, time.Second, slog.Default())
		}},
		{"stalled tracker", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), (*adoptionStalledTrackerFake)(nil), recorder, time.Second, time.Second, slog.Default())
		}},
		{"metrics recorder", func() {
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, &adoptionFinalizerFake{}, termination, killed, pathLocks, resume, slots, mutex, domain.ClockFunc(time.Now), tracker, (*adoptionMetricsFake)(nil), time.Second, time.Second, slog.Default())
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
	snapshot := tasks.entries[id]
	wantAuthority, ok := processSignalAuthority(snapshot)
	if !ok {
		t.Fatal("fixture requires process signal authority")
	}
	if !pendingAuthorityEqual(termination.authority, wantAuthority) {
		t.Fatalf("authority=%#v, want=%#v", termination.authority, wantAuthority)
	}
	pendingEntries := pending.List()
	if len(pendingEntries) != 1 || pendingEntries[0].state != pendingClaimed {
		t.Fatalf("pending=%+v, want one claimed entry", pendingEntries)
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

func TestReconcilePendingTerminationSendFailureReleasesOnlyCurrentAuthority(t *testing.T) {
	for _, tc := range []struct {
		name        string
		loadResult  *reconcileLoadResult
		mutate      func(*domain.TaskSnapshot)
		wantOutcome ClaimOutcome
		wantLog     bool
	}{
		{"transient load failure", &reconcileLoadResult{err: errors.New("temporary load failure")}, nil, ClaimAcquired, true},
		{"current authority", nil, nil, ClaimAcquired, false},
		{"task not found", &reconcileLoadResult{err: domain.ErrTaskNotFound}, nil, ClaimConfirmOnly, false},
		{"authority changed", nil, func(snapshot *domain.TaskSnapshot) {
			pid := 9999
			snapshot.PID = &pid
		}, ClaimConfirmOnly, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc, pending, tasks, _, _, termination, _, _, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
			id := adoptionID(t, "reconcile-"+string(domain.StateTimeout))
			if tc.loadResult != nil {
				tasks.loadResults = map[domain.TaskID][]reconcileLoadResult{id: {*tc.loadResult}}
			}
			var logs bytes.Buffer
			uc.logger = slog.New(slog.NewTextHandler(&logs, nil))
			termination.terminateErr = errors.New("send failed")
			close(termination.release)
			registerReconcilePending(t, pending, tasks.entries[id], PendingSendUnsent)
			authority, ok := processSignalAuthority(tasks.entries[id])
			if !ok {
				t.Fatal("fixture requires process signal authority")
			}
			claim, outcome := pending.ClaimForSend(id, authority)
			if outcome != ClaimAcquired {
				t.Fatalf("initial outcome=%v", outcome)
			}
			if tc.mutate != nil {
				updated := tasks.entries[id]
				tc.mutate(&updated)
				tasks.entries[id] = updated
			}

			uc.sendAndReconcile(context.Background(), claim, tasks.entries[id], true)

			if tc.wantLog != (logs.Len() > 0) {
				t.Fatalf("logs=%q, wantLog=%v", logs.String(), tc.wantLog)
			}
			if _, outcome := pending.ClaimForSend(id, authority); outcome != tc.wantOutcome {
				t.Fatalf("outcome=%v, want %v", outcome, tc.wantOutcome)
			}
		})
	}
}

func TestReconcilePendingInvalidSignalAuthorityDoesNotReloadTask(t *testing.T) {
	uc, pending, tasks, _, _, termination, _, _, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateTimeout))
	snapshot := tasks.entries[id]
	registerReconcilePending(t, pending, snapshot, PendingSendUnsent)
	authority, ok := processSignalAuthority(snapshot)
	if !ok {
		t.Fatal("fixture requires process signal authority")
	}
	claim, outcome := pending.ClaimForSend(id, authority)
	if outcome != ClaimAcquired {
		t.Fatalf("initial outcome=%v", outcome)
	}
	termination.terminateErr = ErrProcessSignalAuthorityInvalid
	close(termination.release)

	uc.sendAndReconcile(context.Background(), claim, snapshot, true)

	if tasks.loads != 0 {
		t.Fatalf("loads=%d, want 0", tasks.loads)
	}
	if _, outcome := pending.ClaimForSend(id, authority); outcome != ClaimConfirmOnly {
		t.Fatalf("outcome=%v, want %v", outcome, ClaimConfirmOnly)
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

func TestSCNDaemon0124AlreadyClaimedSkipsConfirmationAndRecovery(t *testing.T) {
	uc, pending, tasks, _, _, termination, killed, pathLocks, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateTimeout))
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendUnsent)
	authority, ok := processSignalAuthority(tasks.entries[id])
	if !ok {
		t.Fatal("fixture requires process signal authority")
	}
	if _, outcome := pending.ClaimForSend(id, authority); outcome != ClaimAcquired {
		t.Fatalf("claim outcome=%v", outcome)
	}

	uc.reconcileOne(context.Background(), pending.List()[0])
	if termination.send != 0 || termination.confirm != 0 || pathLocks.calls != 0 || killed.calls != 0 {
		t.Fatalf("send=%d confirm=%d pathLocks=%d killed=%d", termination.send, termination.confirm, pathLocks.calls, killed.calls)
	}
}

func TestSCNDaemon0129DeadResultWinsOverTerminationError(t *testing.T) {
	for _, tc := range []struct {
		name      string
		killedErr error
	}{
		{name: "kill confirmation succeeds"},
		{name: "kill confirmation fails", killedErr: errors.New("confirm killed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc, pending, tasks, _, _, termination, killed, _, _, _, _ := newReconcileFixture(t, domain.StateCancelling, false, false)
			id := adoptionID(t, "reconcile-"+string(domain.StateCancelling))
			registerReconcilePending(t, pending, tasks.entries[id], PendingSendUnsent)
			authority, ok := processSignalAuthority(tasks.entries[id])
			if !ok {
				t.Fatal("fixture requires process signal authority")
			}
			claim, outcome := pending.ClaimForSend(id, authority)
			if outcome != ClaimAcquired {
				t.Fatalf("claim outcome=%v", outcome)
			}
			termination.sendDead = true
			termination.terminateErr = errors.New("termination reported an error")
			killed.err = tc.killedErr
			close(termination.release)

			uc.sendAndReconcile(context.Background(), claim, tasks.entries[id], false)
			entries := pending.List()
			if killed.calls != 1 {
				t.Fatalf("killed=%d, want 1", killed.calls)
			}
			if tc.killedErr == nil && len(entries) != 0 {
				t.Fatalf("pending=%+v, want removed", entries)
			}
			if tc.killedErr != nil && (len(entries) != 1 || entries[0].state != pendingConfirmOnly) {
				t.Fatalf("pending=%+v, want confirm-only", entries)
			}
		})
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
	_, testPath, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("caller path unavailable")
	}
	sourcePath, err := filepath.Abs(filepath.Join(filepath.Dir(testPath), "reconcile.go"))
	if err != nil {
		t.Fatal(err)
	}
	assertNoUnconditionalPendingRemovalBeforeDispatch(t, sourcePath, "completeTerminated")

	uc.completeTerminated(context.Background(), id, tasks.entries[id], true, time.Now())
	waitForPendingRemoval(t, pending)

	if pathLocks.calls != 1 || len(pending.List()) != 0 {
		t.Fatalf("pathLocks=%d pending=%+v", pathLocks.calls, pending.List())
	}
}

func TestReconcileResumeRecoveryResolvesClaimPerOutcome(t *testing.T) {
	for _, tc := range []struct {
		name      string
		saveError bool
		wantCount int
		wantState pendingState
	}{
		{name: "success", wantCount: 0},
		{name: "failure", saveError: true, wantCount: 1, wantState: pendingConfirmOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc, pending, _, _, _, _, _, _, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
			session := recoveryTestSession(t)
			resume, store, _, _, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateTimeout, &session, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)})
			if tc.saveError {
				store.saveErr = errors.New("save failed")
				store.saveErrOn = 1
			}
			uc.resume = resume

			snapshot := store.snapshot
			registerReconcilePending(t, pending, snapshot, PendingSendUnsent)
			authority, ok := processSignalAuthority(snapshot)
			if !ok {
				t.Fatal("fixture requires process signal authority")
			}
			claim, outcome := pending.ClaimForSend(snapshot.TaskID, authority)
			if outcome != ClaimAcquired {
				t.Fatalf("outcome=%v, want %v", outcome, ClaimAcquired)
			}

			uc.resumeRecovery(context.Background(), RecoverViaResumeInput{
				TaskID:     snapshot.TaskID,
				SessionRef: snapshot.SessionRef,
				Origin:     domain.RecoveryOriginTimeout,
				OccurredAt: time.Now(),
			}, claim)

			entries := pending.List()
			if len(entries) != tc.wantCount {
				t.Fatalf("pending=%+v, want count=%d", entries, tc.wantCount)
			}
			if tc.wantCount == 1 && entries[0].state != tc.wantState {
				t.Fatalf("state=%v, want %v", entries[0].state, tc.wantState)
			}
		})
	}
}

func TestReconcilePendingSendConfirmErrorRetainsSentEntry(t *testing.T) {
	uc, pending, tasks, _, _, termination, _, _, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateTimeout))
	snapshot := tasks.entries[id]
	registerReconcilePending(t, pending, snapshot, PendingSendUnsent)
	authority, ok := processSignalAuthority(snapshot)
	if !ok {
		t.Fatal("fixture requires process signal authority")
	}
	claim, outcome := pending.ClaimForSend(id, authority)
	if outcome != ClaimAcquired {
		t.Fatalf("initial outcome=%v", outcome)
	}
	termination.sendDead = false
	termination.terminateErr = nil
	termination.confirmErr = errors.New("confirm failed")
	close(termination.release)

	uc.sendAndReconcile(context.Background(), claim, snapshot, true)

	entries := pending.List()
	if len(entries) != 1 || entries[0].state != pendingSent {
		t.Fatalf("pending=%+v, want one sent entry", entries)
	}
	if _, outcome := pending.ClaimForSend(id, authority); outcome != ClaimSent {
		t.Fatalf("outcome=%v, want %v", outcome, ClaimSent)
	}
}

func TestReconcilePendingConfirmErrorRetainsSentEntry(t *testing.T) {
	uc, pending, tasks, _, _, termination, killed, _, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateTimeout))
	termination.confirmDead = true
	termination.confirmOnlyErr = errors.New("confirm failed")
	close(termination.release)
	registerReconcilePending(t, pending, tasks.entries[id], PendingSendSent)

	uc.reconcileOne(context.Background(), pending.List()[0])

	entries := pending.List()
	if killed.calls != 0 || len(entries) != 1 || entries[0].state != pendingSent {
		t.Fatalf("killed=%d pending=%+v, want sent entry retained", killed.calls, entries)
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

func isPendingRemoveCall(expression ast.Expr) bool {
	call, ok := expression.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	switch selector.Sel.Name {
	case "Remove", "RemoveClaim":
	default:
		return false
	}
	pending, ok := selector.X.(*ast.SelectorExpr)
	return ok && pending.Sel.Name == "pending"
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
	uc := NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, &reconcileWriterFake{order: &order}, &adoptionFinalizerFake{}, termination, &reconcileKilledFake{}, &reconcilePathLocksFake{order: &order}, newAdoptionResumeUseCase(t), &reconcileSlotsFake{order: &order}, &reconcileMutexFake{order: &order}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, time.Second, time.Second, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		uc.reconcileTick(ctx)
		close(done)
	}()
	select {
	case <-liveness.called:
	case <-time.After(time.Second):
		t.Fatal("liveness check did not start")
	}
	cancel()
	liveness.release <- struct{}{}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciliation did not stop")
	}
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
	uc := NewReconcilePendingUseCase(pending, tasks, liveness, reader, writer, &adoptionFinalizerFake{}, &reconcileTerminationFake{called: make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{})}, &reconcileKilledFake{}, &reconcilePathLocksFake{order: &order}, newAdoptionResumeUseCase(t), slots, mutex, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, time.Second, time.Second, slog.Default())

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
