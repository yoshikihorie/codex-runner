package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
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

type reconcileReaderFake struct{ present bool }

func (f *reconcileReaderFake) ReadLastMessage(domain.TaskID) (bool, error) { return f.present, nil }
func (*reconcileReaderFake) ReadStderrLog(domain.TaskID) ([]byte, error)   { return nil, nil }

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
}

func (f *reconcileTerminationFake) Confirm(context.Context, domain.TaskID) (bool, error) {
	f.confirm++
	f.once.Do(func() { close(f.called) })
	<-f.release
	f.finishedOnce.Do(func() { close(f.finished) })
	return f.confirmDead, nil
}
func (f *reconcileTerminationFake) SendAndConfirm(_ context.Context, _ domain.TaskID, _ int, grace time.Duration) (bool, error) {
	f.send++
	f.grace = grace
	f.once.Do(func() { close(f.called) })
	<-f.release
	f.finishedOnce.Do(func() { close(f.finished) })
	return f.sendDead, nil
}

type reconcileKilledFake struct{ calls int }

func (f *reconcileKilledFake) ConfirmKilled(context.Context, domain.TaskID, int, bool, time.Time) error {
	f.calls++
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
	uc := NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{present: present}, writer, termination, killed, pathLocks, newAdoptionResumeUseCase(t), slots, mutex, domain.ClockFunc(time.Now), time.Nanosecond, 17*time.Second, slog.Default())
	return uc, pending, tasks, liveness, writer, termination, killed, pathLocks, slots, mutex, &order
}

type reconciliationRun struct {
	releaseLiveness func()
	stop            func()
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
	run := startReconciliation(t, uc, liveness)
	return func() {
		run.releaseLiveness()
		run.stop()
	}
}

func TestReconcilePendingRetainsLiveRecoveringTask(t *testing.T) {
	uc, pending, _, liveness, _, _, _, _, _, _, _ := newReconcileFixture(t, domain.StateRecovering, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	pending.Add(id, true)
	finish := runReconciliation(t, uc, liveness)
	finish()
	if entries := pending.List(); len(entries) != 1 || entries[0].taskID != id {
		t.Fatalf("pending=%+v, want live recovering task retained", entries)
	}
}

func TestReconcilePendingCompletesDeadRecoveringTaskBeforeReleasingSlot(t *testing.T) {
	uc, pending, tasks, liveness, writer, _, _, _, slots, mutex, order := newReconcileFixture(t, domain.StateRecovering, true, true)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	pending.Add(id, true)
	finish := runReconciliation(t, uc, liveness)
	finish()
	if got := tasks.entries[id].State; got != domain.StateRecovered {
		t.Fatalf("state=%q, want %q", got, domain.StateRecovered)
	}
	if mutex.held || writer.events == 0 || slots.calls != 1 || len(pending.List()) != 0 {
		t.Fatalf("held=%v events=%d slots=%d pending=%+v", mutex.held, writer.events, slots.calls, pending.List())
	}
	want := []string{"load", "lock", "load", "adopted-marker", "recovered-marker", "exit-code", "save", "event", "unlock", "slot"}
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
	pending.Add(id, true)
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

func TestReconcilePendingRecoveringWriteFailuresAreFailSoft(t *testing.T) {
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
		{"save", true, func(tasks *reconcileStoreFake, _ *reconcileWriterFake) { tasks.saveErr = errors.New("save") }},
		{"append event", true, func(_ *reconcileStoreFake, writer *reconcileWriterFake) { writer.appendErr = errors.New("event") }},
		{"failure exit code", false, func(_ *reconcileStoreFake, writer *reconcileWriterFake) { writer.exitErr = errors.New("exit") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc, pending, tasks, liveness, writer, _, _, _, slots, mutex, _ := newReconcileFixture(t, domain.StateRecovering, true, tc.present)
			var logs bytes.Buffer
			uc.logger = slog.New(slog.NewTextHandler(&logs, nil))
			tc.configure(tasks, writer)
			id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
			pending.Add(id, true)
			finish := runReconciliation(t, uc, liveness)
			finish()
			if mutex.held || slots.calls != 1 || len(pending.List()) != 0 || writer.events == 0 || logs.Len() == 0 {
				t.Fatalf("held=%v slots=%d pending=%+v events=%d logs=%q", mutex.held, slots.calls, pending.List(), writer.events, logs.String())
			}
		})
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
			NewReconcilePendingUseCase(pending, tasks, liveness, &reconcileReaderFake{}, writer, termination, killed, pathLocks, newAdoptionResumeUseCase(t), slots, mutex, domain.ClockFunc(time.Now), time.Second, grace, slog.Default())
		})
	}
}

func TestReconcilePendingRecoveryWriteOrder(t *testing.T) {
	for _, tc := range []struct {
		name    string
		present bool
		want    []string
	}{
		{"success", true, []string{"load", "lock", "load", "adopted-marker", "recovered-marker", "exit-code", "save", "event", "unlock", "slot"}},
		{"failure", false, []string{"load", "lock", "load", "adopted-marker", "exit-code", "save", "event", "unlock", "slot"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			uc, pending, _, liveness, _, _, _, _, _, _, order := newReconcileFixture(t, domain.StateRecovering, true, tc.present)
			id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
			pending.Add(id, true)
			finish := runReconciliation(t, uc, liveness)
			finish()
			if !reflect.DeepEqual(*order, tc.want) {
				t.Fatalf("order=%v, want %v", *order, tc.want)
			}
		})
	}
}

func TestReconcilePendingTimeoutClaimsBeforeSendingAndUsesTerminationGrace(t *testing.T) {
	uc, pending, _, _, _, termination, _, _, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateTimeout))
	pending.Add(id, false)
	done := make(chan struct{})
	go func() {
		uc.reconcileOne(context.Background(), id)
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
	uc, pending, _, _, _, termination, _, _, _, _, _ := newReconcileFixture(t, domain.StateTimeout, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateTimeout))
	pending.Add(id, true)
	done := make(chan struct{})
	go func() {
		uc.reconcileOne(context.Background(), id)
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
	uc, pending, _, _, _, termination, killed, _, _, _, _ := newReconcileFixture(t, domain.StateCancelling, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateCancelling))
	pending.Add(id, true)
	termination.confirmDead = true
	done := make(chan struct{})
	go func() {
		uc.reconcileOne(context.Background(), id)
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

func TestReconcilePendingWithoutPIDConfirmsInsteadOfSending(t *testing.T) {
	uc, pending, tasks, _, _, termination, _, _, _, _, _ := newReconcileFixture(t, domain.StateCancelling, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateCancelling))
	snapshot := tasks.entries[id]
	snapshot.PID = nil
	snapshot.ProcessStartedAt = nil
	snapshot.AdoptedAfterRestart = true
	tasks.entries[id] = snapshot
	pending.Add(id, false)
	done := make(chan struct{})
	go func() {
		uc.reconcileOne(context.Background(), id)
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
	uc, pending, _, liveness, _, _, _, _, _, _, _ := newReconcileFixture(t, domain.StateRecovering, false, false)
	id := adoptionID(t, "reconcile-"+string(domain.StateRecovering))
	pending.Add(id, true)
	run := startReconciliation(t, uc, liveness)
	run.stop()
}

var _ ContractReader = (*reconcileReaderFake)(nil)
var _ contract.ContractWriter = (*reconcileWriterFake)(nil)
