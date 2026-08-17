package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

type cancelStoreFake struct {
	snapshot                      domain.TaskSnapshot
	loadErr, saveErr, reservedErr error
	reservedErrs                  []error
	reserved                      bool
	saves, loads, reservations    int
}

func (f *cancelStoreFake) Load(domain.TaskID) (domain.TaskSnapshot, error) {
	f.loads++
	if f.loadErr != nil {
		return domain.TaskSnapshot{}, f.loadErr
	}
	return f.snapshot, nil
}
func (f *cancelStoreFake) Save(_ domain.TaskID, s domain.TaskSnapshot) error {
	f.saves++
	if f.saveErr == nil {
		f.snapshot = s
	}
	return f.saveErr
}
func (f *cancelStoreFake) IsReserved(domain.TaskID) (bool, error) {
	idx := f.reservations
	f.reservations++
	if idx < len(f.reservedErrs) {
		return f.reserved, f.reservedErrs[idx]
	}
	return f.reserved, f.reservedErr
}
func (*cancelStoreFake) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	return nil, nil
}
func (*cancelStoreFake) Reserve(domain.TaskID) error { return nil }
func (*cancelStoreFake) Release(domain.TaskID) error { return nil }

type cancelQueueFake struct {
	payload               execution.TaskLaunchPayload
	index                 int
	removed               bool
	panicRemove           bool
	removes, restores     int
	removeBeforeStore     bool
	queueMu               *sync.Mutex
	restoreObservedLocked bool
}

func (f *cancelQueueFake) Remove(domain.TaskID, time.Time) (execution.TaskLaunchPayload, int, bool, []domain.Event) {
	f.removes++
	if f.panicRemove {
		panic("remove panic")
	}
	return f.payload, f.index, f.removed, nil
}
func (f *cancelQueueFake) Restore(payload execution.TaskLaunchPayload, index int, _ time.Time) []domain.Event {
	if f.queueMu != nil {
		if f.queueMu.TryLock() {
			f.queueMu.Unlock()
		} else {
			f.restoreObservedLocked = true
		}
	}
	f.restores++
	f.payload, f.index, f.removed = payload, index, true
	return nil
}

type cancelEventsFake struct {
	events []domain.Event
	err    error
}

type cancelStalledTrackerFake struct {
	calls []struct {
		id domain.TaskID
		at time.Time
	}
}

type cancelMetricsRecorderFake struct{}

func (*cancelMetricsRecorderFake) Execute(context.Context, metrics.RecordTaskMetricsInput) metrics.RecordTaskMetricsOutput {
	return metrics.RecordTaskMetricsOutput{}
}

func (f *cancelStalledTrackerFake) LeaveStalled(id domain.TaskID, at time.Time) int {
	f.calls = append(f.calls, struct {
		id domain.TaskID
		at time.Time
	}{id: id, at: at})
	return 0
}

func (f *cancelEventsFake) AppendEvent(_ domain.TaskID, event domain.Event) error {
	f.events = append(f.events, event)
	return f.err
}

type cancelTerminatorFake struct {
	calls int
	pid   int
	grace time.Duration
	err   error
	order *[]string
}

func (f *cancelTerminatorFake) Terminate(pid int, grace time.Duration) error {
	f.calls++
	f.pid, f.grace = pid, grace
	if f.order != nil {
		*f.order = append(*f.order, "terminate")
	}
	return f.err
}

type cancelDisarmerFake struct {
	calls int
	order *[]string
}

type cancelPendingRegistrarFake struct {
	calls       int
	taskID      domain.TaskID
	disposition recovery.PendingSendDisposition
	authority   *recovery.ProcessSignalAuthority
	err         error
	order       *[]string
}

func (f *cancelPendingRegistrarFake) Register(taskID domain.TaskID, disposition recovery.PendingSendDisposition, authority *recovery.ProcessSignalAuthority) error {
	f.calls, f.taskID, f.disposition = f.calls+1, taskID, disposition
	if authority != nil {
		value := *authority
		f.authority = &value
	}
	if f.order != nil {
		*f.order = append(*f.order, "register")
	}
	return f.err
}
func (*cancelPendingRegistrarFake) ClaimForSend(domain.TaskID, recovery.ProcessSignalAuthority) (recovery.SendClaim, recovery.ClaimOutcome) {
	return recovery.SendClaim{}, recovery.ClaimNotFound
}
func (*cancelPendingRegistrarFake) CompleteSend(recovery.SendClaim) bool   { return false }
func (*cancelPendingRegistrarFake) ReleaseSend(recovery.SendClaim) bool    { return false }
func (*cancelPendingRegistrarFake) InvalidateSend(recovery.SendClaim) bool { return false }
func (*cancelPendingRegistrarFake) RemoveClaim(recovery.SendClaim) bool    { return false }

var _ recovery.PendingRegistrar = (*cancelPendingRegistrarFake)(nil)

func (f *cancelDisarmerFake) Disarm(domain.TaskID) {
	f.calls++
	if f.order != nil {
		*f.order = append(*f.order, "disarm")
	}
}

type cancelWriterFake struct {
	contract.ContractWriter
	events []domain.Event
}

func (*cancelWriterFake) WriteExitCode(domain.TaskID, domain.ExitCode) error { return nil }
func (f *cancelWriterFake) AppendEvent(_ domain.TaskID, e domain.Event) error {
	f.events = append(f.events, e)
	return nil
}

type cancelReaderFake struct{}

func (*cancelReaderFake) ReadStderrLog(domain.TaskID) ([]byte, error)          { return nil, nil }
func (*cancelReaderFake) ReadLastMessage(domain.TaskID) (bool, error)          { return false, nil }
func (*cancelReaderFake) ReadPromptContent(domain.TaskID) ([]byte, error)      { return nil, nil }
func (*cancelReaderFake) ReadLastMessageContent(domain.TaskID) ([]byte, error) { return nil, nil }
func (*cancelReaderFake) ReadPartialOutputContent(domain.TaskID) ([]byte, error) {
	return nil, nil
}
func (*cancelReaderFake) ReadExitCode(domain.TaskID) (int, bool, error) { return 0, false, nil }

type cancelSlotFake struct{ calls int }

func (f *cancelSlotFake) ReleaseAndAdvance(context.Context, domain.TaskID, time.Time) { f.calls++ }

var _ recovery.SlotReleaser = (*cancelSlotFake)(nil)

type cancelReenteringSlotFake struct {
	queueMu *sync.Mutex
	reached chan struct{}
}

func (f *cancelReenteringSlotFake) ReleaseAndAdvance(context.Context, domain.TaskID, time.Time) {
	f.reached <- struct{}{}
	f.queueMu.Lock()
	f.queueMu.Unlock()
}

var _ recovery.SlotReleaser = (*cancelReenteringSlotFake)(nil)

type cancelPathsFake struct{}

func (*cancelPathsFake) List() ([]execution.PathLockSnapshot, error)       { return nil, nil }
func (*cancelPathsFake) Save(domain.TaskID, []domain.NormalizedPath) error { return nil }
func (*cancelPathsFake) Delete(domain.TaskID) error                        { return nil }

func cancelTaskID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260811-120000-a1b2-cancel")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func cancelQueuedPayload(t *testing.T) execution.TaskLaunchPayload {
	t.Helper()
	id := cancelTaskID(t)
	slug, err := domain.NewSlug("cancel")
	if err != nil {
		t.Fatal(err)
	}
	timeout, err := domain.NewTimeout(nil, 1800)
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := domain.NewTask(id, domain.SubcommandImpl, slug, nil, time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC), 1)
	if err != nil {
		t.Fatal(err)
	}
	return execution.TaskLaunchPayload{Task: task, Model: "gpt-5", PromptText: "prompt", ResolvedTimeout: timeout}
}
func cancelFixture(t *testing.T, payload execution.TaskLaunchPayload, removed bool) (*cancelStoreFake, *cancelQueueFake, *cancelEventsFake, *cancelTerminatorFake, *cancelDisarmerFake, *CancelTaskUseCase) {
	t.Helper()
	return cancelFixtureWithQueueMutexAndSlots(t, payload, removed, &sync.Mutex{}, &cancelSlotFake{})
}

func cancelFixtureWithQueueMutexAndSlots(t *testing.T, payload execution.TaskLaunchPayload, removed bool, queueMu *sync.Mutex, slots recovery.SlotReleaser) (*cancelStoreFake, *cancelQueueFake, *cancelEventsFake, *cancelTerminatorFake, *cancelDisarmerFake, *CancelTaskUseCase) {
	t.Helper()
	tasks := &cancelStoreFake{reserved: true}
	queue := &cancelQueueFake{payload: payload, index: 1, removed: removed, queueMu: queueMu}
	events := &cancelEventsFake{}
	terminator := &cancelTerminatorFake{}
	disarmer := &cancelDisarmerFake{}
	writer := &cancelWriterFake{}
	confirmer := execution.NewConfirmTaskKilledUseCase(tasks, writer, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), slots, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, &cancelPendingRegistrarFake{})
	uc := NewCancelTaskUseCase(tasks, queue, queueMu, store.NewTaskMutex(), events, terminator, &cancelPendingRegistrarFake{}, disarmer, confirmer, &cancelStalledTrackerFake{}, domain.ClockFunc(func() time.Time { return time.Date(2026, 8, 11, 12, 1, 0, 0, time.UTC) }))
	return tasks, queue, events, terminator, disarmer, uc
}

func cancelPersistedSnapshot(t *testing.T, state domain.TaskState, withPID bool) domain.TaskSnapshot {
	t.Helper()
	payload := cancelQueuedPayload(t)
	snapshot, err := domain.NewTaskSnapshotFromAdmission(payload.Task, payload.ResolvedTimeout, payload.Model, payload.ReasoningEffort, domain.ExecutionRouteDaemon, time.Date(2026, 8, 11, 12, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	snapshot.State = state
	if withPID {
		pid := 4321
		startedAt := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
		snapshot.PID, snapshot.ProcessStartedAt = &pid, &startedAt
	}
	if err := snapshot.Validate(); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func cancelLiveAdoptedWithoutPIDSnapshot(t *testing.T, payload execution.TaskLaunchPayload, stalled bool) domain.TaskSnapshot {
	t.Helper()
	at := time.Date(2026, 8, 11, 12, 1, 0, 0, time.UTC)
	snapshot, err := domain.NewTaskSnapshotFromAdmission(payload.Task, payload.ResolvedTimeout, payload.Model, payload.ReasoningEffort, domain.ExecutionRouteDaemon, at)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.State = domain.StateStarting
	task, err := snapshot.Restore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.Adopt(false, at); err != nil {
		t.Fatal(err)
	}
	if stalled {
		if _, err := task.MarkStalled(0, at); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err = snapshot.WithTask(task, at)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func TestCancelTaskTypesAreAvailable(t *testing.T) {
	var output CancelTaskOutput
	if output.Events != nil {
		t.Fatal("zero output must not create events")
	}
}

func TestCancelTaskExecute_QueuedReturnsOneTaskCancelRequestedEvent(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, queue, events, _, _, uc := cancelFixture(t, payload, true)
	at := time.Date(2026, 8, 11, 12, 1, 0, 0, time.UTC)
	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), Force: true, OccurredAt: at})
	if err != nil || queue.removes != 1 || tasks.saves != 2 || len(out.Events) != 1 || len(events.events) != 1 || out.State != domain.StateCancelling {
		t.Fatalf("out=%#v err=%v queue=%#v saves=%d events=%#v", out, err, queue, tasks.saves, events.events)
	}
	event, ok := out.Events[0].(domain.TaskCancelRequested)
	if !ok || !event.Force || event.RequestedVia != domain.ProtocolVerbCancel || !event.OccurredAt.Equal(at) {
		t.Fatalf("event=%#v", out.Events[0])
	}
}

func TestCancelTaskExecute_QueuedUnlocksQueueMutexBeforeConfirm(t *testing.T) {
	payload := cancelQueuedPayload(t)
	queueMu := &sync.Mutex{}
	slots := &cancelReenteringSlotFake{queueMu: queueMu, reached: make(chan struct{}, 1)}
	tasks, _, _, _, _, uc := cancelFixtureWithQueueMutexAndSlots(t, payload, true, queueMu, slots)
	type result struct {
		out CancelTaskOutput
		err error
	}
	completed := make(chan result, 1)
	go func() {
		out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
		completed <- result{out: out, err: err}
	}()
	select {
	case <-slots.reached:
	case <-time.After(time.Second):
		t.Fatal("confirmer did not reach slot release")
	}
	select {
	case result := <-completed:
		if result.err != nil || result.out.State != domain.StateCancelling || tasks.snapshot.State != domain.StateKilled {
			t.Fatalf("result=%#v snapshot=%#v", result, tasks.snapshot)
		}
	case <-time.After(time.Second):
		t.Fatal("queued cancel did not complete after confirmer reentered queue mutex")
	}
}

func TestCancelTaskExecute_QueuedFailureRestoresOriginalPositionAndPointer(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, queue, events, _, _, uc := cancelFixture(t, payload, true)
	tasks.saveErr = errors.New("save")
	_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err == nil || queue.restores != 1 || !queue.restoreObservedLocked || queue.index != 1 || queue.payload.Task != payload.Task || payload.Task.State() != domain.StateQueued || len(events.events) != 0 {
		t.Fatalf("err=%v queue=%#v state=%s", err, queue, payload.Task.State())
	}
}

func TestCancelTaskExecute_QueuedSnapshotFailureRestoresOriginalPositionAndPointer(t *testing.T) {
	payload := cancelQueuedPayload(t)
	payload.Model = ""
	tasks, queue, events, _, _, uc := cancelFixture(t, payload, true)
	_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err == nil || tasks.saves != 0 || queue.restores != 1 || !queue.restoreObservedLocked || queue.index != 1 || queue.payload.Task != payload.Task || payload.Task.State() != domain.StateQueued || len(events.events) != 0 {
		t.Fatalf("err=%v saves=%d queue=%#v state=%s", err, tasks.saves, queue, payload.Task.State())
	}
}

func TestCancelTaskExecute_QueuedEventAppendFailureDoesNotRestore(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, queue, events, _, _, uc := cancelFixture(t, payload, true)
	events.err = errors.New("append")
	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || queue.restores != 0 || tasks.saves != 2 || len(out.Events) != 1 {
		t.Fatalf("out=%#v err=%v queue=%#v", out, err, queue)
	}
}

func TestCancelTaskExecute_RemoveFalseChecksReservationBeforeLoad(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, queue, _, _, _, uc := cancelFixture(t, payload, false)
	tasks.reserved = false
	_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if !errors.Is(err, domain.ErrTaskNotFound) || queue.removes != 1 || tasks.reservations != 1 || tasks.loads != 0 {
		t.Fatalf("err=%v reservations=%d loads=%d", err, tasks.reservations, tasks.loads)
	}
}

func TestCancelTaskExecute_PersistedRunningStalledAndAdoptedTerminateBeforeDisarm(t *testing.T) {
	for _, state := range []domain.TaskState{domain.StateRunning, domain.StateStalled, domain.StateAdopted} {
		t.Run(string(state), func(t *testing.T) {
			payload := cancelQueuedPayload(t)
			tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
			tasks.snapshot = cancelPersistedSnapshot(t, state, true)
			order := []string{}
			terminator.order, disarmer.order = &order, &order
			pending := uc.pendingRegistrar.(*cancelPendingRegistrarFake)
			pending.order = &order

			out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			if err != nil || !out.TerminationTriggered || terminator.calls != 1 || terminator.pid != 4321 || terminator.grace != execution.TimeoutKillGrace || pending.calls != 0 || disarmer.calls != 1 || strings.Join(order, ",") != "terminate,disarm" {
				t.Fatalf("out=%#v err=%v terminator=%#v pending=%#v disarmer=%#v order=%#v", out, err, terminator, pending, disarmer, order)
			}
		})
	}
}

func TestCancelTaskExecute_PersistedStartingWithoutPIDDoesNotTerminate(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, false)
	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || out.TerminationTriggered || terminator.calls != 0 || disarmer.calls != 0 {
		t.Fatalf("out=%#v err=%v terminator=%#v disarmer=%#v", out, err, terminator, disarmer)
	}
}

func TestCancelTaskExecute_PersistedStartingWithPIDTerminatesWithoutEarlyDisarm(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, true)
	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || !out.TerminationTriggered || terminator.calls != 1 || disarmer.calls != 0 {
		t.Fatalf("out=%#v err=%v terminator=%#v disarmer=%#v", out, err, terminator, disarmer)
	}
}

func TestCancelTaskExecute_PersistedOrphanedImmediatelyConfirmsKilled(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateOrphaned, false)
	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || out.State != domain.StateCancelling || tasks.saves != 2 || tasks.snapshot.State != domain.StateKilled || terminator.calls != 0 || disarmer.calls != 1 {
		t.Fatalf("out=%#v err=%v snapshot=%#v terminator=%#v disarmer=%#v", out, err, tasks.snapshot, terminator, disarmer)
	}
}

func TestCancelTaskExecute_PersistedAdoptedWithoutPIDRemainsCancellingForReconciliation(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateAdopted, false)
	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || out.State != domain.StateCancelling || out.TerminationTriggered || tasks.saves != 1 || tasks.snapshot.State != domain.StateCancelling || terminator.calls != 0 || disarmer.calls != 1 {
		t.Fatalf("out=%#v err=%v snapshot=%#v terminator=%#v disarmer=%#v", out, err, tasks.snapshot, terminator, disarmer)
	}
}

func TestCancelTaskExecute_PersistedAdoptedWithoutPIDRegistersConfirmOnly(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks := &cancelStoreFake{reserved: true, snapshot: cancelPersistedSnapshot(t, domain.StateAdopted, false)}
	queue := &cancelQueueFake{payload: payload, index: 1, queueMu: &sync.Mutex{}}
	pending := &cancelPendingRegistrarFake{}
	disarmer := &cancelDisarmerFake{}
	terminator := &cancelTerminatorFake{}
	confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, pending)
	uc := NewCancelTaskUseCase(tasks, queue, queue.queueMu, store.NewTaskMutex(), &cancelEventsFake{}, terminator, pending, disarmer, confirmer, &cancelStalledTrackerFake{}, domain.ClockFunc(time.Now))

	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || out.TerminationTriggered || pending.calls != 1 || pending.disposition != recovery.PendingSendConfirmOnly || pending.authority != nil || disarmer.calls != 1 || terminator.calls != 0 || tasks.snapshot.State != domain.StateCancelling {
		t.Fatalf("out=%#v err=%v pending=%#v disarmer=%#v terminator=%#v snapshot=%#v", out, err, pending, disarmer, terminator, tasks.snapshot)
	}
}

func TestCancelTaskExecute_LiveAdoptedWithoutPIDRegistersConfirmOnly(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stalled   bool
		wantState domain.TaskState
	}{
		{name: "running", wantState: domain.StateRunning},
		{name: "stalled", stalled: true, wantState: domain.StateStalled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := cancelQueuedPayload(t)
			tasks := &cancelStoreFake{reserved: true, snapshot: cancelLiveAdoptedWithoutPIDSnapshot(t, payload, tc.stalled)}
			if tasks.snapshot.State != tc.wantState || !tasks.snapshot.AdoptedAfterRestart || tasks.snapshot.PID != nil {
				t.Fatalf("snapshot=%#v", tasks.snapshot)
			}
			queue := &cancelQueueFake{payload: payload, index: 1, queueMu: &sync.Mutex{}}
			pending := &cancelPendingRegistrarFake{}
			disarmer := &cancelDisarmerFake{}
			terminator := &cancelTerminatorFake{}
			confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, pending)
			uc := NewCancelTaskUseCase(tasks, queue, queue.queueMu, store.NewTaskMutex(), &cancelEventsFake{}, terminator, pending, disarmer, confirmer, &cancelStalledTrackerFake{}, domain.ClockFunc(time.Now))

			out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			if err != nil || out.TerminationTriggered || pending.calls != 1 || pending.disposition != recovery.PendingSendConfirmOnly || pending.authority != nil || disarmer.calls != 1 || terminator.calls != 0 || tasks.snapshot.State != domain.StateCancelling {
				t.Fatalf("out=%#v err=%v pending=%#v disarmer=%#v terminator=%#v snapshot=%#v", out, err, pending, disarmer, terminator, tasks.snapshot)
			}
		})
	}
}

func TestCancelTaskExecute_RemovePanicUnlocksQueueMutex(t *testing.T) {
	payload := cancelQueuedPayload(t)
	_, queue, _, _, _, uc := cancelFixture(t, payload, false)
	queue.panicRemove = true
	queueMu := uc.queueMu
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Remove panic was not propagated")
			}
		}()
		_, _ = uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	}()
	locked := make(chan struct{})
	go func() {
		queueMu.Lock()
		close(locked)
		queueMu.Unlock()
	}()
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("queue mutex remained locked after Remove panic")
	}
}

func TestCancelTaskExecute_PersistedCancellingDoesNotResendTerminate(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateCancelling, true)
	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || out.TerminationTriggered || terminator.calls != 0 || disarmer.calls != 0 || tasks.snapshot.State != domain.StateCancelling {
		t.Fatalf("out=%#v err=%v snapshot=%#v terminator=%#v disarmer=%#v", out, err, tasks.snapshot, terminator, disarmer)
	}
}

func TestCancelTaskExecute_PersistedTerminateFailureRetainsCancellingAndLogs(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateRunning, true)
	terminator.err = errors.New("terminate I/O failure")
	order := []string{}
	terminator.order, disarmer.order = &order, &order
	pending := uc.pendingRegistrar.(*cancelPendingRegistrarFake)
	pending.order = &order
	var logs bytes.Buffer
	uc.logger = slog.New(slog.NewJSONHandler(&logs, nil))

	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || !out.TerminationTriggered || terminator.calls != 1 || pending.calls != 1 || pending.disposition != recovery.PendingSendUnsent || pending.authority == nil || pending.authority.TaskID != payload.Task.ID() || pending.authority.PID != 4321 || !pending.authority.ProcessStartedAt.Equal(*tasks.snapshot.ProcessStartedAt) || disarmer.calls != 1 || strings.Join(order, ",") != "terminate,register,disarm" || tasks.snapshot.State != domain.StateCancelling || !strings.Contains(logs.String(), "terminate I/O failure") {
		t.Fatalf("out=%#v err=%v snapshot=%#v terminator=%#v pending=%#v disarmer=%#v order=%#v logs=%q", out, err, tasks.snapshot, terminator, pending, disarmer, order, logs.String())
	}
}

func TestCancelTaskExecute_PersistedTerminateFailurePendingRegistrationFailureRetainsBothCauses(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateRunning, true)
	terminateErr := errors.New("terminate failure")
	registerErr := errors.New("register failure")
	terminator.err = terminateErr
	order := []string{}
	terminator.order, disarmer.order = &order, &order
	pending := uc.pendingRegistrar.(*cancelPendingRegistrarFake)
	pending.err, pending.order = registerErr, &order

	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if !out.TerminationTriggered || pending.calls != 1 || disarmer.calls != 0 || strings.Join(order, ",") != "terminate,register" || !errors.Is(err, terminateErr) || !errors.Is(err, registerErr) || tasks.snapshot.State != domain.StateCancelling {
		t.Fatalf("out=%#v err=%v snapshot=%#v pending=%#v disarmer=%#v order=%#v", out, err, tasks.snapshot, pending, disarmer, order)
	}
}

func TestCancelTaskExecute_PersistedStartingTerminateFailureDoesNotDisarm(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, true)
	terminator.err = errors.New("terminate failure")
	pending := uc.pendingRegistrar.(*cancelPendingRegistrarFake)

	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || !out.TerminationTriggered || pending.calls != 1 || pending.disposition != recovery.PendingSendUnsent || disarmer.calls != 0 {
		t.Fatalf("out=%#v err=%v pending=%#v disarmer=%#v", out, err, pending, disarmer)
	}
}

func TestCancelPendingRegistration(t *testing.T) {
	id := cancelTaskID(t)
	startedAt := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	pid := 4321
	for _, tc := range []struct {
		name             string
		pid              *int
		processStartedAt *time.Time
		wantDisposition  recovery.PendingSendDisposition
		wantAuthority    bool
	}{
		{"complete-pair", &pid, &startedAt, recovery.PendingSendUnsent, true},
		{"missing-pid", nil, &startedAt, recovery.PendingSendConfirmOnly, false},
		{"non-positive-pid", new(int), &startedAt, recovery.PendingSendConfirmOnly, false},
		{"missing-start-time", &pid, nil, recovery.PendingSendConfirmOnly, false},
		{"zero-start-time", &pid, new(time.Time), recovery.PendingSendConfirmOnly, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			disposition, authority := cancelPendingRegistration(id, tc.pid, tc.processStartedAt)
			if disposition != tc.wantDisposition || (authority != nil) != tc.wantAuthority || authority != nil && (authority.TaskID != id || authority.PID != pid || !authority.ProcessStartedAt.Equal(startedAt)) {
				t.Fatalf("disposition=%v authority=%#v", disposition, authority)
			}
		})
	}
}

func TestCancelTaskHandle_Validation(t *testing.T) {
	payload := cancelQueuedPayload(t)
	_, queue, _, _, _, uc := cancelFixture(t, payload, false)
	for _, req := range []transport.Request{{RequestID: "bad-id", TaskID: "invalid", Params: json.RawMessage(`null`)}, {RequestID: "null", TaskID: payload.Task.ID().String(), Params: json.RawMessage(`{"force":null}`)}, {RequestID: "array", TaskID: payload.Task.ID().String(), Params: json.RawMessage(`[]`)}, {RequestID: "json", TaskID: payload.Task.ID().String(), Params: json.RawMessage(`{`)}} {
		response := uc.Handle(req)
		if response.OK || response.Error == nil || queue.removes != 0 {
			t.Fatalf("response=%#v removes=%d", response, queue.removes)
		}
	}
}

// RED-08: the transport contract permits missing params, but rejects every
// non-object or unknown-field variant before invoking CancelTaskUseCase.Execute.
func TestCancelTaskHandleParamsContract(t *testing.T) {
	for _, tc := range []struct {
		name        string
		params      json.RawMessage
		wantExecute int
		wantForce   bool
	}{
		{"params-missing", nil, 1, false},
		{"empty-object", json.RawMessage(`{}`), 1, false},
		{"force-missing-object", json.RawMessage(` { } `), 1, false},
		{"force-true", json.RawMessage(`{"force":true}`), 1, true},
		{"force-false", json.RawMessage(`{"force":false}`), 1, false},
		{"case-insensitive-upper", json.RawMessage(`{"FORCE":true}`), 0, false},
		{"case-insensitive-title", json.RawMessage(`{"Force":true}`), 0, false},
		{"unknown-field", json.RawMessage(`{"unknown":true}`), 0, false},
		{"params-null", json.RawMessage(`null`), 0, false},
		{"params-array", json.RawMessage(`[]`), 0, false},
		{"malformed-json", json.RawMessage(`{`), 0, false},
		{"trailing-token", json.RawMessage(`{"force":true} x`), 0, false},
		{"force-null", json.RawMessage(`{"force":null}`), 0, false},
		{"force-wrong-type", json.RawMessage(`{"force":"true"}`), 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := cancelQueuedPayload(t)
			tasks, queue, events, _, _, uc := cancelFixture(t, payload, false)
			tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, false)
			response := uc.Handle(transport.Request{RequestID: tc.name, TaskID: payload.Task.ID().String(), Params: tc.params})
			if tc.wantExecute == 0 {
				if response.OK || response.Error == nil || response.Error.Code != "CANCEL_PARAMS_MALFORMED" || response.Error.MessageKey != "error.cancel.paramsMalformed" || response.Error.Detail != nil || queue.removes != 0 {
					t.Fatalf("response=%#v removes=%d", response, queue.removes)
				}
				return
			}
			if !response.OK || queue.removes != tc.wantExecute {
				t.Fatalf("response=%#v removes=%d", response, queue.removes)
			}
			var body struct {
				TaskID     string           `json:"task_id"`
				State      domain.TaskState `json:"state"`
				MessageKey string           `json:"message_key"`
			}
			if err := json.Unmarshal(response.Result, &body); err != nil || body.TaskID != payload.Task.ID().String() || body.State != domain.StateCancelling || body.MessageKey != "status.task.cancelling" {
				t.Fatalf("body=%s err=%v", response.Result, err)
			}
			if len(events.events) != 1 {
				t.Fatalf("events=%#v", events.events)
			}
			event, ok := events.events[0].(domain.TaskCancelRequested)
			if !ok || event.Force != tc.wantForce {
				t.Fatalf("event=%#v", events.events[0])
			}
		})
	}
}

func TestCancelTaskHandle_ContractWriteFailuresAreSanitized(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*cancelStoreFake)
	}{
		{"save", func(tasks *cancelStoreFake) {
			tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, false)
			tasks.saveErr = errors.New("/private/contract/task.json secret=cancel-token")
		}},
		{"reservation", func(tasks *cancelStoreFake) {
			tasks.reservedErr = errors.New("/private/contract/reservation secret=cancel-token")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := cancelQueuedPayload(t)
			tasks, _, _, _, _, uc := cancelFixture(t, payload, false)
			tc.setup(tasks)
			response := uc.Handle(transport.Request{RequestID: tc.name, TaskID: payload.Task.ID().String(), Params: json.RawMessage(`{}`)})
			if response.OK || response.Error == nil || response.Error.Code != "CONTRACT_WRITE_FAILED" || response.Error.MessageKey != "error.contract.writeFailed" {
				t.Fatalf("response=%#v", response)
			}
			detail, err := json.Marshal(response.Error.Detail)
			if err != nil || strings.Contains(string(detail), "/private/contract") || strings.Contains(string(detail), "task.json") || strings.Contains(string(detail), "cancel-token") || !strings.Contains(string(detail), payload.Task.ID().String()) {
				t.Fatalf("detail=%s err=%v", detail, err)
			}
		})
	}
}

func TestCancelTaskExecute_ContractWriteFailuresRetainUnderlyingCause(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*cancelStoreFake, error)
	}{
		{"initial-reservation", func(tasks *cancelStoreFake, cause error) { tasks.reservedErr = cause }},
		{"queued-save", func(tasks *cancelStoreFake, cause error) { tasks.saveErr = cause }},
		{"rechecked-reservation", func(tasks *cancelStoreFake, cause error) {
			tasks.loadErr, tasks.reservedErrs = domain.ErrTaskNotFound, []error{nil, cause}
		}},
		{"persisted-load", func(tasks *cancelStoreFake, cause error) {
			tasks.snapshot, tasks.loadErr = cancelPersistedSnapshot(t, domain.StateStarting, false), cause
		}},
		{"persisted-save", func(tasks *cancelStoreFake, cause error) {
			tasks.snapshot, tasks.saveErr = cancelPersistedSnapshot(t, domain.StateStarting, false), cause
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := cancelQueuedPayload(t)
			removed := tc.name == "queued-save"
			tasks, _, _, _, _, uc := cancelFixture(t, payload, removed)
			cause := &os.PathError{Op: "write", Path: "/private/contract/task.json", Err: errors.New("storage failure")}
			tc.setup(tasks, cause)

			_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			var pathErr *os.PathError
			if !errors.Is(err, domain.ErrContractWriteFailed) || !errors.Is(err, cause) || !errors.As(err, &pathErr) || pathErr != cause {
				t.Fatalf("err=%v pathErr=%#v cause=%#v", err, pathErr, cause)
			}
			if tc.name == "rechecked-reservation" && tasks.reservations != 2 {
				t.Fatalf("reservations=%d, want 2", tasks.reservations)
			}
		})
	}
}

// RED: only a persisted stalled task closes the in-memory stalled interval.
func TestCancelTaskStalledTrackerPersistedTransitions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		state     domain.TaskState
		saveErr   error
		wantCalls int
	}{
		{"stalled-save-success", domain.StateStalled, nil, 1},
		{"running-save-success", domain.StateRunning, nil, 0},
		{"starting-save-success", domain.StateStarting, nil, 0},
		{"adopted-save-success", domain.StateAdopted, nil, 0},
		{"orphaned-save-success", domain.StateOrphaned, nil, 0},
		{"cancelling-self-loop", domain.StateCancelling, nil, 0},
		{"stalled-save-failure", domain.StateStalled, errors.New("save"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := cancelQueuedPayload(t)
			tasks := &cancelStoreFake{reserved: true, snapshot: cancelPersistedSnapshot(t, tc.state, true), saveErr: tc.saveErr}
			queue, events := &cancelQueueFake{}, &cancelEventsFake{}
			tracker := &cancelStalledTrackerFake{}
			disarmer := &cancelDisarmerFake{}
			confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, &cancelPendingRegistrarFake{})
			uc := NewCancelTaskUseCase(tasks, queue, &sync.Mutex{}, store.NewTaskMutex(), events, &cancelTerminatorFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, tracker, domain.ClockFunc(time.Now))
			at := time.Date(2026, time.August, 11, 12, 2, 0, 0, time.UTC)
			_, _ = uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: at})
			if len(tracker.calls) != tc.wantCalls {
				t.Fatalf("LeaveStalled calls=%d, want %d", len(tracker.calls), tc.wantCalls)
			}
			if tc.wantCalls == 1 && (tracker.calls[0].id != payload.Task.ID() || !tracker.calls[0].at.Equal(at)) {
				t.Fatalf("LeaveStalled call=%#v", tracker.calls[0])
			}
		})
	}
}

func TestCancelTaskQueuedSaveDoesNotUpdateStalledTracker(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, queue, events := &cancelStoreFake{reserved: true}, &cancelQueueFake{payload: payload, removed: true}, &cancelEventsFake{}
	tracker, disarmer := &cancelStalledTrackerFake{}, &cancelDisarmerFake{}
	confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, &cancelPendingRegistrarFake{})
	uc := NewCancelTaskUseCase(tasks, queue, &sync.Mutex{}, store.NewTaskMutex(), events, &cancelTerminatorFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, tracker, domain.ClockFunc(time.Now))
	_, _ = uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if len(tracker.calls) != 0 {
		t.Fatalf("queued cancel called LeaveStalled: %+v", tracker.calls)
	}
}

func TestNewCancelTaskUseCaseRejectsNilStalledTimeTracker(t *testing.T) {
	for _, tracker := range []stalledTimeTracker{nil, (*metrics.StalledTimeTracker)(nil)} {
		t.Run("nil", func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			payload := cancelQueuedPayload(t)
			tasks, queue, events, terminator, disarmer, _ := cancelFixture(t, payload, false)
			confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, &cancelPendingRegistrarFake{})
			NewCancelTaskUseCase(tasks, queue, &sync.Mutex{}, store.NewTaskMutex(), events, terminator, &cancelPendingRegistrarFake{}, disarmer, confirmer, tracker, domain.ClockFunc(time.Now))
		})
	}
}

func TestNewCancelTaskUseCaseRejectsNilPendingRegistrar(t *testing.T) {
	var pending recovery.PendingRegistrar = (*cancelPendingRegistrarFake)(nil)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	payload := cancelQueuedPayload(t)
	tasks, queue, events, terminator, disarmer, _ := cancelFixture(t, payload, false)
	confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, &cancelPendingRegistrarFake{})
	NewCancelTaskUseCase(tasks, queue, &sync.Mutex{}, store.NewTaskMutex(), events, terminator, pending, disarmer, confirmer, &cancelStalledTrackerFake{}, domain.ClockFunc(time.Now))
}

func TestCancelTaskStalledTrackerPreSaveFailures(t *testing.T) {
	for _, tc := range []struct {
		name     string
		loadErr  error
		reserved bool
		state    domain.TaskState
	}{
		{"load-error", errors.New("load"), true, domain.StateStalled},
		{"cancel-state-changed", domain.ErrTaskNotFound, true, domain.StateStalled},
		{"request-cancel-rejected", nil, true, domain.StateCompleted},
		{"with-task-error", nil, true, domain.StateStalled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := cancelQueuedPayload(t)
			snapshot := cancelPersistedSnapshot(t, tc.state, true)
			if tc.name == "with-task-error" {
				snapshot = domain.TaskSnapshot{TaskID: payload.Task.ID(), State: domain.StateStalled}
			}
			tasks := &cancelStoreFake{reserved: tc.reserved, snapshot: snapshot, loadErr: tc.loadErr}
			tracker, disarmer := &cancelStalledTrackerFake{}, &cancelDisarmerFake{}
			confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, &cancelPendingRegistrarFake{})
			uc := NewCancelTaskUseCase(tasks, &cancelQueueFake{}, &sync.Mutex{}, store.NewTaskMutex(), &cancelEventsFake{}, &cancelTerminatorFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, tracker, domain.ClockFunc(time.Now))
			_, _ = uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			if len(tracker.calls) != 0 {
				t.Fatalf("LeaveStalled calls=%d", len(tracker.calls))
			}
		})
	}
}

func TestCancelTaskStalledTrackerNeverEntersOrTakes(t *testing.T) {
	var _ stalledTimeTracker = (*cancelStalledTrackerFake)(nil)
	payload := cancelQueuedPayload(t)
	tasks := &cancelStoreFake{reserved: true, snapshot: cancelPersistedSnapshot(t, domain.StateStalled, true)}
	tracker, disarmer := &cancelStalledTrackerFake{}, &cancelDisarmerFake{}
	confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, &cancelPendingRegistrarFake{})
	uc := NewCancelTaskUseCase(tasks, &cancelQueueFake{}, &sync.Mutex{}, store.NewTaskMutex(), &cancelEventsFake{}, &cancelTerminatorFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, tracker, domain.ClockFunc(time.Now))
	_, _ = uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if len(tracker.calls) != 1 {
		t.Fatalf("LeaveStalled calls=%d, want 1", len(tracker.calls))
	}
}

func TestCancelTaskHandleResponsesRemainStableWithStalledTracker(t *testing.T) {
	for _, tc := range []struct {
		name     string
		setup    func(*cancelStoreFake)
		wantOK   bool
		wantCode string
		wantKey  string
	}{
		{"success", func(tasks *cancelStoreFake) { tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStalled, true) }, true, "", ""},
		{"TASK_NOT_FOUND", func(tasks *cancelStoreFake) { tasks.reserved = false }, false, "TASK_NOT_FOUND", "error.task.notFound"},
		{"TASK_ALREADY_TERMINAL", func(tasks *cancelStoreFake) { tasks.snapshot = cancelPersistedSnapshot(t, domain.StateCompleted, true) }, false, "TASK_ALREADY_TERMINAL", "error.task.alreadyTerminal"},
		{"TASK_INVALID_TRANSITION", func(tasks *cancelStoreFake) { tasks.snapshot = cancelPersistedSnapshot(t, domain.StateTimeout, true) }, false, "TASK_INVALID_TRANSITION", "error.task.invalidTransition"},
		{"CANCEL_STATE_CHANGED", func(tasks *cancelStoreFake) { tasks.loadErr = domain.ErrTaskNotFound }, false, "CANCEL_STATE_CHANGED", "error.cancel.stateChanged"},
		{"CONTRACT_WRITE_FAILED", func(tasks *cancelStoreFake) {
			tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStalled, true)
			tasks.saveErr = errors.New("save")
		}, false, "CONTRACT_WRITE_FAILED", "error.contract.writeFailed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := cancelQueuedPayload(t)
			tasks, _, _, _, _, uc := cancelFixture(t, payload, false)
			tc.setup(tasks)
			response := uc.Handle(transport.Request{RequestID: tc.name, TaskID: payload.Task.ID().String(), Params: json.RawMessage(`{}`)})
			if response.OK != tc.wantOK {
				t.Fatalf("response=%#v", response)
			}
			if tc.wantOK {
				var body struct {
					State domain.TaskState `json:"state"`
				}
				if err := json.Unmarshal(response.Result, &body); err != nil || body.State != domain.StateCancelling {
					t.Fatalf("body=%s err=%v", response.Result, err)
				}
				return
			}
			if response.Error == nil || response.Error.Code != tc.wantCode || response.Error.MessageKey != tc.wantKey {
				t.Fatalf("response=%#v", response)
			}
		})
	}
}
