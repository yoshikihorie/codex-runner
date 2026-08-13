package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

type cancelStoreFake struct {
	snapshot                      domain.TaskSnapshot
	loadErr, saveErr, reservedErr error
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
	f.reservations++
	return f.reserved, f.reservedErr
}
func (*cancelStoreFake) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	return nil, nil
}
func (*cancelStoreFake) Reserve(domain.TaskID) error { return nil }
func (*cancelStoreFake) Release(domain.TaskID) error { return nil }

type cancelQueueFake struct {
	payload           execution.TaskLaunchPayload
	index             int
	removed           bool
	panicRemove       bool
	removes, restores int
	removeBeforeStore bool
}

func (f *cancelQueueFake) Remove(domain.TaskID, time.Time) (execution.TaskLaunchPayload, int, bool, []domain.Event) {
	f.removes++
	if f.panicRemove {
		panic("remove panic")
	}
	return f.payload, f.index, f.removed, nil
}
func (f *cancelQueueFake) Restore(payload execution.TaskLaunchPayload, index int, _ time.Time) []domain.Event {
	f.restores++
	f.payload, f.index, f.removed = payload, index, true
	return nil
}

type cancelEventsFake struct {
	events []domain.Event
	err    error
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
func (*cancelReaderFake) ReadExitCode(domain.TaskID) (int, bool, error)        { return 0, false, nil }

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
	queue := &cancelQueueFake{payload: payload, index: 1, removed: removed}
	events := &cancelEventsFake{}
	terminator := &cancelTerminatorFake{}
	disarmer := &cancelDisarmerFake{}
	writer := &cancelWriterFake{}
	confirmer := execution.NewConfirmTaskKilledUseCase(tasks, writer, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), slots, domain.ClockFunc(time.Now))
	uc := NewCancelTaskUseCase(tasks, queue, queueMu, store.NewTaskMutex(), events, terminator, disarmer, confirmer, domain.ClockFunc(func() time.Time { return time.Date(2026, 8, 11, 12, 1, 0, 0, time.UTC) }))
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
	if err == nil || queue.restores != 1 || queue.index != 1 || queue.payload.Task != payload.Task || payload.Task.State() != domain.StateQueued || len(events.events) != 0 {
		t.Fatalf("err=%v queue=%#v state=%s", err, queue, payload.Task.State())
	}
}

func TestCancelTaskExecute_QueuedSnapshotFailureRestoresOriginalPositionAndPointer(t *testing.T) {
	payload := cancelQueuedPayload(t)
	payload.Model = ""
	tasks, queue, events, _, _, uc := cancelFixture(t, payload, true)
	_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err == nil || tasks.saves != 0 || queue.restores != 1 || queue.index != 1 || queue.payload.Task != payload.Task || payload.Task.State() != domain.StateQueued || len(events.events) != 0 {
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

			out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			if err != nil || !out.TerminationTriggered || terminator.calls != 1 || disarmer.calls != 1 || strings.Join(order, ",") != "terminate,disarm" {
				t.Fatalf("out=%#v err=%v terminator=%#v disarmer=%#v order=%#v", out, err, terminator, disarmer, order)
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

func TestCancelTaskExecute_PersistedAdoptedWithoutPIDImmediatelyConfirmsKilled(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateAdopted, false)
	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || out.State != domain.StateCancelling || !out.TerminationTriggered || tasks.saves != 2 || tasks.snapshot.State != domain.StateKilled || terminator.calls != 0 || disarmer.calls != 1 {
		t.Fatalf("out=%#v err=%v snapshot=%#v terminator=%#v disarmer=%#v", out, err, tasks.snapshot, terminator, disarmer)
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
	tasks, _, _, terminator, _, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateRunning, true)
	terminator.err = errors.New("terminate I/O failure")
	var logs bytes.Buffer
	uc.logger = slog.New(slog.NewJSONHandler(&logs, nil))

	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || !out.TerminationTriggered || terminator.calls != 1 || tasks.snapshot.State != domain.StateCancelling || !strings.Contains(logs.String(), "terminate I/O failure") {
		t.Fatalf("out=%#v err=%v snapshot=%#v terminator=%#v logs=%q", out, err, tasks.snapshot, terminator, logs.String())
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

func TestCancelTaskHandle_ContractWriteFailuresAreSanitized(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*cancelStoreFake)
	}{
		{"save", func(tasks *cancelStoreFake) {
			tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, false)
			tasks.saveErr = errors.New("/private/contract/task.json")
		}},
		{"reservation", func(tasks *cancelStoreFake) { tasks.reservedErr = errors.New("/private/contract/reservation") }},
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
			if err != nil || strings.Contains(string(detail), "/private/contract") || strings.Contains(string(detail), "task.json") || !strings.Contains(string(detail), payload.Task.ID().String()) {
				t.Fatalf("detail=%s err=%v", detail, err)
			}
		})
	}
}
