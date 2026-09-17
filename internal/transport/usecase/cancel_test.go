package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
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
	loadErrs                      []error
	reservedErrs                  []error
	reserved                      bool
	saves, loads, reservations    int
}

func (f *cancelStoreFake) Load(domain.TaskID) (domain.TaskSnapshot, error) {
	idx := f.loads
	f.loads++
	if idx < len(f.loadErrs) && f.loadErrs[idx] != nil {
		return domain.TaskSnapshot{}, f.loadErrs[idx]
	}
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

type cancelPanickingLogHandler struct {
	calls int
}

func (*cancelPanickingLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *cancelPanickingLogHandler) Handle(context.Context, slog.Record) error {
	h.calls++
	panic("log write panic")
}
func (h *cancelPanickingLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *cancelPanickingLogHandler) WithGroup(string) slog.Handler      { return h }

type cancelQueueReindexLogObservation struct {
	queueMu            *sync.Mutex
	reindexLogUnlocked []bool
}

type cancelQueueUnlockLogHandler struct {
	next        slog.Handler
	observation *cancelQueueReindexLogObservation
}

func (h *cancelQueueUnlockLogHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}
func (h *cancelQueueUnlockLogHandler) Handle(ctx context.Context, record slog.Record) error {
	isQueueReindex := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "queue_reindex_source" {
			isQueueReindex = true
		}
		return true
	})
	if isQueueReindex {
		unlocked := h.observation.queueMu.TryLock()
		if unlocked {
			h.observation.queueMu.Unlock()
		}
		h.observation.reindexLogUnlocked = append(h.observation.reindexLogUnlocked, unlocked)
	}
	return h.next.Handle(ctx, record)
}
func (h *cancelQueueUnlockLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &cancelQueueUnlockLogHandler{next: h.next.WithAttrs(attrs), observation: h.observation}
}
func (h *cancelQueueUnlockLogHandler) WithGroup(name string) slog.Handler {
	return &cancelQueueUnlockLogHandler{next: h.next.WithGroup(name), observation: h.observation}
}

type cancelQueueReindexLogRecord struct {
	EventType     string `json:"event_type"`
	TaskID        string `json:"task_id"`
	QueuePosition int    `json:"queue_position"`
	Source        string `json:"queue_reindex_source"`
}

func queueReindexLogEvents(t *testing.T, logs *bytes.Buffer) []cancelQueueReindexLogRecord {
	t.Helper()
	var records []cancelQueueReindexLogRecord
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var record cancelQueueReindexLogRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	return records
}

func allQueueReindexLogsUnlocked(observation *cancelQueueReindexLogObservation, count int) bool {
	if len(observation.reindexLogUnlocked) != count {
		return false
	}
	for _, unlocked := range observation.reindexLogUnlocked {
		if !unlocked {
			return false
		}
	}
	return true
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

type cancelOwnershipFake struct {
	generation domain.LifecycleGeneration
	unowned    bool
}

func (f *cancelOwnershipFake) Current(domain.TaskID) (domain.LifecycleGeneration, bool) {
	if f.generation == 0 {
		f.generation = 1
	}
	return f.generation, !f.unowned
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
	calls    int
	pid      int
	grace    time.Duration
	termPIDs []int
	killPIDs []int
	err      error
	order    *[]string
}

func (f *cancelTerminatorFake) SendTerminate(pid int) error {
	f.calls++
	f.pid = pid
	f.termPIDs = append(f.termPIDs, pid)
	if f.order != nil {
		*f.order = append(*f.order, "terminate")
	}
	return f.err
}

func (f *cancelTerminatorFake) SendKill(pid int) error {
	f.calls++
	f.pid = pid
	f.killPIDs = append(f.killPIDs, pid)
	if f.order != nil {
		*f.order = append(*f.order, "kill")
	}
	return f.err
}

type cancelTerminationEnsurerContract interface {
	SendAndConfirm(context.Context, domain.TaskID, recovery.ProcessSignalAuthority, time.Duration) recovery.TerminationAttemptResult
}

func TestCancelTerminationAuthorityContractCarriesCancellingState(t *testing.T) {
	id := cancelTaskID(t)
	started := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	generation := domain.LifecycleGeneration(3)
	authority := recovery.ProcessSignalAuthority{TaskID: id, PID: 4321, ProcessStartedAt: started, ExpectedState: domain.StateCancelling, LifecycleGeneration: &generation}
	var _ cancelTerminationEnsurerContract = (*cancelTerminationEnsurerFake)(nil)
	if authority.ExpectedState != domain.StateCancelling || authority.LifecycleGeneration == nil {
		t.Fatalf("authority=%#v", authority)
	}
}

type cancelTerminationEnsurerFake struct {
	calls        int
	confirmCalls int
	taskID       domain.TaskID
	authority    recovery.ProcessSignalAuthority
	grace        time.Duration
	order        *[]string
	result       recovery.TerminationAttemptResult
	confirmDead  bool
	confirmErr   error
}

func (f *cancelTerminationEnsurerFake) Confirm(context.Context, domain.TaskID) (bool, error) {
	f.confirmCalls++
	return f.confirmDead, f.confirmErr
}

func (f *cancelTerminationEnsurerFake) SendAndConfirm(_ context.Context, taskID domain.TaskID, authority recovery.ProcessSignalAuthority, grace time.Duration) recovery.TerminationAttemptResult {
	f.calls++
	f.taskID, f.authority, f.grace = taskID, authority, grace
	if f.order != nil {
		*f.order = append(*f.order, "termination")
	}
	return f.result
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
	calls             int
	taskID            domain.TaskID
	disposition       recovery.PendingSendDisposition
	authority         *recovery.ProcessSignalAuthority
	err               error
	order             *[]string
	set               recovery.PendingReconciliationSet
	initialOutcome    *recovery.ClaimOutcome
	claimInitialCalls int
	completeCalls     int
	releaseCalls      int
	invalidateCalls   int
	removeCalls       int
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
func (f *cancelPendingRegistrarFake) ClaimForSend(id domain.TaskID, authority recovery.ProcessSignalAuthority) (recovery.SendClaim, recovery.ClaimOutcome) {
	return f.set.ClaimForSend(id, authority)
}
func (f *cancelPendingRegistrarFake) ClaimInitialSend(id domain.TaskID, authority recovery.ProcessSignalAuthority) (recovery.SendClaim, recovery.ClaimOutcome) {
	f.claimInitialCalls++
	if f.initialOutcome != nil {
		if *f.initialOutcome == recovery.ClaimAcquired {
			return recovery.SendClaim{TaskID: id, Token: 1, Authority: authority}, *f.initialOutcome
		}
		return recovery.SendClaim{}, *f.initialOutcome
	}
	return f.set.ClaimInitialSend(id, authority)
}
func (f *cancelPendingRegistrarFake) CompleteSend(claim recovery.SendClaim) bool {
	f.completeCalls++
	return f.set.CompleteSend(claim)
}
func (f *cancelPendingRegistrarFake) ReleaseSend(claim recovery.SendClaim) bool {
	f.releaseCalls++
	return f.set.ReleaseSend(claim)
}
func (f *cancelPendingRegistrarFake) InvalidateSend(claim recovery.SendClaim) bool {
	f.invalidateCalls++
	return f.set.InvalidateSend(claim)
}
func (f *cancelPendingRegistrarFake) RemoveClaim(claim recovery.SendClaim) bool {
	f.removeCalls++
	return f.set.RemoveClaim(claim)
}

var _ recovery.PendingRegistrar = (*cancelPendingRegistrarFake)(nil)

type cancelTrackedTaskMutex struct {
	mu     sync.Mutex
	state  sync.Mutex
	locked bool
}

func (m *cancelTrackedTaskMutex) Lock(domain.TaskID) {
	m.mu.Lock()
	m.state.Lock()
	m.locked = true
	m.state.Unlock()
}

func (m *cancelTrackedTaskMutex) Unlock(domain.TaskID) {
	m.state.Lock()
	m.locked = false
	m.state.Unlock()
	m.mu.Unlock()
}

func (m *cancelTrackedTaskMutex) IsLocked() bool {
	m.state.Lock()
	defer m.state.Unlock()
	return m.locked
}

type cancelBarrierPendingRegistrar struct {
	set                    recovery.PendingReconciliationSet
	arrived                chan struct{}
	release                <-chan struct{}
	taskMu                 *cancelTrackedTaskMutex
	mu                     sync.Mutex
	taskMutexHeldAtInitial bool
}

func (*cancelBarrierPendingRegistrar) Register(domain.TaskID, recovery.PendingSendDisposition, *recovery.ProcessSignalAuthority) error {
	return nil
}
func (p *cancelBarrierPendingRegistrar) ClaimForSend(id domain.TaskID, authority recovery.ProcessSignalAuthority) (recovery.SendClaim, recovery.ClaimOutcome) {
	return p.set.ClaimForSend(id, authority)
}
func (p *cancelBarrierPendingRegistrar) ClaimInitialSend(id domain.TaskID, authority recovery.ProcessSignalAuthority) (recovery.SendClaim, recovery.ClaimOutcome) {
	if p.taskMu.IsLocked() {
		p.mu.Lock()
		p.taskMutexHeldAtInitial = true
		p.mu.Unlock()
	}
	p.arrived <- struct{}{}
	<-p.release
	return p.set.ClaimInitialSend(id, authority)
}

func (p *cancelBarrierPendingRegistrar) SawTaskMutexDuringInitialClaim() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.taskMutexHeldAtInitial
}
func (p *cancelBarrierPendingRegistrar) CompleteSend(claim recovery.SendClaim) bool {
	return p.set.CompleteSend(claim)
}
func (p *cancelBarrierPendingRegistrar) ReleaseSend(claim recovery.SendClaim) bool {
	return p.set.ReleaseSend(claim)
}
func (p *cancelBarrierPendingRegistrar) InvalidateSend(claim recovery.SendClaim) bool {
	return p.set.InvalidateSend(claim)
}
func (p *cancelBarrierPendingRegistrar) RemoveClaim(claim recovery.SendClaim) bool {
	return p.set.RemoveClaim(claim)
}

var _ recovery.PendingRegistrar = (*cancelBarrierPendingRegistrar)(nil)

func (f *cancelDisarmerFake) Disarm(domain.TaskID) {
	f.calls++
	if f.order != nil {
		*f.order = append(*f.order, "disarm")
	}
}

type cancelWriterFake struct {
	contract.ContractWriter
	events       []domain.Event
	exits        []domain.ExitCode
	writeExitErr error
}

type cancelConfirmerFake struct {
	calls int
	err   error
}

func (f *cancelConfirmerFake) Execute(context.Context, execution.ConfirmTaskKilledInput) (execution.ConfirmTaskKilledOutput, error) {
	f.calls++
	return execution.ConfirmTaskKilledOutput{}, f.err
}

type cancelConfirmerSpy struct {
	delegate cancelTaskKilledConfirmer
	inputs   []execution.ConfirmTaskKilledInput
}

func (s *cancelConfirmerSpy) Execute(ctx context.Context, in execution.ConfirmTaskKilledInput) (execution.ConfirmTaskKilledOutput, error) {
	s.inputs = append(s.inputs, in)
	return s.delegate.Execute(ctx, in)
}

func (f *cancelWriterFake) WriteExitCode(_ domain.TaskID, code domain.ExitCode) error {
	f.exits = append(f.exits, code)
	return f.writeExitErr
}
func (f *cancelWriterFake) AppendEvent(_ domain.TaskID, e domain.Event) error {
	f.events = append(f.events, e)
	return nil
}

type cancelReaderFake struct {
	exitCode   int
	exitExists bool
	exitErr    error
	exitReads  int
}

func (*cancelReaderFake) ReadStderrLog(domain.TaskID) ([]byte, error)          { return nil, nil }
func (*cancelReaderFake) ReadLastMessage(domain.TaskID) (bool, error)          { return false, nil }
func (*cancelReaderFake) ReadPromptContent(domain.TaskID) ([]byte, error)      { return nil, nil }
func (*cancelReaderFake) ReadLastMessageContent(domain.TaskID) ([]byte, error) { return nil, nil }
func (*cancelReaderFake) ReadPartialOutputContent(domain.TaskID) ([]byte, error) {
	return nil, nil
}
func (f *cancelReaderFake) ReadExitCode(domain.TaskID) (int, bool, error) {
	f.exitReads++
	return f.exitCode, f.exitExists, f.exitErr
}

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
	return cancelQueuedPayloadForID(t, "impl-20260811-120000-a1b2-cancel", 1)
}

func cancelQueuedPayloadForID(t *testing.T, rawID string, position int) execution.TaskLaunchPayload {
	t.Helper()
	id, err := domain.NewTaskID(rawID)
	if err != nil {
		t.Fatal(err)
	}
	slug, err := domain.NewSlug("cancel")
	if err != nil {
		t.Fatal(err)
	}
	timeout, err := domain.NewTimeout(nil, 1800)
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := domain.NewTask(id, domain.SubcommandImpl, slug, nil, time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC), position)
	if err != nil {
		t.Fatal(err)
	}
	return execution.TaskLaunchPayload{Task: task, Model: "gpt-5", SandboxMode: "workspace-write", PromptText: "prompt", ResolvedTimeout: timeout}
}
func cancelFixture(t *testing.T, payload execution.TaskLaunchPayload, removed bool) (*cancelStoreFake, *cancelQueueFake, *cancelEventsFake, *cancelTerminatorFake, *cancelDisarmerFake, *CancelTaskUseCase) {
	t.Helper()
	return cancelFixtureWithQueueMutexAndSlots(t, payload, removed, &sync.Mutex{}, &cancelSlotFake{})
}

func cancelFixtureWithTerminationEnsurer(t *testing.T, payload execution.TaskLaunchPayload, removed bool) (*cancelStoreFake, *cancelQueueFake, *cancelEventsFake, *cancelTerminatorFake, *cancelDisarmerFake, *cancelTerminationEnsurerFake, *CancelTaskUseCase) {
	t.Helper()
	tasks, queue, events, terminator, disarmer, base := cancelFixture(t, payload, removed)
	termination := &cancelTerminationEnsurerFake{}
	uc := NewCancelTaskUseCase(tasks, queue, base.queueMu, base.taskMu, events, terminator, termination, base.pendingRegistrar, disarmer, base.confirmer, base.stalledTracker, &cancelOwnershipFake{}, base.clock)
	return tasks, queue, events, terminator, disarmer, termination, uc
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
	uc := NewCancelTaskUseCase(tasks, queue, queueMu, store.NewTaskMutex(), events, terminator, &cancelTerminationEnsurerFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, &cancelStalledTrackerFake{}, &cancelOwnershipFake{}, domain.ClockFunc(func() time.Time { return time.Date(2026, 8, 11, 12, 1, 0, 0, time.UTC) }))
	return tasks, queue, events, terminator, disarmer, uc
}

func cancelQueuedUseCase(t *testing.T, queue execution.TaskQueueReader, tasks *cancelStoreFake, logger *slog.Logger) (*cancelEventsFake, *CancelTaskUseCase) {
	t.Helper()
	events := &cancelEventsFake{}
	disarmer := &cancelDisarmerFake{}
	confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, &cancelPendingRegistrarFake{})
	uc := NewCancelTaskUseCase(tasks, queue, &sync.Mutex{}, store.NewTaskMutex(), events, &cancelTerminatorFake{}, &cancelTerminationEnsurerFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, &cancelStalledTrackerFake{}, &cancelOwnershipFake{}, domain.ClockFunc(time.Now), logger)
	return events, uc
}

func cancelPersistedSnapshot(t *testing.T, state domain.TaskState, withPID bool) domain.TaskSnapshot {
	t.Helper()
	payload := cancelQueuedPayload(t)
	snapshot, err := domain.NewTaskSnapshotFromAdmission(payload.Task, payload.ResolvedTimeout, payload.Model, payload.ReasoningEffort, payload.SandboxMode, domain.ExecutionRouteDaemon, time.Date(2026, 8, 11, 12, 1, 0, 0, time.UTC))
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
	snapshot, err := domain.NewTaskSnapshotFromAdmission(payload.Task, payload.ResolvedTimeout, payload.Model, payload.ReasoningEffort, payload.SandboxMode, domain.ExecutionRouteDaemon, at)
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

func TestCancelTaskExecute_QueuedSaveSuccessLogsOnlyCommittedRemoveReindex(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 1, 0, 0, time.UTC)
	payloadA := cancelQueuedPayloadForID(t, "impl-20260917-120000-a1b2-alpha", 1)
	payloadB := cancelQueuedPayloadForID(t, "impl-20260917-120000-a1b2-bravo", 2)
	payloadC := cancelQueuedPayloadForID(t, "impl-20260917-120000-a1b2-charlie", 3)
	queue := execution.NewTaskQueue()
	queue.Enqueue(payloadA)
	queue.Enqueue(payloadB)
	queue.Enqueue(payloadC)
	tasks := &cancelStoreFake{reserved: true}
	var logs bytes.Buffer
	observation := &cancelQueueReindexLogObservation{}
	unlockHandler := &cancelQueueUnlockLogHandler{next: slog.NewJSONHandler(&logs, nil), observation: observation}
	events, uc := cancelQueuedUseCase(t, queue, tasks, slog.New(unlockHandler))
	observation.queueMu = uc.queueMu

	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payloadB.Task.ID(), OccurredAt: at})
	position, found, positionErr := queue.QueuePosition(payloadC.Task.ID())
	records := queueReindexLogEvents(t, &logs)
	matched, restored := 0, 0
	for _, record := range records {
		if record.TaskID == payloadC.Task.ID().String() && record.QueuePosition == 2 && record.Source == "remove" && record.EventType == "TaskQueued" {
			matched++
		}
		if record.Source == "restore" {
			restored++
		}
	}
	if err != nil || out.State != domain.StateCancelling || positionErr != nil || !found || position != 2 || matched != 1 || restored != 0 || len(records) != 2 || len(events.events) != 1 || !allQueueReindexLogsUnlocked(observation, len(records)) {
		t.Fatalf("out=%#v err=%v position=%d found=%t positionErr=%v logs=%q events=%#v observation=%#v", out, err, position, found, positionErr, logs.String(), events.events, observation)
	}
}

func TestCancelTaskExecute_QueuedSaveFailureLogsOnlyRestoredFinalReindex(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 1, 0, 0, time.UTC)
	payloadA := cancelQueuedPayloadForID(t, "impl-20260917-120000-a1b2-alpha", 1)
	payloadB := cancelQueuedPayloadForID(t, "impl-20260917-120000-a1b2-bravo", 2)
	payloadC := cancelQueuedPayloadForID(t, "impl-20260917-120000-a1b2-charlie", 3)
	queue := execution.NewTaskQueue()
	queue.Enqueue(payloadA)
	queue.Enqueue(payloadB)
	queue.Enqueue(payloadC)
	saveErr := errors.New("save failed")
	tasks := &cancelStoreFake{reserved: true, saveErr: saveErr}
	var logs bytes.Buffer
	observation := &cancelQueueReindexLogObservation{}
	unlockHandler := &cancelQueueUnlockLogHandler{next: slog.NewJSONHandler(&logs, nil), observation: observation}
	_, uc := cancelQueuedUseCase(t, queue, tasks, slog.New(unlockHandler))
	observation.queueMu = uc.queueMu

	_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payloadB.Task.ID(), OccurredAt: at})
	position, found, positionErr := queue.QueuePosition(payloadC.Task.ID())
	records := queueReindexLogEvents(t, &logs)
	final, intermediate, removed := 0, 0, 0
	for _, record := range records {
		if record.TaskID != payloadC.Task.ID().String() {
			continue
		}
		if record.QueuePosition == 3 && record.Source == "restore" {
			final++
		}
		if record.QueuePosition == 2 {
			intermediate++
		}
		if record.Source == "remove" {
			removed++
		}
	}
	if !errors.Is(err, saveErr) || positionErr != nil || !found || position != 3 || final != 1 || intermediate != 0 || removed != 0 || len(records) != 3 || !allQueueReindexLogsUnlocked(observation, len(records)) {
		t.Fatalf("err=%v position=%d found=%t positionErr=%v logs=%q observation=%#v", err, position, found, positionErr, logs.String(), observation)
	}
}

func TestCancelTaskExecute_QueueReindexLogFailureIsFailSoft(t *testing.T) {
	at := time.Date(2026, 9, 17, 12, 1, 0, 0, time.UTC)
	payloadA := cancelQueuedPayloadForID(t, "impl-20260917-120000-a1b2-alpha", 1)
	payloadB := cancelQueuedPayloadForID(t, "impl-20260917-120000-a1b2-bravo", 2)
	payloadC := cancelQueuedPayloadForID(t, "impl-20260917-120000-a1b2-charlie", 3)
	queue := execution.NewTaskQueue()
	queue.Enqueue(payloadA)
	queue.Enqueue(payloadB)
	queue.Enqueue(payloadC)
	tasks := &cancelStoreFake{reserved: true}
	logHandler := &cancelPanickingLogHandler{}
	_, uc := cancelQueuedUseCase(t, queue, tasks, slog.New(logHandler))

	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payloadB.Task.ID(), OccurredAt: at})
	position, found, positionErr := queue.QueuePosition(payloadC.Task.ID())
	if err != nil || out.State != domain.StateCancelling || tasks.snapshot.State != domain.StateKilled || positionErr != nil || !found || position != 2 || logHandler.calls != 2 {
		t.Fatalf("out=%#v err=%v snapshot=%#v position=%d found=%t positionErr=%v logHandler=%#v", out, err, tasks.snapshot, position, found, positionErr, logHandler)
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
			tasks, _, _, terminator, disarmer, termination, uc := cancelFixtureWithTerminationEnsurer(t, payload, false)
			tasks.snapshot = cancelPersistedSnapshot(t, state, true)
			processStartedAt := *tasks.snapshot.ProcessStartedAt
			order := []string{}
			termination.order, disarmer.order = &order, &order
			pending := uc.pendingRegistrar.(*cancelPendingRegistrarFake)
			pending.order = &order

			out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			if err != nil || !out.TerminationTriggered || termination.calls != 1 || termination.taskID != payload.Task.ID() || termination.authority.PID != 4321 || !termination.authority.ProcessStartedAt.Equal(processStartedAt) || termination.authority.ExpectedState != domain.StateCancelling || termination.authority.LifecycleGeneration == nil || *termination.authority.LifecycleGeneration != 1 || termination.grace != execution.TimeoutKillGrace || len(terminator.termPIDs) != 0 || pending.calls != 1 || pending.disposition != recovery.PendingSendSent || pending.authority != nil || disarmer.calls != 1 || strings.Join(order, ",") != "termination,register,disarm" {
				t.Fatalf("out=%#v err=%v termination=%#v terminator=%#v pending=%#v disarmer=%#v order=%#v", out, err, termination, terminator, pending, disarmer, order)
			}
		})
	}
}

func TestCancelTaskExecute_WaiterOwnedRunningDefersConfirmation_SCNProto0332(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks := &cancelStoreFake{reserved: true, snapshot: cancelPersistedSnapshot(t, domain.StateRunning, true)}
	queueMu := &sync.Mutex{}
	events := &cancelEventsFake{}
	disarmer := &cancelDisarmerFake{}
	termination := &cancelTerminationEnsurerFake{result: recovery.TerminationAttemptResult{Dead: true}}
	writer := &cancelWriterFake{}
	pending := &cancelPendingRegistrarFake{}
	confirmer := execution.NewConfirmTaskKilledUseCase(tasks, writer, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, pending)
	uc := NewCancelTaskUseCase(tasks, &cancelQueueFake{payload: payload, queueMu: queueMu}, queueMu, store.NewTaskMutex(), events, &cancelTerminatorFake{}, termination, pending, disarmer, confirmer, &cancelStalledTrackerFake{}, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))
	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || out.State != domain.StateCancelling || tasks.snapshot.State != domain.StateCancelling || termination.calls != 1 || disarmer.calls != 1 {
		t.Fatalf("out=%#v err=%v snapshot=%#v termination=%#v disarmer=%#v", out, err, tasks.snapshot, termination, disarmer)
	}
	if len(events.events) != 1 || len(writer.events) != 0 {
		t.Fatalf("cancel events=%#v killed events=%#v", events.events, writer.events)
	}
	if _, ok := events.events[0].(domain.TaskCancelRequested); !ok {
		t.Fatalf("cancel event=%T", events.events[0])
	}
}

func TestCancelTaskExecute_StartingConcurrentInitialSendClaimsConvergeToOneSender(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks := &cancelStoreFake{reserved: true, snapshot: cancelPersistedSnapshot(t, domain.StateStarting, true)}
	taskMu := &cancelTrackedTaskMutex{}
	queueMu := &sync.Mutex{}
	release := make(chan struct{})
	pending := &cancelBarrierPendingRegistrar{arrived: make(chan struct{}, 3), release: release, taskMu: taskMu}
	terminator := &cancelTerminatorFake{}
	disarmer := &cancelDisarmerFake{}
	confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, pending)
	uc := NewCancelTaskUseCase(tasks, &cancelQueueFake{payload: payload, queueMu: queueMu}, queueMu, taskMu, &cancelEventsFake{}, terminator, &cancelTerminationEnsurerFake{}, pending, disarmer, confirmer, &cancelStalledTrackerFake{}, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))
	startedAt := *tasks.snapshot.ProcessStartedAt
	generation := domain.LifecycleGeneration(1)
	authority := recovery.ProcessSignalAuthority{TaskID: payload.Task.ID(), PID: *tasks.snapshot.PID, ProcessStartedAt: startedAt, ExpectedState: domain.StateCancelling, LifecycleGeneration: &generation}
	type result struct {
		outcome recovery.ClaimOutcome
		err     error
	}
	completed := make(chan result, 3)
	go func() {
		out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
		if err == nil && out.TerminationTriggered {
			completed <- result{outcome: recovery.ClaimAcquired}
			return
		}
		completed <- result{err: err}
	}()
	for range 2 {
		go func() {
			claim, outcome := pending.ClaimInitialSend(payload.Task.ID(), authority)
			if outcome == recovery.ClaimAcquired {
				taskMu.Lock(payload.Task.ID())
				tasks.snapshot.State = domain.StateCancelling
				taskMu.Unlock(payload.Task.ID())
				_ = terminator.SendTerminate(claim.Authority.PID)
			}
			completed <- result{outcome: outcome}
		}()
	}
	for range 3 {
		select {
		case <-pending.arrived:
		case <-time.After(time.Second):
			t.Fatal("ClaimInitialSend callers did not reach the barrier")
		}
	}
	close(release)
	acquired := 0
	for range 3 {
		select {
		case result := <-completed:
			if result.err != nil {
				t.Fatal(result.err)
			}
			if result.outcome == recovery.ClaimAcquired {
				acquired++
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent cancel callers did not complete")
		}
	}
	if acquired != 1 || terminator.calls != 1 || pending.SawTaskMutexDuringInitialClaim() || tasks.snapshot.State != domain.StateCancelling {
		t.Fatalf("acquired=%d terminator=%#v taskMuAtClaim=%t snapshot=%#v", acquired, terminator, pending.SawTaskMutexDuringInitialClaim(), tasks.snapshot)
	}
}

func TestCancelTaskExecute_DoesNotWriteSynthetic130BeforeWaiter_SCNProto0333(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks := &cancelStoreFake{reserved: true, snapshot: cancelPersistedSnapshot(t, domain.StateRunning, true)}
	queueMu := &sync.Mutex{}
	events := &cancelEventsFake{}
	disarmer := &cancelDisarmerFake{}
	termination := &cancelTerminationEnsurerFake{result: recovery.TerminationAttemptResult{TerminateErr: nil, Dead: true}}
	writer := &cancelWriterFake{}
	pending := &cancelPendingRegistrarFake{}
	confirmer := execution.NewConfirmTaskKilledUseCase(tasks, writer, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, pending)
	spy := &cancelConfirmerSpy{delegate: confirmer}
	uc := NewCancelTaskUseCase(tasks, &cancelQueueFake{payload: payload, queueMu: queueMu}, queueMu, store.NewTaskMutex(), events, &cancelTerminatorFake{}, termination, pending, disarmer, spy, &cancelStalledTrackerFake{}, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))
	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || out.State != domain.StateCancelling || tasks.snapshot.State != domain.StateCancelling || termination.calls != 1 || disarmer.calls != 1 || len(events.events) != 1 || len(spy.inputs) != 0 || len(writer.exits) != 0 || len(writer.events) != 0 {
		t.Fatalf("out=%#v err=%v snapshot=%#v termination=%#v disarmer=%#v events=%#v confirmationInputs=%#v exits=%#v killed=%#v", out, err, tasks.snapshot, termination, disarmer, events.events, spy.inputs, writer.exits, writer.events)
	}

	_, err = spy.Execute(context.Background(), execution.ConfirmTaskKilledInput{TaskID: payload.Task.ID(), RawExitCode: 143, Estimated: false, OccurredAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if len(spy.inputs) != 1 || spy.inputs[0].RawExitCode != 143 || spy.inputs[0].Estimated {
		t.Fatalf("confirmation inputs=%#v", spy.inputs)
	}
	if tasks.snapshot.State != domain.StateKilled || tasks.snapshot.ExitCode == nil || tasks.snapshot.ExitCode.Raw() != 143 {
		t.Fatalf("snapshot=%#v", tasks.snapshot)
	}
	if len(writer.exits) != 1 || writer.exits[0].Raw() != 143 {
		t.Fatalf("exit-code writes=%#v", writer.exits)
	}
	if len(writer.events) != 1 {
		t.Fatalf("killed events=%#v", writer.events)
	}
	killed, ok := writer.events[0].(domain.TaskKilled)
	if !ok || killed.ExitCode.Raw() != 143 || killed.Estimated {
		t.Fatalf("killed event=%#v", writer.events[0])
	}
	for _, exit := range writer.exits {
		if exit.Raw() == 130 {
			t.Fatalf("synthetic exit-code written: %#v", writer.exits)
		}
	}
	for _, event := range writer.events {
		if killed, ok := event.(domain.TaskKilled); ok && killed.ExitCode.Raw() == 130 {
			t.Fatalf("synthetic TaskKilled appended: %#v", writer.events)
		}
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
	if err != nil || !out.TerminationTriggered || terminator.calls != 1 || terminator.pid != 4321 || uc.termination.(*cancelTerminationEnsurerFake).calls != 0 || disarmer.calls != 0 {
		t.Fatalf("out=%#v err=%v terminator=%#v disarmer=%#v", out, err, terminator, disarmer)
	}
}

func TestCancelTaskExecute_PersistedStartingWithPIDClaimOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name          string
		outcome       recovery.ClaimOutcome
		wantTerminate int
		wantConfirm   int
		wantComplete  int
	}{
		{name: "acquired", outcome: recovery.ClaimAcquired, wantTerminate: 1, wantComplete: 1},
		{name: "already-claimed", outcome: recovery.ClaimAlreadyClaimed},
		{name: "sent", outcome: recovery.ClaimSent, wantConfirm: 1},
		{name: "confirm-only", outcome: recovery.ClaimConfirmOnly},
		{name: "not-found", outcome: recovery.ClaimNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := cancelQueuedPayload(t)
			tasks, _, _, terminator, _, uc := cancelFixture(t, payload, false)
			tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, true)
			pending := uc.pendingRegistrar.(*cancelPendingRegistrarFake)
			pending.initialOutcome = &tc.outcome
			termination := uc.termination.(*cancelTerminationEnsurerFake)

			out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			if err != nil || pending.claimInitialCalls != 1 || terminator.calls != tc.wantTerminate || termination.confirmCalls != tc.wantConfirm || pending.completeCalls != tc.wantComplete || pending.releaseCalls != 0 || pending.invalidateCalls != 0 || pending.removeCalls != 0 {
				t.Fatalf("out=%#v err=%v pending=%#v terminator=%#v termination=%#v", out, err, pending, terminator, termination)
			}
			if tc.outcome == recovery.ClaimAcquired && !out.TerminationTriggered {
				t.Fatalf("out=%#v", out)
			}
			if tc.outcome != recovery.ClaimAcquired && out.TerminationTriggered {
				t.Fatalf("out=%#v", out)
			}
		})
	}
}

func TestCancelTaskClaimSentConfirmFailure(t *testing.T) {
	newUseCase := func(t *testing.T, cause error, logs *bytes.Buffer) (*execution.TaskLaunchPayload, *cancelTerminationEnsurerFake, *CancelTaskUseCase) {
		t.Helper()
		payload := cancelQueuedPayload(t)
		tasks, _, _, _, _, uc := cancelFixture(t, payload, false)
		tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, true)
		outcome := recovery.ClaimSent
		uc.pendingRegistrar.(*cancelPendingRegistrarFake).initialOutcome = &outcome
		termination := uc.termination.(*cancelTerminationEnsurerFake)
		termination.confirmErr = cause
		uc.logger = slog.New(slog.NewJSONHandler(logs, nil))
		return &payload, termination, uc
	}

	t.Run("Execute propagates the confirmation failure", func(t *testing.T) {
		cause := errors.New("claim-sent confirmation failure")
		var logs bytes.Buffer
		payload, termination, uc := newUseCase(t, cause, &logs)

		out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
		if out.State != domain.StateCancelling || !errors.Is(err, cause) || termination.confirmCalls != 1 {
			t.Fatalf("out=%#v err=%v confirmCalls=%d", out, err, termination.confirmCalls)
		}
		if !strings.Contains(logs.String(), "confirm cancelled task termination") || !strings.Contains(logs.String(), cause.Error()) {
			t.Fatalf("logs=%q", logs.String())
		}
	})

	t.Run("Handle returns sanitized cancel failed response", func(t *testing.T) {
		cause := errors.New("claim-sent internal confirmation failure")
		var logs bytes.Buffer
		payload, termination, uc := newUseCase(t, cause, &logs)

		response := uc.Handle(transport.Request{RequestID: "claim-sent-confirm-failure", TaskID: payload.Task.ID().String(), Params: json.RawMessage(`{}`)})
		if response.OK || response.Error == nil || response.Error.Code != "CANCEL_FAILED" || response.Error.MessageKey != "error.cancel.failed" || termination.confirmCalls != 1 {
			t.Fatalf("response=%#v confirmCalls=%d", response, termination.confirmCalls)
		}
		detail, err := json.Marshal(response.Error.Detail)
		if err != nil || string(detail) != `{"task_id":"`+payload.Task.ID().String()+`"}` || strings.Contains(string(detail), cause.Error()) {
			t.Fatalf("detail=%s err=%v", detail, err)
		}
		if !strings.Contains(logs.String(), "confirm cancelled task termination") || !strings.Contains(logs.String(), "cancel task failed") || !strings.Contains(logs.String(), cause.Error()) {
			t.Fatalf("logs=%q", logs.String())
		}
	})
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
	uc := NewCancelTaskUseCase(tasks, queue, queue.queueMu, store.NewTaskMutex(), &cancelEventsFake{}, terminator, &cancelTerminationEnsurerFake{}, pending, disarmer, confirmer, &cancelStalledTrackerFake{}, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))

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
			uc := NewCancelTaskUseCase(tasks, queue, queue.queueMu, store.NewTaskMutex(), &cancelEventsFake{}, terminator, &cancelTerminationEnsurerFake{}, pending, disarmer, confirmer, &cancelStalledTrackerFake{}, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))

			out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			if err != nil || out.TerminationTriggered || pending.calls != 1 || pending.disposition != recovery.PendingSendConfirmOnly || pending.authority != nil || disarmer.calls != 1 || terminator.calls != 0 || tasks.snapshot.State != domain.StateCancelling {
				t.Fatalf("out=%#v err=%v pending=%#v disarmer=%#v terminator=%#v snapshot=%#v", out, err, pending, disarmer, terminator, tasks.snapshot)
			}
		})
	}
}

func TestCancelTaskHandle_IncompleteProcessIdentityFailsClosedWithoutPersistence(t *testing.T) {
	payload := cancelQueuedPayload(t)
	snapshot := cancelPersistedSnapshot(t, domain.StateRunning, true)
	snapshot.ProcessStartedAt = nil
	tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
	tasks.reserved = true
	tasks.snapshot = snapshot

	response := uc.Handle(transport.Request{RequestID: "incomplete-process-identity", TaskID: payload.Task.ID().String(), Params: json.RawMessage(`{}`)})
	if response.OK || response.Error == nil || response.Error.Code != "CANCEL_FAILED" || response.Error.MessageKey != "error.cancel.failed" || tasks.saves != 0 || tasks.snapshot.PID == nil || tasks.snapshot.ProcessStartedAt != nil || tasks.snapshot.State != domain.StateRunning || terminator.calls != 0 || disarmer.calls != 0 {
		t.Fatalf("response=%#v snapshot=%#v saves=%d terminator=%#v disarmer=%#v", response, tasks.snapshot, tasks.saves, terminator, disarmer)
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
	tasks, _, _, terminator, disarmer, termination, uc := cancelFixtureWithTerminationEnsurer(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateRunning, true)
	termination.result.TerminateErr = errors.New("terminate I/O failure")
	order := []string{}
	termination.order, disarmer.order = &order, &order
	pending := uc.pendingRegistrar.(*cancelPendingRegistrarFake)
	pending.order = &order
	var logs bytes.Buffer
	uc.logger = slog.New(slog.NewJSONHandler(&logs, nil))

	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || !out.TerminationTriggered || termination.calls != 1 || terminator.calls != 0 || pending.calls != 1 || pending.disposition != recovery.PendingSendUnsent || pending.authority == nil || pending.authority.TaskID != payload.Task.ID() || pending.authority.PID != 4321 || !pending.authority.ProcessStartedAt.Equal(*tasks.snapshot.ProcessStartedAt) || pending.authority.LifecycleGeneration == nil || *pending.authority.LifecycleGeneration != 1 || disarmer.calls != 1 || strings.Join(order, ",") != "termination,register,disarm" || tasks.snapshot.State != domain.StateCancelling || !strings.Contains(logs.String(), "terminate I/O failure") {
		t.Fatalf("out=%#v err=%v snapshot=%#v terminator=%#v pending=%#v disarmer=%#v order=%#v logs=%q", out, err, tasks.snapshot, terminator, pending, disarmer, order, logs.String())
	}
}

func TestCancelTaskExecute_PersistedTerminateFailurePendingRegistrationFailureRetainsBothCauses(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, terminator, disarmer, termination, uc := cancelFixtureWithTerminationEnsurer(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateRunning, true)
	terminateErr := errors.New("terminate failure")
	registerErr := errors.New("register failure")
	termination.result.TerminateErr = terminateErr
	order := []string{}
	termination.order, disarmer.order = &order, &order
	pending := uc.pendingRegistrar.(*cancelPendingRegistrarFake)
	pending.err, pending.order = registerErr, &order

	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if !out.TerminationTriggered || termination.calls != 1 || terminator.calls != 0 || pending.calls != 1 || disarmer.calls != 0 || strings.Join(order, ",") != "termination,register" || !errors.Is(err, terminateErr) || !errors.Is(err, registerErr) || tasks.snapshot.State != domain.StateCancelling {
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
	if err != nil || !out.TerminationTriggered || pending.calls != 0 || terminator.calls != 1 || disarmer.calls != 0 {
		t.Fatalf("out=%#v err=%v pending=%#v disarmer=%#v", out, err, pending, disarmer)
	}
}

func TestCancelPendingRegistration(t *testing.T) {
	id := cancelTaskID(t)
	startedAt := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	pid := 4321
	generation := domain.LifecycleGeneration(7)
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
			disposition, authority := cancelPendingRegistration(id, tc.pid, tc.processStartedAt, &generation)
			if disposition != tc.wantDisposition || (authority != nil) != tc.wantAuthority || authority != nil && (authority.TaskID != id || authority.PID != pid || !authority.ProcessStartedAt.Equal(startedAt) || authority.LifecycleGeneration == nil || *authority.LifecycleGeneration != generation) {
				t.Fatalf("disposition=%v authority=%#v", disposition, authority)
			}
		})
	}
}

func TestCancelTaskExecute_StartingWithPIDWithoutOwnershipFailsClosed(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, terminator, _, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, true)
	uc.ownership = &cancelOwnershipFake{unowned: true}

	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if !errors.Is(err, errCancelStateChanged) || out.State != domain.StateCancelling || tasks.snapshot.State != domain.StateStarting || terminator.calls != 0 {
		t.Fatalf("out=%#v err=%v snapshot=%#v terminator=%#v", out, err, tasks.snapshot, terminator)
	}
}

func TestCancelTaskExecute_LiveProcessWithoutOwnershipFailsClosed(t *testing.T) {
	for _, state := range []domain.TaskState{domain.StateRunning, domain.StateStalled, domain.StateAdopted} {
		t.Run(string(state), func(t *testing.T) {
			payload := cancelQueuedPayload(t)
			tasks, _, _, terminator, disarmer, uc := cancelFixture(t, payload, false)
			tasks.snapshot = cancelPersistedSnapshot(t, state, true)
			uc.ownership = &cancelOwnershipFake{unowned: true}

			out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			if !errors.Is(err, errCancelStateChanged) || out.State != domain.StateCancelling || tasks.snapshot.State != state || tasks.saves != 0 || terminator.calls != 0 || disarmer.calls != 0 {
				t.Fatalf("out=%#v err=%v snapshot=%#v saves=%d terminator=%#v disarmer=%#v", out, err, tasks.snapshot, tasks.saves, terminator, disarmer)
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
			tasks.saveErr = errors.Join(domain.ErrContractWriteFailed, errors.New("/private/contract/task.json secret=cancel-token"))
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
			wantCode, wantMessage := "CONTRACT_WRITE_FAILED", "error.contract.writeFailed"
			if tc.name == "reservation" {
				wantCode, wantMessage = "CANCEL_FAILED", "error.cancel.failed"
			}
			if response.OK || response.Error == nil || response.Error.Code != wantCode || response.Error.MessageKey != wantMessage {
				t.Fatalf("response=%#v", response)
			}
			detail, err := json.Marshal(response.Error.Detail)
			if err != nil || strings.Contains(string(detail), "/private/contract") || strings.Contains(string(detail), "task.json") || strings.Contains(string(detail), "cancel-token") || !strings.Contains(string(detail), payload.Task.ID().String()) {
				t.Fatalf("detail=%s err=%v", detail, err)
			}
		})
	}
}

func TestCancelTaskHandle_FileTaskStoreWriteAtomicFailureReturnsContractWriteFailed(t *testing.T) {
	root := t.TempDir()
	tasks, err := store.NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	payload := cancelQueuedPayload(t)
	id := payload.Task.ID()
	if err := tasks.Reserve(id); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, id.String(), "task.json"), 0o700); err != nil {
		t.Fatal(err)
	}
	queueMu := &sync.Mutex{}
	queue := &cancelQueueFake{payload: payload, index: 1, removed: true, queueMu: queueMu}
	uc := NewCancelTaskUseCase(tasks, queue, queueMu, store.NewTaskMutex(), &cancelEventsFake{}, &cancelTerminatorFake{}, &cancelTerminationEnsurerFake{}, &cancelPendingRegistrarFake{}, &cancelDisarmerFake{}, &cancelConfirmerFake{}, &cancelStalledTrackerFake{}, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))

	response := uc.Handle(transport.Request{RequestID: "write-atomic", TaskID: id.String(), Params: json.RawMessage(`{}`)})
	if response.OK || response.Error == nil || response.Error.Code != "CONTRACT_WRITE_FAILED" || response.Error.MessageKey != "error.contract.writeFailed" {
		t.Fatalf("response=%#v", response)
	}
	detail, marshalErr := json.Marshal(response.Error.Detail)
	if marshalErr != nil || string(detail) != `{"task_id":"`+id.String()+`"}` || strings.Contains(string(detail), root) || strings.Contains(string(detail), "task.json") || strings.Contains(string(detail), "rename") {
		t.Fatalf("detail=%s err=%v", detail, marshalErr)
	}
}

func TestCancelTaskHandle_SaveFailuresReturnCancelFailed(t *testing.T) {
	type fixture struct {
		tasks   *cancelStoreFake
		queue   *cancelQueueFake
		pending *cancelPendingRegistrarFake
		uc      *CancelTaskUseCase
	}
	for _, tc := range []struct {
		name       string
		cause      error
		newFixture func(*testing.T, error) fixture
		checkRoute func(*testing.T, fixture)
	}{
		{
			name:  "queued-save",
			cause: errors.New("snapshot validation failure /private/internal secret=cancel-token"),
			newFixture: func(t *testing.T, cause error) fixture {
				payload := cancelQueuedPayload(t)
				tasks, queue, _, _, _, uc := cancelFixture(t, payload, true)
				tasks.saveErr = cause
				return fixture{tasks: tasks, queue: queue, pending: uc.pendingRegistrar.(*cancelPendingRegistrarFake), uc: uc}
			},
			checkRoute: func(t *testing.T, f fixture) {
				t.Helper()
				if f.queue.restores != 1 || f.pending.claimInitialCalls != 0 {
					t.Fatalf("restores=%d claimInitialCalls=%d", f.queue.restores, f.pending.claimInitialCalls)
				}
			},
		},
		{
			name:  "persisted-save",
			cause: errors.New("snapshot marshal failure /private/internal secret=cancel-token"),
			newFixture: func(t *testing.T, cause error) fixture {
				payload := cancelQueuedPayload(t)
				tasks, queue, _, _, _, uc := cancelFixture(t, payload, false)
				tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, false)
				tasks.saveErr = cause
				return fixture{tasks: tasks, queue: queue, pending: uc.pendingRegistrar.(*cancelPendingRegistrarFake), uc: uc}
			},
			checkRoute: func(t *testing.T, f fixture) {
				t.Helper()
				if f.queue.restores != 0 || f.pending.claimInitialCalls != 0 {
					t.Fatalf("restores=%d claimInitialCalls=%d", f.queue.restores, f.pending.claimInitialCalls)
				}
			},
		},
		{
			name:  "starting-claimed-save",
			cause: errors.New("task directory validation failure /private/internal secret=cancel-token"),
			newFixture: func(t *testing.T, cause error) fixture {
				payload := cancelQueuedPayload(t)
				tasks, queue, _, _, _, uc := cancelFixture(t, payload, false)
				tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, true)
				tasks.saveErr = cause
				pending := uc.pendingRegistrar.(*cancelPendingRegistrarFake)
				outcome := recovery.ClaimAcquired
				pending.initialOutcome = &outcome
				return fixture{tasks: tasks, queue: queue, pending: pending, uc: uc}
			},
			checkRoute: func(t *testing.T, f fixture) {
				t.Helper()
				if f.pending.claimInitialCalls != 1 || f.pending.removeCalls != 1 {
					t.Fatalf("claimInitialCalls=%d removeCalls=%d", f.pending.claimInitialCalls, f.pending.removeCalls)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			executeFixture := tc.newFixture(t, tc.cause)
			id := cancelTaskID(t)
			_, err := executeFixture.uc.Execute(context.Background(), CancelTaskInput{TaskID: id, OccurredAt: time.Now()})
			if !errors.Is(err, tc.cause) || errors.Is(err, domain.ErrContractWriteFailed) || executeFixture.tasks.saves != 1 {
				t.Fatalf("err=%v saves=%d", err, executeFixture.tasks.saves)
			}
			tc.checkRoute(t, executeFixture)

			handleFixture := tc.newFixture(t, tc.cause)
			response := handleFixture.uc.Handle(transport.Request{RequestID: tc.name, TaskID: id.String(), Params: json.RawMessage(`{}`)})
			if response.OK || response.Error == nil || response.Error.Code != "CANCEL_FAILED" || response.Error.MessageKey != "error.cancel.failed" || handleFixture.tasks.saves != 1 {
				t.Fatalf("response=%#v saves=%d", response, handleFixture.tasks.saves)
			}
			detail, marshalErr := json.Marshal(response.Error.Detail)
			if marshalErr != nil || string(detail) != `{"task_id":"`+id.String()+`"}` || strings.Contains(string(detail), tc.cause.Error()) || strings.Contains(string(detail), "/private/internal") || strings.Contains(string(detail), "cancel-token") {
				t.Fatalf("detail=%s err=%v", detail, marshalErr)
			}
			tc.checkRoute(t, handleFixture)
		})
	}
}

func TestCancelTaskExecute_FailuresRetainUnderlyingCause(t *testing.T) {
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
			if errors.Is(err, domain.ErrContractWriteFailed) || !errors.Is(err, cause) || !errors.As(err, &pathErr) || pathErr != cause {
				t.Fatalf("err=%v pathErr=%#v cause=%#v", err, pathErr, cause)
			}
			if tc.name == "rechecked-reservation" && tasks.reservations != 2 {
				t.Fatalf("reservations=%d, want 2", tasks.reservations)
			}
		})
	}
}

func TestCancelTaskHandle_UnclassifiedLoadFailureReturnsCancelFailed_SCNProto0329(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, _, _, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, false)
	tasks.loadErr = errors.New("load I/O failure")
	var logs bytes.Buffer
	uc.logger = slog.New(slog.NewJSONHandler(&logs, nil))

	response := uc.Handle(transport.Request{RequestID: "load-failure", TaskID: payload.Task.ID().String(), Params: json.RawMessage(`{}`)})
	if response.OK || response.Error == nil || response.Error.Code != "CANCEL_FAILED" || response.Error.MessageKey != "error.cancel.failed" {
		t.Fatalf("response=%#v", response)
	}
	detail, err := json.Marshal(response.Error.Detail)
	if err != nil || string(detail) != `{"task_id":"`+payload.Task.ID().String()+`"}` || strings.Contains(logs.String(), "load I/O failure") == false {
		t.Fatalf("detail=%s err=%v logs=%q", detail, err, logs.String())
	}
}

func TestCancelTaskHandle_ContractWriteFailureRemainsContractWriteFailed_SCNProto0330(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, _, _, uc := cancelFixture(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, false)
	tasks.saveErr = errors.Join(domain.ErrContractWriteFailed, errors.New("task.json write failure"))

	response := uc.Handle(transport.Request{RequestID: "write-failure", TaskID: payload.Task.ID().String(), Params: json.RawMessage(`{}`)})
	if response.OK || response.Error == nil || response.Error.Code != "CONTRACT_WRITE_FAILED" || response.Error.MessageKey != "error.contract.writeFailed" {
		t.Fatalf("response=%#v", response)
	}
}

func TestCancelTaskHandle_ExitCodeValidationAndWriteFailureClassification_SCNProto0334(t *testing.T) {
	readErr := errors.New("exit-code read failure")
	writeErr := errors.New("exit-code write failure")
	for _, tc := range []struct {
		name           string
		reader         cancelReaderFake
		writeErr       error
		wantCode       string
		wantExitWrites int
		wantLogCause   string
	}{
		{"read failure is cancel failed", cancelReaderFake{exitErr: readErr}, nil, "CANCEL_FAILED", 0, "exit-code read failure"},
		{"mismatch is cancel failed", cancelReaderFake{exitCode: 1, exitExists: true}, nil, "CANCEL_FAILED", 0, "exit-code mismatch: existing=1 attempted=130"},
		{"write failure is contract write failed", cancelReaderFake{}, writeErr, "CONTRACT_WRITE_FAILED", 1, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := cancelQueuedPayload(t)
			tasks := &cancelStoreFake{reserved: true, snapshot: cancelPersistedSnapshot(t, domain.StateOrphaned, false)}
			queueMu := &sync.Mutex{}
			queue := &cancelQueueFake{payload: payload, index: 1, queueMu: queueMu}
			acceptEvents := &cancelEventsFake{}
			writer := &cancelWriterFake{writeExitErr: tc.writeErr}
			reader := tc.reader
			disarmer := &cancelDisarmerFake{}
			confirmer := execution.NewConfirmTaskKilledUseCase(tasks, writer, &reader, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, &cancelPendingRegistrarFake{})
			uc := NewCancelTaskUseCase(tasks, queue, queueMu, store.NewTaskMutex(), acceptEvents, &cancelTerminatorFake{}, &cancelTerminationEnsurerFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, &cancelStalledTrackerFake{}, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))
			var logs bytes.Buffer
			uc.logger = slog.New(slog.NewJSONHandler(&logs, nil))

			response := uc.Handle(transport.Request{RequestID: tc.name, TaskID: payload.Task.ID().String(), Params: json.RawMessage(`{}`)})
			if response.OK || response.Error == nil || response.Error.Code != tc.wantCode {
				t.Fatalf("response=%#v", response)
			}
			detail, err := json.Marshal(response.Error.Detail)
			if err != nil || string(detail) != `{"task_id":"`+payload.Task.ID().String()+`"}` {
				t.Fatalf("detail=%s err=%v", detail, err)
			}
			if tc.wantLogCause != "" && !strings.Contains(logs.String(), tc.wantLogCause) {
				t.Fatalf("logs=%q, want cause %q", logs.String(), tc.wantLogCause)
			}
			if reader.exitReads != 1 || len(writer.exits) != tc.wantExitWrites {
				t.Fatalf("exit reads=%d writes=%d, want reads=1 writes=%d", reader.exitReads, len(writer.exits), tc.wantExitWrites)
			}
			if tasks.saves != 1 || tasks.snapshot.State != domain.StateCancelling || len(acceptEvents.events) != 1 || len(writer.events) != 0 {
				t.Fatalf("saves=%d state=%s accept events=%d terminal events=%d", tasks.saves, tasks.snapshot.State, len(acceptEvents.events), len(writer.events))
			}
		})
	}
}

func TestCancelTaskExecute_NonContractFailuresDoNotGainContractWriteSentinel(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(*testing.T, error) error
	}{
		{"initial-reservation", func(t *testing.T, cause error) error {
			payload := cancelQueuedPayload(t)
			tasks, _, _, _, _, uc := cancelFixture(t, payload, false)
			tasks.reservedErr = cause
			_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			return err
		}},
		{"rechecked-reservation", func(t *testing.T, cause error) error {
			payload := cancelQueuedPayload(t)
			tasks, _, _, _, _, uc := cancelFixture(t, payload, false)
			tasks.loadErr, tasks.reservedErrs = domain.ErrTaskNotFound, []error{nil, cause}
			_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			return err
		}},
		{"persisted-load", func(t *testing.T, cause error) error {
			payload := cancelQueuedPayload(t)
			tasks, _, _, _, _, uc := cancelFixture(t, payload, false)
			tasks.loadErr = cause
			_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			return err
		}},
		{"pidless-adopted-pending", func(t *testing.T, cause error) error {
			payload := cancelQueuedPayload(t)
			tasks, _, _, _, _, uc := cancelFixture(t, payload, false)
			tasks.snapshot = cancelPersistedSnapshot(t, domain.StateAdopted, false)
			uc.pendingRegistrar.(*cancelPendingRegistrarFake).err = cause
			_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			return err
		}},
		{"dead-confirm-and-pending-join", func(t *testing.T, cause error) error {
			payload := cancelQueuedPayload(t)
			tasks, _, _, _, _, termination, uc := cancelFixtureWithTerminationEnsurer(t, payload, false)
			tasks.snapshot = cancelPersistedSnapshot(t, domain.StateRunning, true)
			tasks.snapshot.AdoptedAfterRestart = true
			termination.result.Dead = true
			uc.confirmer = &cancelConfirmerFake{err: errors.New("confirmation failure")}
			uc.pendingRegistrar.(*cancelPendingRegistrarFake).err = cause
			_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			return err
		}},
		{"not-dead-pending", func(t *testing.T, cause error) error {
			payload := cancelQueuedPayload(t)
			tasks, _, _, _, _, termination, uc := cancelFixtureWithTerminationEnsurer(t, payload, false)
			tasks.snapshot = cancelPersistedSnapshot(t, domain.StateRunning, true)
			termination.result.TerminateErr = nil
			uc.pendingRegistrar.(*cancelPendingRegistrarFake).err = cause
			_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			return err
		}},
		{"signal-and-pending-join", func(t *testing.T, cause error) error {
			payload := cancelQueuedPayload(t)
			tasks, _, _, _, _, termination, uc := cancelFixtureWithTerminationEnsurer(t, payload, false)
			tasks.snapshot = cancelPersistedSnapshot(t, domain.StateRunning, true)
			termination.result.TerminateErr = errors.New("signal failure")
			uc.pendingRegistrar.(*cancelPendingRegistrarFake).err = cause
			_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			return err
		}},
		{"starting-claimed-load", func(t *testing.T, cause error) error {
			payload := cancelQueuedPayload(t)
			tasks, _, _, _, _, uc := cancelFixture(t, payload, false)
			tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStarting, true)
			tasks.loadErrs = []error{nil, cause}
			_, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cause := errors.New(tc.name + " failure")
			err := tc.run(t, cause)
			if !errors.Is(err, cause) || errors.Is(err, domain.ErrContractWriteFailed) {
				t.Fatalf("err=%v cause=%v", err, cause)
			}
			response := (&CancelTaskUseCase{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}).cancelMappedError("request", cancelTaskID(t), err)
			if response.OK || response.Error == nil || response.Error.Code != "CANCEL_FAILED" || response.Error.MessageKey != "error.cancel.failed" {
				t.Fatalf("response=%#v", response)
			}
		})
	}
}

func TestCancelTaskExecute_AdoptedAfterRestartWithPIDUsesEstimated130_SCNProto0331(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, _, _, _, disarmer, termination, uc := cancelFixtureWithTerminationEnsurer(t, payload, false)
	tasks.snapshot = cancelPersistedSnapshot(t, domain.StateRunning, true)
	tasks.snapshot.AdoptedAfterRestart = true
	termination.result.Dead = true
	spy := &cancelConfirmerSpy{delegate: uc.confirmer}
	uc.confirmer = spy

	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: payload.Task.ID(), OccurredAt: time.Now()})
	if err != nil || !out.TerminationTriggered || tasks.snapshot.State != domain.StateKilled || disarmer.calls != 1 {
		t.Fatalf("out=%#v err=%v snapshot=%#v disarmer=%#v", out, err, tasks.snapshot, disarmer)
	}
	if len(spy.inputs) != 1 || spy.inputs[0].RawExitCode != 130 || !spy.inputs[0].Estimated {
		t.Fatalf("confirmation inputs=%#v", spy.inputs)
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
			uc := NewCancelTaskUseCase(tasks, queue, &sync.Mutex{}, store.NewTaskMutex(), events, &cancelTerminatorFake{}, &cancelTerminationEnsurerFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, tracker, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))
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
	uc := NewCancelTaskUseCase(tasks, queue, &sync.Mutex{}, store.NewTaskMutex(), events, &cancelTerminatorFake{}, &cancelTerminationEnsurerFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, tracker, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))
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
			NewCancelTaskUseCase(tasks, queue, &sync.Mutex{}, store.NewTaskMutex(), events, terminator, &cancelTerminationEnsurerFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, tracker, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))
		})
	}
}

func TestNewCancelTaskUseCaseRejectsNilLifecycleOwnership(t *testing.T) {
	for _, ownership := range []cancelLifecycleOwnership{nil, (*cancelOwnershipFake)(nil)} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			payload := cancelQueuedPayload(t)
			tasks, queue, events, terminator, disarmer, _ := cancelFixture(t, payload, false)
			confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, &cancelPendingRegistrarFake{})
			NewCancelTaskUseCase(tasks, queue, &sync.Mutex{}, store.NewTaskMutex(), events, terminator, &cancelTerminationEnsurerFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, &cancelStalledTrackerFake{}, ownership, domain.ClockFunc(time.Now))
		}()
	}
}

func TestNewCancelTaskUseCaseRejectsNilTerminationEnsurer(t *testing.T) {
	payload := cancelQueuedPayload(t)
	tasks, queue, events, terminator, disarmer, _ := cancelFixture(t, payload, false)
	confirmer := execution.NewConfirmTaskKilledUseCase(tasks, &cancelWriterFake{}, &cancelReaderFake{}, store.NewTaskMutex(), disarmer, execution.NewReleasePathLockUseCase(&cancelPathsFake{}), &cancelSlotFake{}, domain.ClockFunc(time.Now), &cancelMetricsRecorderFake{}, &metrics.StalledTimeTracker{}, &cancelPendingRegistrarFake{})
	for _, termination := range []cancelTerminationEnsurer{nil, (*cancelTerminationEnsurerFake)(nil)} {
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			NewCancelTaskUseCase(tasks, queue, &sync.Mutex{}, store.NewTaskMutex(), events, terminator, termination, &cancelPendingRegistrarFake{}, disarmer, confirmer, &cancelStalledTrackerFake{}, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))
		}()
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
	NewCancelTaskUseCase(tasks, queue, &sync.Mutex{}, store.NewTaskMutex(), events, terminator, &cancelTerminationEnsurerFake{}, pending, disarmer, confirmer, &cancelStalledTrackerFake{}, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))
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
			uc := NewCancelTaskUseCase(tasks, &cancelQueueFake{}, &sync.Mutex{}, store.NewTaskMutex(), &cancelEventsFake{}, &cancelTerminatorFake{}, &cancelTerminationEnsurerFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, tracker, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))
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
	uc := NewCancelTaskUseCase(tasks, &cancelQueueFake{}, &sync.Mutex{}, store.NewTaskMutex(), &cancelEventsFake{}, &cancelTerminatorFake{}, &cancelTerminationEnsurerFake{}, &cancelPendingRegistrarFake{}, disarmer, confirmer, tracker, &cancelOwnershipFake{}, domain.ClockFunc(time.Now))
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
		{"TASK_NOT_FOUND_after_load", func(tasks *cancelStoreFake) { tasks.loadErr, tasks.reserved = domain.ErrTaskNotFound, false }, false, "TASK_NOT_FOUND", "error.task.notFound"},
		{"CONTRACT_WRITE_FAILED", func(tasks *cancelStoreFake) {
			tasks.snapshot = cancelPersistedSnapshot(t, domain.StateStalled, true)
			tasks.saveErr = errors.Join(domain.ErrContractWriteFailed, errors.New("save"))
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
			if tc.wantCode == "TASK_ALREADY_TERMINAL" && response.Error.Detail["state"] != domain.StateCompleted {
				t.Fatalf("response=%#v", response)
			}
		})
	}
}
