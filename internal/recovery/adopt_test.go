package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
)

type adoptionStalledTrackerFake struct {
	calls []struct {
		id domain.TaskID
		at time.Time
	}
	takes  int
	takeID domain.TaskID
	total  int
}

type adoptionOrphanResumeDispatcherFake struct {
	calls   int
	execute bool
}

func (f *adoptionOrphanResumeDispatcherFake) dispatch(callback func()) {
	f.calls++
	if f.execute {
		callback()
	}
}

func (f *adoptionStalledTrackerFake) LeaveStalled(id domain.TaskID, at time.Time) int {
	f.calls = append(f.calls, struct {
		id domain.TaskID
		at time.Time
	}{id: id, at: at})
	return 0
}

func (f *adoptionStalledTrackerFake) TakeTotal(id domain.TaskID) int {
	f.takes++
	f.takeID = id
	return f.total
}

type adoptionMetricsFake struct {
	inputs        []metrics.RecordTaskMetricsInput
	record        metrics.RecordTaskMetricsOutput
	mutex         *adoptionMutexFake
	beforeExecute func()
}

func (f *adoptionMetricsFake) Execute(_ context.Context, in metrics.RecordTaskMetricsInput) metrics.RecordTaskMetricsOutput {
	if f.mutex != nil && f.mutex.held {
		panic("metrics called while task mutex is held")
	}
	if f.beforeExecute != nil {
		f.beforeExecute()
	}
	f.inputs = append(f.inputs, in)
	return f.record
}

// These tests deliberately exercise the public adoption boundary.  The fakes
// keep the restart path deterministic; production wiring is covered separately.
type adoptionStoreFake struct {
	listed      []domain.TaskSnapshot
	entries     map[domain.TaskID]domain.TaskSnapshot
	listErr     error
	loadErr     map[domain.TaskID]error
	loadResults map[domain.TaskID][]adoptionLoadResult
	saves       int
	saveErr     error
	saveErrs    []error
	saved       *domain.TaskSnapshot
	savedAll    []domain.TaskSnapshot
	order       *[]string
	onLoad      func(domain.TaskID)
	onSave      func(domain.TaskID)
}

type adoptionLoadResult struct {
	snapshot domain.TaskSnapshot
	err      error
}

func (f *adoptionStoreFake) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	return f.listed, f.listErr
}
func (f *adoptionStoreFake) Load(id domain.TaskID) (domain.TaskSnapshot, error) {
	if f.onLoad != nil {
		f.onLoad(id)
	}
	if results := f.loadResults[id]; len(results) > 0 {
		result := results[0]
		f.loadResults[id] = results[1:]
		return result.snapshot, result.err
	}
	if err := f.loadErr[id]; err != nil {
		return domain.TaskSnapshot{}, err
	}
	return f.entries[id], nil
}
func (f *adoptionStoreFake) Save(id domain.TaskID, s domain.TaskSnapshot) error {
	if f.onSave != nil {
		f.onSave(id)
	}
	f.saves++
	f.saved = &s
	f.savedAll = append(f.savedAll, s)
	if f.order != nil {
		*f.order = append(*f.order, "save")
	}
	err := f.saveErr
	if len(f.saveErrs) > 0 {
		err = f.saveErrs[0]
		f.saveErrs = f.saveErrs[1:]
	}
	if err == nil {
		f.entries[id] = s
	}
	return err
}

type adoptionLivenessFake struct {
	dead      map[domain.TaskID]bool
	err       map[domain.TaskID]error
	calls     int
	onExecute func(domain.TaskID)
}

func (f *adoptionLivenessFake) Execute(_ context.Context, id domain.TaskID) (bool, error) {
	f.calls++
	if f.onExecute != nil {
		f.onExecute(id)
	}
	return f.dead[id], f.err[id]
}

type adoptionReaderFake struct {
	present    bool
	err        error
	calls      int
	exitCode   int
	exitExists bool
	exitErr    error
	results    []adoptionReaderResult
}

type adoptionReaderResult struct {
	present bool
	err     error
}

func (f *adoptionReaderFake) ReadLastMessage(domain.TaskID) (bool, error) {
	f.calls++
	if len(f.results) > 0 {
		result := f.results[0]
		f.results = f.results[1:]
		return result.present, result.err
	}
	return f.present, f.err
}
func (*adoptionReaderFake) ReadStderrLog(domain.TaskID) ([]byte, error) { return nil, nil }
func (f *adoptionReaderFake) ReadExitCode(domain.TaskID) (int, bool, error) {
	return f.exitCode, f.exitExists, f.exitErr
}

type adoptionWriterFake struct {
	events           []domain.Event
	appendErr        error
	exitCode         *domain.ExitCode
	adoptedMarkers   int
	adoptedMarkerAt  []time.Time
	recoveredMarkers int
	exitCodes        int
	adoptedErr       error
	recoveredErr     error
	exitErr          error
	order            *[]string
	onWriteExitCode  func(domain.ExitCode)
}

func (*adoptionWriterFake) WritePrompt(domain.TaskID, []byte) error         { return nil }
func (*adoptionWriterFake) WriteReviewInput(domain.TaskID, []byte) error    { return nil }
func (*adoptionWriterFake) WriteCombinedPrompt(domain.TaskID, []byte) error { return nil }
func (*adoptionWriterFake) OpenExecutionLogs(domain.TaskID) (*contract.ExecutionLogs, error) {
	return nil, nil
}
func (f *adoptionWriterFake) WriteExitCode(_ domain.TaskID, exitCode domain.ExitCode) error {
	f.exitCode = &exitCode
	f.exitCodes++
	if f.order != nil {
		*f.order = append(*f.order, "exit-code")
	}
	if f.onWriteExitCode != nil {
		f.onWriteExitCode(exitCode)
	}
	return f.exitErr
}
func (*adoptionWriterFake) WritePartialOutput(domain.TaskID, string) error { return nil }
func (f *adoptionWriterFake) WriteRecoveredMarker(domain.TaskID, time.Time) error {
	f.recoveredMarkers++
	if f.order != nil {
		*f.order = append(*f.order, "recovered-marker")
	}
	return f.recoveredErr
}
func (f *adoptionWriterFake) WriteAdoptedMarker(_ domain.TaskID, at time.Time) error {
	f.adoptedMarkers++
	f.adoptedMarkerAt = append(f.adoptedMarkerAt, at)
	if f.order != nil {
		*f.order = append(*f.order, "adopted-marker")
	}
	return f.adoptedErr
}
func (f *adoptionWriterFake) AppendEvent(_ domain.TaskID, event domain.Event) error {
	f.events = append(f.events, event)
	if f.order != nil {
		*f.order = append(*f.order, "event")
	}
	return f.appendErr
}
func (*adoptionWriterFake) AppendRawEvent(domain.TaskID, string, json.RawMessage) error { return nil }

type adoptionFinalizerFake struct {
	calls              int
	taskID             domain.TaskID
	estimated, adopted bool
	err                error
}

func (f *adoptionFinalizerFake) Finalize(id domain.TaskID, raw int, estimated, adopted bool, _ time.Time) error {
	if raw != 0 {
		return errors.New("unexpected exit code")
	}
	f.calls++
	f.taskID = id
	f.estimated = estimated
	f.adopted = adopted
	return f.err
}

type adoptionSlotsFake struct {
	resets    [][]domain.TaskID
	releases  int
	onRelease func()
}

func (f *adoptionSlotsFake) Reset(ids []domain.TaskID) {
	f.resets = append(f.resets, append([]domain.TaskID(nil), ids...))
}
func (f *adoptionSlotsFake) ReleaseAndAdvance(context.Context, domain.TaskID, time.Time) {
	if f.onRelease != nil {
		f.onRelease()
	}
	f.releases++
}

type adoptionMutexFake struct{ held bool }

func (f *adoptionMutexFake) Lock(domain.TaskID) {
	if f.held {
		panic("double lock")
	}
	f.held = true
}
func (f *adoptionMutexFake) Unlock(domain.TaskID) {
	if !f.held {
		panic("unlock")
	}
	f.held = false
}

type adoptionTerminationFake struct {
	calls        int
	authorities  []ProcessSignalAuthority
	sendDead     bool
	terminateErr error
	confirmErr   error
}

func (f *adoptionTerminationFake) Confirm(context.Context, domain.TaskID) (bool, error) {
	f.calls++
	return false, nil
}
func (f *adoptionTerminationFake) SendAndConfirm(_ context.Context, _ domain.TaskID, authority ProcessSignalAuthority, _ time.Duration) TerminationAttemptResult {
	f.calls++
	f.authorities = append(f.authorities, authority)
	return TerminationAttemptResult{Dead: f.sendDead, TerminateErr: f.terminateErr, ConfirmErr: f.confirmErr}
}

func TestAdoptionTerminationAuthorityUsesStateAndNilGeneration(t *testing.T) {
	id := adoptionID(t, "authority")
	snapshot := adoptionSnapshot(t, id, domain.StateTimeout)

	authority, ok := processSignalAuthority(snapshot)
	if !ok {
		t.Fatal("processSignalAuthority returned false")
	}
	if authority.TaskID != id || authority.PID != *snapshot.PID ||
		!authority.ProcessStartedAt.Equal(*snapshot.ProcessStartedAt) ||
		authority.ExpectedState != domain.StateTimeout || authority.LifecycleGeneration != nil {
		t.Fatalf("authority=%+v", authority)
	}
}

type adoptionKilledFake struct{ calls int }

func (f *adoptionKilledFake) ConfirmKilled(context.Context, domain.TaskID, int, bool, time.Time) error {
	f.calls++
	return nil
}

type adoptionPathLocksFake struct {
	calls     int
	err       error
	onRelease func()
}

func (f *adoptionPathLocksFake) Release(context.Context, domain.TaskID) error {
	f.calls++
	if f.onRelease != nil {
		f.onRelease()
	}
	return f.err
}

func pendingDisposition(entry PendingEntry) PendingSendDisposition {
	switch entry.state {
	case pendingUnsent:
		return PendingSendUnsent
	case pendingSent:
		return PendingSendSent
	default:
		return PendingSendConfirmOnly
	}
}

func adoptionID(t *testing.T, suffix string) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260814-120000-abcd-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func adoptionSnapshot(t *testing.T, id domain.TaskID, state domain.TaskState) domain.TaskSnapshot {
	t.Helper()
	at := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	slug, _ := domain.NewSlug("adopt")
	task, _, err := domain.NewTask(id, domain.SubcommandImpl, slug, nil, at, 1)
	if err != nil {
		t.Fatal(err)
	}
	timeout, _ := domain.NewTimeout(nil, 1800)
	if _, err = task.Start(timeout, "gpt-5", at); err != nil {
		t.Fatal(err)
	}
	snapshot, err := domain.NewTaskSnapshotFromAdmission(task, timeout, "gpt-5", nil, domain.ExecutionRouteDaemon, at)
	if err != nil {
		t.Fatal(err)
	}
	if state != domain.StateStarting {
		if _, err = task.RecordProcessInfo(123, at, at); err != nil {
			t.Fatal(err)
		}
		if err = task.ConfirmRunning(at); err != nil {
			t.Fatal(err)
		}
	}
	if state == domain.StateStalled {
		if _, err = task.MarkStalled(1, at); err != nil {
			t.Fatal(err)
		}
	}
	switch state {
	case domain.StateTimeout:
		if _, err = task.MarkTimedOut(nil, at); err != nil {
			t.Fatal(err)
		}
	case domain.StateRecovering:
		if _, err = task.MarkTimedOut(nil, at); err != nil {
			t.Fatal(err)
		}
		if _, err = task.BeginRecovery(nil, at); err != nil {
			t.Fatal(err)
		}
	case domain.StateOrphaned:
		if _, err = task.DetectOrphan("running", at); err != nil {
			t.Fatal(err)
		}
	case domain.StateCancelling:
		if _, err = task.RequestCancel(false, at); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err = snapshot.WithTask(task, at)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func adoptionRecoveringSnapshot(t *testing.T, id domain.TaskID, origin domain.RecoveryOrigin) domain.TaskSnapshot {
	t.Helper()
	state := domain.StateTimeout
	if origin == domain.RecoveryOriginOrphan {
		state = domain.StateOrphaned
	}
	snapshot := adoptionSnapshot(t, id, state)
	task, err := snapshot.Restore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.BeginRecovery(snapshot.SessionRef, snapshot.StateUpdatedAt); err != nil {
		t.Fatal(err)
	}
	snapshot, err = snapshot.WithTask(task, snapshot.StateUpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func newAdoptionResumeUseCase(t *testing.T) *RecoverViaResumeUseCase {
	t.Helper()
	resume, _, _, _, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, nil, RecoveryResult{})
	return resume
}

func waitForPendingRemoval(t *testing.T, pending *PendingReconciliationSet) {
	t.Helper()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if len(pending.List()) == 0 {
			return
		}
		select {
		case <-tick.C:
		case <-timeout.C:
			t.Fatalf("pending recovery did not complete: %+v", pending.List())
		}
	}
}

func newAdoptionUseCase(tasks *adoptionStoreFake, liveness *adoptionLivenessFake, reader *adoptionReaderFake, writer *adoptionWriterFake, finalizer *adoptionFinalizerFake, resume *RecoverViaResumeUseCase, slots *adoptionSlotsFake, mutex *adoptionMutexFake) *AdoptRunningTasksUseCase {
	return NewAdoptRunningTasksUseCase(tasks, liveness, reader, writer, finalizer, resume, slots, slots, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, mutex, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())
}

func newOrphanAdoptionUseCase(t *testing.T, tasks *adoptionStoreFake, liveness *adoptionLivenessFake, reader *adoptionReaderFake, finalizer *adoptionFinalizerFake, dispatcher orphanResumeDispatcher, tracker stalledTimeTracker, logger *slog.Logger) *AdoptRunningTasksUseCase {
	t.Helper()
	return NewAdoptRunningTasksUseCase(tasks, liveness, reader, &adoptionWriterFake{}, finalizer, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), tracker, &adoptionMetricsFake{}, logger, orphanResumeDispatcherOption{dispatcher: dispatcher})
}

func TestAdoptRunningTasksOrphanReadErrorStopsRecovery(t *testing.T) {
	for _, tc := range []struct {
		name  string
		slug  string
		state domain.TaskState
	}{
		{"already-orphaned", "orr-err", domain.StateOrphaned},
		{"dead-running-adopted-as-orphan", "run-err", domain.StateRunning},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, tc.slug)
			snapshot := adoptionSnapshot(t, id, tc.state)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			reader := &adoptionReaderFake{err: errors.New("read last message")}
			finalizer := &adoptionFinalizerFake{}
			dispatcher := &adoptionOrphanResumeDispatcherFake{}
			var logs bytes.Buffer
			uc := newOrphanAdoptionUseCase(t, tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, reader, finalizer, dispatcher, &adoptionStalledTrackerFake{}, slog.New(slog.NewTextHandler(&logs, nil)))
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != adoptionOutcomeError {
				t.Fatalf("out=(%+v,%v)", out, err)
			}
			if reader.calls != 1 || finalizer.calls != 0 || dispatcher.calls != 0 {
				t.Fatalf("reader=%d finalizer=%d dispatcher=%d", reader.calls, finalizer.calls, dispatcher.calls)
			}
			if strings.Count(logs.String(), "level=ERROR") != 1 || !strings.Contains(logs.String(), "read last message for adopted orphan failed") {
				t.Fatalf("logs=%q", logs.String())
			}
		})
	}
}

func TestAdoptRunningTasksOrphanLastMessageBranchesRemainUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name         string
		slug         string
		state        domain.TaskState
		present      bool
		wantFinal    int
		wantDispatch int
		wantOutcome  string
	}{
		{"already-orphaned-output-present", "orr-present", domain.StateOrphaned, true, 1, 0, adoptionOutcomeOrphanRecovered},
		{"dead-running-output-present", "run-present", domain.StateRunning, true, 1, 0, adoptionOutcomeOrphanRecovered},
		{"already-orphaned-output-absent", "orr-absent", domain.StateOrphaned, false, 0, 1, adoptionOutcomeOrphanRecoveryStarted},
		{"dead-running-output-absent", "run-absent", domain.StateRunning, false, 0, 1, adoptionOutcomeOrphanRecoveryStarted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, tc.slug)
			snapshot := adoptionSnapshot(t, id, tc.state)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			reader := &adoptionReaderFake{present: tc.present}
			finalizer := &adoptionFinalizerFake{}
			dispatcher := &adoptionOrphanResumeDispatcherFake{}
			uc := newOrphanAdoptionUseCase(t, tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, reader, finalizer, dispatcher, &adoptionStalledTrackerFake{}, slog.Default())
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != tc.wantOutcome {
				t.Fatalf("out=(%+v,%v)", out, err)
			}
			if reader.calls != 1 || finalizer.calls != tc.wantFinal || dispatcher.calls != tc.wantDispatch {
				t.Fatalf("reader=%d finalizer=%d dispatcher=%d", reader.calls, finalizer.calls, dispatcher.calls)
			}
		})
	}
}

func TestAdoptTimeoutRecoveryReadsClockAfterPathLockRelease(t *testing.T) {
	id := adoptionID(t, "timeout-dispatch-time")
	snapshot := adoptionSnapshot(t, id, domain.StateTimeout)
	observedAt := time.Date(2026, time.August, 14, 12, 1, 0, 0, time.UTC)
	dispatchAt := observedAt.Add(time.Minute)
	clockCalls := 0
	clock := domain.ClockFunc(func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return observedAt
		}
		return dispatchAt
	})
	pathLocks := &adoptionPathLocksFake{onRelease: func() {
		if clockCalls != 1 {
			t.Fatalf("clock calls at path lock release=%d, want 1", clockCalls)
		}
	}}
	resume, _, _, _, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateTimeout, nil, RecoveryResult{})
	uc := NewAdoptRunningTasksUseCase(&adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, resume, &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, pathLocks, &PendingReconciliationSet{}, &adoptionMutexFake{}, clock, &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())

	if out, err := uc.Execute(context.Background()); err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != adoptionOutcomeOrphanRecoveryStarted {
		t.Fatalf("out=(%+v,%v)", out, err)
	}
	if pathLocks.calls != 1 || clockCalls != 3 {
		t.Fatalf("path lock calls=%d clock calls=%d, want 1 and 3", pathLocks.calls, clockCalls)
	}
}

func TestAdoptOrphanRecoveryUsesRecoveryDispatchTime(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state domain.TaskState
	}{
		{name: "already orphaned", state: domain.StateOrphaned},
		{name: "dead running adopted as orphan", state: domain.StateRunning},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "orphan-dispatch-"+tc.name[:1])
			snapshot := adoptionSnapshot(t, id, tc.state)
			observedAt := time.Date(2026, time.August, 14, 12, 1, 0, 0, time.UTC)
			dispatchAt := observedAt.Add(time.Minute)
			clockCalls := 0
			clock := domain.ClockFunc(func() time.Time {
				clockCalls++
				if clockCalls == 1 {
					return observedAt
				}
				return dispatchAt
			})
			resume, _, writer, _, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, nil, RecoveryResult{})
			dispatcher := &adoptionOrphanResumeDispatcherFake{execute: true}
			uc := NewAdoptRunningTasksUseCase(&adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, resume, &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, clock, &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default(), orphanResumeDispatcherOption{dispatcher: dispatcher})

			if out, err := uc.Execute(context.Background()); err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != adoptionOutcomeOrphanRecoveryStarted {
				t.Fatalf("out=(%+v,%v)", out, err)
			}
			attempted := recoveryEvent[domain.RecoveryAttempted](t, writer.events)
			if dispatcher.calls != 1 || clockCalls != 3 || !attempted.OccurredAt.Equal(dispatchAt) {
				t.Fatalf("dispatches=%d clock calls=%d attempted=%+v", dispatcher.calls, clockCalls, attempted)
			}
		})
	}
}

func TestAdoptRunningTasksOrphanFinalizeFailureRemainsError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		slug  string
		state domain.TaskState
	}{
		{"already-orphaned", "orr-finerr", domain.StateOrphaned},
		{"dead-running-adopted-as-orphan", "run-finerr", domain.StateRunning},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, tc.slug)
			snapshot := adoptionSnapshot(t, id, tc.state)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			finalizer := &adoptionFinalizerFake{err: errors.New("finalize")}
			dispatcher := &adoptionOrphanResumeDispatcherFake{}
			var logs bytes.Buffer
			uc := newOrphanAdoptionUseCase(t, tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{present: true}, finalizer, dispatcher, &adoptionStalledTrackerFake{}, slog.New(slog.NewTextHandler(&logs, nil)))
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != adoptionOutcomeError {
				t.Fatalf("out=(%+v,%v)", out, err)
			}
			if finalizer.calls != 1 || dispatcher.calls != 0 {
				t.Fatalf("finalizer=%d dispatcher=%d", finalizer.calls, dispatcher.calls)
			}
			if strings.Count(logs.String(), "level=ERROR") != 1 || !strings.Contains(logs.String(), "finalize adopted orphan failed") {
				t.Fatalf("logs=%q", logs.String())
			}
		})
	}
}

func TestRecoverOrphanWithoutResumeRemainsError(t *testing.T) {
	for _, tc := range []struct {
		name string
		slug string
	}{
		{"output-absent-resume-unavailable", "no-resume"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, tc.slug)
			dispatcher := &adoptionOrphanResumeDispatcherFake{}
			reader := &adoptionReaderFake{}
			finalizer := &adoptionFinalizerFake{}
			var logs bytes.Buffer
			uc := newOrphanAdoptionUseCase(t, &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{}}, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, reader, finalizer, dispatcher, &adoptionStalledTrackerFake{}, slog.New(slog.NewTextHandler(&logs, nil)))
			uc.resume = nil
			if got, completed := uc.recoverOrphan(context.Background(), id, nil, time.Now()); !completed || got != adoptionOutcomeError {
				t.Fatalf("outcome=%q", got)
			}
			if reader.calls != 1 || finalizer.calls != 0 || dispatcher.calls != 0 || !strings.Contains(logs.String(), "resume recovery for adopted orphan is unavailable") || strings.Count(logs.String(), "level=ERROR") != 1 {
				t.Fatalf("reader=%d finalizer=%d dispatcher=%d logs=%q", reader.calls, finalizer.calls, dispatcher.calls, logs.String())
			}
		})
	}
}

func TestAdoptRunningTasksOrphanReadErrorPreservesStalledTrackerRules(t *testing.T) {
	for _, tc := range []struct {
		name       string
		slug       string
		state      domain.TaskState
		wantLeaves int
	}{
		{"stalled-dead-save-success-leaves-once", "stall-leave", domain.StateStalled, 1},
		{"already-orphaned-does-not-leave", "orr-noleave", domain.StateOrphaned, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, tc.slug)
			snapshot := adoptionSnapshot(t, id, tc.state)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			tracker := &adoptionStalledTrackerFake{}
			dispatcher := &adoptionOrphanResumeDispatcherFake{}
			uc := newOrphanAdoptionUseCase(t, tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{err: errors.New("read last message")}, &adoptionFinalizerFake{}, dispatcher, tracker, slog.Default())
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != adoptionOutcomeError {
				t.Fatalf("out=(%+v,%v)", out, err)
			}
			entries := uc.pending.List()
			if len(tracker.calls) != tc.wantLeaves || dispatcher.calls != 0 || len(entries) != 1 || pendingDisposition(entries[0]) != PendingSendConfirmOnly {
				t.Fatalf("leaves=%d dispatcher=%d pending=%+v", len(tracker.calls), dispatcher.calls, entries)
			}
		})
	}
}

func TestAdoptRunningTasksResumesStartingRunningAndStalled(t *testing.T) {
	for _, state := range []domain.TaskState{domain.StateStarting, domain.StateRunning, domain.StateStalled} {
		t.Run(string(state), func(t *testing.T) {
			id := adoptionID(t, string(state))
			snapshot := adoptionSnapshot(t, id, state)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			liveness := &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}
			slots, mutex := &adoptionSlotsFake{}, &adoptionMutexFake{}
			out, err := newAdoptionUseCase(tasks, liveness, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, mutex).Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != "resumed-monitoring" {
				t.Fatalf("out=(%+v,%v)", out, err)
			}
			if tasks.entries[id].State != domain.StateRunning || !tasks.entries[id].AdoptedAfterRestart || liveness.calls != 1 || len(slots.resets) != 1 || mutex.held {
				t.Fatalf("snapshot=%+v liveness=%d reset=%v held=%v", tasks.entries[id], liveness.calls, slots.resets, mutex.held)
			}
		})
	}
}

func TestAdoptRunningTasksAdoptsStartingWithoutRecordedProcess(t *testing.T) {
	t.Run("live task resumes monitoring", func(t *testing.T) {
		id := adoptionID(t, "starting-no-process-live")
		snapshot := adoptionSnapshot(t, id, domain.StateStarting)
		if snapshot.PID != nil || snapshot.ProcessStartedAt != nil {
			t.Fatalf("starting snapshot unexpectedly has process identity: %+v", snapshot)
		}
		tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
		liveness := &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}

		out, err := newAdoptionUseCase(tasks, liveness, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionMutexFake{}).Execute(context.Background())

		if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != adoptionOutcomeResumedMonitoring || liveness.calls != 1 || tasks.entries[id].State != domain.StateRunning || !tasks.entries[id].AdoptedAfterRestart {
			t.Fatalf("out=(%+v,%v) liveness=%d snapshot=%+v", out, err, liveness.calls, tasks.entries[id])
		}
	})

	t.Run("dead task becomes orphaned and requests recovery", func(t *testing.T) {
		id := adoptionID(t, "starting-no-process-dead")
		snapshot := adoptionSnapshot(t, id, domain.StateStarting)
		if snapshot.PID != nil || snapshot.ProcessStartedAt != nil {
			t.Fatalf("starting snapshot unexpectedly has process identity: %+v", snapshot)
		}
		tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
		liveness := &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}
		dispatcher := &adoptionOrphanResumeDispatcherFake{}

		out, err := newOrphanAdoptionUseCase(t, tasks, liveness, &adoptionReaderFake{}, &adoptionFinalizerFake{}, dispatcher, &adoptionStalledTrackerFake{}, slog.Default()).Execute(context.Background())

		if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != adoptionOutcomeOrphanRecoveryStarted || liveness.calls != 1 || tasks.saved == nil || tasks.saved.State != domain.StateOrphaned || tasks.saved.PID != nil || tasks.saved.ProcessStartedAt != nil || dispatcher.calls != 1 {
			t.Fatalf("out=(%+v,%v) liveness=%d saved=%+v dispatch=%d", out, err, liveness.calls, tasks.saved, dispatcher.calls)
		}
	})
}

func TestAdoptRunningTasksFinalizesDeadTaskWithOutput(t *testing.T) {
	id := adoptionID(t, "dead-output")
	snapshot := adoptionSnapshot(t, id, domain.StateRunning)
	tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
	finalizer := &adoptionFinalizerFake{}
	out, err := newAdoptionUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{present: true}, &adoptionWriterFake{}, finalizer, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionMutexFake{}).Execute(context.Background())
	if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != "orphan-recovered" {
		t.Fatalf("out=(%+v,%v)", out, err)
	}
	if finalizer.calls != 1 || !finalizer.estimated || !finalizer.adopted || tasks.entries[id].State != domain.StateOrphaned {
		t.Fatalf("finalizer=%+v state=%q", finalizer, tasks.entries[id].State)
	}
}

func TestAdoptRunningTasksZeroAndEnumerationFailure(t *testing.T) {
	for _, tc := range []struct {
		name    string
		listErr error
		wantErr bool
	}{{"empty", nil, false}, {"list failure", errors.New("list"), true}} {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{}, listErr: tc.listErr}
			live, slots := &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionSlotsFake{}
			out, err := newAdoptionUseCase(tasks, live, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, &adoptionMutexFake{}).Execute(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v", err)
			}
			if !tc.wantErr && (out.Outcomes == nil || len(out.Outcomes) != 0 || live.calls != 0 || len(slots.resets) != 1) {
				t.Fatalf("out=%+v calls=%d resets=%v", out, live.calls, slots.resets)
			}
		})
	}
}

func TestAdoptRunningTasksReconcilesAndDefersLatestStateWithoutSideEffects(t *testing.T) {
	id := adoptionID(t, "redispatch")
	for _, state := range []domain.TaskState{domain.StateQueued, domain.StateAdopted, domain.StateCompleted, domain.StateFailed, domain.StateKilled, domain.StateRecovered, domain.StateTimeoutLost, domain.StateLost} {
		t.Run(string(state), func(t *testing.T) {
			listed := adoptionSnapshot(t, id, domain.StateRunning)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{listed}, entries: map[domain.TaskID]domain.TaskSnapshot{id: {TaskID: id, State: state}}}
			live, slots, mutex := &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionSlotsFake{}, &adoptionMutexFake{}
			out, err := newAdoptionUseCase(tasks, live, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, mutex).Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != "reconciled" || live.calls != 0 || tasks.saves != 0 || mutex.held {
				t.Fatalf("out=(%+v,%v) live=%d saves=%d", out, err, live.calls, tasks.saves)
			}
		})
	}
}

func TestAdoptRunningTasksRecoversRecoveringState(t *testing.T) {
	for _, tc := range []struct {
		name            string
		dead            bool
		liveErr         error
		present         bool
		wantOutcome     string
		wantState       domain.TaskState
		wantPending     bool
		wantDisposition PendingSendDisposition
	}{
		{"live is deferred", false, nil, false, "deferred", domain.StateRecovering, true, PendingSendConfirmOnly},
		{"dead with output completes recovery", true, nil, true, "orphan-recovered", domain.StateRecovered, false, PendingSendConfirmOnly},
		{"dead without output fails recovery", true, nil, false, "error", domain.StateTimeoutLost, false, PendingSendConfirmOnly},
		{"liveness failure is pending without signal", false, errors.New("liveness"), false, "error", domain.StateRecovering, true, PendingSendConfirmOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "recovering-"+tc.name[:1])
			snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			reader, writer, slots := &adoptionReaderFake{present: tc.present}, &adoptionWriterFake{}, &adoptionSlotsFake{}
			pending := &PendingReconciliationSet{}
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: tc.dead}, err: map[domain.TaskID]error{id: tc.liveErr}}, reader, writer, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, slots, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != tc.wantOutcome || tasks.entries[id].State != tc.wantState {
				t.Fatalf("out=(%+v,%v) state=%q", out, err, tasks.entries[id].State)
			}
			entries := pending.List()
			if tc.wantPending != (len(entries) == 1) || (len(entries) == 1 && pendingDisposition(entries[0]) != tc.wantDisposition) {
				t.Fatalf("pending=%+v", entries)
			}
			if !tc.dead && tc.liveErr == nil && reader.calls != 0 {
				t.Fatalf("live recovering task read output %d times", reader.calls)
			}
			if tc.wantState == domain.StateRecovered && (writer.exitCode == nil || writer.exitCode.Raw() != 0 || slots.releases != 1) {
				t.Fatalf("exit=%v releases=%d", writer.exitCode, slots.releases)
			}
			if tc.dead && tc.liveErr == nil && (writer.adoptedMarkers != 1 || !tasks.entries[id].AdoptedAfterRestart) {
				t.Fatalf("adopted markers=%d snapshot=%+v", writer.adoptedMarkers, tasks.entries[id])
			}
		})
	}
}

func TestAdoptRunningTasksRecoveringRecordsTerminalMetrics(t *testing.T) {
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
			id := adoptionID(t, "metrics-"+tc.name[:1])
			snapshot := adoptionRecoveringSnapshot(t, id, tc.origin)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			mutex := &adoptionMutexFake{}
			tracker := &adoptionStalledTrackerFake{total: 29}
			metricsRecorder := &adoptionMetricsFake{record: metrics.RecordTaskMetricsOutput{Recorded: tc.recorded}, mutex: mutex}
			slots := &adoptionSlotsFake{onRelease: func() {
				if len(metricsRecorder.inputs) != 1 {
					t.Fatalf("slot release preceded metrics: %+v", metricsRecorder.inputs)
				}
			}}
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{present: tc.present}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, slots, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, mutex, domain.ClockFunc(time.Now), tracker, metricsRecorder, slog.Default())
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || tasks.entries[id].State != tc.wantState || tracker.takes != 1 || tracker.takeID != id || slots.releases != 1 || len(metricsRecorder.inputs) != 1 {
				t.Fatalf("out=(%+v,%v) state=%q takes=%d slots=%d metrics=%+v", out, err, tasks.entries[id].State, tracker.takes, slots.releases, metricsRecorder.inputs)
			}
			in := metricsRecorder.inputs[0]
			if in.TaskID != id || in.FinalState != tc.wantState || !in.Estimated || in.StalledTotalMs != tracker.total {
				t.Fatalf("metrics=%+v", in)
			}
		})
	}
}

func TestNewAdoptRunningTasksUseCaseUsesDefaultGrace(t *testing.T) {
	uc := newAdoptionUseCase(
		&adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{}},
		&adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}},
		&adoptionReaderFake{},
		&adoptionWriterFake{},
		&adoptionFinalizerFake{},
		newAdoptionResumeUseCase(t),
		&adoptionSlotsFake{},
		&adoptionMutexFake{},
	)
	if uc.grace != defaultRecoveryTerminationGrace {
		t.Fatalf("grace=%s, want %s", uc.grace, defaultRecoveryTerminationGrace)
	}
}

func TestNewAdoptRunningTasksUseCasePanicsForNonPositiveGrace(t *testing.T) {
	for _, grace := range []time.Duration{0, -time.Second} {
		t.Run(grace.String(), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("NewAdoptRunningTasksUseCase did not panic")
				}
			}()
			NewAdoptRunningTasksUseCase(
				&adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{}},
				&adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}},
				&adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t),
				&adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{},
				&PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, grace,
			)
		})
	}
}

func TestAdoptRunningTasksRecoveringPersistenceOrder(t *testing.T) {
	for _, tc := range []struct {
		name    string
		present bool
		want    []string
	}{
		{"success", true, []string{"adopted-marker", "recovered-marker", "exit-code", "save"}},
		{"failure", false, []string{"adopted-marker", "exit-code", "save"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "recovering-order-"+tc.name[:1])
			snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
			order := []string{}
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, order: &order}
			writer := &adoptionWriterFake{order: &order}
			slots := &adoptionSlotsFake{}
			uc := newAdoptionUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{present: tc.present}, writer, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, &adoptionMutexFake{})
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || slots.releases != 1 || tasks.saved == nil || !tasks.saved.AdoptedAfterRestart {
				t.Fatalf("out=(%+v,%v) releases=%d saved=%+v", out, err, slots.releases, tasks.saved)
			}
			if len(order) < len(tc.want) {
				t.Fatalf("order=%v, want prefix %v", order, tc.want)
			}
			for i, want := range tc.want {
				if order[i] != want {
					t.Fatalf("order=%v, want prefix %v", order, tc.want)
				}
			}
			if !tc.present && (writer.recoveredMarkers != 0 || writer.exitCode == nil || writer.exitCode.Raw() != 1 || writer.exitCode.Class() != domain.ExitCodeClassFailure) {
				t.Fatalf("writer=%+v", writer)
			}
		})
	}
}

func TestAdoptRunningTasksRecoveringNonSaveWriteFailuresAreFailSoft(t *testing.T) {
	for _, tc := range []struct {
		name      string
		present   bool
		configure func(*adoptionStoreFake, *adoptionWriterFake)
	}{
		{"adopted marker", true, func(_ *adoptionStoreFake, writer *adoptionWriterFake) { writer.adoptedErr = errors.New("adopted") }},
		{"recovered marker", true, func(_ *adoptionStoreFake, writer *adoptionWriterFake) { writer.recoveredErr = errors.New("recovered") }},
		{"exit code", true, func(_ *adoptionStoreFake, writer *adoptionWriterFake) { writer.exitErr = errors.New("exit") }},
		{"append event", true, func(_ *adoptionStoreFake, writer *adoptionWriterFake) { writer.appendErr = errors.New("event") }},
		{"failure exit code", false, func(_ *adoptionStoreFake, writer *adoptionWriterFake) { writer.exitErr = errors.New("exit") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "recovering-fail-soft-"+tc.name[:1])
			snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			writer, slots, mutex := &adoptionWriterFake{}, &adoptionSlotsFake{}, &adoptionMutexFake{}
			tc.configure(tasks, writer)
			var logs bytes.Buffer
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{present: tc.present}, writer, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, slots, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, mutex, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.New(slog.NewTextHandler(&logs, nil)))
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || mutex.held || slots.releases != 1 || len(logs.String()) == 0 || len(writer.events) == 0 {
				t.Fatalf("out=(%+v,%v) held=%v slots=%d logs=%q events=%d", out, err, mutex.held, slots.releases, logs.String(), len(writer.events))
			}
		})
	}
}

func TestResolveRecoveringLockedReturnsSaveErrorWithoutAppendingEvent(t *testing.T) {
	id := adoptionID(t, "recovering-save-error")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	saveErr := errors.New("save")
	tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, saveErr: saveErr}
	writer := &adoptionWriterFake{}
	var logs bytes.Buffer

	err := resolveRecoveringLocked(tasks, &adoptionReaderFake{}, writer, slog.New(slog.NewTextHandler(&logs, nil)), id, snapshot, true, time.Now())
	if !errors.Is(err, saveErr) || len(writer.events) != 0 || !strings.Contains(logs.String(), "save recovered task failed") {
		t.Fatalf("err=%v events=%d logs=%q", err, len(writer.events), logs.String())
	}
}

func TestResolveRecoveringLockedWritesMissingExitCodeBeforeSave(t *testing.T) {
	id := adoptionID(t, "recovering-missing-exit")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	order := []string{}
	tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, order: &order}
	writer := &adoptionWriterFake{order: &order}
	if err := resolveRecoveringLocked(tasks, &adoptionReaderFake{}, writer, slog.Default(), id, snapshot, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	if writer.exitCodes != 1 || tasks.saves != 1 || len(writer.events) == 0 || !reflect.DeepEqual(order, []string{"adopted-marker", "recovered-marker", "exit-code", "save", "event"}) {
		t.Fatalf("writes=%d saves=%d events=%d order=%v", writer.exitCodes, tasks.saves, len(writer.events), order)
	}
}

func TestResolveRecoveringLockedSkipsSameExitCodeOnRetry(t *testing.T) {
	id := adoptionID(t, "recovering-same-exit")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
	writer := &adoptionWriterFake{}
	if err := resolveRecoveringLocked(tasks, &adoptionReaderFake{exitCode: 0, exitExists: true}, writer, slog.Default(), id, snapshot, true, time.Now()); err != nil {
		t.Fatal(err)
	}
	if writer.exitCodes != 0 || tasks.saves != 1 || len(writer.events) == 0 {
		t.Fatalf("writes=%d saves=%d events=%d", writer.exitCodes, tasks.saves, len(writer.events))
	}
}

func TestResolveRecoveringLockedRejectsExitCodeMismatchWithoutOverwrite(t *testing.T) {
	id := adoptionID(t, "recovering-mismatch")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
	writer := &adoptionWriterFake{}
	err := resolveRecoveringLocked(tasks, &adoptionReaderFake{exitCode: 1, exitExists: true}, writer, slog.Default(), id, snapshot, true, time.Now())
	if !errors.Is(err, domain.ErrContractWriteFailed) || writer.exitCodes != 0 || tasks.saves != 0 || len(writer.events) != 0 {
		t.Fatalf("err=%v writes=%d saves=%d events=%d", err, writer.exitCodes, tasks.saves, len(writer.events))
	}
}

func TestResolveRecoveringLockedReadExitCodeFailureFailsClosed(t *testing.T) {
	id := adoptionID(t, "recovering-read-error")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
	writer := &adoptionWriterFake{}
	err := resolveRecoveringLocked(tasks, &adoptionReaderFake{exitErr: errors.New("read exit")}, writer, slog.Default(), id, snapshot, true, time.Now())
	if err == nil || writer.exitCodes != 0 || tasks.saves != 0 || len(writer.events) != 0 {
		t.Fatalf("err=%v writes=%d saves=%d events=%d", err, writer.exitCodes, tasks.saves, len(writer.events))
	}
}

func TestAdoptRunningTasksRecoveringSaveFailureRetryDoesNotRewriteExitCode(t *testing.T) {
	id := adoptionID(t, "recovering-save-retry")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	reader := &adoptionReaderFake{present: true}
	writer := &adoptionWriterFake{onWriteExitCode: func(code domain.ExitCode) {
		reader.exitCode, reader.exitExists = code.Raw(), true
	}}
	tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, saveErrs: []error{errors.New("save"), nil}}
	slots := &adoptionSlotsFake{}
	uc := newAdoptionUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, reader, writer, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, &adoptionMutexFake{})
	_, _ = uc.Execute(context.Background())
	_, _ = uc.Execute(context.Background())
	if writer.exitCodes != 1 || tasks.saves != 2 || slots.releases != 1 {
		t.Fatalf("writes=%d saves=%d slots=%d", writer.exitCodes, tasks.saves, slots.releases)
	}
}

func TestAdoptRunningTasksRecoveringRetryDoesNotReplaceDifferentExitCode(t *testing.T) {
	id := adoptionID(t, "recovering-different-retry")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	reader := &adoptionReaderFake{results: []adoptionReaderResult{{present: true}, {present: false}}}
	writer := &adoptionWriterFake{onWriteExitCode: func(code domain.ExitCode) {
		reader.exitCode, reader.exitExists = code.Raw(), true
	}}
	tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, saveErrs: []error{errors.New("save")}}
	slots := &adoptionSlotsFake{}
	uc := newAdoptionUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, reader, writer, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, &adoptionMutexFake{})
	_, _ = uc.Execute(context.Background())
	_, _ = uc.Execute(context.Background())
	if writer.exitCodes != 1 || reader.exitCode != 0 || tasks.saves != 1 || len(writer.events) != 0 || slots.releases != 0 {
		t.Fatalf("writes=%d exit=%d saves=%d events=%d slots=%d", writer.exitCodes, reader.exitCode, tasks.saves, len(writer.events), slots.releases)
	}
}

func TestAdoptRunningTasksRecoveringSaveFailureRetainsPending(t *testing.T) {
	id := adoptionID(t, "recovering-save-pending")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	tasks := &adoptionStoreFake{
		listed:  []domain.TaskSnapshot{snapshot},
		entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot},
		loadResults: map[domain.TaskID][]adoptionLoadResult{id: {
			{snapshot: snapshot},
			{err: errors.New("reload")},
		}},
		saveErr: errors.New("save"),
	}
	pending, slots, mutex := &PendingReconciliationSet{}, &adoptionSlotsFake{}, &adoptionMutexFake{}
	tracker, recorder := &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}
	uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{present: true}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, slots, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, pending, mutex, domain.ClockFunc(time.Now), tracker, recorder, slog.Default())

	out, err := uc.Execute(context.Background())
	entries := pending.List()
	if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != adoptionOutcomeError || tasks.entries[id].State != domain.StateRecovering || len(entries) != 1 || pendingDisposition(entries[0]) != PendingSendConfirmOnly || slots.releases != 0 || tracker.takes != 0 || len(recorder.inputs) != 0 || mutex.held {
		t.Fatalf("out=(%+v,%v) state=%s pending=%+v slots=%d takes=%d metrics=%+v held=%t", out, err, tasks.entries[id].State, entries, slots.releases, tracker.takes, recorder.inputs, mutex.held)
	}
}

func TestAdoptRunningTasksRecoveringSaveFailureRemovesPendingWhenReloadIsTerminal(t *testing.T) {
	id := adoptionID(t, "recovering-save-terminal")
	snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
	task, err := snapshot.Restore()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.CompleteRecovery(domain.NewExitCode(0), time.Now()); err != nil {
		t.Fatal(err)
	}
	terminal, err := snapshot.WithTask(task, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	tasks := &adoptionStoreFake{
		listed:  []domain.TaskSnapshot{snapshot},
		entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot},
		loadResults: map[domain.TaskID][]adoptionLoadResult{id: {
			{snapshot: snapshot},
			{snapshot: terminal},
		}},
		saveErr: errors.New("save"),
	}
	pending := &PendingReconciliationSet{}
	uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{present: true}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())

	out, executeErr := uc.Execute(context.Background())
	if executeErr != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != adoptionOutcomeError || len(pending.List()) != 0 {
		t.Fatalf("out=(%+v,%v) pending=%+v", out, executeErr, pending.List())
	}
}

func TestAdoptRunningTasksFinalizesOrphanedAndCancellingStates(t *testing.T) {
	for _, tc := range []struct {
		name         string
		state        domain.TaskState
		present      bool
		wantOutcome  string
		wantFinalize int
		wantKilled   int
	}{
		{"orphaned with output", domain.StateOrphaned, true, "orphan-recovered", 1, 0},
		{"cancelling dead", domain.StateCancelling, false, "reconciled", 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "terminal-"+tc.name[:1])
			snapshot := adoptionSnapshot(t, id, tc.state)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			finalizer, killed := &adoptionFinalizerFake{}, &adoptionKilledFake{}
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{present: tc.present}, &adoptionWriterFake{}, finalizer, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, killed, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != tc.wantOutcome || finalizer.calls != tc.wantFinalize || killed.calls != tc.wantKilled {
				t.Fatalf("out=(%+v,%v) finalizer=%d killed=%d", out, err, finalizer.calls, killed.calls)
			}
		})
	}
}

func TestAdoptRunningTasksReconcilesTimeoutState(t *testing.T) {
	for _, tc := range []struct {
		name            string
		dead            bool
		liveErr         error
		withoutPID      bool
		wantOutcome     string
		wantPathLocks   int
		wantPending     bool
		wantDisposition PendingSendDisposition
	}{
		{"dead releases impl path lock before recovery", true, nil, false, "orphan-recovery-started", 1, false, PendingSendConfirmOnly},
		{"live without pid is pending without send", false, nil, true, "deferred", 0, true, PendingSendConfirmOnly},
		{"liveness failure is pending without signal", false, errors.New("liveness"), false, "error", 0, true, PendingSendUnsent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "timeout-"+tc.name[:1])
			snapshot := adoptionSnapshot(t, id, domain.StateTimeout)
			if tc.withoutPID {
				snapshot.PID = nil
				snapshot.ProcessStartedAt = nil
				snapshot.AdoptedAfterRestart = true
			}
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			pathLocks, pending := &adoptionPathLocksFake{}, &PendingReconciliationSet{}
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: tc.dead}, err: map[domain.TaskID]error{id: tc.liveErr}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, pathLocks, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != tc.wantOutcome || pathLocks.calls != tc.wantPathLocks {
				t.Fatalf("out=(%+v,%v) pathLocks=%d", out, err, pathLocks.calls)
			}
			entries := pending.List()
			if tc.wantPending != (len(entries) == 1) || (len(entries) == 1 && pendingDisposition(entries[0]) != tc.wantDisposition) {
				t.Fatalf("pending=%+v", entries)
			}
		})
	}
}

func TestAdoptRunningTasksRegistersCanonicalPendingDispositions(t *testing.T) {
	for _, tc := range []struct {
		name             string
		state            domain.TaskState
		withoutAuthority bool
		wantDisposition  PendingSendDisposition
	}{
		{"recovering", domain.StateRecovering, false, PendingSendConfirmOnly},
		{"orphaned", domain.StateOrphaned, false, PendingSendConfirmOnly},
		{"timeout", domain.StateTimeout, false, PendingSendUnsent},
		{"cancelling", domain.StateCancelling, false, PendingSendUnsent},
		{"timeout without process pair", domain.StateTimeout, true, PendingSendConfirmOnly},
		{"cancelling without process pair", domain.StateCancelling, true, PendingSendConfirmOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "pending-"+tc.name[:1])
			snapshot := adoptionSnapshot(t, id, tc.state)
			if tc.withoutAuthority {
				snapshot.PID = nil
				snapshot.ProcessStartedAt = nil
			}
			pending := &PendingReconciliationSet{}
			disposition, authority := pendingRegistration(snapshot)
			if err := pending.Register(id, disposition, authority); err != nil {
				t.Fatal(err)
			}
			entries := pending.List()
			if len(entries) != 1 || pendingDisposition(entries[0]) != tc.wantDisposition {
				t.Fatalf("pending=%+v", entries)
			}
			if tc.wantDisposition == PendingSendUnsent {
				if entries[0].authority.TaskID != id || entries[0].authority.PID <= 0 || entries[0].authority.ProcessStartedAt.IsZero() {
					t.Fatalf("authority=%+v", entries[0].authority)
				}
			} else if entries[0].authority != (ProcessSignalAuthority{}) {
				t.Fatalf("confirm-only authority=%+v", entries[0].authority)
			}
		})
	}
}

func TestAdoptTimeoutTerminationRemovesPendingAfterResumeSucceeds(t *testing.T) {
	id := adoptionID(t, "timeout-confirmed-dispatch")
	snapshot := adoptionSnapshot(t, id, domain.StateTimeout)
	tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
	pending := &PendingReconciliationSet{}
	authority, ok := processSignalAuthority(snapshot)
	if !ok {
		t.Fatal("fixture requires signal authority")
	}
	if err := pending.Register(id, PendingSendUnsent, &authority); err != nil {
		t.Fatal(err)
	}
	termination := &adoptionTerminationFake{sendDead: true}
	pathLocks := &adoptionPathLocksFake{}
	uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, termination, &adoptionKilledFake{}, pathLocks, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())

	uc.confirmTimeoutTermination(context.Background(), id, snapshot)
	waitForPendingRemoval(t, pending)

	if termination.calls != 1 || pathLocks.calls != 1 || len(pending.List()) != 0 {
		t.Fatalf("termination=%d pathLocks=%d pending=%+v", termination.calls, pathLocks.calls, pending.List())
	}
}

func TestAdoptResumeRecoveryWithoutClaimRemovesPendingAfterSuccess(t *testing.T) {
	id := adoptionID(t, "resume-without-claim")
	pending := &PendingReconciliationSet{}
	if err := pending.Register(id, PendingSendConfirmOnly, nil); err != nil {
		t.Fatal(err)
	}
	uc := NewAdoptRunningTasksUseCase(&adoptionStoreFake{}, &adoptionLivenessFake{}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())

	uc.resumeRecovery(context.Background(), RecoverViaResumeInput{TaskID: id, Origin: domain.RecoveryOriginOrphan, OccurredAt: time.Now()}, SendClaim{})

	if len(pending.List()) != 0 {
		t.Fatalf("pending=%+v", pending.List())
	}
}

func TestAdoptTimeoutTerminationFailureAfterConfirmedDeathIsConfirmOnly(t *testing.T) {
	id := adoptionID(t, "timeout-confirmed-path-lock-failure")
	snapshot := adoptionSnapshot(t, id, domain.StateTimeout)
	tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
	pending := &PendingReconciliationSet{}
	authority, ok := processSignalAuthority(snapshot)
	if !ok {
		t.Fatal("fixture requires signal authority")
	}
	if err := pending.Register(id, PendingSendUnsent, &authority); err != nil {
		t.Fatal(err)
	}
	termination := &adoptionTerminationFake{sendDead: true}
	pathLocks := &adoptionPathLocksFake{err: errors.New("release")}
	uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, termination, &adoptionKilledFake{}, pathLocks, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())

	uc.confirmTimeoutTermination(context.Background(), id, snapshot)

	entries := pending.List()
	if len(entries) != 1 || pendingDisposition(entries[0]) != PendingSendConfirmOnly || entries[0].authority != (ProcessSignalAuthority{}) {
		t.Fatalf("pending=%+v, want confirm-only without authority", entries)
	}
}

func TestAdoptRunningTasksContinuesAfterLivenessAndLoadFailures(t *testing.T) {
	first, second := adoptionID(t, "first"), adoptionID(t, "second")
	one, two := adoptionSnapshot(t, first, domain.StateRunning), adoptionSnapshot(t, second, domain.StateRunning)
	tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{one, two}, entries: map[domain.TaskID]domain.TaskSnapshot{first: one, second: two}, loadErr: map[domain.TaskID]error{first: domain.ErrTaskNotFound}}
	live := &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}
	out, err := newAdoptionUseCase(tasks, live, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionMutexFake{}).Execute(context.Background())
	if err != nil || len(out.Outcomes) != 2 || out.Outcomes[0].Outcome != "error" || out.Outcomes[1].Outcome != "resumed-monitoring" {
		t.Fatalf("out=(%+v,%v)", out, err)
	}
}

func TestAdoptRunningTasksRegistersListedSnapshotAfterTransientLoadFailure(t *testing.T) {
	for _, tc := range []struct {
		name        string
		state       domain.TaskState
		disposition PendingSendDisposition
	}{
		{"timeout", domain.StateTimeout, PendingSendUnsent},
		{"cancelling", domain.StateCancelling, PendingSendUnsent},
		{"recovering", domain.StateRecovering, PendingSendConfirmOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "load-"+tc.name)
			listed := adoptionSnapshot(t, id, tc.state)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{listed}, entries: map[domain.TaskID]domain.TaskSnapshot{id: listed}, loadErr: map[domain.TaskID]error{id: errors.New("temporary load")}}
			pending := &PendingReconciliationSet{}
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())

			out, err := uc.Execute(context.Background())
			entries := pending.List()
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != adoptionOutcomeError || len(entries) != 1 || pendingDisposition(entries[0]) != tc.disposition {
				t.Fatalf("out=(%+v,%v) pending=%+v", out, err, entries)
			}
			if tc.disposition == PendingSendUnsent && (entries[0].authority.TaskID != id || entries[0].authority.PID != *listed.PID || !entries[0].authority.ProcessStartedAt.Equal(*listed.ProcessStartedAt)) {
				t.Fatalf("authority=%+v listed=%+v", entries[0].authority, listed)
			}
		})
	}
}

func TestAdoptRunningTasksDoesNotRegisterPendingForMissingLoad(t *testing.T) {
	id := adoptionID(t, "load-not-found")
	listed := adoptionSnapshot(t, id, domain.StateTimeout)
	tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{listed}, entries: map[domain.TaskID]domain.TaskSnapshot{}, loadErr: map[domain.TaskID]error{id: domain.ErrTaskNotFound}}
	pending := &PendingReconciliationSet{}
	uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())

	out, err := uc.Execute(context.Background())
	if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != adoptionOutcomeError || len(pending.List()) != 0 {
		t.Fatalf("out=(%+v,%v) pending=%+v", out, err, pending.List())
	}
}

func TestAdoptTerminationTransientAuthorityLoadFailureReleasesSend(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state domain.TaskState
	}{
		{"timeout", domain.StateTimeout},
		{"cancelling", domain.StateCancelling},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "authority-load-"+tc.name)
			snapshot := adoptionSnapshot(t, id, tc.state)
			tasks := &adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, loadErr: map[domain.TaskID]error{id: errors.New("temporary load")}}
			pending := &PendingReconciliationSet{}
			authority, ok := processSignalAuthority(snapshot)
			if !ok {
				t.Fatal("fixture requires authority")
			}
			if err := pending.Register(id, PendingSendUnsent, &authority); err != nil {
				t.Fatal(err)
			}
			termination := &adoptionTerminationFake{terminateErr: errors.New("signal")}
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, termination, &adoptionKilledFake{}, &adoptionPathLocksFake{}, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())

			if tc.state == domain.StateTimeout {
				uc.confirmTimeoutTermination(context.Background(), id, snapshot)
			} else {
				uc.confirmCancellationTermination(context.Background(), id, snapshot)
			}
			entries := pending.List()
			if len(entries) != 1 || pendingDisposition(entries[0]) != PendingSendUnsent || entries[0].authority != authority {
				t.Fatalf("pending=%+v want unsent authority=%+v", entries, authority)
			}
			if _, outcome := pending.ClaimForSend(id, authority); outcome != ClaimAcquired {
				t.Fatalf("claim outcome=%v", outcome)
			}
		})
	}
}

func TestAdoptRunningTasksCancellationStopsBeforeNextTask(t *testing.T) {
	first, second := adoptionID(t, "cancel-first"), adoptionID(t, "cancel-second")
	one, two := adoptionSnapshot(t, first, domain.StateRunning), adoptionSnapshot(t, second, domain.StateRunning)
	tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{one, two}, entries: map[domain.TaskID]domain.TaskSnapshot{first: one, second: two}}
	ctx, cancel := context.WithCancel(context.Background())
	liveness := &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}, onExecute: func(domain.TaskID) { cancel() }}
	slots := &adoptionSlotsFake{}

	out, err := newAdoptionUseCase(tasks, liveness, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, &adoptionMutexFake{}).Execute(ctx)

	if err != nil || len(out.Outcomes) != 0 || liveness.calls != 1 || len(slots.resets) != 1 || !reflect.DeepEqual(slots.resets[0], []domain.TaskID{first, second}) || out.ElapsedMillis < 0 {
		t.Fatalf("out=(%+v,%v) liveness=%d resets=%v", out, err, liveness.calls, slots.resets)
	}
}

func TestAdoptRunningTasksShutdownAfterLoadStopsCurrentTask(t *testing.T) {
	id := adoptionID(t, "cancel-load")
	snapshot := adoptionSnapshot(t, id, domain.StateRunning)
	ctx, cancel := context.WithCancel(context.Background())
	tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, onLoad: func(domain.TaskID) { cancel() }}
	liveness := &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}
	writer := &adoptionWriterFake{}
	mutex := &adoptionMutexFake{}
	out, err := newAdoptionUseCase(tasks, liveness, &adoptionReaderFake{}, writer, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, mutex).Execute(ctx)
	if err != nil || len(out.Outcomes) != 0 || liveness.calls != 0 || tasks.saves != 0 || writer.adoptedMarkers != 0 || len(writer.events) != 0 || mutex.held {
		t.Fatalf("out=(%+v,%v) liveness=%d saves=%d markers=%d events=%d held=%v", out, err, liveness.calls, tasks.saves, writer.adoptedMarkers, len(writer.events), mutex.held)
	}
}

func TestAdoptRunningTasksUsesSingleAdoptionObservedAt(t *testing.T) {
	first, second := adoptionID(t, "observed-first"), adoptionID(t, "observed-second")
	one, two := adoptionSnapshot(t, first, domain.StateRunning), adoptionSnapshot(t, second, domain.StateRunning)
	observedAt := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
	later := observedAt.Add(time.Hour)
	clockCalls := 0
	clock := domain.ClockFunc(func() time.Time {
		clockCalls++
		if clockCalls == 1 {
			return observedAt
		}
		return later
	})
	tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{one, two}, entries: map[domain.TaskID]domain.TaskSnapshot{first: one, second: two}}
	writer := &adoptionWriterFake{}
	uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, writer, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, clock, &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.Default())
	out, err := uc.Execute(context.Background())
	if err != nil || len(out.Outcomes) != 2 || len(tasks.savedAll) != 2 || len(writer.adoptedMarkerAt) != 2 {
		t.Fatalf("out=(%+v,%v) saves=%d markers=%v", out, err, len(tasks.savedAll), writer.adoptedMarkerAt)
	}
	for _, saved := range tasks.savedAll {
		if !saved.StateUpdatedAt.Equal(observedAt) {
			t.Fatalf("saved state update=%s want %s", saved.StateUpdatedAt, observedAt)
		}
	}
	for _, at := range writer.adoptedMarkerAt {
		if !at.Equal(observedAt) {
			t.Fatalf("marker=%s want %s", at, observedAt)
		}
	}
}

func TestAdoptUnconfirmedTerminationWarns(t *testing.T) {
	id := adoptionID(t, "unconfirmed-warning")
	snapshot := adoptionSnapshot(t, id, domain.StateTimeout)
	authority, ok := processSignalAuthority(snapshot)
	if !ok {
		t.Fatal("fixture requires authority")
	}
	pending := &PendingReconciliationSet{}
	if err := pending.Register(id, PendingSendUnsent, &authority); err != nil {
		t.Fatal(err)
	}
	claim, outcome := pending.ClaimForSend(id, authority)
	if outcome != ClaimAcquired {
		t.Fatalf("claim outcome=%v", outcome)
	}
	var logs bytes.Buffer
	uc := NewAdoptRunningTasksUseCase(&adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}, &adoptionLivenessFake{}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, &adoptionMetricsFake{}, slog.New(slog.NewTextHandler(&logs, nil)))
	confirmErr := errors.New("confirm failed")
	uc.handleUnconfirmedTermination(id, claim, TerminationAttemptResult{ConfirmErr: confirmErr})
	if strings.Count(logs.String(), "adopted task termination remains unconfirmed") != 1 || !strings.Contains(logs.String(), "confirm failed") {
		t.Fatalf("logs=%q", logs.String())
	}
	entries := pending.List()
	if len(entries) != 1 || pendingDisposition(entries[0]) != PendingSendSent {
		t.Fatalf("pending=%+v", entries)
	}
}

// RED: the stalled adoption transition must leave the shared tracker after Save.
func TestAdoptRunningTasksStalledTrackerTransitions(t *testing.T) {
	for _, tc := range []struct {
		name      string
		slug      string
		state     domain.TaskState
		dead      bool
		saveErr   error
		wantCalls int
	}{
		{"stalled-save-success-live", "stl-lv", domain.StateStalled, false, nil, 1},
		{"stalled-save-success-dead", "stl-dd", domain.StateStalled, true, nil, 1},
		{"running-save-success", "run", domain.StateRunning, false, nil, 0},
		{"starting-save-success", "start", domain.StateStarting, false, nil, 0},
		{"stalled-save-failure", "stl-sv", domain.StateStalled, false, errors.New("save"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "tracker-"+tc.slug)
			snapshot := adoptionSnapshot(t, id, tc.state)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, saveErr: tc.saveErr}
			tracker := &adoptionStalledTrackerFake{}
			at := time.Date(2026, time.August, 14, 13, 0, 0, 0, time.UTC)
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: tc.dead}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(func() time.Time { return at }), tracker, &adoptionMetricsFake{}, slog.Default())
			_, _ = uc.Execute(context.Background())
			if len(tracker.calls) != tc.wantCalls {
				t.Fatalf("LeaveStalled calls=%d, want %d", len(tracker.calls), tc.wantCalls)
			}
		})
	}
}

func TestNewAdoptRunningTasksUseCaseRejectsNilStalledTimeTracker(t *testing.T) {
	for _, tracker := range []stalledTimeTracker{nil, (*metrics.StalledTimeTracker)(nil)} {
		t.Run("nil", func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			NewAdoptRunningTasksUseCase(&adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{}}, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), tracker, &adoptionMetricsFake{})
		})
	}
}

func TestNewAdoptRunningTasksUseCaseRejectsNilMetricsRecorder(t *testing.T) {
	for _, recorder := range []MetricsRecorder{nil, (*adoptionMetricsFake)(nil)} {
		t.Run("nil", func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			NewAdoptRunningTasksUseCase(&adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{}}, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, recorder)
		})
	}
}

func TestAdoptRunningTasksStalledTrackerPreSaveFailures(t *testing.T) {
	for _, tc := range []struct {
		name      string
		slug      string
		state     domain.TaskState
		liveness  error
		loadErr   error
		wantCalls int
	}{
		{"liveness-error", "live", domain.StateStalled, errors.New("liveness"), nil, 0},
		{"restore-error", "rest", domain.StateStalled, nil, nil, 0},
		{"with-task-error", "task", domain.StateStalled, nil, nil, 0},
		{"adopt-rejection-is-unreachable-after-state-routing", "adopt", domain.StateTimeout, nil, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "pre-save-"+tc.slug)
			snapshot := adoptionSnapshot(t, id, tc.state)
			clock := domain.Clock(domain.ClockFunc(time.Now))
			if tc.name == "restore-error" {
				snapshot = domain.TaskSnapshot{TaskID: id, State: domain.StateStalled}
			}
			if tc.name == "with-task-error" {
				clock = domain.ClockFunc(func() time.Time { return time.Time{} })
			}
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, loadErr: map[domain.TaskID]error{id: tc.loadErr}}
			tracker := &adoptionStalledTrackerFake{}
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{id: tc.liveness}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, clock, tracker, &adoptionMetricsFake{}, slog.Default())
			_, _ = uc.Execute(context.Background())
			if len(tracker.calls) != tc.wantCalls {
				t.Fatalf("LeaveStalled calls=%d", len(tracker.calls))
			}
		})
	}
}

func TestAdoptRunningTasksEmptyConcreteStalledTrackerIsNoop(t *testing.T) {
	id := adoptionID(t, "empty-tracker")
	snapshot := adoptionSnapshot(t, id, domain.StateStalled)
	tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
	tracker := &metrics.StalledTimeTracker{}
	at := time.Date(2026, time.August, 14, 13, 0, 0, 0, time.UTC)
	uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(func() time.Time { return at }), tracker, &adoptionMetricsFake{}, slog.Default())
	if _, err := uc.Execute(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := tracker.LeaveStalled(id, at); got != 0 {
		t.Fatalf("LeaveStalled=%d, want 0", got)
	}
}

func TestAdoptRunningTasksDelegatedStatesDoNotLeaveStalled(t *testing.T) {
	for _, state := range []domain.TaskState{domain.StateRecovering, domain.StateTimeout, domain.StateOrphaned, domain.StateCancelling} {
		t.Run(string(state), func(t *testing.T) {
			id := adoptionID(t, string(state))
			snapshot := adoptionSnapshot(t, id, state)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			tracker := &adoptionStalledTrackerFake{}
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), tracker, &adoptionMetricsFake{}, slog.Default())
			_, _ = uc.Execute(context.Background())
			if len(tracker.calls) != 0 {
				t.Fatalf("LeaveStalled calls=%d", len(tracker.calls))
			}
		})
	}
}

func TestAdoptRunningTasksStalledTrackerNeverEntersOrTakes(t *testing.T) {
	var _ stalledTimeTracker = (*adoptionStalledTrackerFake)(nil)
	id := adoptionID(t, "never-enter")
	snapshot := adoptionSnapshot(t, id, domain.StateStalled)
	tracker := &adoptionStalledTrackerFake{}
	uc := NewAdoptRunningTasksUseCase(&adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), tracker, &adoptionMetricsFake{}, slog.Default())
	_, _ = uc.Execute(context.Background())
	if len(tracker.calls) != 1 || tracker.takes != 0 {
		t.Fatalf("LeaveStalled calls=%d, TakeTotal calls=%d; want 1 and 0", len(tracker.calls), tracker.takes)
	}
}
