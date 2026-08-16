package recovery

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
}

type adoptionOrphanResumeDispatcherFake struct{ calls int }

func (f *adoptionOrphanResumeDispatcherFake) dispatch(func()) {
	f.calls++
}

func (f *adoptionStalledTrackerFake) LeaveStalled(id domain.TaskID, at time.Time) int {
	f.calls = append(f.calls, struct {
		id domain.TaskID
		at time.Time
	}{id: id, at: at})
	return 0
}

// These tests deliberately exercise the public adoption boundary.  The fakes
// keep the restart path deterministic; production wiring is covered separately.
type adoptionStoreFake struct {
	listed  []domain.TaskSnapshot
	entries map[domain.TaskID]domain.TaskSnapshot
	listErr error
	loadErr map[domain.TaskID]error
	saves   int
	saveErr error
	saved   *domain.TaskSnapshot
	order   *[]string
}

func (f *adoptionStoreFake) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	return f.listed, f.listErr
}
func (f *adoptionStoreFake) Load(id domain.TaskID) (domain.TaskSnapshot, error) {
	if err := f.loadErr[id]; err != nil {
		return domain.TaskSnapshot{}, err
	}
	return f.entries[id], nil
}
func (f *adoptionStoreFake) Save(id domain.TaskID, s domain.TaskSnapshot) error {
	f.saves++
	f.saved = &s
	if f.order != nil {
		*f.order = append(*f.order, "save")
	}
	f.entries[id] = s
	return f.saveErr
}

type adoptionLivenessFake struct {
	dead  map[domain.TaskID]bool
	err   map[domain.TaskID]error
	calls int
}

func (f *adoptionLivenessFake) Execute(_ context.Context, id domain.TaskID) (bool, error) {
	f.calls++
	return f.dead[id], f.err[id]
}

type adoptionReaderFake struct {
	present bool
	err     error
	calls   int
}

func (f *adoptionReaderFake) ReadLastMessage(domain.TaskID) (bool, error) {
	f.calls++
	return f.present, f.err
}
func (*adoptionReaderFake) ReadStderrLog(domain.TaskID) ([]byte, error) { return nil, nil }

type adoptionWriterFake struct {
	events           []domain.Event
	appendErr        error
	exitCode         *domain.ExitCode
	adoptedMarkers   int
	recoveredMarkers int
	exitCodes        int
	adoptedErr       error
	recoveredErr     error
	exitErr          error
	order            *[]string
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
func (f *adoptionWriterFake) WriteAdoptedMarker(domain.TaskID, time.Time) error {
	f.adoptedMarkers++
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
	resets   [][]domain.TaskID
	releases int
}

func (f *adoptionSlotsFake) Reset(ids []domain.TaskID) {
	f.resets = append(f.resets, append([]domain.TaskID(nil), ids...))
}
func (f *adoptionSlotsFake) ReleaseAndAdvance(context.Context, domain.TaskID, time.Time) {
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

type adoptionTerminationFake struct{ calls int }

func (f *adoptionTerminationFake) Confirm(context.Context, domain.TaskID) (bool, error) {
	f.calls++
	return false, nil
}
func (f *adoptionTerminationFake) SendAndConfirm(context.Context, domain.TaskID, int, time.Duration) (bool, error) {
	f.calls++
	return false, nil
}

type adoptionKilledFake struct{ calls int }

func (f *adoptionKilledFake) ConfirmKilled(context.Context, domain.TaskID, int, bool, time.Time) error {
	f.calls++
	return nil
}

type adoptionPathLocksFake struct{ calls int }

func (f *adoptionPathLocksFake) Release(context.Context, domain.TaskID) error { f.calls++; return nil }

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

func newAdoptionResumeUseCase(t *testing.T) *RecoverViaResumeUseCase {
	t.Helper()
	resume, _, _, _, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, nil, RecoveryResult{})
	return resume
}

func newAdoptionUseCase(tasks *adoptionStoreFake, liveness *adoptionLivenessFake, reader *adoptionReaderFake, writer *adoptionWriterFake, finalizer *adoptionFinalizerFake, resume *RecoverViaResumeUseCase, slots *adoptionSlotsFake, mutex *adoptionMutexFake) *AdoptRunningTasksUseCase {
	return NewAdoptRunningTasksUseCase(tasks, liveness, reader, writer, finalizer, resume, slots, slots, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, mutex, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, slog.Default())
}

func newOrphanAdoptionUseCase(t *testing.T, tasks *adoptionStoreFake, liveness *adoptionLivenessFake, reader *adoptionReaderFake, finalizer *adoptionFinalizerFake, dispatcher orphanResumeDispatcher, tracker stalledTimeTracker, logger *slog.Logger) *AdoptRunningTasksUseCase {
	t.Helper()
	return NewAdoptRunningTasksUseCase(tasks, liveness, reader, &adoptionWriterFake{}, finalizer, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), tracker, logger, orphanResumeDispatcherOption{dispatcher: dispatcher})
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
			if got := uc.recoverOrphan(context.Background(), id, nil, time.Now()); got != adoptionOutcomeError {
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
			if len(tracker.calls) != tc.wantLeaves || dispatcher.calls != 0 {
				t.Fatalf("leaves=%d dispatcher=%d", len(tracker.calls), dispatcher.calls)
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
		name           string
		dead           bool
		liveErr        error
		present        bool
		wantOutcome    string
		wantState      domain.TaskState
		wantPending    bool
		wantSignalSent bool
	}{
		{"live is deferred", false, nil, false, "deferred", domain.StateRecovering, true, true},
		{"dead with output completes recovery", true, nil, true, "orphan-recovered", domain.StateRecovered, false, false},
		{"dead without output fails recovery", true, nil, false, "error", domain.StateTimeoutLost, false, false},
		{"liveness failure is pending without signal", false, errors.New("liveness"), false, "error", domain.StateRecovering, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := adoptionID(t, "recovering-"+tc.name[:1])
			snapshot := adoptionSnapshot(t, id, domain.StateRecovering)
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}
			reader, writer, slots := &adoptionReaderFake{present: tc.present}, &adoptionWriterFake{}, &adoptionSlotsFake{}
			pending := &PendingReconciliationSet{}
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: tc.dead}, err: map[domain.TaskID]error{id: tc.liveErr}}, reader, writer, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, slots, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, slog.Default())
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != tc.wantOutcome || tasks.entries[id].State != tc.wantState {
				t.Fatalf("out=(%+v,%v) state=%q", out, err, tasks.entries[id].State)
			}
			entries := pending.List()
			if tc.wantPending != (len(entries) == 1) || (len(entries) == 1 && entries[0].signalSent != tc.wantSignalSent) {
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
				&PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, grace,
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

func TestAdoptRunningTasksRecoveringWriteFailuresAreFailSoft(t *testing.T) {
	for _, tc := range []struct {
		name      string
		present   bool
		configure func(*adoptionStoreFake, *adoptionWriterFake)
	}{
		{"adopted marker", true, func(_ *adoptionStoreFake, writer *adoptionWriterFake) { writer.adoptedErr = errors.New("adopted") }},
		{"recovered marker", true, func(_ *adoptionStoreFake, writer *adoptionWriterFake) { writer.recoveredErr = errors.New("recovered") }},
		{"exit code", true, func(_ *adoptionStoreFake, writer *adoptionWriterFake) { writer.exitErr = errors.New("exit") }},
		{"save", true, func(tasks *adoptionStoreFake, _ *adoptionWriterFake) { tasks.saveErr = errors.New("save") }},
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
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{present: tc.present}, writer, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), slots, slots, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, mutex, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, slog.New(slog.NewTextHandler(&logs, nil)))
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || mutex.held || slots.releases != 1 || len(logs.String()) == 0 || len(writer.events) == 0 {
				t.Fatalf("out=(%+v,%v) held=%v slots=%d logs=%q events=%d", out, err, mutex.held, slots.releases, logs.String(), len(writer.events))
			}
		})
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
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: true}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{present: tc.present}, &adoptionWriterFake{}, finalizer, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, killed, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, slog.Default())
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != tc.wantOutcome || finalizer.calls != tc.wantFinalize || killed.calls != tc.wantKilled {
				t.Fatalf("out=(%+v,%v) finalizer=%d killed=%d", out, err, finalizer.calls, killed.calls)
			}
		})
	}
}

func TestAdoptRunningTasksReconcilesTimeoutState(t *testing.T) {
	for _, tc := range []struct {
		name          string
		dead          bool
		liveErr       error
		withoutPID    bool
		wantOutcome   string
		wantPathLocks int
		wantPending   bool
		signalSent    bool
	}{
		{"dead releases impl path lock before recovery", true, nil, false, "orphan-recovery-started", 1, false, false},
		{"live without pid is pending without send", false, nil, true, "deferred", 0, true, true},
		{"liveness failure is pending without signal", false, errors.New("liveness"), false, "error", 0, true, false},
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
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: tc.dead}, err: map[domain.TaskID]error{id: tc.liveErr}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, pathLocks, pending, &adoptionMutexFake{}, domain.ClockFunc(time.Now), &adoptionStalledTrackerFake{}, slog.Default())
			out, err := uc.Execute(context.Background())
			if err != nil || len(out.Outcomes) != 1 || out.Outcomes[0].Outcome != tc.wantOutcome || pathLocks.calls != tc.wantPathLocks {
				t.Fatalf("out=(%+v,%v) pathLocks=%d", out, err, pathLocks.calls)
			}
			entries := pending.List()
			if tc.wantPending != (len(entries) == 1) || (len(entries) == 1 && entries[0].signalSent != tc.signalSent) {
				t.Fatalf("pending=%+v", entries)
			}
		})
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
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{id: tc.dead}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(func() time.Time { return at }), tracker, slog.Default())
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
			NewAdoptRunningTasksUseCase(&adoptionStoreFake{entries: map[domain.TaskID]domain.TaskSnapshot{}}, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), tracker)
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
			if tc.name == "restore-error" || tc.name == "with-task-error" {
				snapshot = domain.TaskSnapshot{TaskID: id, State: domain.StateStalled}
			}
			tasks := &adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}, loadErr: map[domain.TaskID]error{id: tc.loadErr}}
			tracker := &adoptionStalledTrackerFake{}
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{id: tc.liveness}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), tracker, slog.Default())
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
	uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(func() time.Time { return at }), tracker, slog.Default())
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
			uc := NewAdoptRunningTasksUseCase(tasks, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), tracker, slog.Default())
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
	uc := NewAdoptRunningTasksUseCase(&adoptionStoreFake{listed: []domain.TaskSnapshot{snapshot}, entries: map[domain.TaskID]domain.TaskSnapshot{id: snapshot}}, &adoptionLivenessFake{dead: map[domain.TaskID]bool{}, err: map[domain.TaskID]error{}}, &adoptionReaderFake{}, &adoptionWriterFake{}, &adoptionFinalizerFake{}, newAdoptionResumeUseCase(t), &adoptionSlotsFake{}, &adoptionSlotsFake{}, &adoptionTerminationFake{}, &adoptionKilledFake{}, &adoptionPathLocksFake{}, &PendingReconciliationSet{}, &adoptionMutexFake{}, domain.ClockFunc(time.Now), tracker, slog.Default())
	_, _ = uc.Execute(context.Background())
	if len(tracker.calls) != 1 {
		t.Fatalf("LeaveStalled calls=%d, want 1", len(tracker.calls))
	}
}
