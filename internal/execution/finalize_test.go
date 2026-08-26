package execution

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

// finalizeTrace is shared by the fakes so that concurrent tests observe one
// mutex-protected order rather than duplicating unsafe call bookkeeping.
type finalizeTrace struct {
	mu    sync.Mutex
	calls []string
}

func (t *finalizeTrace) add(call string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls = append(t.calls, call)
}
func (t *finalizeTrace) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.calls...)
}

type loadResult struct {
	snapshot domain.TaskSnapshot
	err      error
}
type finalizeStoreFake struct {
	mu        sync.Mutex
	trace     *finalizeTrace
	loads     []loadResult
	loadIndex int
	latest    domain.TaskSnapshot
	saveErrs  []error
	saved     []domain.TaskSnapshot
	saveCalls int
}

func (f *finalizeStoreFake) Load(domain.TaskID) (domain.TaskSnapshot, error) {
	f.trace.add("load")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadIndex < len(f.loads) {
		r := f.loads[f.loadIndex]
		f.loadIndex++
		return r.snapshot, r.err
	}
	return f.latest, nil
}
func (f *finalizeStoreFake) Save(_ domain.TaskID, s domain.TaskSnapshot) error {
	f.trace.add("save")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveCalls++
	var err error
	if len(f.saveErrs) > 0 {
		err, f.saveErrs = f.saveErrs[0], f.saveErrs[1:]
	}
	if err == nil {
		f.saved = append(f.saved, s)
		f.latest = s
	}
	return err
}
func (f *finalizeStoreFake) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	return nil, nil
}
func (f *finalizeStoreFake) Reserve(domain.TaskID) error            { return nil }
func (f *finalizeStoreFake) Release(domain.TaskID) error            { return nil }
func (f *finalizeStoreFake) IsReserved(domain.TaskID) (bool, error) { return true, nil }
func (f *finalizeStoreFake) counts() (int, []domain.TaskSnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saveCalls, append([]domain.TaskSnapshot(nil), f.saved...)
}

type finalizeReaderFake struct {
	mu         sync.Mutex
	trace      *finalizeTrace
	present    bool
	presentErr error
	lastCalls  int
	exits      []struct {
		code   int
		exists bool
		err    error
	}
	exitIndex int
}

func (f *finalizeReaderFake) ReadStderrLog(domain.TaskID) ([]byte, error) { return nil, nil }
func (f *finalizeReaderFake) ReadLastMessage(domain.TaskID) (bool, error) {
	f.trace.add("last-message")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastCalls++
	return f.present, f.presentErr
}
func (f *finalizeReaderFake) ReadPromptContent(domain.TaskID) ([]byte, error)      { return nil, nil }
func (f *finalizeReaderFake) ReadLastMessageContent(domain.TaskID) ([]byte, error) { return nil, nil }
func (f *finalizeReaderFake) ReadPartialOutputContent(domain.TaskID) ([]byte, error) {
	return nil, nil
}
func (f *finalizeReaderFake) ReadExitCode(domain.TaskID) (int, bool, error) {
	f.trace.add("read-exit-code")
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.exitIndex < len(f.exits) {
		r := f.exits[f.exitIndex]
		f.exitIndex++
		return r.code, r.exists, r.err
	}
	return 0, false, nil
}

type finalizeWriterFake struct {
	contract.ContractWriter
	mu         sync.Mutex
	trace      *finalizeTrace
	writeErrs  []error
	appendErrs []error
	writes     []domain.ExitCode
	events     []domain.Event
}

func (f *finalizeWriterFake) WriteExitCode(_ domain.TaskID, code domain.ExitCode) error {
	f.trace.add("write-exit-code")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writes = append(f.writes, code)
	if len(f.writeErrs) == 0 {
		return nil
	}
	err := f.writeErrs[0]
	f.writeErrs = f.writeErrs[1:]
	return err
}
func (f *finalizeWriterFake) AppendEvent(_ domain.TaskID, event domain.Event) error {
	f.trace.add("append-" + event.Type())
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
	if len(f.appendErrs) == 0 {
		return nil
	}
	err := f.appendErrs[0]
	f.appendErrs = f.appendErrs[1:]
	return err
}
func (f *finalizeWriterFake) snapshot() ([]domain.ExitCode, []domain.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.ExitCode(nil), f.writes...), append([]domain.Event(nil), f.events...)
}

type finalizeSlotFake struct {
	mu    sync.Mutex
	trace *finalizeTrace
	calls int
	id    domain.TaskID
	now   time.Time
}

func (f *finalizeSlotFake) ReleaseAndAdvance(_ context.Context, id domain.TaskID, now time.Time) {
	f.trace.add("release-slot")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.id, f.now = id, now
}
func (f *finalizeSlotFake) snapshot() (int, domain.TaskID, time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.id, f.now
}

var _ recovery.SlotReleaser = (*finalizeSlotFake)(nil)

type finalizeTimeoutFake struct {
	mu     sync.Mutex
	trace  *finalizeTrace
	taskMu *store.TaskMutex
	calls  int
	id     domain.TaskID
}

func (f *finalizeTimeoutFake) Disarm(id domain.TaskID) {
	f.trace.add("disarm")
	if f.taskMu != nil {
		f.taskMu.Lock(id)
		f.taskMu.Unlock(id)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.id = id
}

func (f *finalizeTimeoutFake) snapshot() (int, domain.TaskID) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls, f.id
}

var _ TimeoutDisarmer = (*finalizeTimeoutFake)(nil)

type finalizeStalledTrackerFake struct {
	mu         sync.Mutex
	trace      *finalizeTrace
	leaveCalls int
	takeCalls  int
	total      int
}

func (f *finalizeStalledTrackerFake) LeaveStalled(domain.TaskID, time.Time) int {
	f.trace.add("leave-stalled")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leaveCalls++
	return f.total
}

func (f *finalizeStalledTrackerFake) TakeTotal(domain.TaskID) int {
	f.trace.add("take-total")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.takeCalls++
	return f.total
}

func (f *finalizeStalledTrackerFake) snapshot() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leaveCalls, f.takeCalls
}

type finalizeMetricsFake struct {
	mu     sync.Mutex
	trace  *finalizeTrace
	inputs []metrics.RecordTaskMetricsInput
	output metrics.RecordTaskMetricsOutput
}

type finalizePathLockStore struct {
	mu        sync.Mutex
	trace     *finalizeTrace
	deleted   []domain.TaskID
	deleteErr error
}

func (s *finalizePathLockStore) List() ([]PathLockSnapshot, error)                 { return nil, nil }
func (s *finalizePathLockStore) Save(domain.TaskID, []domain.NormalizedPath) error { return nil }
func (s *finalizePathLockStore) Delete(taskID domain.TaskID) error {
	s.trace.add("release-path-lock")
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleted = append(s.deleted, taskID)
	return s.deleteErr
}
func (s *finalizePathLockStore) deletedSnapshot() []domain.TaskID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]domain.TaskID(nil), s.deleted...)
}

func (f *finalizeMetricsFake) Execute(_ context.Context, in metrics.RecordTaskMetricsInput) metrics.RecordTaskMetricsOutput {
	f.trace.add("metrics")
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inputs = append(f.inputs, in)
	return f.output
}

func (f *finalizeMetricsFake) snapshot() []metrics.RecordTaskMetricsInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]metrics.RecordTaskMetricsInput(nil), f.inputs...)
}

func finalizeID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260809-120000-abcd-finalize")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func finalizeSnapshot(t *testing.T, state domain.TaskState) domain.TaskSnapshot {
	t.Helper()
	id := finalizeID(t)
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	pid := 1
	return domain.TaskSnapshot{TaskID: id, Subcommand: domain.SubcommandImpl, PID: &pid, ProcessStartedAt: &now, ResolvedTimeoutSeconds: 1800, Model: "gpt-5", RequestedAt: now, Route: domain.ExecutionRouteDaemon, State: state, StateUpdatedAt: now, SchemaVersion: 1}
}
func finalizeFixtures(t *testing.T, state domain.TaskState, now time.Time) (*finalizeStoreFake, *finalizeReaderFake, *finalizeWriterFake, *finalizeSlotFake, *finalizeTimeoutFake, *FinalizeTaskUseCase) {
	t.Helper()
	trace := &finalizeTrace{}
	s := &finalizeStoreFake{trace: trace, latest: finalizeSnapshot(t, state)}
	r := &finalizeReaderFake{trace: trace, present: true}
	w := &finalizeWriterFake{trace: trace}
	slot := &finalizeSlotFake{trace: trace}
	taskMu := store.NewTaskMutex()
	timeout := &finalizeTimeoutFake{trace: trace, taskMu: taskMu}
	tracker := &finalizeStalledTrackerFake{trace: trace}
	recorder := &finalizeMetricsFake{trace: trace}
	pathLocks := &finalizePathLockStore{trace: trace}
	return s, r, w, slot, timeout, NewFinalizeTaskUseCase(s, w, r, domain.ClockFunc(func() time.Time { return now }), taskMu, slot, timeout, NewReleasePathLockUseCase(pathLocks), recorder, tracker)
}
func finalizeInput(id domain.TaskID, at time.Time) FinalizeTaskInput {
	return FinalizeTaskInput{TaskID: id, RawExitCode: 0, OccurredAt: at}
}
func requireEvents(t *testing.T, events []domain.Event, state domain.TaskState, at time.Time) {
	t.Helper()
	if len(events) != 2 {
		t.Fatalf("events=%#v", events)
	}
	exited, ok := events[0].(domain.TaskExited)
	if !ok {
		t.Fatalf("first=%T", events[0])
	}
	if !exited.OccurredAt.Equal(at) {
		t.Fatalf("exited at=%v", exited.OccurredAt)
	}
	if state == domain.StateCompleted {
		if _, ok := events[1].(domain.TaskCompleted); !ok {
			t.Fatalf("second=%T", events[1])
		}
	} else if failed, ok := events[1].(domain.TaskFailed); !ok || failed.Reason == "" {
		t.Fatalf("second=%#v", events[1])
	}
}

func TestFinalizeTaskUseCasePrepareReadsLastMessageOutsideTaskMutexExactlyOnce(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"last-message-present", nil},
		{"last-message-absent", nil},
		{"read-error", errors.New("read")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, _, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
			r.presentErr = tc.err
			_, err := uc.Prepare(context.Background(), finalizeInput(s.latest.TaskID, now))
			if !errors.Is(err, tc.err) || r.lastCalls != 1 {
				t.Fatalf("err=%v reads=%d", err, r.lastCalls)
			}
			if saves, _ := s.counts(); saves != 0 {
				t.Fatalf("saves=%d", saves)
			}
			if calls, _ := timeout.snapshot(); calls != 0 {
				t.Fatalf("disarms=%d", calls)
			}
			if calls, _, _ := slot.snapshot(); calls != 0 {
				t.Fatalf("releases=%d", calls)
			}
		})
	}
}

func TestFinalizeTaskUseCaseExecuteLockedUsesPreparedResultWithoutSecondRead(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		present bool
		want    domain.TaskState
	}{
		{"prepared-present-completes", true, domain.StateCompleted},
		{"prepared-absent-fails-no-output", false, domain.StateFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, _, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
			r.present = tc.present
			prepared, err := uc.Prepare(context.Background(), finalizeInput(s.latest.TaskID, now))
			if err != nil {
				t.Fatal(err)
			}
			uc.taskMu.Lock(s.latest.TaskID)
			result, err := uc.ExecuteLocked(context.Background(), prepared)
			uc.taskMu.Unlock(s.latest.TaskID)
			if err != nil || result.Output.ResultState != tc.want || r.lastCalls != 1 {
				t.Fatalf("result=%+v err=%v reads=%d", result, err, r.lastCalls)
			}
			if calls, _ := timeout.snapshot(); calls != 0 {
				t.Fatalf("disarms=%d", calls)
			}
			if calls, _, _ := slot.snapshot(); calls != 0 {
				t.Fatalf("releases=%d", calls)
			}
		})
	}
}

func TestFinalizeTaskUseCaseRecordExitedReleaseContractAfterTwoPhaseAPI(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		configure func(*finalizeStoreFake)
		wantExit  bool
	}{
		{"initial-save-success", func(*finalizeStoreFake) {}, true},
		{"initial-save-fails-retry-save-succeeds", func(s *finalizeStoreFake) { s.saveErrs = []error{errors.New("initial")} }, true},
		{"initial-and-retry-save-fail", func(s *finalizeStoreFake) { s.saveErrs = []error{errors.New("initial"), errors.New("retry")} }, true},
		{"load-fails-before-record-exit", func(s *finalizeStoreFake) { s.loads = []loadResult{{err: errors.New("load")}} }, false},
		{"record-exit-rejected", func(s *finalizeStoreFake) { s.latest.State = domain.StateCompleted }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
			tc.configure(s)
			prepared, err := uc.Prepare(context.Background(), finalizeInput(s.latest.TaskID, now))
			if err != nil {
				t.Fatal(err)
			}
			uc.taskMu.Lock(s.latest.TaskID)
			result, _ := uc.ExecuteLocked(context.Background(), prepared)
			uc.taskMu.Unlock(s.latest.TaskID)
			if result.RecordExited != tc.wantExit {
				t.Fatalf("recordExited=%t", result.RecordExited)
			}
			if result.RecordExited {
				uc.ReleaseAfterFinalization(context.Background(), result, s.latest.TaskID)
			}
			want := 0
			if tc.wantExit {
				want = 1
			}
			if calls, _ := timeout.snapshot(); calls != want {
				t.Fatalf("disarms=%d", calls)
			}
			if calls, _, _ := slot.snapshot(); calls != want {
				t.Fatalf("releases=%d", calls)
			}
		})
	}
}

func TestFinalizeTaskUseCasePublicEntryPointsUsePreparedLockedReleasePath(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	for _, tc := range []struct{ name string }{{"Execute"}, {"Finalize-adapter"}} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, _, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
			var err error
			if tc.name == "Execute" {
				_, err = uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
			} else {
				err = uc.Finalize(s.latest.TaskID, 0, true, true, now)
			}
			if err != nil || r.lastCalls != 1 {
				t.Fatalf("err=%v reads=%d", err, r.lastCalls)
			}
			if calls, _ := timeout.snapshot(); calls != 1 {
				t.Fatalf("disarms=%d", calls)
			}
			if calls, _, _ := slot.snapshot(); calls != 1 {
				t.Fatalf("releases=%d", calls)
			}
		})
	}
}

func TestFinalizeTaskUseCaseScenarios(t *testing.T) {
	now, occurred := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC), time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		raw     int
		present bool
		want    domain.TaskState
		reason  string
	}{
		{"SCN-exec-05-01", 0, true, domain.StateCompleted, ""}, {"SCN-exec-05-02", 0, false, domain.StateFailed, domain.ReasonNoOutput}, {"SCN-exec-05-03", 1, true, domain.StateFailed, domain.ReasonAbnormalExit}, {"SCN-exec-05-07", 0, false, domain.StateFailed, domain.ReasonNoOutput}, {"SCN-exec-05-08", 999, true, domain.StateFailed, domain.ReasonAbnormalExit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, w, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
			r.present = tc.present
			out, err := uc.Execute(context.Background(), FinalizeTaskInput{TaskID: s.latest.TaskID, RawExitCode: tc.raw, OccurredAt: occurred})
			if err != nil || out.ResultState != tc.want {
				t.Fatalf("out=%+v err=%v", out, err)
			}
			requireEvents(t, out.Events, tc.want, occurred)
			writes, _ := w.snapshot()
			if len(writes) != 1 || writes[0].Raw() != tc.raw {
				t.Fatalf("writes=%v", writes)
			}
			saves, saved := s.counts()
			if saves != 1 || saved[0].State != tc.want || saved[0].ExitCode == nil || saved[0].ExitCode.Raw() != tc.raw {
				t.Fatalf("saved=%#v", saved)
			}
			calls, _, slotAt := slot.snapshot()
			if calls != 1 || !slotAt.Equal(now) {
				t.Fatalf("slot=%d at=%v", calls, slotAt)
			}
			disarms, disarmedID := timeout.snapshot()
			if disarms != 1 || disarmedID != s.latest.TaskID {
				t.Fatalf("disarms=%d id=%v", disarms, disarmedID)
			}
			callsTrace := w.trace.snapshot()
			if len(callsTrace) < 3 || callsTrace[len(callsTrace)-3] != "disarm" || callsTrace[len(callsTrace)-2] != "release-path-lock" || callsTrace[len(callsTrace)-1] != "release-slot" {
				t.Fatalf("post-processing order=%v", callsTrace)
			}
			if tc.reason != "" {
				failed, ok := out.Events[1].(domain.TaskFailed)
				if !ok || failed.Reason != tc.reason {
					t.Fatalf("failed=%#v", out.Events[1])
				}
			}
		})
	}
}

// These three tests are the intentional Red-to-Green regression tests: before
// the output-retention fix each returned a zero ResultState and nil Events.
func TestFinalizeTaskUseCaseFailsafePreparationRetainsInitialResult(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		configure func(*finalizeStoreFake)
	}{
		{"04d-Load", func(s *finalizeStoreFake) { s.loads = []loadResult{{snapshot: s.latest}, {err: errors.New("reload")}} }},
		{"04d-Restore", func(s *finalizeStoreFake) {
			bad := s.latest
			bad.State = "invalid"
			s.loads = []loadResult{{snapshot: s.latest}, {snapshot: bad}}
		}},
		{"04d-RecordExit", func(s *finalizeStoreFake) {
			terminal := s.latest
			terminal.State = domain.StateCompleted
			code := domain.NewExitCode(0)
			terminal.ExitCode = &code
			s.loads = []loadResult{{snapshot: s.latest}, {snapshot: terminal}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, w, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
			tc.configure(s)
			initialWrite := &os.PathError{Op: "write", Path: "/private/contract/initial", Err: errors.New("initial write")}
			w.writeErrs = []error{initialWrite}
			out, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
			if err == nil || out.ResultState != domain.StateCompleted || out.Events == nil {
				t.Fatalf("out=%+v err=%v", out, err)
			}
			requireEvents(t, out.Events, domain.StateCompleted, now)
			var initialPathErr *os.PathError
			if !errors.Is(out.ContractWriteError, domain.ErrContractWriteFailed) || !errors.Is(out.ContractWriteError, initialWrite) || !errors.As(out.ContractWriteError, &initialPathErr) || initialPathErr != initialWrite {
				t.Fatalf("contract err=%v", out.ContractWriteError)
			}
			calls, _, _ := slot.snapshot()
			if calls != 1 {
				t.Fatalf("slot=%d", calls)
			}
			if disarms, _ := timeout.snapshot(); disarms != 1 {
				t.Fatalf("disarms=%d", disarms)
			}
		})
	}
}

func TestFinalizeTaskUseCaseFailsafeErrorsRetainInitialAndSecondaryCauses(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		configure func(*finalizeStoreFake, *finalizeReaderFake, *finalizeWriterFake) error
		stage     string
		restore   bool
	}{
		{
			name: "reload",
			configure: func(s *finalizeStoreFake, _ *finalizeReaderFake, _ *finalizeWriterFake) error {
				secondary := &os.LinkError{Op: "read", Old: "/private/contract/reload", New: "/private/contract/reload", Err: errors.New("reload")}
				s.loads = []loadResult{{snapshot: s.latest}, {err: secondary}}
				return secondary
			},
			stage: "reload for failsafe",
		},
		{
			name: "restore",
			configure: func(s *finalizeStoreFake, _ *finalizeReaderFake, _ *finalizeWriterFake) error {
				bad := s.latest
				bad.State = "invalid"
				_, restoreErr := bad.Restore()
				s.loads = []loadResult{{snapshot: s.latest}, {snapshot: bad}}
				return restoreErr
			},
			stage:   "restore for failsafe",
			restore: true,
		},
		{
			name: "record-exit",
			configure: func(s *finalizeStoreFake, _ *finalizeReaderFake, _ *finalizeWriterFake) error {
				terminal := s.latest
				terminal.State = domain.StateCompleted
				code := domain.NewExitCode(0)
				terminal.ExitCode = &code
				s.loads = []loadResult{{snapshot: s.latest}, {snapshot: terminal}}
				return domain.ErrInvalidStateTransition
			},
			stage: "record exit for failsafe",
		},
		{
			name: "retry-non-retryable",
			configure: func(_ *finalizeStoreFake, r *finalizeReaderFake, _ *finalizeWriterFake) error {
				secondary := &os.LinkError{Op: "read", Old: "/private/contract/retry-read", New: "/private/contract/retry-read", Err: errors.New("retry read")}
				r.exits = []struct {
					code   int
					exists bool
					err    error
				}{{}, {err: secondary}}
				return secondary
			},
		},
		{
			name: "retry-write-failure",
			configure: func(_ *finalizeStoreFake, _ *finalizeReaderFake, w *finalizeWriterFake) error {
				secondary := &os.LinkError{Op: "write", Old: "/private/contract/retry", New: "/private/contract/retry", Err: errors.New("retry write")}
				w.writeErrs = append(w.writeErrs, secondary)
				return secondary
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, w, _, _, uc := finalizeFixtures(t, domain.StateRunning, now)
			initial := &os.PathError{Op: "write", Path: "/private/contract/initial", Err: errors.New("initial write")}
			w.writeErrs = []error{initial}
			secondary := tc.configure(s, r, w)
			out, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
			var initialPathErr *os.PathError
			if err == nil || !errors.Is(err, domain.ErrContractWriteFailed) || !errors.Is(err, initial) || !errors.As(err, &initialPathErr) || initialPathErr != initial || !tc.restore && !errors.Is(err, secondary) {
				t.Fatalf("out=%+v err=%v initial=%#v secondary=%#v", out, err, initial, secondary)
			}
			if tc.stage != "" && !strings.Contains(err.Error(), tc.stage) {
				t.Fatalf("err=%v, missing stage %q", err, tc.stage)
			}
			if wantLinkErr, ok := secondary.(*os.LinkError); ok {
				var linkErr *os.LinkError
				if !errors.As(err, &linkErr) || linkErr != wantLinkErr {
					t.Fatalf("link error=%#v want=%#v", linkErr, wantLinkErr)
				}
			}
			if tc.restore {
				if !containsErrorShape(err, secondary) {
					t.Fatalf("restore error tree=%v want type=%T message=%q", err, secondary, secondary.Error())
				}
			}
		})
	}
}

func containsErrorShape(err, want error) bool {
	if err == nil {
		return false
	}
	if reflect.TypeOf(err) == reflect.TypeOf(want) && err.Error() == want.Error() {
		return true
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range joined.Unwrap() {
			if containsErrorShape(child, want) {
				return true
			}
		}
		return false
	}
	return containsErrorShape(errors.Unwrap(err), want)
}

func TestFinalizeTaskUseCaseWriteFailuresAndContractRules(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	t.Run("04a retry write succeeds", func(t *testing.T) {
		s, _, w, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
		w.writeErrs = []error{errors.New("first")}
		out, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
		writes, _ := w.snapshot()
		calls, _, _ := slot.snapshot()
		if disarms, _ := timeout.snapshot(); err != nil || len(writes) != 2 || calls != 1 || disarms != 1 || !errors.Is(out.ContractWriteError, domain.ErrContractWriteFailed) {
			t.Fatalf("out=%+v err=%v writes=%d", out, err, len(writes))
		}
	})
	t.Run("04b retry save succeeds", func(t *testing.T) {
		s, _, _, _, _, uc := finalizeFixtures(t, domain.StateRunning, now)
		s.saveErrs = []error{errors.New("first")}
		out, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
		saves, _ := s.counts()
		if err != nil || saves != 2 || !errors.Is(out.ContractWriteError, domain.ErrContractWriteFailed) {
			t.Fatalf("out=%+v err=%v saves=%d", out, err, saves)
		}
	})
	t.Run("04c retry failure", func(t *testing.T) {
		s, _, w, _, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
		w.writeErrs = []error{errors.New("first"), errors.New("retry")}
		out, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
		if disarms, _ := timeout.snapshot(); err == nil || disarms != 1 || out.ResultState == "" || len(out.Events) != 2 || !errors.Is(out.ContractWriteError, domain.ErrContractWriteFailed) {
			t.Fatalf("out=%+v err=%v", out, err)
		}
	})
	t.Run("04g same exit code continues", func(t *testing.T) {
		s, r, w, _, _, uc := finalizeFixtures(t, domain.StateRunning, now)
		r.exits = []struct {
			code   int
			exists bool
			err    error
		}{{code: 0, exists: true}}
		if _, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now)); err != nil {
			t.Fatal(err)
		}
		writes, _ := w.snapshot()
		saves, _ := s.counts()
		if len(writes) != 0 || saves != 1 {
			t.Fatalf("writes=%d saves=%d", len(writes), saves)
		}
	})
	t.Run("04g mismatch fails closed", func(t *testing.T) {
		s, r, w, _, _, uc := finalizeFixtures(t, domain.StateRunning, now)
		r.exits = []struct {
			code   int
			exists bool
			err    error
		}{{code: 1, exists: true}}
		out, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
		writes, events := w.snapshot()
		saves, _ := s.counts()
		if err == nil || out.ResultState != domain.StateCompleted || len(out.Events) != 2 || len(writes) != 0 || len(events) != 0 || saves != 0 {
			t.Fatalf("out=%+v err=%v", out, err)
		}
	})
	t.Run("04g validation does not retry", func(t *testing.T) {
		s, _, w, slot, _, uc := finalizeFixtures(t, domain.StateRunning, now)
		out, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, time.Time{}))
		writes, events := w.snapshot()
		saves, _ := s.counts()
		calls, _, _ := slot.snapshot()
		if err == nil || out.ResultState != domain.StateCompleted || len(out.Events) != 2 || len(writes) != 1 || len(events) != 0 || saves != 0 || calls != 1 {
			t.Fatalf("out=%+v err=%v", out, err)
		}
	})
}

func TestFinalizeTaskUseCaseAdditionalScenarios(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	t.Run("SCN-exec-05-05 terminal is rejected", func(t *testing.T) {
		s, _, w, slot, timeout, uc := finalizeFixtures(t, domain.StateCompleted, now)
		_, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
		writes, events := w.snapshot()
		saves, _ := s.counts()
		calls, _, _ := slot.snapshot()
		disarms, _ := timeout.snapshot()
		if !errors.Is(err, domain.ErrInvalidStateTransition) || saves != 0 || len(writes) != 0 || len(events) != 0 || calls != 0 || disarms != 0 {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("SCN-exec-05-06 orphan finalization", func(t *testing.T) {
		s, _, _, _, _, uc := finalizeFixtures(t, domain.StateOrphaned, now)
		s.latest.AdoptedAfterRestart = true
		out, err := uc.Execute(context.Background(), FinalizeTaskInput{TaskID: s.latest.TaskID, RawExitCode: 0, Estimated: true, AdoptedAfterRestart: true, OccurredAt: now})
		exited := out.Events[0].(domain.TaskExited)
		if err != nil || out.ResultState != domain.StateCompleted || !exited.Estimated || !exited.AdoptedAfterRestart {
			t.Fatalf("out=%+v err=%v", out, err)
		}
	})
	t.Run("SCN-exec-05-09 not found", func(t *testing.T) {
		s, _, w, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
		s.loads = []loadResult{{err: domain.ErrTaskNotFound}}
		_, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
		writes, events := w.snapshot()
		saves, _ := s.counts()
		calls, _, _ := slot.snapshot()
		disarms, _ := timeout.snapshot()
		if !errors.Is(err, domain.ErrTaskNotFound) || saves != 0 || len(writes) != 0 || len(events) != 0 || calls != 0 || disarms != 0 {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("events append is fail soft", func(t *testing.T) {
		s, _, w, slot, _, uc := finalizeFixtures(t, domain.StateRunning, now)
		w.appendErrs = []error{errors.New("append")}
		out, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
		_, events := w.snapshot()
		calls, _, _ := slot.snapshot()
		if err != nil || out.ContractWriteError != nil || len(events) != 2 || calls != 1 {
			t.Fatalf("out=%+v err=%v", out, err)
		}
	})
}

func TestFinalizeTaskUseCaseConcurrentDuplicateExecute(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	s, _, w, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
	id := s.latest.TaskID
	input := finalizeInput(id, now)
	start := make(chan struct{})
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { <-start; _, err := uc.Execute(context.Background(), input); results <- err }()
	}
	close(start)
	var successful, rejected int
	for i := 0; i < 2; i++ {
		select {
		case err := <-results:
			if err == nil {
				successful++
			} else if errors.Is(err, domain.ErrInvalidStateTransition) {
				rejected++
			} else {
				t.Fatalf("err=%v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("concurrent finalize timed out")
		}
	}
	writes, events := w.snapshot()
	saves, saved := s.counts()
	calls, _, _ := slot.snapshot()
	disarms, _ := timeout.snapshot()
	if successful != 1 || rejected != 1 || saves != 1 || len(writes) != 1 || len(events) != 2 || calls != 1 || disarms != 1 || len(saved) != 1 || saved[0].State != domain.StateCompleted {
		t.Fatalf("success=%d rejected=%d saves=%d writes=%d events=%d slots=%d", successful, rejected, saves, len(writes), len(events), calls)
	}
}

func TestFinalizeTaskUseCaseUnlocksBeforeDisarm(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	s, _, _, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
	result := make(chan error, 1)
	go func() {
		_, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("finalize did not unlock task mutex before disarm")
	}
	disarms, disarmedID := timeout.snapshot()
	calls, _, _ := slot.snapshot()
	if disarms != 1 || disarmedID != s.latest.TaskID || calls != 1 {
		t.Fatalf("disarms=%d id=%v slots=%d", disarms, disarmedID, calls)
	}
	callsTrace := timeout.trace.snapshot()
	if len(callsTrace) < 3 || callsTrace[len(callsTrace)-3] != "disarm" || callsTrace[len(callsTrace)-2] != "release-path-lock" || callsTrace[len(callsTrace)-1] != "release-slot" {
		t.Fatalf("post-processing order=%v", callsTrace)
	}
}

func TestNewFinalizeTaskUseCaseContract(t *testing.T) {
	now := time.Now()
	s, r, w, slot, timeout, _ := finalizeFixtures(t, domain.StateRunning, now)
	clock := domain.ClockFunc(func() time.Time { return now })
	mu := store.NewTaskMutex()
	tracker := &finalizeStalledTrackerFake{trace: &finalizeTrace{}}
	recorder := &finalizeMetricsFake{trace: &finalizeTrace{}}
	pathLocks := NewReleasePathLockUseCase(&finalizePathLockStore{trace: &finalizeTrace{}})
	mustPanic := func(t *testing.T, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatal("did not panic")
			}
		}()
		f()
	}
	for _, tc := range []struct {
		name string
		f    func()
	}{
		{"tasks", func() { NewFinalizeTaskUseCase(nil, w, r, clock, mu, slot, timeout, pathLocks, recorder, tracker) }}, {"writer", func() { NewFinalizeTaskUseCase(s, nil, r, clock, mu, slot, timeout, pathLocks, recorder, tracker) }}, {"reader", func() { NewFinalizeTaskUseCase(s, w, nil, clock, mu, slot, timeout, pathLocks, recorder, tracker) }}, {"clock", func() { NewFinalizeTaskUseCase(s, w, r, nil, mu, slot, timeout, pathLocks, recorder, tracker) }}, {"mutex", func() { NewFinalizeTaskUseCase(s, w, r, clock, nil, slot, timeout, pathLocks, recorder, tracker) }}, {"slot", func() { NewFinalizeTaskUseCase(s, w, r, clock, mu, nil, timeout, pathLocks, recorder, tracker) }}, {"timeout", func() { NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, nil, pathLocks, recorder, tracker) }}, {"path lock nil", func() { NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, nil, recorder, tracker) }}, {"path lock typed nil", func() {
			NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, (*ReleasePathLockUseCase)(nil), recorder, tracker)
		}}, {"metrics nil", func() { NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, pathLocks, nil, tracker) }}, {"metrics typed nil", func() {
			NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, pathLocks, (*finalizeMetricsFake)(nil), tracker)
		}}, {"tracker nil", func() { NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, pathLocks, recorder, nil) }}, {"tracker typed nil", func() {
			NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, pathLocks, recorder, (*finalizeStalledTrackerFake)(nil))
		}}, {"many loggers", func() {
			NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, pathLocks, recorder, tracker, slog.Default(), slog.Default())
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { mustPanic(t, tc.f) })
	}
	if got := NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, pathLocks, recorder, tracker); got.logger != slog.Default() || got.timeoutDisarmer != timeout || got.pathLockReleaser != pathLocks || got.metricsRecorder != recorder || got.stalledTracker != tracker {
		t.Fatal("default logger not used")
	}
	if got := NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, pathLocks, recorder, tracker, nil); got.logger != slog.Default() {
		t.Fatal("nil logger not default")
	}
	logger := slog.New(slog.NewTextHandler(testingWriter{t}, nil))
	if got := NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, pathLocks, recorder, tracker, logger); got.logger != logger {
		t.Fatal("provided logger not retained")
	}
}

type testingWriter struct{ t *testing.T }

func (w testingWriter) Write(p []byte) (int, error) { return len(p), nil }

type capturedLog struct {
	level slog.Level
	msg   string
	attrs map[string]any
}

type logCapture struct {
	mu   sync.Mutex
	logs []capturedLog
}

func (h *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (h *logCapture) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]any{}
	r.Attrs(func(a slog.Attr) bool { attrs[a.Key] = a.Value.Any(); return true })
	h.mu.Lock()
	defer h.mu.Unlock()
	h.logs = append(h.logs, capturedLog{level: r.Level, msg: r.Message, attrs: attrs})
	return nil
}
func (h *logCapture) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *logCapture) WithGroup(string) slog.Handler      { return h }
func (h *logCapture) snapshot() []capturedLog {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]capturedLog(nil), h.logs...)
}

func TestFinalizeTaskUseCaseStructuredContractWriteLogs(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	s, _, w, _, _, uc := finalizeFixtures(t, domain.StateRunning, now)
	initial, retry := errors.New("initial"), errors.New("retry")
	w.writeErrs = []error{initial, retry}
	capture := &logCapture{}
	uc.logger = slog.New(capture)
	_, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
	if err == nil {
		t.Fatal("retry failure did not return an error")
	}
	logs := capture.snapshot()
	if len(logs) != 2 {
		t.Fatalf("logs=%#v", logs)
	}
	for i, want := range []struct {
		attempt string
		cause   error
	}{{"initial", initial}, {"retry", retry}} {
		got := logs[i]
		if got.level != slog.LevelError || got.attrs["code"] != machineCodeContractWriteFailed || got.attrs["stage"] != "exit-code" || got.attrs["attempt"] != want.attempt || got.attrs["task_id"] != s.latest.TaskID.String() || fmt.Sprint(got.attrs["error"]) != want.cause.Error() {
			t.Fatalf("log[%d]=%#v", i, got)
		}
	}
}

func TestFinalizeTaskUseCaseTerminalPersistenceFinalizesStalledMetrics(t *testing.T) {
	now := time.Date(2026, 8, 17, 12, 1, 0, 0, time.UTC)
	for _, tc := range []struct {
		name           string
		state          domain.TaskState
		rawExitCode    int
		present        bool
		configure      func(*finalizeStoreFake, *finalizeWriterFake)
		wantPersisted  bool
		wantLeave      int
		wantTake       int
		wantMetrics    int
		wantFinalState domain.TaskState
		wantEstimated  bool
	}{
		{
			name:  "completed saved from stalled records corrected estimated event",
			state: domain.StateStalled, present: true, wantPersisted: true, wantLeave: 1, wantTake: 1, wantMetrics: 1, wantFinalState: domain.StateCompleted, wantEstimated: true,
		},
		{
			name:  "failed retry save uses retry stalled snapshot",
			state: domain.StateRunning, rawExitCode: 1, present: true, wantPersisted: true, wantLeave: 1, wantTake: 1, wantMetrics: 1, wantFinalState: domain.StateFailed,
			configure: func(s *finalizeStoreFake, _ *finalizeWriterFake) {
				retry := s.latest
				retry.State = domain.StateStalled
				s.loads = []loadResult{{snapshot: s.latest}, {snapshot: retry}}
				s.saveErrs = []error{errors.New("initial save")}
			},
		},
		{
			name:  "event append failure retains persisted terminal state",
			state: domain.StateStalled, present: true, wantPersisted: true, wantLeave: 1, wantTake: 1, wantMetrics: 1, wantFinalState: domain.StateCompleted,
			configure: func(_ *finalizeStoreFake, w *finalizeWriterFake) { w.appendErrs = []error{errors.New("append")} },
		},
		{
			name:  "all saves fail retains resource release without metrics",
			state: domain.StateStalled, present: true, configure: func(s *finalizeStoreFake, _ *finalizeWriterFake) {
				s.saveErrs = []error{errors.New("initial"), errors.New("retry")}
			},
		},
		{
			name:  "past stalled total is taken when current state is not stalled",
			state: domain.StateRunning, present: false, wantPersisted: true, wantTake: 1, wantMetrics: 1, wantFinalState: domain.StateFailed,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, w, slot, timeout, uc := finalizeFixtures(t, tc.state, now)
			r.present = tc.present
			tracker := uc.stalledTracker.(*finalizeStalledTrackerFake)
			tracker.total = 321
			recorder := uc.metricsRecorder.(*finalizeMetricsFake)
			if tc.configure != nil {
				tc.configure(s, w)
			}
			if tc.name == "completed saved from stalled records corrected estimated event" {
				s.latest.AdoptedAfterRestart = true
			}
			prepared, err := uc.Prepare(context.Background(), FinalizeTaskInput{TaskID: s.latest.TaskID, RawExitCode: tc.rawExitCode, Estimated: false, OccurredAt: now})
			if err != nil {
				t.Fatal(err)
			}
			uc.taskMu.Lock(s.latest.TaskID)
			result, _ := uc.ExecuteLocked(context.Background(), prepared)
			uc.taskMu.Unlock(s.latest.TaskID)
			if result.TerminalPersisted != tc.wantPersisted || len(recorder.snapshot()) != 0 {
				t.Fatalf("result=%+v metrics=%v", result, recorder.snapshot())
			}
			uc.ReleaseAfterFinalization(context.Background(), result, s.latest.TaskID)
			leave, take := tracker.snapshot()
			inputs := recorder.snapshot()
			if leave != tc.wantLeave || take != tc.wantTake || len(inputs) != tc.wantMetrics {
				t.Fatalf("leave=%d take=%d metrics=%v", leave, take, inputs)
			}
			if tc.wantMetrics == 1 {
				in := inputs[0]
				if in.TaskID != s.latest.TaskID || in.FinalState != tc.wantFinalState || !in.OccurredAt.Equal(now) || in.StalledTotalMs != 321 {
					t.Fatalf("metrics input=%+v", in)
				}
				if in.Estimated != tc.wantEstimated {
					t.Fatalf("metrics estimated=%t want %t", in.Estimated, tc.wantEstimated)
				}
			}
			wantRelease := 0
			if result.RecordExited {
				wantRelease = 1
			}
			if calls, _ := timeout.snapshot(); calls != wantRelease {
				t.Fatalf("disarms=%d", calls)
			}
			if calls, _, _ := slot.snapshot(); calls != wantRelease {
				t.Fatalf("releases=%d", calls)
			}
			if tc.wantMetrics == 1 {
				calls := uc.contractW.(*finalizeWriterFake).trace.snapshot()
				wantSuffix := []string{"leave-stalled", "take-total", "metrics", "disarm", "release-path-lock", "release-slot"}
				if tc.wantLeave == 0 {
					wantSuffix = wantSuffix[1:]
				}
				if len(calls) < len(wantSuffix) {
					t.Fatalf("trace=%v", calls)
				}
				for i, want := range wantSuffix {
					if calls[len(calls)-len(wantSuffix)+i] != want {
						t.Fatalf("trace=%v want suffix=%v", calls, wantSuffix)
					}
				}
			}
		})
	}
}

func TestFinalizeTaskUseCaseReleasesPathLockForImplAfterFinalization_SCNLock0104(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		raw  int
		want domain.TaskState
	}{
		{"completed", 0, domain.StateCompleted},
		{"failed", 1, domain.StateFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, _, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
			locks := &finalizePathLockStore{trace: uc.contractW.(*finalizeWriterFake).trace}
			uc.pathLockReleaser = NewReleasePathLockUseCase(locks)
			out, err := uc.Execute(context.Background(), FinalizeTaskInput{TaskID: s.latest.TaskID, RawExitCode: tc.raw, OccurredAt: now})
			if err != nil || out.ResultState != tc.want {
				t.Fatalf("out=%+v err=%v", out, err)
			}
			if got := locks.deletedSnapshot(); !reflect.DeepEqual(got, []domain.TaskID{s.latest.TaskID}) {
				t.Fatalf("deleted=%v", got)
			}
			if calls, _ := timeout.snapshot(); calls != 1 {
				t.Fatalf("disarms=%d", calls)
			}
			if calls, _, _ := slot.snapshot(); calls != 1 {
				t.Fatalf("slots=%d", calls)
			}
		})
	}
}

func TestFinalizeTaskUseCaseDoesNotReleasePathLockForNonImpl(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	s, _, _, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
	s.latest.Subcommand = domain.SubcommandReview
	locks := &finalizePathLockStore{trace: uc.contractW.(*finalizeWriterFake).trace}
	uc.pathLockReleaser = NewReleasePathLockUseCase(locks)
	if _, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now)); err != nil {
		t.Fatal(err)
	}
	if got := locks.deletedSnapshot(); len(got) != 0 {
		t.Fatalf("deleted=%v", got)
	}
	if calls, _ := timeout.snapshot(); calls != 1 {
		t.Fatalf("disarms=%d", calls)
	}
	if calls, _, _ := slot.snapshot(); calls != 1 {
		t.Fatalf("slots=%d", calls)
	}
}

func TestFinalizeTaskUseCaseDoesNotReleasePathLockWhenRecordExitDidNotOccur(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	s, _, _, slot, timeout, uc := finalizeFixtures(t, domain.StateRunning, now)
	locks := &finalizePathLockStore{trace: uc.contractW.(*finalizeWriterFake).trace}
	uc.pathLockReleaser = NewReleasePathLockUseCase(locks)
	uc.ReleaseAfterFinalization(context.Background(), LockedFinalizeResult{RecordExited: false, Subcommand: domain.SubcommandImpl}, s.latest.TaskID)
	if got := locks.deletedSnapshot(); len(got) != 0 {
		t.Fatalf("deleted=%v", got)
	}
	if calls, _ := timeout.snapshot(); calls != 0 {
		t.Fatalf("disarms=%d", calls)
	}
	if calls, _, _ := slot.snapshot(); calls != 0 {
		t.Fatalf("slots=%d", calls)
	}
	if got := uc.metricsRecorder.(*finalizeMetricsFake).snapshot(); len(got) != 0 {
		t.Fatalf("metrics=%v", got)
	}
}

func TestFinalizeTaskUseCasePathLockReleaseFailureIsFailSoft(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	s, _, _, slot, _, uc := finalizeFixtures(t, domain.StateRunning, now)
	failure := errors.New("delete path lock")
	locks := &finalizePathLockStore{trace: uc.contractW.(*finalizeWriterFake).trace, deleteErr: failure}
	uc.pathLockReleaser = NewReleasePathLockUseCase(locks)
	capture := &logCapture{}
	uc.logger = slog.New(capture)
	out, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
	if err != nil || out.ResultState != domain.StateCompleted {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if calls, _, _ := slot.snapshot(); calls != 1 {
		t.Fatalf("slots=%d", calls)
	}
	logs := capture.snapshot()
	if len(logs) != 1 || logs[0].level != slog.LevelWarn || logs[0].msg != "release path lock after finalization" || logs[0].attrs["task_id"] != s.latest.TaskID.String() || !errors.Is(logs[0].attrs["error"].(error), failure) {
		t.Fatalf("logs=%#v", logs)
	}
}

func TestFinalizeTaskUseCaseExecuteLockedPropagatesImplSubcommandOnRecordedExitPaths(t *testing.T) {
	now := time.Date(2026, 8, 9, 12, 1, 0, 0, time.UTC)
	for _, tc := range []struct {
		name      string
		configure func(*finalizeStoreFake, *finalizeReaderFake, *finalizeWriterFake)
	}{
		{"initial non-retryable", func(_ *finalizeStoreFake, r *finalizeReaderFake, _ *finalizeWriterFake) {
			r.exits = []struct {
				code   int
				exists bool
				err    error
			}{{err: errors.New("initial")}}
		}},
		{"reload fails", func(s *finalizeStoreFake, _ *finalizeReaderFake, w *finalizeWriterFake) {
			w.writeErrs = []error{errors.New("initial")}
			s.loads = []loadResult{{snapshot: s.latest}, {err: errors.New("reload")}}
		}},
		{"restore fails", func(s *finalizeStoreFake, _ *finalizeReaderFake, w *finalizeWriterFake) {
			bad := s.latest
			bad.State = "invalid"
			w.writeErrs = []error{errors.New("initial")}
			s.loads = []loadResult{{snapshot: s.latest}, {snapshot: bad}}
		}},
		{"record exit fails", func(s *finalizeStoreFake, _ *finalizeReaderFake, w *finalizeWriterFake) {
			terminal := s.latest
			terminal.State = domain.StateCompleted
			code := domain.NewExitCode(0)
			terminal.ExitCode = &code
			w.writeErrs = []error{errors.New("initial")}
			s.loads = []loadResult{{snapshot: s.latest}, {snapshot: terminal}}
		}},
		{"retry non-retryable", func(_ *finalizeStoreFake, r *finalizeReaderFake, w *finalizeWriterFake) {
			w.writeErrs = []error{errors.New("initial")}
			r.exits = []struct {
				code   int
				exists bool
				err    error
			}{{}, {err: errors.New("retry")}}
		}},
		{"retry persisted", func(s *finalizeStoreFake, _ *finalizeReaderFake, w *finalizeWriterFake) {
			w.writeErrs = []error{errors.New("initial")}
			s.saveErrs = []error{errors.New("initial save")}
		}},
		{"retry write fails", func(_ *finalizeStoreFake, _ *finalizeReaderFake, w *finalizeWriterFake) {
			w.writeErrs = []error{errors.New("initial"), errors.New("retry")}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, r, w, _, _, uc := finalizeFixtures(t, domain.StateRunning, now)
			locks := &finalizePathLockStore{trace: w.trace}
			uc.pathLockReleaser = NewReleasePathLockUseCase(locks)
			tc.configure(s, r, w)
			prepared, err := uc.Prepare(context.Background(), finalizeInput(s.latest.TaskID, now))
			if err != nil {
				t.Fatal(err)
			}
			uc.taskMu.Lock(s.latest.TaskID)
			result, _ := uc.ExecuteLocked(context.Background(), prepared)
			uc.taskMu.Unlock(s.latest.TaskID)
			if !result.RecordExited || result.Subcommand != domain.SubcommandImpl {
				t.Fatalf("result=%+v", result)
			}
			uc.ReleaseAfterFinalization(context.Background(), result, s.latest.TaskID)
			if got := locks.deletedSnapshot(); !reflect.DeepEqual(got, []domain.TaskID{s.latest.TaskID}) {
				t.Fatalf("deleted=%v", got)
			}
		})
	}
}
