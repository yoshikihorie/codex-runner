package execution

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
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
func (f *finalizeStoreFake) Reserve(domain.TaskID) error { return nil }
func (f *finalizeStoreFake) Release(domain.TaskID) error { return nil }
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
	return f.present, f.presentErr
}
func (f *finalizeReaderFake) ReadPromptContent(domain.TaskID) ([]byte, error)      { return nil, nil }
func (f *finalizeReaderFake) ReadLastMessageContent(domain.TaskID) ([]byte, error) { return nil, nil }
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
	return s, r, w, slot, timeout, NewFinalizeTaskUseCase(s, w, r, domain.ClockFunc(func() time.Time { return now }), taskMu, slot, timeout)
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
			if len(callsTrace) < 2 || callsTrace[len(callsTrace)-2] != "disarm" || callsTrace[len(callsTrace)-1] != "release-slot" {
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
	initialWrite := errors.New("initial write")
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
			w.writeErrs = []error{initialWrite}
			out, err := uc.Execute(context.Background(), finalizeInput(s.latest.TaskID, now))
			if err == nil || out.ResultState != domain.StateCompleted || out.Events == nil {
				t.Fatalf("out=%+v err=%v", out, err)
			}
			requireEvents(t, out.Events, domain.StateCompleted, now)
			if !errors.Is(out.ContractWriteError, domain.ErrContractWriteFailed) {
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
	if len(callsTrace) < 2 || callsTrace[len(callsTrace)-2] != "disarm" || callsTrace[len(callsTrace)-1] != "release-slot" {
		t.Fatalf("post-processing order=%v", callsTrace)
	}
}

func TestNewFinalizeTaskUseCaseContract(t *testing.T) {
	now := time.Now()
	s, r, w, slot, timeout, _ := finalizeFixtures(t, domain.StateRunning, now)
	clock := domain.ClockFunc(func() time.Time { return now })
	mu := store.NewTaskMutex()
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
		{"tasks", func() { NewFinalizeTaskUseCase(nil, w, r, clock, mu, slot, timeout) }}, {"writer", func() { NewFinalizeTaskUseCase(s, nil, r, clock, mu, slot, timeout) }}, {"reader", func() { NewFinalizeTaskUseCase(s, w, nil, clock, mu, slot, timeout) }}, {"clock", func() { NewFinalizeTaskUseCase(s, w, r, nil, mu, slot, timeout) }}, {"mutex", func() { NewFinalizeTaskUseCase(s, w, r, clock, nil, slot, timeout) }}, {"slot", func() { NewFinalizeTaskUseCase(s, w, r, clock, mu, nil, timeout) }}, {"timeout", func() { NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, nil) }}, {"many loggers", func() { NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, slog.Default(), slog.Default()) }},
	} {
		t.Run(tc.name, func(t *testing.T) { mustPanic(t, tc.f) })
	}
	if got := NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout); got.logger != slog.Default() || got.timeoutDisarmer != timeout {
		t.Fatal("default logger not used")
	}
	if got := NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, nil); got.logger != slog.Default() {
		t.Fatal("nil logger not default")
	}
	logger := slog.New(slog.NewTextHandler(testingWriter{t}, nil))
	if got := NewFinalizeTaskUseCase(s, w, r, clock, mu, slot, timeout, logger); got.logger != logger {
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
