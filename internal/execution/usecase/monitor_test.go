package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type testRecorder struct{ ops []string }

func (r *testRecorder) add(s string) { r.ops = append(r.ops, s) }

type stalledTrackerCall struct {
	kind string
	id   domain.TaskID
	at   time.Time
}

type testStalledTimeTracker struct {
	r         *testRecorder
	locker    *testLocker
	calls     []stalledTrackerCall
	held      []bool
	takeCalls int
}

func (t *testStalledTimeTracker) EnterStalled(id domain.TaskID, at time.Time) {
	t.calls = append(t.calls, stalledTrackerCall{kind: "enter", id: id, at: at})
	if t.locker != nil {
		t.held = append(t.held, t.locker.held[id.String()])
	}
	t.r.add("enter-stalled:" + id.String())
}

func (t *testStalledTimeTracker) LeaveStalled(id domain.TaskID, at time.Time) int {
	t.calls = append(t.calls, stalledTrackerCall{kind: "leave", id: id, at: at})
	if t.locker != nil {
		t.held = append(t.held, t.locker.held[id.String()])
	}
	t.r.add("leave-stalled:" + id.String())
	return 0
}

func (t *testStalledTimeTracker) TakeTotal(id domain.TaskID) int {
	t.takeCalls++
	return 0
}

func testTracker(r *testRecorder) *testStalledTimeTracker {
	return &testStalledTimeTracker{r: r}
}

type testLocker struct {
	r    *testRecorder
	held map[string]bool
}

func (m *testLocker) Lock(id domain.TaskID) {
	if m.held == nil {
		m.held = map[string]bool{}
	}
	if m.held[id.String()] {
		panic("double lock")
	}
	m.held[id.String()] = true
	m.r.add("lock:" + id.String())
}
func (m *testLocker) Unlock(id domain.TaskID) {
	if !m.held[id.String()] {
		panic("unlock without lock")
	}
	delete(m.held, id.String())
	m.r.add("unlock:" + id.String())
}

type testStore struct {
	store.TaskStore
	r          *testRecorder
	loads      map[string]domain.TaskSnapshot
	loadErr    map[string]error
	saveErr    map[string]error
	saved      []domain.TaskSnapshot
	saveCalls  int
	saveErrs   []error
	loadValues []domain.TaskSnapshot
	loadCalls  int
	lists      [][]domain.TaskSnapshot
	listErr    []error
	listCalls  int
	listStates [][]domain.TaskState
}

func (s *testStore) Load(id domain.TaskID) (domain.TaskSnapshot, error) {
	s.r.add("load:" + id.String())
	if s.loadCalls < len(s.loadValues) {
		value := s.loadValues[s.loadCalls]
		s.loadCalls++
		return value, nil
	}
	if e := s.loadErr[id.String()]; e != nil {
		return domain.TaskSnapshot{}, e
	}
	return s.loads[id.String()], nil
}
func (s *testStore) Save(id domain.TaskID, v domain.TaskSnapshot) error {
	s.r.add("save:" + id.String())
	if s.saveCalls < len(s.saveErrs) {
		err := s.saveErrs[s.saveCalls]
		s.saveCalls++
		if err != nil {
			return err
		}
	}
	if err := s.saveErr[id.String()]; err != nil {
		return err
	}
	s.saved = append(s.saved, v)
	return nil
}
func (s *testStore) ListByStates(states []domain.TaskState) ([]domain.TaskSnapshot, error) {
	s.r.add("list-by-states")
	s.listStates = append(s.listStates, append([]domain.TaskState(nil), states...))
	i := s.listCalls
	s.listCalls++
	if i < len(s.listErr) && s.listErr[i] != nil {
		return nil, s.listErr[i]
	}
	if i < len(s.lists) {
		return s.lists[i], nil
	}
	return nil, nil
}

type testWriter struct {
	contract.ContractWriter
	r                    *testRecorder
	rawErr, eventErr     error
	rawErrs, eventErrs   []error
	rawCalls, eventCalls int
	raw, events          []string
}

func (w *testWriter) AppendRawEvent(id domain.TaskID, typ string, _ json.RawMessage) error {
	w.r.add("append-raw:" + id.String() + ":" + typ)
	w.raw = append(w.raw, typ)
	if w.rawCalls < len(w.rawErrs) {
		err := w.rawErrs[w.rawCalls]
		w.rawCalls++
		return err
	}
	return w.rawErr
}
func (w *testWriter) AppendEvent(id domain.TaskID, e domain.Event) error {
	w.r.add("append-event:" + id.String() + ":" + e.Type())
	w.events = append(w.events, e.Type())
	if w.eventCalls < len(w.eventErrs) {
		err := w.eventErrs[w.eventCalls]
		w.eventCalls++
		return err
	}
	return w.eventErr
}

type testClock struct {
	at    time.Time
	r     *testRecorder
	id    domain.TaskID
	calls int
}

func (c *testClock) Now() time.Time { c.calls++; c.r.add("clock:" + c.id.String()); return c.at }

func testID(t *testing.T, suffix string) domain.TaskID {
	t.Helper()
	id, e := domain.NewTaskID("impl-20260812-120000-a1b2-" + suffix)
	if e != nil {
		t.Fatal(e)
	}
	return id
}
func testSnapshot(t *testing.T, id domain.TaskID, state domain.TaskState, start time.Time, last *time.Time) domain.TaskSnapshot {
	t.Helper()
	slug, e := domain.NewSlug(strings.TrimPrefix(id.String(), "impl-20260812-120000-a1b2-"))
	if e != nil {
		t.Fatal(e)
	}
	task, _, e := domain.NewTask(id, domain.SubcommandImpl, slug, nil, start, 1)
	if e != nil {
		t.Fatal(e)
	}
	timeout, e := domain.NewTimeout(nil, 1800)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = task.Start(timeout, "gpt-5", start); e != nil {
		t.Fatal(e)
	}
	snap, e := domain.NewTaskSnapshotFromAdmission(task, timeout, "gpt-5", nil, domain.ExecutionRouteDaemon, start)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = task.RecordProcessInfo(7, start, start); e != nil {
		t.Fatal(e)
	}
	if e = task.ConfirmRunning(start); e != nil {
		t.Fatal(e)
	}
	switch state {
	case domain.StateRunning:
	case domain.StateStalled:
		if _, e = task.MarkStalled(1201, start); e != nil {
			t.Fatal(e)
		}
	case domain.StateCompleted:
		if _, e = task.RecordExit(domain.NewExitCode(0), true, false, false, start); e != nil {
			t.Fatal(e)
		}
	default:
		t.Fatalf("unsupported test snapshot state: %s", state)
	}
	snap, e = snap.WithTask(task, start)
	if e != nil {
		t.Fatal(e)
	}
	if last != nil {
		snap.LastEventAt = last
	}
	return snap
}
func ptr[T any](v T) *T { return &v }
func testLive(r *testRecorder, id domain.TaskID, dead bool, err error) *execution.CheckLivenessUseCase {
	return execution.NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { r.add("liveness:" + id.String()); return dead, err }), func(domain.TaskID) string { return "lock" })
}

func TestMonitorKnownEventUpdatesSnapshotAndOrdering(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "monitor")
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	now := start.Add(time.Minute)
	s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): testSnapshot(t, id, domain.StateRunning, start, nil)}}
	w := &testWriter{r: r}
	u := NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(_ context.Context, _ io.Reader, known func(string, json.RawMessage), _ func(string, json.RawMessage)) error {
		known("item.completed", json.RawMessage(`{"type":"item.completed"}`))
		return nil
	}), s, &testLocker{r: r}, w, &testClock{at: now, r: r, id: id}, testTracker(r))
	if err := u.Run(context.Background(), id, strings.NewReader("")); err != nil {
		t.Fatal(err)
	}
	if len(s.saved) != 1 || s.saved[0].LastEventAt == nil || !s.saved[0].LastEventAt.Equal(now) || !s.saved[0].StateUpdatedAt.Equal(now) {
		t.Fatalf("saved=%#v", s.saved)
	}
	want := []string{"lock:" + id.String(), "load:" + id.String(), "clock:" + id.String(), "save:" + id.String(), "append-raw:" + id.String() + ":item.completed", "append-event:" + id.String() + ":TaskEventObserved", "unlock:" + id.String()}
	if fmt.Sprint(r.ops) != fmt.Sprint(want) {
		t.Fatalf("ops=%v", r.ops)
	}
}

func TestMonitorUnknownOnlyAppendsRawWithoutLock(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "unknown")
	w := &testWriter{r: r}
	u := NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(_ context.Context, _ io.Reader, _ func(string, json.RawMessage), unknown func(string, json.RawMessage)) error {
		unknown("unknown", json.RawMessage(`{"payload":"MONITOR_SECRET_SENTINEL"}`))
		return nil
	}), &testStore{r: r}, &testLocker{r: r}, w, &testClock{r: r, id: id}, testTracker(r))
	_ = u.Run(context.Background(), id, strings.NewReader(""))
	if fmt.Sprint(r.ops) != fmt.Sprint([]string{"append-raw:" + id.String() + ":unknown"}) {
		t.Fatalf("ops=%v", r.ops)
	}
}

func TestMonitorErrorsContinueAndUnlock(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "save")
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): testSnapshot(t, id, domain.StateRunning, start, nil)}, saveErr: map[string]error{id.String(): errors.New("save failed")}}
	w := &testWriter{r: r}
	var logs bytes.Buffer
	u := NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(_ context.Context, _ io.Reader, k func(string, json.RawMessage), _ func(string, json.RawMessage)) error {
		k("item.completed", json.RawMessage(`{}`))
		k("turn.completed", json.RawMessage(`{}`))
		return nil
	}), s, &testLocker{r: r}, w, &testClock{at: start.Add(time.Second), r: r, id: id}, testTracker(r), slog.New(slog.NewTextHandler(&logs, nil)))
	_ = u.Run(context.Background(), id, strings.NewReader(""))
	if len(w.raw) != 2 || len(w.events) != 0 || strings.Count(logs.String(), contractWriteFailedCode) != 2 {
		t.Fatalf("raw=%v events=%v logs=%s", w.raw, w.events, logs.String())
	}
	if strings.Count(fmt.Sprint(r.ops), "unlock:"+id.String()) != 2 {
		t.Fatalf("ops=%v", r.ops)
	}
}

func TestMonitorStateReadAndInvalidTransitionLogs(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "read")
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	u := NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(_ context.Context, _ io.Reader, k func(string, json.RawMessage), _ func(string, json.RawMessage)) error {
		k("item.completed", json.RawMessage(`{}`))
		return nil
	}), &testStore{r: r, loadErr: map[string]error{id.String(): errors.New("load failed")}}, &testLocker{r: r}, &testWriter{r: r}, &testClock{r: r, id: id}, testTracker(r), logger)
	_ = u.Run(context.Background(), id, strings.NewReader(""))
	logInvalidTransition(logger, id, "observe-event", domain.StateCompleted, domain.ErrInvalidStateTransition)
	for _, x := range []string{taskStateReadFailedCode, "operation=load", taskInvalidTransitionCode, "operation=observe-event", "state=completed", "error=\"invalid task state transition\""} {
		if !strings.Contains(logs.String(), x) {
			t.Fatalf("missing %q logs=%s", x, logs.String())
		}
	}
}

func TestMonitorRunPropagatesContextErrors(t *testing.T) {
	for _, want := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(want.Error(), func(t *testing.T) {
			u := NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(context.Context, io.Reader, func(string, json.RawMessage), func(string, json.RawMessage)) error {
				return want
			}), &testStore{r: &testRecorder{}}, &testLocker{r: &testRecorder{}}, &testWriter{r: &testRecorder{}}, &testClock{r: &testRecorder{}, id: testID(t, "ctx")}, testTracker(&testRecorder{}))
			if got := u.Run(context.Background(), testID(t, "ctx"), strings.NewReader("")); got != want {
				t.Fatalf("got=%v", got)
			}
		})
	}
}

func TestMonitorConstructorValidation(t *testing.T) {
	id := testID(t, "constructor")
	r := &testRecorder{}
	good := func() *MonitorTaskEventsUseCase {
		return NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(context.Context, io.Reader, func(string, json.RawMessage), func(string, json.RawMessage)) error {
			return nil
		}), &testStore{r: r}, &testLocker{r: r}, &testWriter{r: r}, &testClock{r: r, id: id}, testTracker(r))
	}
	if good() == nil {
		t.Fatal("nil")
	}
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	NewMonitorTaskEventsUseCase(nil, &testStore{r: r}, &testLocker{r: r}, &testWriter{r: r}, &testClock{r: r, id: id}, testTracker(r))
}

func TestMonitorConstructorRejectsNilTracker(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "monitor-nil-tracker")
	base := func(tracker stalledTimeTracker) {
		NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(context.Context, io.Reader, func(string, json.RawMessage), func(string, json.RawMessage)) error {
			return nil
		}), &testStore{r: r}, &testLocker{r: r}, &testWriter{r: r}, &testClock{r: r, id: id}, tracker)
	}
	didPanic := func(call func()) (panicked bool) {
		defer func() {
			panicked = recover() != nil
		}()
		call()
		return false
	}
	if didPanic(func() {
		base(testTracker(r))
	}) {
		t.Fatal("valid tracker panicked")
	}
	for _, tc := range []struct {
		name    string
		tracker stalledTimeTracker
	}{
		{"nil-tracker", nil},
		{"typed-nil-tracker", (*testStalledTimeTracker)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if !didPanic(func() {
				base(tc.tracker)
			}) {
				t.Fatal("expected panic")
			}
		})
	}
}

func TestMonitorRunRejectsNilInputsWithoutObserving(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "nil-input")
	called := false
	u := NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(context.Context, io.Reader, func(string, json.RawMessage), func(string, json.RawMessage)) error {
		called = true
		return nil
	}), &testStore{r: r}, &testLocker{r: r}, &testWriter{r: r}, &testClock{r: r, id: id}, testTracker(r))
	var typedNil *strings.Reader
	for _, input := range []struct {
		ctx    context.Context
		stdout io.Reader
	}{{nil, strings.NewReader("")}, {context.Background(), nil}, {context.Background(), typedNil}} {
		if err := u.Run(input.ctx, id, input.stdout); err == nil {
			t.Fatal("expected error")
		}
	}
	if called {
		t.Fatal("monitor called")
	}
}

func TestMonitorStalledTrackerLeaveConditions(t *testing.T) {
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	now := start.Add(time.Minute)
	for _, tc := range []struct {
		name  string
		state domain.TaskState
		save  error
		leave int
	}{
		{"stalled-save-success", domain.StateStalled, nil, 1},
		{"running-save-success", domain.StateRunning, nil, 0},
		{"stalled-save-failure", domain.StateStalled, errors.New("save failed"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &testRecorder{}
			id := testID(t, "monitor-tracker-"+tc.name)
			s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): testSnapshot(t, id, tc.state, start, nil)}, saveErr: map[string]error{id.String(): tc.save}}
			w := &testWriter{r: r}
			tracker := testTracker(r)
			u := NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(_ context.Context, _ io.Reader, known func(string, json.RawMessage), _ func(string, json.RawMessage)) error {
				known("item.completed", json.RawMessage(`{}`))
				return nil
			}), s, &testLocker{r: r}, w, &testClock{at: now, r: r, id: id}, tracker)
			if err := u.Run(context.Background(), id, strings.NewReader("")); err != nil {
				t.Fatal(err)
			}
			if len(tracker.calls) != tc.leave || tracker.takeCalls != 0 {
				t.Fatalf("calls=%#v take=%d", tracker.calls, tracker.takeCalls)
			}
			if tc.leave == 1 && (tracker.calls[0].kind != "leave" || tracker.calls[0].id != id || !tracker.calls[0].at.Equal(now)) {
				t.Fatalf("calls=%#v", tracker.calls)
			}
			if tc.save == nil && len(s.saved) != 1 {
				t.Fatalf("saved=%#v", s.saved)
			}
			if tc.name == "stalled-save-success" && (s.saved[0].State != domain.StateRunning || s.saved[0].LastEventAt == nil || !s.saved[0].LastEventAt.Equal(now) || fmt.Sprint(w.events) != "[TaskEventObserved]") {
				t.Fatalf("saved=%#v events=%v", s.saved, w.events)
			}
		})
	}
}

func TestMonitorInvalidTransitionDoesNotPersistOrTrack(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "monitor-invalid-tracker")
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): testSnapshot(t, id, domain.StateCompleted, start, nil)}}
	w, tracker := &testWriter{r: r}, testTracker(r)
	u := NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(_ context.Context, _ io.Reader, known func(string, json.RawMessage), _ func(string, json.RawMessage)) error {
		known("item.completed", json.RawMessage(`{}`))
		return nil
	}), s, &testLocker{r: r}, w, &testClock{at: start, r: r, id: id}, tracker)
	_ = u.Run(context.Background(), id, strings.NewReader(""))
	if len(s.saved) != 0 || len(tracker.calls) != 0 || tracker.takeCalls != 0 || len(w.raw) != 1 || len(w.events) != 0 {
		t.Fatalf("saved=%v calls=%v take=%d raw=%v events=%v", s.saved, tracker.calls, tracker.takeCalls, w.raw, w.events)
	}
}

func TestMonitorStalledTrackerIgnoresEventAppendFailures(t *testing.T) {
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name       string
		raw, event []error
	}{
		{"append-raw-failure", []error{errors.New("raw failed"), nil}, []error{nil, nil}},
		{"append-event-failure", []error{nil, nil}, []error{errors.New("event failed"), nil}},
		{"both-fail", []error{errors.New("raw failed"), nil}, []error{errors.New("event failed"), nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &testRecorder{}
			id := testID(t, "monitor-append-"+tc.name)
			stalled := testSnapshot(t, id, domain.StateStalled, start, nil)
			running := testSnapshot(t, id, domain.StateRunning, start, nil)
			s := &testStore{r: r, loadValues: []domain.TaskSnapshot{stalled, running}}
			w, tracker := &testWriter{r: r, rawErrs: tc.raw, eventErrs: tc.event}, testTracker(r)
			u := NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(_ context.Context, _ io.Reader, known func(string, json.RawMessage), _ func(string, json.RawMessage)) error {
				known("item.completed", json.RawMessage(`{}`))
				known("turn.completed", json.RawMessage(`{}`))
				return nil
			}), s, &testLocker{r: r}, w, &testClock{at: start.Add(time.Second), r: r, id: id}, tracker)
			_ = u.Run(context.Background(), id, strings.NewReader(""))
			if len(s.saved) != 2 || len(w.raw) != 2 || len(w.events) != 2 || len(tracker.calls) != 1 || tracker.takeCalls != 0 || strings.Count(fmt.Sprint(r.ops), "unlock:"+id.String()) != 2 {
				t.Fatalf("saved=%v raw=%v events=%v calls=%v take=%d ops=%v", s.saved, w.raw, w.events, tracker.calls, tracker.takeCalls, r.ops)
			}
		})
	}
}

func TestMonitorTrackerCallOccursWhileTaskLockHeld(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "monitor-lock-tracker")
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	locker := &testLocker{r: r}
	tracker := testTracker(r)
	tracker.locker = locker
	s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): testSnapshot(t, id, domain.StateStalled, start, nil)}}
	u := NewMonitorTaskEventsUseCase(execution.EventMonitorFunc(func(_ context.Context, _ io.Reader, known func(string, json.RawMessage), _ func(string, json.RawMessage)) error {
		known("item.completed", json.RawMessage(`{}`))
		return nil
	}), s, locker, &testWriter{r: r}, &testClock{at: start, r: r, id: id}, tracker)
	_ = u.Run(context.Background(), id, strings.NewReader(""))
	if len(tracker.calls) != 1 || len(tracker.held) != 1 || !tracker.held[0] {
		t.Fatalf("calls=%v held=%v", tracker.calls, tracker.held)
	}
	assertOrderedSubsequence(t, r.ops, []string{"lock:" + id.String(), "load:" + id.String(), "clock:" + id.String(), "save:" + id.String(), "leave-stalled:" + id.String(), "append-raw:" + id.String() + ":item.completed", "append-event:" + id.String() + ":TaskEventObserved", "unlock:" + id.String()})
}

func assertOrderedSubsequence(t *testing.T, got, want []string) {
	t.Helper()
	i := 0
	for _, operation := range got {
		if i < len(want) && operation == want[i] {
			i++
		}
	}
	if i != len(want) {
		t.Fatalf("operations=%v want subsequence=%v", got, want)
	}
}
