package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
)

type testTicker struct {
	ch    chan time.Time
	r     *testRecorder
	stops int
}

func (t *testTicker) C() <-chan time.Time { return t.ch }
func (t *testTicker) Stop()               { t.stops++; t.r.add("ticker-stop") }

type testTickerFactory struct{ ticker *testTicker }

func (f testTickerFactory) NewTicker(time.Duration) stallTicker { return f.ticker }

func stallUC(t *testing.T, id domain.TaskID, s *testStore, dead bool, liveErr error, now time.Time, logger *slog.Logger) *checkStallUseCase {
	t.Helper()
	r := s.r
	return newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, dead, liveErr), &testWriter{r: r}, &testClock{at: now, r: r, id: id}, testTracker(r), time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, execution.NewLifecycleOwnershipRegistry(), logger)
}

type stallOwnershipSequence struct {
	values []bool
	calls  int
}

type stallLockProbe struct{ held bool }

func (p *stallLockProbe) Lock(domain.TaskID) {
	if p.held {
		panic("task lock re-acquired while held")
	}
	p.held = true
}
func (p *stallLockProbe) Unlock(domain.TaskID) {
	if !p.held {
		panic("task lock unlocked while not held")
	}
	p.held = false
}

func TestCheckStallReleasesTaskLockWhenNoStallStartTime(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "no-stall-start-time")
	start := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	snapshot := testSnapshot(t, id, domain.StateRunning, start, nil)
	snapshot.LastEventAt = nil
	snapshot.PID, snapshot.ProcessStartedAt = nil, nil
	snapshot.AdoptedAfterRestart = true
	store := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): snapshot}}
	writer := &testWriter{r: r}
	tracker := testTracker(r)
	lock := &stallLockProbe{}
	uc := newCheckStallUseCase(store, lock, testLive(r, id, false, nil), writer, &testClock{at: start, r: r, id: id}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, execution.NewLifecycleOwnershipRegistry())

	uc.checkOne(context.Background(), id)
	if lock.held {
		t.Fatal("task lock remains held")
	}
	if len(store.saved) != 0 || len(writer.events) != 0 || len(tracker.calls) != 0 || tracker.takeCalls != 0 {
		t.Fatalf("nil start changed task: saved=%v events=%v tracker=%v take=%d", store.saved, writer.events, tracker.calls, tracker.takeCalls)
	}
	lock.Lock(id)
	lock.Unlock(id)
}

type stallCoordinatorProbe struct {
	lock  *stallLockProbe
	calls int
	id    domain.TaskID
}

func (p *stallCoordinatorProbe) Handle(_ context.Context, id domain.TaskID, _ *domain.SessionRef, _ time.Time) recovery.OrphanTransitionResult {
	if p.lock.held {
		panic("orphan coordinator called with task lock held")
	}
	// Re-acquiring the same lock proves finalization/resume can safely enter
	// their own task-serialized path after the stall transition.
	p.lock.Lock(id)
	p.lock.Unlock(id)
	p.calls++
	p.id = id
	return recovery.OrphanTransitionResult{Finalized: true}
}

func (s *stallOwnershipSequence) Acquire(domain.TaskID) (domain.LifecycleGeneration, func(), bool) {
	return 0, func() {}, false
}

func (*stallOwnershipSequence) Current(domain.TaskID) (domain.LifecycleGeneration, bool) {
	return 0, false
}

func (*stallOwnershipSequence) WithCurrent(domain.TaskID, domain.LifecycleGeneration, func() error) (bool, error) {
	return false, nil
}

func (s *stallOwnershipSequence) IsOwned(domain.TaskID) bool {
	if s.calls >= len(s.values) {
		return s.values[len(s.values)-1]
	}
	value := s.values[s.calls]
	s.calls++
	return value
}

func TestCheckStallThresholdAndNilLastEvent(t *testing.T) {
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		gap  time.Duration
		want bool
		last *time.Time
	}{{"exact", 1200 * time.Second, false, &start}, {"over", 1200*time.Second + time.Nanosecond, true, &start}, {"1201", 1201 * time.Second, true, &start}, {"nil-last", 1201 * time.Second, true, nil}} {
		t.Run(tc.name, func(t *testing.T) {
			r := &testRecorder{}
			id := testID(t, "stall-"+tc.name)
			snap := testSnapshot(t, id, domain.StateRunning, start, tc.last)
			s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): snap}}
			w := &testWriter{r: r}
			uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, false, nil), w, &testClock{at: start.Add(tc.gap), r: r, id: id}, testTracker(r), time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, execution.NewLifecycleOwnershipRegistry())
			uc.checkOne(context.Background(), id)
			if (len(w.events) > 0) != tc.want {
				t.Fatalf("events=%v", w.events)
			}
			if tc.want {
				if s.saved[0].State != domain.StateStalled {
					t.Fatalf("saved=%#v", s.saved)
				}
				if tc.name == "over" && len(w.events) != 1 {
					t.Fatalf("events=%v", w.events)
				}
				if tc.last == nil && s.saved[0].LastEventAt != nil {
					t.Fatalf("last=%v", s.saved[0].LastEventAt)
				}
			}
		})
	}
}

func TestCheckStallStopsScanAfterContextCancellation(t *testing.T) {
	r := &testRecorder{}
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	one, two := testID(t, "cancel-one"), testID(t, "cancel-two")
	a, b := testSnapshot(t, one, domain.StateRunning, start, &start), testSnapshot(t, two, domain.StateRunning, start, &start)
	s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{one.String(): a, two.String(): b}, lists: [][]domain.TaskSnapshot{{a, b}}}
	ctx, cancel := context.WithCancel(context.Background())
	uc := newCheckStallUseCase(s, &testLocker{r: r}, execution.NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { cancel(); return false, nil }), func(domain.TaskID) string { return "lock" }), &testWriter{r: r}, &testClock{at: start.Add(1201 * time.Second), r: r, id: one}, testTracker(r), time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, execution.NewLifecycleOwnershipRegistry())
	uc.scan(ctx)
	if strings.Contains(fmt.Sprint(r.ops), "load:"+two.String()) {
		t.Fatalf("ops=%v", r.ops)
	}
}

func TestCheckStallChecksRunningAndStalledAndOrphans(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "orphan")
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	snap := testSnapshot(t, id, domain.StateStalled, now, nil)
	s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): snap}, lists: [][]domain.TaskSnapshot{{snap}}}
	w := &testWriter{r: r}
	uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, true, nil), w, &testClock{at: now, r: r, id: id}, testTracker(r), time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, execution.NewLifecycleOwnershipRegistry())
	uc.scan(context.Background())
	if fmt.Sprint(s.listStates[0]) != fmt.Sprint([]domain.TaskState{domain.StateRunning, domain.StateStalled}) || len(w.events) != 1 || w.events[0] != "TaskOrphanDetected" || s.saved[0].State != domain.StateOrphaned {
		t.Fatalf("states=%v events=%v saved=%#v", s.listStates, w.events, s.saved)
	}
}

func TestCheckStallReleasesTaskLockBeforeOrphanCoordinator(t *testing.T) {
	start := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	id := testID(t, "orphan-coordinator-unlocked")
	snapshot := testSnapshot(t, id, domain.StateRunning, start, &start)
	recorder := &testRecorder{}
	store := &testStore{r: recorder, loads: map[string]domain.TaskSnapshot{id.String(): snapshot}}
	lock := &stallLockProbe{}
	coordinator := &stallCoordinatorProbe{lock: lock}
	uc := newCheckStallUseCase(store, lock, testLive(recorder, id, true, nil), &testWriter{r: recorder}, &testClock{at: start, r: recorder, id: id}, testTracker(recorder), time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: recorder}}, execution.NewLifecycleOwnershipRegistry())
	uc.orphanCoordinator = coordinator

	done := make(chan struct{})
	go func() {
		uc.checkOne(context.Background(), id)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("stall check deadlocked while handing off orphan recovery")
	}
	if coordinator.calls != 1 || coordinator.id != id || lock.held {
		t.Fatalf("calls=%d id=%s held=%t", coordinator.calls, coordinator.id, lock.held)
	}
	if len(store.saved) != 1 || store.saved[0].State != domain.StateOrphaned {
		t.Fatalf("saved=%+v", store.saved)
	}
}

func TestCheckStallFailuresUnlockAndContinue(t *testing.T) {
	r := &testRecorder{}
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	one, two := testID(t, "one"), testID(t, "two")
	a, b := testSnapshot(t, one, domain.StateRunning, start, &start), testSnapshot(t, two, domain.StateRunning, start, &start)
	s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{one.String(): a, two.String(): b}, saveErr: map[string]error{one.String(): errors.New("save failed")}, lists: [][]domain.TaskSnapshot{{a, b}}}
	w := &testWriter{r: r}
	var logs bytes.Buffer
	uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, one, false, nil), w, &testClock{at: start.Add(1201 * time.Second), r: r, id: one}, testTracker(r), time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, execution.NewLifecycleOwnershipRegistry(), slog.New(slog.NewTextHandler(&logs, nil)))
	uc.scan(context.Background())
	ops := fmt.Sprint(r.ops)
	if !strings.Contains(ops, "unlock:"+one.String()+" lock:"+two.String()) {
		t.Fatalf("ops=%v", r.ops)
	}
	if !strings.Contains(logs.String(), contractWriteFailedCode) || len(w.events) == 0 {
		t.Fatalf("logs=%s events=%v", logs.String(), w.events)
	}
}

func TestCheckStallReadAndLivenessErrorClassification(t *testing.T) {
	id := testID(t, "errors")
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name         string
		err          error
		wantIOLog    bool
		wantPlainLog bool
	}{
		{name: "io", err: errors.New("io failed"), wantIOLog: true, wantPlainLog: true},
		{name: "task-not-found", err: domain.ErrTaskNotFound, wantPlainLog: true},
		{name: "canceled", err: context.Canceled},
		{name: "deadline-exceeded", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &testRecorder{}
			snap := testSnapshot(t, id, domain.StateRunning, start, nil)
			s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): snap}}
			w := &testWriter{r: r}
			var logs bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&logs, nil))
			uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, false, tc.err), w, &testClock{at: start, r: r, id: id}, testTracker(r), time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, execution.NewLifecycleOwnershipRegistry(), logger)

			uc.checkOne(context.Background(), id)

			records := decodeStallLogRecords(t, logs.String())
			ioLogs, plainLogs := 0, 0
			for _, record := range records {
				if record["msg"] == "liveness lock I/O error" {
					ioLogs++
					if record["code"] != "LIVENESS_LOCK_IO_ERROR" {
						t.Fatalf("I/O record=%v", record)
					}
				}
				if record["msg"] == "stall liveness check returned an error" {
					plainLogs++
					if record["task_id"] != id.String() || !strings.Contains(record["error"].(string), tc.err.Error()) {
						t.Fatalf("plain record=%v", record)
					}
					if _, hasCode := record["code"]; hasCode {
						t.Fatalf("plain record unexpectedly has code: %v", record)
					}
				}
			}
			if got := ioLogs == 1; got != tc.wantIOLog {
				t.Fatalf("I/O logs=%d records=%v", ioLogs, records)
			}
			if got := plainLogs == 1; got != tc.wantPlainLog {
				t.Fatalf("plain logs=%d records=%v", plainLogs, records)
			}
			if len(s.saved) != 0 || len(w.events) != 0 || strings.Contains(fmt.Sprint(r.ops), "save:") || strings.Contains(fmt.Sprint(r.ops), "append-event:") {
				t.Fatalf("liveness error changed task: saved=%v events=%v ops=%v", s.saved, w.events, r.ops)
			}
		})
	}
}

func TestCheckStallLivenessErrorOverThresholdIsFailClosedAndContinues(t *testing.T) {
	r := &testRecorder{}
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	now := start.Add(1201 * time.Second)
	one, two := testID(t, "liveness-error-one"), testID(t, "liveness-error-two")
	secondLast := now.Add(-time.Second)
	first := testSnapshot(t, one, domain.StateRunning, start, &start)
	second := testSnapshot(t, two, domain.StateRunning, start, &secondLast)
	store := &testStore{r: r, loads: map[string]domain.TaskSnapshot{one.String(): first, two.String(): second}, lists: [][]domain.TaskSnapshot{{first, second}}}
	writer := &testWriter{r: r}
	tracker := testTracker(r)
	livenessCalls := 0
	liveness := execution.NewCheckLivenessUseCase(domain.LivenessLockFunc(func(path string) (bool, error) {
		r.add("liveness:" + path)
		livenessCalls++
		if livenessCalls == 1 {
			return false, errors.New("liveness I/O failure")
		}
		return false, nil
	}), func(id domain.TaskID) string { return id.String() })
	uc := newCheckStallUseCase(store, &testLocker{r: r}, liveness, writer, &testClock{at: now, r: r, id: one}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, execution.NewLifecycleOwnershipRegistry())

	uc.scan(context.Background())

	if len(store.saved) != 0 || len(writer.events) != 0 || len(tracker.calls) != 0 || tracker.takeCalls != 0 {
		t.Fatalf("liveness error changed task: saved=%v events=%v tracker=%v take=%d", store.saved, writer.events, tracker.calls, tracker.takeCalls)
	}
	if livenessCalls != 2 {
		t.Fatalf("liveness calls=%d ops=%v", livenessCalls, r.ops)
	}
	ops := fmt.Sprint(r.ops)
	for _, operation := range []string{"load:" + two.String(), "liveness:" + two.String()} {
		if strings.Count(ops, operation) != 1 {
			t.Fatalf("operation %q count=%d ops=%v", operation, strings.Count(ops, operation), r.ops)
		}
	}
	for _, id := range []domain.TaskID{one, two} {
		if strings.Count(ops, "unlock:"+id.String()) != 1 {
			t.Fatalf("unlock count for %s=%d ops=%v", id, strings.Count(ops, "unlock:"+id.String()), r.ops)
		}
	}
	assertOrderedSubsequence(t, r.ops, []string{"liveness:" + one.String(), "unlock:" + one.String(), "lock:" + two.String(), "load:" + two.String(), "liveness:" + two.String(), "unlock:" + two.String()})
}

func TestCheckStallLivenessErrorDoesNotReachStateTransitionPaths(t *testing.T) {
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		idSuffix string
		state    domain.TaskState
		now      time.Time
	}{
		{name: "running-under-threshold", idSuffix: "liveness-error-run", state: domain.StateRunning, now: start.Add(1199 * time.Second)},
		{name: "stalled-over-threshold-orphan-eligible", idSuffix: "liveness-error-stalled", state: domain.StateStalled, now: start.Add(1201 * time.Second)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &testRecorder{}
			id := testID(t, tc.idSuffix)
			last := start
			snapshot := testSnapshot(t, id, tc.state, start, &last)
			store := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): snapshot}}
			writer := &testWriter{r: r}
			tracker := testTracker(r)
			uc := newCheckStallUseCase(store, &testLocker{r: r}, testLive(r, id, false, errors.New("liveness I/O failure")), writer, &testClock{at: tc.now, r: r, id: id}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, execution.NewLifecycleOwnershipRegistry())

			uc.checkOne(context.Background(), id)

			if len(store.saved) != 0 || len(writer.events) != 0 || len(tracker.calls) != 0 || tracker.takeCalls != 0 {
				t.Fatalf("liveness error changed task: saved=%v events=%v tracker=%v take=%d", store.saved, writer.events, tracker.calls, tracker.takeCalls)
			}
		})
	}
}

func TestCheckStallOwnedBeforeOrAfterLockSkipsLivenessAndPreservesStallEvaluation(t *testing.T) {
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		owned []bool
	}{
		{name: "owned-before-lock", owned: []bool{true, true}},
		{name: "owned-after-lock", owned: []bool{false, true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &testRecorder{}
			id := testID(t, "owned-"+tc.name)
			last := start
			snapshot := testSnapshot(t, id, domain.StateRunning, start, &last)
			store := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): snapshot}}
			writer := &testWriter{r: r}
			tracker := testTracker(r)
			ownership := &stallOwnershipSequence{values: tc.owned}
			uc := newCheckStallUseCaseWithOwnership(store, &testLocker{r: r}, testLive(r, id, false, nil), writer, &testClock{at: start.Add(1201 * time.Second), r: r, id: id}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, ownership)

			uc.checkOne(context.Background(), id)

			if strings.Contains(fmt.Sprint(r.ops), "liveness:"+id.String()) {
				t.Fatalf("owned task checked liveness: ops=%v", r.ops)
			}
			if len(store.saved) != 1 || len(writer.events) != 1 || writer.events[0] != "TaskStalled" || len(tracker.calls) != 1 || tracker.calls[0].kind != "enter" || tracker.takeCalls != 0 {
				t.Fatalf("owned task did not preserve stall evaluation: saved=%v events=%v tracker=%v take=%d", store.saved, writer.events, tracker.calls, tracker.takeCalls)
			}
		})
	}
}

func TestCheckStallPersistWithTaskFailureLogsSnapshotValidationWithoutStateReadCode(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "with-task-failure")
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	snapshot := testSnapshot(t, id, domain.StateRunning, now, nil)
	task, err := snapshot.Restore()
	if err != nil {
		t.Fatal(err)
	}
	snapshot.Route = domain.ExecutionRouteLegacy
	store := &testStore{r: r}
	writer := &testWriter{r: r}
	var logs bytes.Buffer
	uc := newCheckStallUseCase(store, &testLocker{r: r}, testLive(r, id, false, nil), writer, &testClock{at: now, r: r, id: id}, testTracker(r), time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, execution.NewLifecycleOwnershipRegistry(), slog.New(slog.NewJSONHandler(&logs, nil)))

	uc.persist(id, snapshot, task, nil, now, "mark-stalled")

	records := decodeStallLogRecords(t, logs.String())
	if len(records) != 1 {
		t.Fatalf("records=%v", records)
	}
	record := records[0]
	if record["msg"] != "snapshot validation failed after task update" || record["task_id"] != id.String() || record["operation"] != "with-task" {
		t.Fatalf("record=%v", record)
	}
	if errorText, ok := record["error"].(string); !ok || errorText == "" {
		t.Fatalf("record=%v", record)
	}
	if _, hasCode := record["code"]; hasCode || strings.Contains(logs.String(), taskStateReadFailedCode) {
		t.Fatalf("record unexpectedly has state-read classification: %v", record)
	}
	if len(store.saved) != 0 || len(writer.events) != 0 || strings.Contains(fmt.Sprint(r.ops), "save:") || strings.Contains(fmt.Sprint(r.ops), "append-event:") {
		t.Fatalf("with-task failure persisted data: saved=%v events=%v ops=%v", store.saved, writer.events, r.ops)
	}
}

func decodeStallLogRecords(t *testing.T, logs string) []map[string]any {
	t.Helper()
	var records []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("invalid log record %q: %v", line, err)
		}
		records = append(records, record)
	}
	return records
}

func TestCheckStallStateReadAndGuard(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "guard")
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	terminal := testSnapshot(t, id, domain.StateCompleted, start, nil)
	s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): terminal}}
	uc := stallUC(t, id, s, false, nil, start, nil)
	uc.checkOne(context.Background(), id)
	if strings.Contains(fmt.Sprint(r.ops), "clock:") || strings.Contains(fmt.Sprint(r.ops), "liveness:") || len(s.saved) != 0 {
		t.Fatalf("ops=%v", r.ops)
	}
}

func TestCheckStallTickerStopsOnCanceledContext(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "ticker")
	s := &testStore{r: r}
	ticker := &testTicker{ch: make(chan time.Time, 1), r: r}
	uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, false, nil), &testWriter{r: r}, &testClock{r: r, id: id}, testTracker(r), time.Second, testTickerFactory{ticker}, execution.NewLifecycleOwnershipRegistry())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	uc.Run(ctx)
	if ticker.stops != 1 || fmt.Sprint(r.ops) != "[ticker-stop]" {
		t.Fatalf("stops=%d ops=%v", ticker.stops, r.ops)
	}
}

func TestCheckStallOwnershipSuppressesOnlyOrphanTransition(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "owned-dead")
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	last := start.Add(-time.Duration(stallThresholdSeconds+1) * time.Second)
	snapshot := testSnapshot(t, id, domain.StateRunning, start, &last)
	store := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): snapshot}}
	writer := &testWriter{r: r}
	ownership := execution.NewLifecycleOwnershipRegistry()
	_, release, acquired := ownership.Acquire(id)
	if !acquired {
		t.Fatal("ownership acquisition failed")
	}
	uc := newCheckStallUseCaseWithOwnership(store, &testLocker{r: r}, testLive(r, id, true, nil), writer, &testClock{at: start, r: r, id: id}, testTracker(r), time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time), r: r}}, ownership)
	uc.checkOne(context.Background(), id)
	if len(store.saved) != 1 || store.saved[0].State != domain.StateStalled {
		t.Fatalf("owned dead task should continue stall evaluation: saved=%#v", store.saved)
	}
	if len(writer.events) != 1 || writer.events[0] != "TaskStalled" {
		t.Fatalf("events=%v", writer.events)
	}
	release()
	store.saved, writer.events = nil, nil
	uc.checkOne(context.Background(), id)
	if len(store.saved) != 1 || store.saved[0].State != domain.StateOrphaned || len(writer.events) != 1 || writer.events[0] != "TaskOrphanDetected" {
		t.Fatalf("released dead task should be orphaned: saved=%#v events=%v", store.saved, writer.events)
	}
}

func TestCheckStallConstructorAndInvalidTransitionLogger(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "constructor")
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	newCheckStallUseCase(nil, &testLocker{r: r}, testLive(r, id, false, nil), &testWriter{r: r}, &testClock{r: r, id: id}, testTracker(r), time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, execution.NewLifecycleOwnershipRegistry())
}

func TestCheckStallConstructorRejectsTypedNilDependencies(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "typed-nil")
	validTasks := &testStore{r: r}
	validTaskMu := &testLocker{r: r}
	validLiveness := testLive(r, id, false, nil)
	validContract := &testWriter{r: r}
	validClock := &testClock{r: r, id: id}
	validTracker := testTracker(r)
	validTickers := testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}
	validOwnership := execution.NewLifecycleOwnershipRegistry()

	if newCheckStallUseCase(validTasks, validTaskMu, validLiveness, validContract, validClock, validTracker, time.Second, validTickers, validOwnership) == nil {
		t.Fatal("expected use case")
	}
	for _, tc := range []struct {
		name string
		call func()
	}{
		{"tasks", func() {
			newCheckStallUseCase((*testStore)(nil), validTaskMu, validLiveness, validContract, validClock, testTracker(r), time.Second, validTickers, validOwnership)
		}},
		{"task-mutex", func() {
			newCheckStallUseCase(validTasks, (*testLocker)(nil), validLiveness, validContract, validClock, testTracker(r), time.Second, validTickers, validOwnership)
		}},
		{"liveness", func() {
			newCheckStallUseCase(validTasks, validTaskMu, (*execution.CheckLivenessUseCase)(nil), validContract, validClock, testTracker(r), time.Second, validTickers, validOwnership)
		}},
		{"contract-writer", func() {
			newCheckStallUseCase(validTasks, validTaskMu, validLiveness, (*testWriter)(nil), validClock, testTracker(r), time.Second, validTickers, validOwnership)
		}},
		{"clock", func() {
			newCheckStallUseCase(validTasks, validTaskMu, validLiveness, validContract, (*testClock)(nil), testTracker(r), time.Second, validTickers, validOwnership)
		}},
		{"tracker", func() {
			newCheckStallUseCase(validTasks, validTaskMu, validLiveness, validContract, validClock, (*testStalledTimeTracker)(nil), time.Second, validTickers, validOwnership)
		}},
		{"tickers", func() {
			newCheckStallUseCase(validTasks, validTaskMu, validLiveness, validContract, validClock, testTracker(r), time.Second, (*testTickerFactory)(nil), validOwnership)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.call()
		})
	}
}

func TestCheckStallInvalidTransitionLogsAreSeparated(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "invalid-transition")
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	terminal := testSnapshot(t, id, domain.StateCompleted, start, nil)
	task, err := terminal.Restore()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, operation string
		call            func(*checkStallUseCase, *domain.Task)
	}{
		{"stall", "mark-stalled", func(uc *checkStallUseCase, task *domain.Task) { uc.stall(id, terminal, task, 1201, start) }},
		{"orphan", "detect-orphan", func(uc *checkStallUseCase, task *domain.Task) { uc.orphan(id, terminal, task, start) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs bytes.Buffer
			uc := stallUC(t, id, &testStore{r: r}, false, nil, start, slog.New(slog.NewTextHandler(&logs, nil)))
			tc.call(uc, task)
			for _, want := range []string{taskInvalidTransitionCode, "operation=" + tc.operation, "task_id=" + id.String(), "state=completed"} {
				if !strings.Contains(logs.String(), want) {
					t.Fatalf("missing %q: %s", want, logs.String())
				}
			}
		})
	}
}

func TestCheckStallTrackerRunningToStalled(t *testing.T) {
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		save  error
		calls int
	}{
		{"save-success", nil, 1},
		{"save-failure", errors.New("save failed"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &testRecorder{}
			id := testID(t, "tracker-stall-"+tc.name)
			tracker := testTracker(r)
			now := start.Add(1201 * time.Second)
			s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): testSnapshot(t, id, domain.StateRunning, start, &start)}, saveErr: map[string]error{id.String(): tc.save}}
			uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, false, nil), &testWriter{r: r}, &testClock{at: now, r: r, id: id}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time), r: r}}, execution.NewLifecycleOwnershipRegistry())
			uc.checkOne(context.Background(), id)
			if len(tracker.calls) != tc.calls || tracker.takeCalls != 0 {
				t.Fatalf("calls=%v take=%d", tracker.calls, tracker.takeCalls)
			}
			if tc.calls == 1 && (tracker.calls[0].kind != "enter" || tracker.calls[0].id != id || !tracker.calls[0].at.Equal(now)) {
				t.Fatalf("calls=%v", tracker.calls)
			}
		})
	}
}

func TestCheckStallTrackerStalledSelfLoopDoesNotRestart(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "tracker-self-loop")
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	last := start.Add(-1201 * time.Second)
	tracker := testTracker(r)
	s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): testSnapshot(t, id, domain.StateStalled, start, &last)}}
	uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, false, nil), &testWriter{r: r}, &testClock{at: start, r: r, id: id}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time), r: r}}, execution.NewLifecycleOwnershipRegistry())
	uc.checkOne(context.Background(), id)
	if len(s.saved) != 1 || len(tracker.calls) != 0 || tracker.takeCalls != 0 {
		t.Fatalf("saved=%v calls=%v take=%d", s.saved, tracker.calls, tracker.takeCalls)
	}
}

func TestCheckStallTrackerOrphanTransitions(t *testing.T) {
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name  string
		state domain.TaskState
		save  error
		calls int
	}{
		{"stalled-save-success", domain.StateStalled, nil, 1},
		{"stalled-save-failure", domain.StateStalled, errors.New("save failed"), 0},
		{"running-save-success", domain.StateRunning, nil, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &testRecorder{}
			id := testID(t, "tracker-orphan-"+tc.name)
			tracker := testTracker(r)
			s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): testSnapshot(t, id, tc.state, start, nil)}, saveErr: map[string]error{id.String(): tc.save}}
			uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, true, nil), &testWriter{r: r}, &testClock{at: start, r: r, id: id}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time), r: r}}, execution.NewLifecycleOwnershipRegistry())
			uc.checkOne(context.Background(), id)
			if len(tracker.calls) != tc.calls || tracker.takeCalls != 0 {
				t.Fatalf("calls=%v take=%d", tracker.calls, tracker.takeCalls)
			}
			if tc.calls == 1 && (tracker.calls[0].kind != "leave" || tracker.calls[0].id != id || !tracker.calls[0].at.Equal(start)) {
				t.Fatalf("calls=%v", tracker.calls)
			}
		})
	}
}

func TestCheckStallTrackerIgnoresAppendEventResult(t *testing.T) {
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, state := range []domain.TaskState{domain.StateRunning, domain.StateStalled} {
		r := &testRecorder{}
		id := testID(t, "tracker-append-"+string(state))
		tracker := testTracker(r)
		last := start.Add(-1201 * time.Second)
		if state == domain.StateStalled {
			last = start
		}
		s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): testSnapshot(t, id, state, start, &last)}}
		uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, state == domain.StateStalled, nil), &testWriter{r: r, eventErr: errors.New("append failed")}, &testClock{at: start, r: r, id: id}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time), r: r}}, execution.NewLifecycleOwnershipRegistry())
		uc.checkOne(context.Background(), id)
		if len(tracker.calls) != 1 || tracker.takeCalls != 0 {
			t.Fatalf("state=%s calls=%v take=%d", state, tracker.calls, tracker.takeCalls)
		}
	}
}

func TestCheckStallTrackerCallsWhileTaskLockHeld(t *testing.T) {
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name             string
		state            domain.TaskState
		dead             bool
		trackerOp, event string
	}{
		{"enter", domain.StateRunning, false, "enter-stalled", "TaskStalled"},
		{"leave", domain.StateStalled, true, "leave-stalled", "TaskOrphanDetected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &testRecorder{}
			id := testID(t, "tracker-lock-"+tc.name)
			locker := &testLocker{r: r}
			tracker := testTracker(r)
			tracker.locker = locker
			last := start.Add(-1201 * time.Second)
			if tc.state == domain.StateStalled {
				last = start
			}
			s := &testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): testSnapshot(t, id, tc.state, start, &last)}}
			uc := newCheckStallUseCase(s, locker, testLive(r, id, tc.dead, nil), &testWriter{r: r}, &testClock{at: start, r: r, id: id}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time), r: r}}, execution.NewLifecycleOwnershipRegistry())
			uc.checkOne(context.Background(), id)
			if len(tracker.held) != 1 || !tracker.held[0] {
				t.Fatalf("held=%v", tracker.held)
			}
			assertOrderedSubsequence(t, r.ops, []string{"lock:" + id.String(), "load:" + id.String(), "clock:" + id.String(), "liveness:" + id.String(), "save:" + id.String(), tc.trackerOp + ":" + id.String(), "append-event:" + id.String() + ":" + tc.event, "unlock:" + id.String()})
		})
	}
}

func TestCheckStallTrackerIgnoresOutOfScopeStatePair(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "tracker-out-of-scope")
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	snapshot := testSnapshot(t, id, domain.StateRunning, now, nil)
	task, err := snapshot.Restore()
	if err != nil {
		t.Fatal(err)
	}
	tracker := testTracker(r)
	uc := newCheckStallUseCase(&testStore{r: r}, &testLocker{r: r}, testLive(r, id, false, nil), &testWriter{r: r}, &testClock{at: now, r: r, id: id}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time), r: r}}, execution.NewLifecycleOwnershipRegistry())
	uc.persist(id, snapshot, task, nil, now, "mark-stalled")
	if len(tracker.calls) != 0 || tracker.takeCalls != 0 {
		t.Fatalf("calls=%v take=%d", tracker.calls, tracker.takeCalls)
	}
}

func TestCheckStallInvalidTransitionDoesNotPersistOrTrack(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "tracker-invalid")
	now := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	terminal := testSnapshot(t, id, domain.StateCompleted, now, nil)
	task, err := terminal.Restore()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		call func(*checkStallUseCase)
	}{
		{"mark-stalled", func(uc *checkStallUseCase) { uc.stall(id, terminal, task, 1201, now) }},
		{"detect-orphan", func(uc *checkStallUseCase) { uc.orphan(id, terminal, task, now) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, writer, tracker := &testStore{r: r}, &testWriter{r: r}, testTracker(r)
			uc := newCheckStallUseCase(store, &testLocker{r: r}, testLive(r, id, false, nil), writer, &testClock{at: now, r: r, id: id}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time), r: r}}, execution.NewLifecycleOwnershipRegistry())
			tc.call(uc)
			if len(store.saved) != 0 || len(writer.events) != 0 || len(tracker.calls) != 0 || tracker.takeCalls != 0 {
				t.Fatalf("saved=%v events=%v calls=%v take=%d", store.saved, writer.events, tracker.calls, tracker.takeCalls)
			}
		})
	}
}

func TestCheckStallTrackerNeverTakesTotal(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "tracker-no-total")
	start := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	last := start.Add(-1201 * time.Second)
	tracker := testTracker(r)
	uc := newCheckStallUseCase(&testStore{r: r, loads: map[string]domain.TaskSnapshot{id.String(): testSnapshot(t, id, domain.StateRunning, start, &last)}}, &testLocker{r: r}, testLive(r, id, false, nil), &testWriter{r: r}, &testClock{at: start, r: r, id: id}, tracker, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time), r: r}}, execution.NewLifecycleOwnershipRegistry())
	uc.checkOne(context.Background(), id)
	if tracker.takeCalls != 0 {
		t.Fatalf("take total calls=%d", tracker.takeCalls)
	}
}
