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
	return newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, dead, liveErr), &testWriter{r: r}, &testClock{at: now, r: r, id: id}, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, logger)
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
			uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, false, nil), w, &testClock{at: start.Add(tc.gap), r: r, id: id}, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}})
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
	uc := newCheckStallUseCase(s, &testLocker{r: r}, execution.NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { cancel(); return false, nil }), func(domain.TaskID) string { return "lock" }), &testWriter{r: r}, &testClock{at: start.Add(1201 * time.Second), r: r, id: one}, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}})
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
	uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, true, nil), w, &testClock{at: now, r: r, id: id}, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}})
	uc.scan(context.Background())
	if fmt.Sprint(s.listStates[0]) != fmt.Sprint([]domain.TaskState{domain.StateRunning, domain.StateStalled}) || len(w.events) != 1 || w.events[0] != "TaskOrphanDetected" || s.saved[0].State != domain.StateOrphaned {
		t.Fatalf("states=%v events=%v saved=%#v", s.listStates, w.events, s.saved)
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
	uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, one, false, nil), w, &testClock{at: start.Add(1201 * time.Second), r: r, id: one}, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, slog.New(slog.NewTextHandler(&logs, nil)))
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
			uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, false, tc.err), w, &testClock{at: start, r: r, id: id}, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, logger)

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
	uc := newCheckStallUseCase(store, &testLocker{r: r}, testLive(r, id, false, nil), writer, &testClock{at: now, r: r, id: id}, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}, slog.New(slog.NewJSONHandler(&logs, nil)))

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
	uc := newCheckStallUseCase(s, &testLocker{r: r}, testLive(r, id, false, nil), &testWriter{r: r}, &testClock{r: r, id: id}, time.Second, testTickerFactory{ticker})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	uc.Run(ctx)
	if ticker.stops != 1 || fmt.Sprint(r.ops) != "[ticker-stop]" {
		t.Fatalf("stops=%d ops=%v", ticker.stops, r.ops)
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
	newCheckStallUseCase(nil, &testLocker{r: r}, testLive(r, id, false, nil), &testWriter{r: r}, &testClock{r: r, id: id}, time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}})
}

func TestCheckStallConstructorRejectsTypedNilDependencies(t *testing.T) {
	r := &testRecorder{}
	id := testID(t, "typed-nil")
	validTasks := &testStore{r: r}
	validTaskMu := &testLocker{r: r}
	validLiveness := testLive(r, id, false, nil)
	validContract := &testWriter{r: r}
	validClock := &testClock{r: r, id: id}
	validTickers := testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: r}}

	if newCheckStallUseCase(validTasks, validTaskMu, validLiveness, validContract, validClock, time.Second, validTickers) == nil {
		t.Fatal("expected use case")
	}
	for _, tc := range []struct {
		name string
		call func()
	}{
		{"tasks", func() {
			newCheckStallUseCase((*testStore)(nil), validTaskMu, validLiveness, validContract, validClock, time.Second, validTickers)
		}},
		{"task-mutex", func() {
			newCheckStallUseCase(validTasks, (*testLocker)(nil), validLiveness, validContract, validClock, time.Second, validTickers)
		}},
		{"liveness", func() {
			newCheckStallUseCase(validTasks, validTaskMu, (*execution.CheckLivenessUseCase)(nil), validContract, validClock, time.Second, validTickers)
		}},
		{"contract-writer", func() {
			newCheckStallUseCase(validTasks, validTaskMu, validLiveness, (*testWriter)(nil), validClock, time.Second, validTickers)
		}},
		{"clock", func() {
			newCheckStallUseCase(validTasks, validTaskMu, validLiveness, validContract, (*testClock)(nil), time.Second, validTickers)
		}},
		{"tickers", func() {
			newCheckStallUseCase(validTasks, validTaskMu, validLiveness, validContract, validClock, time.Second, (*testTickerFactory)(nil))
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
