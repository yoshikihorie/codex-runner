package metrics

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type recordStoreFake struct {
	snapshot domain.TaskSnapshot
	err      error
	calls    int
}

func (f *recordStoreFake) Load(domain.TaskID) (domain.TaskSnapshot, error) {
	f.calls++
	return f.snapshot, f.err
}
func (*recordStoreFake) Save(domain.TaskID, domain.TaskSnapshot) error { return nil }
func (*recordStoreFake) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	return nil, nil
}
func (*recordStoreFake) Reserve(domain.TaskID) error            { return nil }
func (*recordStoreFake) Release(domain.TaskID) error            { return nil }
func (*recordStoreFake) IsReserved(domain.TaskID) (bool, error) { return false, nil }

var _ store.TaskStore = (*recordStoreFake)(nil)

type recordEventsFake struct {
	events []store.EventRecord
	err    error
	calls  int
}

func (f *recordEventsFake) ReadFrom(domain.TaskID, int) ([]store.EventRecord, error) {
	f.calls++
	return f.events, f.err
}

type recordContractFake struct {
	prompt, last, partial       []byte
	promptErr, lastErr, partErr error
	calls                       []string
}

func (*recordContractFake) ReadStderrLog(domain.TaskID) ([]byte, error) { return nil, nil }
func (*recordContractFake) ReadLastMessage(domain.TaskID) (bool, error) { return false, nil }
func (f *recordContractFake) ReadPromptContent(domain.TaskID) ([]byte, error) {
	f.calls = append(f.calls, "prompt")
	return f.prompt, f.promptErr
}
func (f *recordContractFake) ReadLastMessageContent(domain.TaskID) ([]byte, error) {
	f.calls = append(f.calls, "last-message")
	return f.last, f.lastErr
}
func (f *recordContractFake) ReadPartialOutputContent(domain.TaskID) ([]byte, error) {
	f.calls = append(f.calls, "partial-output")
	return f.partial, f.partErr
}
func (*recordContractFake) ReadExitCode(domain.TaskID) (int, bool, error) { return 0, false, nil }

var _ store.ContractReader = (*recordContractFake)(nil)

type recordWriterFake struct {
	mu    sync.Mutex
	lines [][]byte
	month []string
	err   error
}

func (f *recordWriterFake) Append(_ domain.TaskID, month string, line []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.month = append(f.month, month)
	f.lines = append(f.lines, append([]byte(nil), line...))
	return f.err
}

func recordID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260816-120000-a1b2-metrics")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func recordSnapshot(t *testing.T, state domain.TaskState) domain.TaskSnapshot {
	t.Helper()
	requested := time.Date(2026, 7, 31, 23, 59, 0, 0, time.UTC)
	started := requested.Add(time.Minute)
	finished := started.Add(2 * time.Minute)
	return domain.TaskSnapshot{TaskID: recordID(t), Subcommand: domain.SubcommandImpl, Model: "test-model", RequestedAt: requested, ProcessStartedAt: &started, State: state, StateUpdatedAt: finished, Route: domain.ExecutionRouteDaemon}
}

func newRecordUseCase(t *testing.T, state domain.TaskState, content bool) (*RecordTaskMetricsUseCase, *recordStoreFake, *recordEventsFake, *recordContractFake, *recordWriterFake) {
	t.Helper()
	tasks := &recordStoreFake{snapshot: recordSnapshot(t, state)}
	events := &recordEventsFake{}
	contract := &recordContractFake{prompt: []byte("prompt\n"), last: []byte("last\n")}
	writer := &recordWriterFake{}
	uc := NewRecordTaskMetricsUseCase(tasks, events, contract, writer, content, domain.ClockFunc(func() time.Time { return time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC) }), "daemon-test", nil)
	return uc, tasks, events, contract, writer
}

func TestRecordTaskMetricsExecute_SCNMetrics0101_CompletedWritesOneRecord(t *testing.T) {
	uc, _, _, _, writer := newRecordUseCase(t, domain.StateCompleted, false)
	out := uc.Execute(context.Background(), RecordTaskMetricsInput{TaskID: recordID(t), FinalState: domain.StateCompleted, OccurredAt: time.Date(2026, 7, 31, 23, 59, 0, 0, time.UTC)})
	if !out.Recorded || len(writer.lines) != 1 || len(writer.month) != 1 || writer.month[0] != "2026-07" {
		t.Fatalf("output=%+v lines=%d months=%v", out, len(writer.lines), writer.month)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(writer.lines[0], &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 41 {
		t.Fatalf("record has %d keys, want 41", len(fields))
	}
	for _, name := range metricsRecordKeys() {
		if _, ok := fields[name]; !ok {
			t.Fatalf("missing key %q", name)
		}
	}
}

func metricsRecordKeys() []string {
	return []string{
		"schema_version", "task_id", "subcommand", "model", "reasoning_effort", "route", "requested_at", "started_at", "finished_at", "queued_ms", "startup_ms", "run_ms", "total_ms", "final_state", "exit_code", "exit_code_class", "estimated", "prompt_bytes", "prompt_lines", "prompt_sha256", "prompt_body", "last_message_bytes", "last_message_lines", "last_message_sha256", "last_message_body", "event_count", "max_event_gap_ms", "stalled_total_ms", "recovered", "recovery_origin", "partial_output_bytes", "timeout_requested_seconds", "timeout_resolved_seconds", "timed_out", "cancel_requested", "input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens", "daemon_version", "codex_cli_version",
	}
}

func TestRecordTaskMetricsExecute_SCNMetrics0103_AllTerminalStatesUseSameContract(t *testing.T) {
	for _, state := range []domain.TaskState{domain.StateCompleted, domain.StateFailed, domain.StateRecovered, domain.StateTimeoutLost, domain.StateKilled, domain.StateLost} {
		t.Run(string(state), func(t *testing.T) {
			uc, _, _, _, writer := newRecordUseCase(t, state, false)
			out := uc.Execute(context.Background(), RecordTaskMetricsInput{TaskID: recordID(t), FinalState: state, OccurredAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)})
			if !out.Recorded || len(writer.lines) != 1 {
				t.Fatalf("output=%+v writes=%d", out, len(writer.lines))
			}
		})
	}
}

func TestRecordTaskMetricsExecute_SCNMetrics0104_FailuresAreFailSoft(t *testing.T) {
	uc, tasks, events, contract, writer := newRecordUseCase(t, domain.StateCompleted, false)
	events.err = errors.New("read events failed")
	out := uc.Execute(context.Background(), RecordTaskMetricsInput{TaskID: recordID(t), FinalState: domain.StateCompleted, OccurredAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)})
	if out.Recorded || tasks.calls != 1 || events.calls != 1 || len(contract.calls) != 0 || len(writer.lines) != 0 {
		t.Fatalf("output=%+v task=%d events=%d contract=%v writes=%d", out, tasks.calls, events.calls, contract.calls, len(writer.lines))
	}
}

func TestRecordTaskMetricsExecute_SCNMetrics0107_ContentDisabledKeepsBodiesNull(t *testing.T) {
	uc, _, _, _, writer := newRecordUseCase(t, domain.StateCompleted, false)
	if out := uc.Execute(context.Background(), RecordTaskMetricsInput{TaskID: recordID(t), FinalState: domain.StateCompleted, OccurredAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)}); !out.Recorded {
		t.Fatal("record was not written")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(writer.lines[0], &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["prompt_body"]) != "null" || string(fields["last_message_body"]) != "null" {
		t.Fatalf("bodies=%s,%s", fields["prompt_body"], fields["last_message_body"])
	}
}

func TestRecordTaskMetricsExecute_SCNMetrics0110_AllNonTerminalStatesAreRejected(t *testing.T) {
	for _, state := range []domain.TaskState{domain.StateQueued, domain.StateStarting, domain.StateRunning, domain.StateStalled, domain.StateTimeout, domain.StateRecovering, domain.StateCancelling, domain.StateAdopted, domain.StateOrphaned} {
		t.Run(string(state), func(t *testing.T) {
			uc, tasks, events, contract, writer := newRecordUseCase(t, domain.StateCompleted, false)
			out := uc.Execute(context.Background(), RecordTaskMetricsInput{TaskID: recordID(t), FinalState: state, OccurredAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)})
			if out.Recorded || tasks.calls != 0 || events.calls != 0 || len(contract.calls) != 0 || len(writer.lines) != 0 {
				t.Fatalf("output=%+v task=%d events=%d contract=%v writes=%d", out, tasks.calls, events.calls, contract.calls, len(writer.lines))
			}
		})
	}
}

func executeRecord(t *testing.T, uc *RecordTaskMetricsUseCase, state domain.TaskState) RecordTaskMetricsOutput {
	t.Helper()
	return uc.Execute(context.Background(), RecordTaskMetricsInput{
		TaskID: recordID(t), FinalState: state, OccurredAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), StalledTotalMs: 7,
	})
}

func recordFields(t *testing.T, writer *recordWriterFake) map[string]json.RawMessage {
	t.Helper()
	if len(writer.lines) != 1 {
		t.Fatalf("writes=%d", len(writer.lines))
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(writer.lines[0], &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func fieldString(t *testing.T, fields map[string]json.RawMessage, name string) string {
	t.Helper()
	return string(fields[name])
}

func TestRecordTaskMetricsExecute_SCNMetrics0102_FailedWithoutLastMessageRecordsZeroLength(t *testing.T) {
	for _, last := range [][]byte{nil, {}} {
		t.Run(fmt.Sprintf("last-%v", last == nil), func(t *testing.T) {
			uc, _, _, contract, writer := newRecordUseCase(t, domain.StateFailed, false)
			contract.last = last
			if out := executeRecord(t, uc, domain.StateFailed); !out.Recorded {
				t.Fatalf("output=%+v", out)
			}
			fields := recordFields(t, writer)
			if fieldString(t, fields, "last_message_bytes") != "0" || fieldString(t, fields, "last_message_lines") != "0" || fieldString(t, fields, "last_message_sha256") != "null" {
				t.Fatalf("last message fields=%s,%s,%s", fields["last_message_bytes"], fields["last_message_lines"], fields["last_message_sha256"])
			}
		})
	}
}

func TestRecordTaskMetricsExecute_SCNMetrics0105_AdoptedTaskUsesInputEstimatedAndRecoveryOrigin(t *testing.T) {
	uc, tasks, _, _, writer := newRecordUseCase(t, domain.StateRecovered, false)
	origin := domain.RecoveryOriginOrphan
	tasks.snapshot.AdoptedAfterRestart, tasks.snapshot.Recovered, tasks.snapshot.RecoveryOrigin = true, true, &origin
	if out := uc.Execute(context.Background(), RecordTaskMetricsInput{
		TaskID: recordID(t), FinalState: domain.StateRecovered, Estimated: true,
		OccurredAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), StalledTotalMs: 7,
	}); !out.Recorded {
		t.Fatalf("output=%+v", out)
	}
	fields := recordFields(t, writer)
	if fieldString(t, fields, "estimated") != "true" || fieldString(t, fields, "recovered") != "true" || fieldString(t, fields, "recovery_origin") != `"orphan"` || fieldString(t, fields, "stalled_total_ms") != "7" {
		t.Fatalf("adoption fields: estimated=%s recovered=%s origin=%s stalled=%s", fields["estimated"], fields["recovered"], fields["recovery_origin"], fields["stalled_total_ms"])
	}
}

func TestRecordTaskMetricsExecute_EstimatedIsIndependentOfSnapshotAdoptedAfterRestart(t *testing.T) {
	cases := []struct {
		name                string
		estimated           bool
		adoptedAfterRestart bool
		wantEstimatedJSON   string
	}{
		{name: "live orphan finalize", estimated: true, adoptedAfterRestart: false, wantEstimatedJSON: "true"},
		{name: "estimated kill", estimated: true, adoptedAfterRestart: false, wantEstimatedJSON: "true"},
		{name: "startup adoption recovery", estimated: true, adoptedAfterRestart: true, wantEstimatedJSON: "true"},
		{name: "pending recovery reconciliation", estimated: true, adoptedAfterRestart: true, wantEstimatedJSON: "true"},
		{name: "timeout or live-orphan resume", estimated: true, adoptedAfterRestart: false, wantEstimatedJSON: "true"},
		{name: "inverse independence", estimated: false, adoptedAfterRestart: true, wantEstimatedJSON: "false"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uc, tasks, _, _, writer := newRecordUseCase(t, domain.StateRecovered, false)
			tasks.snapshot.AdoptedAfterRestart = tc.adoptedAfterRestart
			out := uc.Execute(context.Background(), RecordTaskMetricsInput{
				TaskID: recordID(t), FinalState: domain.StateRecovered, Estimated: tc.estimated,
				OccurredAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
			})
			if !out.Recorded {
				t.Fatalf("output=%+v", out)
			}
			if got := fieldString(t, recordFields(t, writer), "estimated"); got != tc.wantEstimatedJSON {
				t.Fatalf("estimated=%s, want %s", got, tc.wantEstimatedJSON)
			}
		})
	}
}

func TestRecordTaskMetricsExecute_SCNMetrics0106_OccurredAtSelectsMonth(t *testing.T) {
	for _, occurred := range []time.Time{time.Date(2026, 7, 31, 23, 59, 0, 0, time.UTC), time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)} {
		uc, _, _, _, writer := newRecordUseCase(t, domain.StateCompleted, false)
		out := uc.Execute(context.Background(), RecordTaskMetricsInput{TaskID: recordID(t), FinalState: domain.StateCompleted, OccurredAt: occurred})
		if !out.Recorded || writer.month[0] != occurred.Format("2006-01") {
			t.Fatalf("output=%+v month=%v", out, writer.month)
		}
	}
}

func TestRecordTaskMetricsExecute_SCNMetrics0108_ContentEnabledStoresBodiesAndSameDerivatives(t *testing.T) {
	disabled, _, _, disabledContract, disabledWriter := newRecordUseCase(t, domain.StateCompleted, false)
	enabled, _, _, enabledContract, enabledWriter := newRecordUseCase(t, domain.StateCompleted, true)
	contentPrompt, contentLast := []byte("日本語\n🙂"), []byte("last\nmessage")
	disabledContract.prompt, disabledContract.last = contentPrompt, contentLast
	enabledContract.prompt, enabledContract.last = contentPrompt, contentLast
	if !executeRecord(t, disabled, domain.StateCompleted).Recorded || !executeRecord(t, enabled, domain.StateCompleted).Recorded {
		t.Fatal("record was not written")
	}
	a, b := recordFields(t, disabledWriter), recordFields(t, enabledWriter)
	if fieldString(t, a, "prompt_body") != "null" || fieldString(t, a, "last_message_body") != "null" || fieldString(t, b, "prompt_body") != `"日本語\n🙂"` || fieldString(t, b, "last_message_body") != `"last\nmessage"` {
		t.Fatalf("body mismatch")
	}
	for _, name := range []string{"prompt_bytes", "prompt_lines", "prompt_sha256", "last_message_bytes", "last_message_lines", "last_message_sha256"} {
		if !bytes.Equal(a[name], b[name]) {
			t.Fatalf("%s differs: %s != %s", name, a[name], b[name])
		}
	}
}

func TestRecordTaskMetricsExecute_SCNMetrics0109_ConcurrentCallsProduceFourIndependentRecords(t *testing.T) {
	writer := &recordWriterFake{}
	var group sync.WaitGroup
	ids := make(chan string, 4)
	for i := 0; i < 4; i++ {
		i := i
		group.Add(1)
		go func() {
			defer group.Done()
			uc, _, _, _, _ := newRecordUseCase(t, domain.StateCompleted, false)
			id, err := domain.NewTaskID(fmt.Sprintf("impl-20260816-12000%d-a1b2-metrics", i))
			if err != nil {
				t.Error(err)
				return
			}
			uc.tasks.(*recordStoreFake).snapshot.TaskID = id
			uc.writer = writer
			out := uc.Execute(context.Background(), RecordTaskMetricsInput{TaskID: id, FinalState: domain.StateCompleted, OccurredAt: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)})
			if !out.Recorded {
				t.Error("record failed")
				return
			}
			ids <- id.String()
		}()
	}
	group.Wait()
	close(ids)
	if len(writer.lines) != 4 {
		t.Fatalf("writes=%d", len(writer.lines))
	}
	seen := map[string]bool{}
	for _, line := range writer.lines {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(line, &fields); err != nil || len(fields) != 41 {
			t.Fatalf("line=%s err=%v", line, err)
		}
		var taskID string
		if err := json.Unmarshal(fields["task_id"], &taskID); err != nil {
			t.Fatal(err)
		}
		seen[taskID] = true
	}
	for id := range ids {
		if !seen[id] {
			t.Fatalf("missing task id %s", id)
		}
	}
}

func TestRecordTaskMetricsExecute_FailureMatrixStopsAtFailingStage(t *testing.T) {
	for _, stage := range []string{"load", "events", "prompt", "last", "partial", "marshal", "writer"} {
		t.Run(stage, func(t *testing.T) {
			uc, tasks, events, contract, writer := newRecordUseCase(t, domain.StateCompleted, false)
			switch stage {
			case "load":
				tasks.err = errors.New("load")
			case "events":
				events.err = errors.New("events")
			case "prompt":
				contract.promptErr = errors.New("prompt")
			case "last":
				contract.lastErr = errors.New("last")
			case "partial":
				contract.partErr = errors.New("partial")
			case "marshal":
				uc.marshal = func(any) ([]byte, error) { return nil, errors.New("marshal") }
			case "writer":
				writer.err = errors.New("writer")
			}
			if out := executeRecord(t, uc, domain.StateCompleted); out.Recorded {
				t.Fatal("failure was recorded")
			}
			if tasks.calls != 1 || events.calls > 1 || len(contract.calls) > 3 || len(writer.lines) > 1 {
				t.Fatalf("retried dependencies: task=%d events=%d contract=%v writer=%d", tasks.calls, events.calls, contract.calls, len(writer.lines))
			}
		})
	}
}

func TestRecordTaskMetricsConstructorValidationLoggerAndVersionCopy(t *testing.T) {
	uc, _, _, _, _ := newRecordUseCase(t, domain.StateCompleted, false)
	if uc.logger == nil {
		t.Fatal("default logger is nil")
	}
	version := "1.0"
	uc = NewRecordTaskMetricsUseCase(uc.tasks, uc.events, uc.contract, uc.writer, false, uc.clock, "daemon", &version, nil)
	version = "2.0"
	if *uc.codexCLIVersion != "1.0" {
		t.Fatalf("version=%q", *uc.codexCLIVersion)
	}
	for _, dependency := range []int{0, 1, 2, 3, 4} {
		t.Run(fmt.Sprintf("nil-%d", dependency), func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("want panic")
				}
			}()
			tasks, events, contract, writer, clock := uc.tasks, uc.events, uc.contract, uc.writer, uc.clock
			switch dependency {
			case 0:
				tasks = nil
			case 1:
				events = nil
			case 2:
				contract = nil
			case 3:
				writer = nil
			case 4:
				clock = nil
			}
			NewRecordTaskMetricsUseCase(tasks, events, contract, writer, false, clock, "", nil)
		})
	}
	defer func() {
		if recover() == nil {
			t.Fatal("want logger panic")
		}
	}()
	NewRecordTaskMetricsUseCase(uc.tasks, uc.events, uc.contract, uc.writer, false, uc.clock, "", nil, slog.Default(), slog.Default())
}

func TestRecordTaskMetricsCallOrderAndStaleSnapshot(t *testing.T) {
	uc, tasks, events, contract, writer := newRecordUseCase(t, domain.StateCompleted, false)
	if !executeRecord(t, uc, domain.StateCompleted).Recorded {
		t.Fatal("record failed")
	}
	if tasks.calls != 1 || events.calls != 1 || strings.Join(contract.calls, ",") != "prompt,last-message,partial-output" || len(writer.lines) != 1 {
		t.Fatalf("calls task=%d events=%d contract=%v writer=%d", tasks.calls, events.calls, contract.calls, len(writer.lines))
	}
	uc, tasks, events, contract, writer = newRecordUseCase(t, domain.StateCompleted, false)
	tasks.snapshot.State = domain.StateRecovering
	if out := executeRecord(t, uc, domain.StateCompleted); out.Recorded || tasks.calls != 1 || events.calls != 0 || len(contract.calls) != 0 || len(writer.lines) != 0 {
		t.Fatalf("stale snapshot was processed")
	}
}

func TestRecordTaskMetricsDurationsEventsAndTokens(t *testing.T) {
	uc, tasks, events, _, writer := newRecordUseCase(t, domain.StateCompleted, false)
	requested := tasks.snapshot.RequestedAt
	events.events = []store.EventRecord{
		{Seq: 3, RecordedAt: requested.Add(9 * time.Minute), EventType: "turn.completed", Raw: map[string]any{"usage": map[string]any{"input_tokens": float64(4), "cached_input_tokens": "bad", "output_tokens": float64(2.5), "reasoning_output_tokens": float64(3)}}},
		{Seq: 1, RecordedAt: requested.Add(30 * time.Second), EventType: "TaskStarted", Raw: map[string]any{"occurred_at": requested.Add(30 * time.Second).Format(time.RFC3339)}},
		{Seq: 2, RecordedAt: requested.Add(2 * time.Minute), EventType: "TaskCancelRequested"},
		{Seq: 4, RecordedAt: requested.Add(10 * time.Minute), EventType: "TaskExited", Raw: map[string]any{"occurred_at": requested.Add(4 * time.Minute).Format(time.RFC3339)}},
	}
	original := append([]store.EventRecord(nil), events.events...)
	if !executeRecord(t, uc, domain.StateCompleted).Recorded {
		t.Fatal("record failed")
	}
	fields := recordFields(t, writer)
	for name, want := range map[string]string{"queued_ms": "30000", "startup_ms": "30000", "run_ms": "180000", "total_ms": "180000", "event_count": "4", "max_event_gap_ms": "420000", "cancel_requested": "true", "input_tokens": "4", "cached_input_tokens": "null", "output_tokens": "null", "reasoning_output_tokens": "3"} {
		if fieldString(t, fields, name) != want {
			t.Fatalf("%s=%s want %s", name, fields[name], want)
		}
	}
	for i := range original {
		if events.events[i].Seq != original[i].Seq {
			t.Fatal("events were mutated")
		}
	}
	uc, _, events, _, writer = newRecordUseCase(t, domain.StateCompleted, false)
	if !executeRecord(t, uc, domain.StateCompleted).Recorded {
		t.Fatal("record without TaskStarted failed")
	}
	fields = recordFields(t, writer)
	if fieldString(t, fields, "queued_ms") != "180000" || fieldString(t, fields, "startup_ms") != "null" || fieldString(t, fields, "run_ms") != "null" {
		t.Fatalf("no TaskStarted durations=%s,%s,%s", fields["queued_ms"], fields["startup_ms"], fields["run_ms"])
	}
}

func TestRecordTaskMetricsBodiesHashesAndPartialOutput(t *testing.T) {
	uc, _, _, contract, writer := newRecordUseCase(t, domain.StateCompleted, true)
	contract.prompt, contract.last, contract.partial = []byte{}, []byte("a\nb\n"), []byte{}
	if !executeRecord(t, uc, domain.StateCompleted).Recorded {
		t.Fatal("record failed")
	}
	fields := recordFields(t, writer)
	if fieldString(t, fields, "prompt_sha256") != `"e3b0c44298fc1c14"` || fieldString(t, fields, "last_message_lines") != "2" || fieldString(t, fields, "partial_output_bytes") != "0" {
		t.Fatalf("hash/line/partial mismatch")
	}
	uc, _, _, contract, writer = newRecordUseCase(t, domain.StateCompleted, false)
	contract.partial = nil
	if !executeRecord(t, uc, domain.StateCompleted).Recorded || fieldString(t, recordFields(t, writer), "partial_output_bytes") != "null" {
		t.Fatal("missing partial output is not null")
	}
}

func TestRecordTaskMetricsInvalidInputDerivedValuesMarshalAndSafeLogs(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	uc, tasks, events, contract, writer := newRecordUseCase(t, domain.StateCompleted, false)
	uc.logger = logger
	for _, input := range []RecordTaskMetricsInput{{TaskID: recordID(t), FinalState: domain.StateCompleted}, {TaskID: recordID(t), FinalState: domain.StateCompleted, OccurredAt: time.Now(), StalledTotalMs: -1}} {
		if uc.Execute(context.Background(), input).Recorded {
			t.Fatal("invalid input recorded")
		}
	}
	if tasks.calls != 0 || events.calls != 0 || len(contract.calls) != 0 || len(writer.lines) != 0 {
		t.Fatal("invalid input called dependency")
	}
	uc, tasks, events, _, writer = newRecordUseCase(t, domain.StateCompleted, false)
	events.events = []store.EventRecord{{Seq: 1, RecordedAt: time.Now(), EventType: "TaskStarted", Raw: map[string]any{"occurred_at": tasks.snapshot.RequestedAt.Add(-time.Hour).Format(time.RFC3339)}}}
	if executeRecord(t, uc, domain.StateCompleted).Recorded || len(writer.lines) != 0 {
		t.Fatal("negative duration recorded")
	}
	uc, _, _, contract, writer = newRecordUseCase(t, domain.StateCompleted, false)
	contract.promptErr = errors.New("secret-body")
	uc.logger = logger
	if executeRecord(t, uc, domain.StateCompleted).Recorded || strings.Contains(logs.String(), "secret-body") || !strings.Contains(logs.String(), "stage=read-prompt") || !strings.Contains(logs.String(), "task_id=") {
		t.Fatalf("unsafe or incomplete log: %s", logs.String())
	}
	uc, _, _, _, writer = newRecordUseCase(t, domain.StateCompleted, false)
	uc.marshal = func(any) ([]byte, error) { return nil, errors.New("marshal") }
	if executeRecord(t, uc, domain.StateCompleted).Recorded || len(writer.lines) != 0 {
		t.Fatal("marshal failure wrote record")
	}
}

func TestRecordTaskMetricsVersionsComeOnlyFromConstructor(t *testing.T) {
	uc, _, _, _, writer := newRecordUseCase(t, domain.StateCompleted, false)
	version := "cli-1"
	uc.codexCLIVersion = &version
	uc.daemonVersion = "daemon-1"
	if !executeRecord(t, uc, domain.StateCompleted).Recorded {
		t.Fatal("record failed")
	}
	fields := recordFields(t, writer)
	if fieldString(t, fields, "daemon_version") != `"daemon-1"` || fieldString(t, fields, "codex_cli_version") != `"cli-1"` {
		t.Fatalf("versions daemon=%s cli=%s", fields["daemon_version"], fields["codex_cli_version"])
	}
}

func TestRecordTaskMetricsTokenInvalidValuesAreNull(t *testing.T) {
	uc, _, events, _, writer := newRecordUseCase(t, domain.StateCompleted, false)
	events.events = []store.EventRecord{{Seq: 1, EventType: "turn.completed", Raw: map[string]any{"usage": map[string]any{"input_tokens": float64(-1), "cached_input_tokens": true, "output_tokens": math.Inf(1), "reasoning_output_tokens": float64(math.MaxInt64)}}}}
	if !executeRecord(t, uc, domain.StateCompleted).Recorded {
		t.Fatal("record failed")
	}
	fields := recordFields(t, writer)
	for _, name := range []string{"input_tokens", "cached_input_tokens", "output_tokens", "reasoning_output_tokens"} {
		if fieldString(t, fields, name) != "null" {
			t.Fatalf("%s=%s", name, fields[name])
		}
	}
}
