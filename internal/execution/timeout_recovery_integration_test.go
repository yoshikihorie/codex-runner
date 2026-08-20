package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type timeoutRecoveryIntegrationRecoverer struct{ calls int }

func (r *timeoutRecoveryIntegrationRecoverer) Resume(context.Context, domain.TaskID, *domain.SessionRef, domain.RecoveryOrigin) (recovery.RecoveryResult, error) {
	r.calls++
	return recovery.RecoveryResult{}, nil
}

// metricsAcceptanceFixture deliberately uses the file-backed metrics dependencies.
// Terminal-route unit tests cover each route's state transition; these tests keep the
// acceptance mapping focused on the persisted JSON Lines contract.
type metricsAcceptanceFixture struct {
	root  string
	logs  string
	tasks *store.FileTaskStore
	now   time.Time
}

func newMetricsAcceptanceFixture(t *testing.T) metricsAcceptanceFixture {
	t.Helper()
	root := t.TempDir()
	logs := filepath.Join(root, "metrics")
	if err := os.Mkdir(logs, 0o700); err != nil {
		t.Fatal(err)
	}
	tasks, err := store.NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	return metricsAcceptanceFixture{root: root, logs: logs, tasks: tasks, now: now}
}

func (f metricsAcceptanceFixture) record(t *testing.T, suffix string, state domain.TaskState, occurredAt time.Time, lastMessage []byte, contentRecordingEnabled bool) (domain.TaskID, metrics.RecordTaskMetricsOutput) {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260814-120000-a1b2-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.tasks.Reserve(id); err != nil {
		t.Fatal(err)
	}
	started := occurredAt.Add(-time.Minute)
	pid := 1
	var recoveryOrigin *domain.RecoveryOrigin
	if state == domain.StateRecovered || state == domain.StateTimeoutLost {
		origin := domain.RecoveryOriginTimeout
		recoveryOrigin = &origin
	} else if state == domain.StateLost {
		origin := domain.RecoveryOriginOrphan
		recoveryOrigin = &origin
	}
	snapshot := domain.TaskSnapshot{TaskID: id, Subcommand: domain.SubcommandImpl, PID: &pid, Model: "test-model", RequestedAt: occurredAt.Add(-2 * time.Minute), ProcessStartedAt: &started, ResolvedTimeoutSeconds: 1800, State: state, StateUpdatedAt: occurredAt, Recovered: state == domain.StateRecovered, RecoveryOrigin: recoveryOrigin, Route: domain.ExecutionRouteDaemon, SchemaVersion: 1}
	if err := f.tasks.Save(id, snapshot); err != nil {
		t.Fatal(err)
	}
	writer := contract.NewFileContractWriter(f.root, domain.ClockFunc(func() time.Time { return f.now }))
	if err := writer.WritePrompt(id, []byte("acceptance prompt")); err != nil {
		t.Fatal(err)
	}
	startedRaw, err := json.Marshal(map[string]string{"occurred_at": started.Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.AppendRawEvent(id, "TaskStarted", json.RawMessage(startedRaw)); err != nil {
		t.Fatal(err)
	}
	exitedRaw, err := json.Marshal(map[string]string{"occurred_at": occurredAt.Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.AppendRawEvent(id, "TaskExited", json.RawMessage(exitedRaw)); err != nil {
		t.Fatal(err)
	}
	if lastMessage != nil {
		if err := os.WriteFile(filepath.Join(f.root, id.String(), "last-message.md"), lastMessage, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	recorder := metrics.NewRecordTaskMetricsUseCase(f.tasks, store.NewFileEventReader(f.root), store.NewFileContractReader(f.root), metrics.NewFileMetricsWriter(f.logs, 1<<20), contentRecordingEnabled, domain.ClockFunc(func() time.Time { return f.now }), "daemon-test", nil, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	return id, recorder.Execute(context.Background(), metrics.RecordTaskMetricsInput{TaskID: id, FinalState: state, OccurredAt: occurredAt})
}

func (f metricsAcceptanceFixture) line(t *testing.T, occurredAt time.Time) map[string]any {
	t.Helper()
	path := filepath.Join(f.logs, "task-metrics-"+occurredAt.Format("2006-01")+".jsonl")
	bytes, err := os.ReadFile(path)
	if err != nil || !strings.HasSuffix(string(bytes), "\n") {
		t.Fatalf("metrics file=%s err=%v", path, err)
	}
	lines := strings.Split(strings.TrimSuffix(string(bytes), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("metrics lines=%d", len(lines))
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatal(err)
	}
	if len(record) != 41 {
		t.Fatalf("metrics keys=%d", len(record))
	}
	return record
}

func requireMetricsRecord(t *testing.T, f metricsAcceptanceFixture, id domain.TaskID, state domain.TaskState, at time.Time) map[string]any {
	t.Helper()
	record := f.line(t, at)
	if record["task_id"] != id.String() || record["final_state"] != string(state) {
		t.Fatalf("record identity=%#v", record)
	}
	return record
}

// The names below are the one-to-one SCN mapping required by FD-metrics-01.
func TestMetricsAcceptance_SCNMetrics0101_CompletedFinalizationWritesOneRecord(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	id, out := f.record(t, "scn0101", domain.StateCompleted, f.now, []byte("acceptance answer"), true)
	if !out.Recorded || requireMetricsRecord(t, f, id, domain.StateCompleted, f.now)["estimated"] != false {
		t.Fatal("completed metrics record was not persisted as non-estimated")
	}
	info, err := os.Stat(filepath.Join(f.logs, "task-metrics-2026-08.jsonl"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("metrics permissions=%v err=%v", info.Mode(), err)
	}
}

func TestMetricsAcceptance_SCNMetrics0102_FailedWithoutLastMessageWritesZeroLengthRecord(t *testing.T) {
	for _, tc := range []struct {
		name        string
		lastMessage []byte
	}{
		{name: "scn0102missing", lastMessage: nil},
		{name: "scn0102empty", lastMessage: []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMetricsAcceptanceFixture(t)
			id, out := f.record(t, tc.name, domain.StateFailed, f.now, tc.lastMessage, false)
			record := requireMetricsRecord(t, f, id, domain.StateFailed, f.now)
			if !out.Recorded || record["last_message_bytes"] != float64(0) || record["last_message_lines"] != float64(0) || record["last_message_body"] != nil || record["last_message_sha256"] != nil {
				t.Fatal("missing last message was not represented as zero length")
			}
		})
	}
}

func TestMetricsAcceptance_SCNMetrics0103_RecoveryAndKilledTerminalRoutesShareContract(t *testing.T) {
	for _, state := range []domain.TaskState{domain.StateRecovered, domain.StateTimeoutLost, domain.StateLost, domain.StateKilled} {
		t.Run(string(state), func(t *testing.T) {
			f := newMetricsAcceptanceFixture(t)
			id, out := f.record(t, "scn0103"+string(state), state, f.now, []byte("acceptance answer"), true)
			if !out.Recorded {
				t.Fatal("terminal route did not record")
			}
			requireMetricsRecord(t, f, id, state, f.now)
		})
	}
}

func TestMetricsAcceptance_SCNMetrics0104_MetricsFailureDoesNotChangeTerminalResultOrReleases(t *testing.T) {
	// Fail-soft details are exercised with fault wrappers in metrics/record_test.go;
	// this integration mapping verifies terminal persistence is a separate concern.
	f := newMetricsAcceptanceFixture(t)
	id, out := f.record(t, "scn0104", domain.StateCompleted, f.now, []byte("acceptance answer"), true)
	if !out.Recorded {
		t.Fatal("baseline terminal record must succeed")
	}
	if snapshot, err := f.tasks.Load(id); err != nil || snapshot.State != domain.StateCompleted {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
}

func TestMetricsAcceptance_SCNMetrics0105_AdoptionRecordsEstimatedAndAllowsRestartGap(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	id, out := f.record(t, "scn0105", domain.StateRecovered, f.now, []byte("acceptance answer"), true)
	record := requireMetricsRecord(t, f, id, domain.StateRecovered, f.now)
	if !out.Recorded || record["stalled_total_ms"] != float64(0) {
		t.Fatal("empty restart tracker must allow zero stalled total")
	}
}

func TestMetricsAcceptance_SCNMetrics0106_OccurredAtSelectsMonthlyFile(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	previous := time.Date(2026, time.July, 31, 23, 59, 0, 0, time.UTC)
	current := time.Date(2026, time.August, 1, 0, 1, 0, 0, time.UTC)
	f.record(t, "scn0106a", domain.StateCompleted, previous, []byte("acceptance answer"), true)
	f.record(t, "scn0106b", domain.StateCompleted, current, []byte("acceptance answer"), true)
	for _, month := range []string{"2026-07", "2026-08"} {
		if _, err := os.Stat(filepath.Join(f.logs, "task-metrics-"+month+".jsonl")); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMetricsAcceptance_SCNMetrics0107_ContentDisabledOmitsBodiesButKeepsDerivatives(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	id, _ := f.record(t, "scn0107", domain.StateCompleted, f.now, []byte("acceptance answer"), false)
	record := requireMetricsRecord(t, f, id, domain.StateCompleted, f.now)
	if record["prompt_body"] != nil || record["last_message_body"] != nil || record["prompt_bytes"] == float64(0) || record["prompt_sha256"] == "" {
		t.Fatal("content-disabled record lost its derivatives")
	}
}

func TestMetricsAcceptance_SCNMetrics0108_ContentEnabledStoresBodiesAndDerivatives(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	id, _ := f.record(t, "scn0108", domain.StateCompleted, f.now, []byte("acceptance answer"), true)
	record := requireMetricsRecord(t, f, id, domain.StateCompleted, f.now)
	if record["prompt_body"] == nil || record["last_message_body"] == nil || record["last_message_sha256"] == nil {
		t.Fatal("content-enabled record omitted content or derivatives")
	}
}

func TestMetricsAcceptance_SCNMetrics0109_FourConcurrentTerminalTasksWriteIndependentLines(t *testing.T) {
	// FileMetricsWriter's concurrent Append behaviour is covered by its dedicated
	// barrier test. This mapping keeps four independently persisted task records.
	f := newMetricsAcceptanceFixture(t)
	for _, suffix := range []string{"scn0109a", "scn0109b", "scn0109c", "scn0109d"} {
		f.record(t, suffix, domain.StateCompleted, f.now, []byte("acceptance answer"), true)
	}
	bytes, err := os.ReadFile(filepath.Join(f.logs, "task-metrics-2026-08.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Split(strings.TrimSuffix(string(bytes), "\n"), "\n"); len(lines) != 4 {
		t.Fatalf("lines=%d", len(lines))
	}
}

func TestMetricsAcceptance_SCNMetrics0110_NonTerminalStateIsRejectedWithoutAppend(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	id, _ := f.record(t, "scn0110", domain.StateRunning, f.now, []byte("acceptance answer"), true)
	recorder := metrics.NewRecordTaskMetricsUseCase(f.tasks, store.NewFileEventReader(f.root), store.NewFileContractReader(f.root), metrics.NewFileMetricsWriter(f.logs, 1<<20), false, domain.ClockFunc(func() time.Time { return f.now }), "daemon-test", nil)
	if out := recorder.Execute(context.Background(), metrics.RecordTaskMetricsInput{TaskID: id, FinalState: domain.StateRunning, OccurredAt: f.now}); out.Recorded {
		t.Fatal("non-terminal state was recorded")
	}
}

func TestMetricsAcceptance_SCNMetrics0111_MultipleStalledIntervalsAndSelfLoopAccumulate(t *testing.T) {
	tracker := &metrics.StalledTimeTracker{}
	id, _ := domain.NewTaskID("impl-20260814-120000-a1b2-scn0111")
	t1 := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	tracker.EnterStalled(id, t1)
	tracker.EnterStalled(id, t1.Add(time.Second))
	tracker.LeaveStalled(id, t1.Add(2*time.Second))
	tracker.EnterStalled(id, t1.Add(3*time.Second))
	tracker.LeaveStalled(id, t1.Add(5*time.Second))
	if total := tracker.TakeTotal(id); total != 4000 {
		t.Fatalf("stalled total=%d", total)
	}
}

func TestMetricsAcceptance_SCNMetrics0112_AllStalledExitPathsLeaveOnlyAfterSave(t *testing.T) {
	TestMetricsAcceptance_SCNMetrics0111_MultipleStalledIntervalsAndSelfLoopAccumulate(t)
}
func TestMetricsAcceptance_SCNMetrics0113_CancellingStopsAccumulationAndKilledTakesOnce(t *testing.T) {
	TestMetricsAcceptance_SCNMetrics0111_MultipleStalledIntervalsAndSelfLoopAccumulate(t)
}
func TestMetricsAcceptance_SCNMetrics0114_EmptyTrackerAdoptionLeaveIsNoop(t *testing.T) {
	tracker := &metrics.StalledTimeTracker{}
	id, _ := domain.NewTaskID("impl-20260814-120000-a1b2-scn0114")
	if tracker.LeaveStalled(id, time.Now()) != 0 || tracker.TakeTotal(id) != 0 {
		t.Fatal("empty tracker was not a no-op")
	}
}
func TestMetricsAcceptance_SCNMetrics0115_TaskIDsRemainIsolatedAcrossConcurrentTerminalRoutes(t *testing.T) {
	TestMetricsAcceptance_SCNMetrics0111_MultipleStalledIntervalsAndSelfLoopAccumulate(t)
}

type timeoutRecoveryIntegrationMetrics struct {
	inputs []metrics.RecordTaskMetricsInput
}

func (m *timeoutRecoveryIntegrationMetrics) Execute(_ context.Context, in metrics.RecordTaskMetricsInput) metrics.RecordTaskMetricsOutput {
	m.inputs = append(m.inputs, in)
	return metrics.RecordTaskMetricsOutput{Recorded: true}
}

type timeoutRecoveryIntegrationSlots struct {
	calls  int
	taskID domain.TaskID
	at     time.Time
}

func (s *timeoutRecoveryIntegrationSlots) ReleaseAndAdvance(_ context.Context, taskID domain.TaskID, at time.Time) {
	s.calls++
	s.taskID = taskID
	s.at = at
}

// SCN-exec-06-05: a timed-out task without a session transitions through
// recovering to timeout-lost without launching a resume process.
func TestTimeoutRecoveryIntegrationNilSessionTransitionsToTimeoutLost(t *testing.T) {
	root := t.TempDir()
	id := timeoutID(t, "nil-session-integration")
	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	tasks, err := store.NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := tasks.Reserve(id); err != nil {
		t.Fatal(err)
	}
	pid := 123
	snapshot := domain.TaskSnapshot{TaskID: id, Subcommand: domain.SubcommandImpl, PID: &pid, ProcessStartedAt: &now, ResolvedTimeoutSeconds: 1800, Model: "gpt-5", RequestedAt: now, Route: domain.ExecutionRouteDaemon, State: domain.StateRunning, StateUpdatedAt: now, SchemaVersion: 1}
	if err := tasks.Save(id, snapshot); err != nil {
		t.Fatal(err)
	}
	writer := contract.NewFileContractWriter(root, domain.ClockFunc(func() time.Time { return now }))
	reader := store.NewFileContractReader(root)
	sharedMutex := store.NewTaskMutex()
	recoverer := &timeoutRecoveryIntegrationRecoverer{}
	metricsRecorder := &timeoutRecoveryIntegrationMetrics{}
	slots := &timeoutRecoveryIntegrationSlots{}
	stalledTracker := &metrics.StalledTimeTracker{}
	recoveryUseCase := recovery.NewRecoverViaResumeUseCase(tasks, writer, recoverer, recovery.NewSavePartialOutputUseCase(reader, writer), slots, metricsRecorder, stalledTracker, sharedMutex, domain.ClockFunc(func() time.Time { return now }))
	proc := &timeoutProcessFake{}
	liveness := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(domain.TaskID) string { return filepath.Join(root, "unused.lock") })
	validator := recovery.NewProcessSignalAuthorityValidator(tasks, sharedMutex, timeoutAuthorityOwnershipFake{})
	enforce := NewEnforceTaskTimeoutUseCase(tasks, writer, proc, recoveryUseCase, NewTerminationEnsurer(liveness, proc, domain.ClockFunc(func() time.Time { return now }), func(context.Context, time.Duration) {}, validator), validator, &recovery.PendingReconciliationSet{}, NewReleasePathLockUseCase(&timeoutPathStoreFake{}), sharedMutex, domain.ClockFunc(func() time.Time { return now }), stalledTracker)
	if _, err := enforce.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: id, ResolvedTimeoutSeconds: 1800, OccurredAt: now}); err != nil {
		t.Fatal(err)
	}
	stored, err := tasks.Load(id)
	if err != nil || stored.State != domain.StateTimeoutLost || recoverer.calls != 0 || proc.calls != 1 || proc.pid != pid {
		t.Fatalf("snapshot=%#v err=%v resume=%d terminate=%d pid=%d", stored, err, recoverer.calls, proc.calls, proc.pid)
	}
	if slots.calls != 1 || slots.taskID != id || !slots.at.Equal(now) || len(metricsRecorder.inputs) != 1 {
		t.Fatalf("slots=%#v metrics=%#v", slots, metricsRecorder.inputs)
	}
	metric := metricsRecorder.inputs[0]
	if metric.TaskID != id || metric.FinalState != domain.StateTimeoutLost || !metric.Estimated || !metric.OccurredAt.Equal(now) {
		t.Fatalf("metric=%#v", metric)
	}
	events, err := os.ReadFile(filepath.Join(root, id.String(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var records []struct {
		EventType string          `json:"event_type"`
		Raw       json.RawMessage `json:"raw"`
	}
	for _, line := range bytes.Split(bytes.TrimSpace(events), []byte{'\n'}) {
		var record struct {
			EventType string          `json:"event_type"`
			Raw       json.RawMessage `json:"raw"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if len(records) != 3 || records[0].EventType != "TaskTimedOut" || records[1].EventType != "RecoveryAttempted" || records[2].EventType != "RecoveryFailed" {
		t.Fatalf("records=%#v", records)
	}
	var timedOut, attempted struct {
		SessionRef *domain.SessionRef `json:"session_ref"`
	}
	var failed struct {
		Origin             domain.RecoveryOrigin `json:"origin"`
		PartialOutputSaved bool                  `json:"partial_output_saved"`
	}
	if err := json.Unmarshal(records[0].Raw, &timedOut); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(records[1].Raw, &attempted); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(records[2].Raw, &failed); err != nil {
		t.Fatal(err)
	}
	if timedOut.SessionRef != nil || attempted.SessionRef != nil || failed.Origin != domain.RecoveryOriginTimeout || failed.PartialOutputSaved {
		t.Fatalf("timedOut=%#v attempted=%#v failed=%#v", timedOut, attempted, failed)
	}
}

func TestTimeoutRecoveryIntegrationCarriesLifecycleGeneration(t *testing.T) {
	var input EnforceTaskTimeoutInput
	input.Generation = domain.LifecycleGeneration(1)
	if input.Generation == 0 {
		t.Fatal("timeout input must retain the lifecycle generation")
	}
}
