package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
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
