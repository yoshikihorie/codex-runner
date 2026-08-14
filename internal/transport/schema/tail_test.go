package schema

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const testTailTaskID = "impl-20260814-123456-abcd-tail-test"

func mustTailTaskID(t *testing.T) domain.TaskID {
	t.Helper()
	taskID, err := domain.NewTaskID(testTailTaskID)
	if err != nil {
		t.Fatal(err)
	}
	return taskID
}

func TestTailTaskInputJSON(t *testing.T) {
	taskID := mustTailTaskID(t)
	withFromSeq, err := json.Marshal(TailTaskInput{TaskID: taskID, FromSeq: 7})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(withFromSeq), `{"task_id":"impl-20260814-123456-abcd-tail-test","from_seq":7}`; got != want {
		t.Fatalf("marshal with from_seq = %s, want %s", got, want)
	}

	withoutFromSeq, err := json.Marshal(TailTaskInput{TaskID: taskID})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(withoutFromSeq), `{"task_id":"impl-20260814-123456-abcd-tail-test"}`; got != want {
		t.Fatalf("marshal without from_seq = %s, want %s", got, want)
	}

	var decoded TailTaskInput
	if err := json.Unmarshal([]byte(`{"task_id":"impl-20260814-123456-abcd-tail-test","from_seq":9}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.TaskID.String() != testTailTaskID || decoded.FromSeq != 9 {
		t.Fatalf("unmarshal = %#v", decoded)
	}
}

func TestProgressLineJSON(t *testing.T) {
	recordedAt := time.Date(2026, time.August, 14, 12, 34, 56, 0, time.UTC)
	line := ProgressLine{
		LineType:   LineTypeProgress,
		Seq:        3,
		RecordedAt: recordedAt,
		EventType:  "item.completed",
		Raw:        map[string]any{"kind": "message", "value": 42},
		TaskState:  domain.StateRunning,
	}
	encoded, err := json.Marshal(line)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"line_type":"progress","seq":3,"recorded_at":"2026-08-14T12:34:56Z","event_type":"item.completed","raw":{"kind":"message","value":42},"task_state":"running"}`; got != want {
		t.Fatalf("marshal = %s, want %s", got, want)
	}

	var decoded ProgressLine
	if err := json.Unmarshal([]byte(`{"line_type":"progress","seq":5,"recorded_at":"2026-08-14T12:35:56Z","event_type":"turn.completed","raw":{"answer":"ok"},"task_state":"completed"}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.LineType != LineTypeProgress || decoded.Seq != 5 || !decoded.RecordedAt.Equal(time.Date(2026, time.August, 14, 12, 35, 56, 0, time.UTC)) || decoded.EventType != "turn.completed" || decoded.TaskState != domain.StateCompleted {
		t.Fatalf("unmarshal = %#v", decoded)
	}
	raw, ok := decoded.Raw.(map[string]any)
	if !ok || raw["answer"] != "ok" {
		t.Fatalf("raw = %#v", decoded.Raw)
	}
}

func TestCompleteLineJSON(t *testing.T) {
	line := CompleteLine{
		LineType:  LineTypeComplete,
		Reason:    CompleteReasonTaskTerminal,
		TaskState: domain.StateCompleted,
		LastSeq:   8,
	}
	encoded, err := json.Marshal(line)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(encoded), `{"line_type":"complete","reason":"task-terminal","task_state":"completed","last_seq":8}`; got != want {
		t.Fatalf("marshal = %s, want %s", got, want)
	}

	var decoded CompleteLine
	if err := json.Unmarshal([]byte(`{"line_type":"complete","reason":"idle-timeout","task_state":"stalled","last_seq":11}`), &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.LineType != LineTypeComplete || decoded.Reason != CompleteReasonIdleTimeout || decoded.TaskState != domain.StateStalled || decoded.LastSeq != 11 {
		t.Fatalf("unmarshal = %#v", decoded)
	}
}

func TestTailLineConstants(t *testing.T) {
	if LineTypeProgress != "progress" {
		t.Fatalf("LineTypeProgress = %q", LineTypeProgress)
	}
	if LineTypeComplete != "complete" {
		t.Fatalf("LineTypeComplete = %q", LineTypeComplete)
	}
	if CompleteReasonTaskTerminal != "task-terminal" {
		t.Fatalf("CompleteReasonTaskTerminal = %q", CompleteReasonTaskTerminal)
	}
	if CompleteReasonIdleTimeout != "idle-timeout" {
		t.Fatalf("CompleteReasonIdleTimeout = %q", CompleteReasonIdleTimeout)
	}
}
