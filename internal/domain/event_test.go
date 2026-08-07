package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEventTypesAreClosedAndCanonical(t *testing.T) {
	var _ Event = TaskQueued{}
	var _ Event = TaskStarted{}
	var _ Event = TaskEventObserved{}
	var _ Event = TaskStalled{}
	var _ Event = TaskExited{}
	var _ Event = TaskCompleted{}
	var _ Event = TaskFailed{}
	var _ Event = TaskTimedOut{}
	var _ Event = RecoveryAttempted{}
	var _ Event = RecoverySucceeded{}
	var _ Event = RecoveryFailed{}
	var _ Event = TaskAdopted{}
	var _ Event = TaskOrphanDetected{}
	var _ Event = TaskCancelRequested{}
	var _ Event = TaskKilled{}

	events := []Event{TaskQueued{}, TaskStarted{}, TaskEventObserved{}, TaskStalled{}, TaskExited{}, TaskCompleted{}, TaskFailed{}, TaskTimedOut{}, RecoveryAttempted{}, RecoverySucceeded{}, RecoveryFailed{}, TaskAdopted{}, TaskOrphanDetected{}, TaskCancelRequested{}, TaskKilled{}}
	want := []string{"TaskQueued", "TaskStarted", "TaskEventObserved", "TaskStalled", "TaskExited", "TaskCompleted", "TaskFailed", "TaskTimedOut", "RecoveryAttempted", "RecoverySucceeded", "RecoveryFailed", "TaskAdopted", "TaskOrphanDetected", "TaskCancelRequested", "TaskKilled"}
	for index, event := range events {
		if event.Type() != want[index] {
			t.Errorf("event %d type = %q, want %q", index, event.Type(), want[index])
		}
	}
}

func TestEventJSONKeysAndOptionalFields(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	id, _ := NewTaskID("impl-20260806-120000-a1b2-example")
	timeout, _ := NewTimeout(nil, 1800)
	process, _ := NewProcessStartTime(1, now)
	events := []Event{
		TaskQueued{TaskID: id, RequestedTimeoutSeconds: nil},
		TaskStarted{TaskID: id, ProcessStartTime: process},
		TaskEventObserved{TaskID: id}, TaskStalled{TaskID: id},
		TaskExited{TaskID: id, ExitCode: NewExitCode(0)}, TaskCompleted{TaskID: id, ExitCode: NewExitCode(0)}, TaskFailed{TaskID: id, ExitCode: NewExitCode(1)},
		TaskTimedOut{TaskID: id, ResolvedTimeoutSeconds: timeout.ResolvedSeconds(), SessionRef: nil},
		RecoveryAttempted{TaskID: id, SessionRef: nil}, RecoverySucceeded{TaskID: id, ExitCode: NewExitCode(0)}, RecoveryFailed{TaskID: id},
		TaskAdopted{TaskID: id}, TaskOrphanDetected{TaskID: id}, TaskCancelRequested{TaskID: id}, TaskKilled{TaskID: id, ExitCode: NewExitCode(130)},
	}
	for _, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), "\"task_id\"") {
			t.Errorf("%T omitted task_id: %s", event, data)
		}
	}
	queued, _ := json.Marshal(events[0])
	if strings.Contains(string(queued), "requested_timeout_seconds") {
		t.Fatal("nil requested timeout was not omitted")
	}
	for _, event := range []Event{events[7], events[8]} {
		data, _ := json.Marshal(event)
		if !strings.Contains(string(data), "\"session_ref\":null") {
			t.Errorf("%T omitted nullable session_ref: %s", event, data)
		}
	}
	started, _ := json.Marshal(events[1])
	if !strings.Contains(string(started), "\"pid\":1") {
		t.Fatalf("nested value object did not marshal: %s", started)
	}
}
