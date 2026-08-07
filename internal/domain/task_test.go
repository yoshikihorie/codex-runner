package domain

import (
	"errors"
	"testing"
	"time"
)

func testTask(t *testing.T) *Task {
	id, _ := NewTaskID("impl-20260806-120000-a1b2-example")
	slug, _ := NewSlug("example")
	task, _, err := NewTask(id, SubcommandImpl, slug, nil, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return task
}
func TestTaskFlow(t *testing.T) {
	task := testTask(t)
	timeout, _ := NewTimeout(nil, 1800)
	if _, err := task.Start(timeout, "model", time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := task.RecordProcessInfo(1, time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := task.ConfirmRunning(time.Now()); err != nil {
		t.Fatal(err)
	}
	events, err := task.RecordExit(NewExitCode(0), true, false, false, time.Now())
	if err != nil || len(events) != 2 {
		t.Fatal(err)
	}
	if _, err = task.RequestCancel(false, time.Now()); !errors.Is(err, ErrTaskAlreadyTerminal) {
		t.Fatal(err)
	}
}

func TestStateMachineAll225Cells(t *testing.T) {
	states := []TaskState{StateQueued, StateStarting, StateRunning, StateStalled, StateCompleted, StateFailed, StateTimeout, StateRecovering, StateRecovered, StateTimeoutLost, StateCancelling, StateKilled, StateAdopted, StateOrphaned, StateLost}
	events := []string{eventTaskQueued, eventTaskStarted, eventTaskEventObserved, eventTaskStalled, eventTaskExited, "TaskCompleted", "TaskFailed", eventTaskTimedOut, eventRecoveryAttempted, eventRecoverySucceeded, eventRecoveryFailed, eventTaskAdopted, eventTaskOrphanDetected, eventTaskCancelRequested, eventTaskKilled}
	allowed := map[TaskState]map[string]TaskState{
		StateQueued:     {eventTaskQueued: StateQueued, eventTaskStarted: StateStarting, eventTaskCancelRequested: StateCancelling},
		StateStarting:   {eventTaskExited: StateFailed, eventTaskAdopted: StateAdopted, eventTaskOrphanDetected: StateOrphaned, eventTaskCancelRequested: StateCancelling},
		StateRunning:    {eventTaskEventObserved: StateRunning, eventTaskStalled: StateStalled, eventTaskExited: StateFailed, eventTaskTimedOut: StateTimeout, eventTaskAdopted: StateAdopted, eventTaskOrphanDetected: StateOrphaned, eventTaskCancelRequested: StateCancelling},
		StateStalled:    {eventTaskEventObserved: StateRunning, eventTaskStalled: StateStalled, eventTaskExited: StateFailed, eventTaskTimedOut: StateTimeout, eventTaskAdopted: StateAdopted, eventTaskOrphanDetected: StateOrphaned, eventTaskCancelRequested: StateCancelling},
		StateTimeout:    {eventRecoveryAttempted: StateRecovering},
		StateRecovering: {eventRecoverySucceeded: StateRecovered, eventRecoveryFailed: StateTimeoutLost},
		StateCancelling: {eventTaskCancelRequested: StateCancelling, eventTaskKilled: StateKilled},
		StateAdopted:    {eventTaskExited: StateFailed, eventTaskOrphanDetected: StateOrphaned, eventTaskCancelRequested: StateCancelling},
		StateOrphaned:   {eventTaskExited: StateFailed, eventRecoveryAttempted: StateRecovering, eventTaskCancelRequested: StateCancelling},
	}
	count, allowedCount, invalidCount, terminalCount := 0, 0, 0, 0
	for _, state := range states {
		for _, event := range events {
			count++
			task := testTask(t)
			task.state = state
			before := task.state
			if destination, ok := allowed[state][event]; ok {
				if err := task.transition(event, destination); err != nil {
					t.Errorf("%s/%s: %v", state, event, err)
				}
				if task.state != destination {
					t.Errorf("%s/%s = %s, want %s", state, event, task.state, destination)
				}
				allowedCount++
				continue
			}
			err := task.transition(event, StateLost)
			if state.terminal() && event == eventTaskCancelRequested {
				terminalCount++
				if !errors.Is(err, ErrTaskAlreadyTerminal) {
					t.Errorf("%s/%s: %v", state, event, err)
				}
			} else {
				invalidCount++
				if !errors.Is(err, ErrInvalidStateTransition) {
					t.Errorf("%s/%s: %v", state, event, err)
				}
			}
			if task.state != before {
				t.Errorf("rejected %s/%s changed state", state, event)
			}
		}
	}
	if count != 225 || allowedCount != 32 || invalidCount != 187 || terminalCount != 6 {
		t.Fatalf("cells=%d allowed=%d invalid=%d terminal=%d", count, allowedCount, invalidCount, terminalCount)
	}
}

func TestRecordExitBranchesAndRecoveryOrigins(t *testing.T) {
	for _, tc := range []struct {
		code    ExitCode
		present bool
		state   TaskState
		want    TaskState
		reason  string
	}{{NewExitCode(0), true, StateRunning, StateCompleted, ""}, {NewExitCode(0), false, StateRunning, StateFailed, ReasonNoOutput}, {NewExitCode(1), false, StateRunning, StateFailed, ReasonAbnormalExit}, {NewExitCode(1), true, StateStarting, StateFailed, ReasonAbnormalExit}} {
		task := testTask(t)
		task.state = tc.state
		events, err := task.RecordExit(tc.code, tc.present, false, false, time.Now())
		if err != nil || task.state != tc.want || len(events) != 2 {
			t.Fatalf("exit case: state=%s events=%T,%T err=%v", task.state, events[0], events[1], err)
		}
		if tc.reason != "" && events[1].(TaskFailed).Reason != tc.reason {
			t.Errorf("reason = %q", events[1].(TaskFailed).Reason)
		}
	}
	for _, tc := range []struct {
		start  TaskState
		origin RecoveryOrigin
		want   TaskState
	}{{StateTimeout, RecoveryOriginTimeout, StateTimeoutLost}, {StateOrphaned, RecoveryOriginOrphan, StateLost}} {
		task := testTask(t)
		task.state = tc.start
		if _, err := task.BeginRecovery(nil, time.Now()); err != nil {
			t.Fatal(err)
		}
		if task.recoveryOrigin != tc.origin {
			t.Errorf("origin=%s want=%s", task.recoveryOrigin, tc.origin)
		}
		if _, err := task.FailRecovery(false, time.Now()); err != nil || task.state != tc.want {
			t.Errorf("recovery state=%s err=%v", task.state, err)
		}
	}
}

func TestAdoptAndStartProcessContracts(t *testing.T) {
	for _, start := range []TaskState{StateStarting, StateRunning, StateStalled} {
		task := testTask(t)
		task.state = start
		events, err := task.Adopt(false, time.Now())
		if err != nil || task.state != StateRunning || len(events) != 1 {
			t.Errorf("adopt live from %s: state=%s err=%v", start, task.state, err)
		}
	}
	task := testTask(t)
	task.state = StateRunning
	events, err := task.Adopt(true, time.Now())
	if err != nil || task.state != StateOrphaned || len(events) != 2 {
		t.Fatalf("adopt dead: state=%s events=%d err=%v", task.state, len(events), err)
	}
	if _, err := task.RecordExit(NewExitCode(0), true, false, false, time.Now()); err == nil {
		t.Fatal("orphan exit flags were not required")
	}

	task = testTask(t)
	timeout, _ := NewTimeout(nil, 1800)
	events, err = task.Start(timeout, "model", time.Now())
	if err != nil || task.state != StateStarting || len(events) != 0 {
		t.Fatal("Start must not emit TaskStarted")
	}
	events, err = task.RecordProcessInfo(1, time.Now(), time.Now())
	if err != nil || len(events) != 1 {
		t.Fatal("RecordProcessInfo must emit TaskStarted")
	}
	if _, err := task.RecordProcessInfo(2, time.Now(), time.Now()); !errors.Is(err, ErrInvalidStateTransition) {
		t.Fatal(err)
	}
}

func TestCancelKeepsRaw137InKilledContext(t *testing.T) {
	task := testTask(t)
	if _, err := task.RequestCancel(false, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := task.ConfirmKilled(NewExitCode(137), false, time.Now()); err != nil || task.state != StateKilled {
		t.Fatalf("state=%s err=%v", task.state, err)
	}
}
