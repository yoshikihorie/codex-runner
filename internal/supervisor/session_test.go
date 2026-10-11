package supervisor

import (
	"errors"
	"testing"
	"time"
)

func TestTransitionTableAll160Cells(t *testing.T) {
	states := []SupervisorSessionState{SessionStarting, SessionIdle, SessionWorking, SessionAwaitingPermission, SessionAwaitingRecommendation, SessionRotating, SessionRecovering, SessionStopped, SessionExited, SessionFailed}
	events := []TransitionEvent{EventDetectorIdle, EventDetectorWorking, EventDetectorPermission, EventDetectorRecommendation, EventDetectorUndetermined, EventUnexpectedExit, EventHandoffExit, EventStartupTimeout, EventContextRotate, EventRotationDone, EventRotationFailed, EventStop, EventResume, EventRecoveryWaitDone, EventStopSession, EventReconcile}
	// Specification-derived expected cells. '-' means rejected; '=' means accepted without changing state.
	rows := []string{
		"idle,working,awaiting_permission,-,=,recovering,-,failed,-,-,-,stopped,-,-,exited,exited",
		"=,working,awaiting_permission,awaiting_recommendation,=,recovering,-,-,rotating,-,-,stopped,-,-,exited,-",
		"idle,=,awaiting_permission,awaiting_recommendation,=,recovering,-,-,=,-,-,stopped,-,-,exited,-",
		"idle,working,=,awaiting_recommendation,=,recovering,-,-,=,-,-,stopped,-,-,exited,-",
		"idle,working,awaiting_permission,=,=,recovering,-,-,rotating,-,-,stopped,-,-,exited,-",
		"=,=,=,=,=,=,=,-,=,idle,stopped,stopped,-,-,exited,-",
		"=,=,=,=,=,-,-,-,-,-,-,stopped,-,starting,exited,-",
		"=,=,=,=,=,=,=,-,-,-,-,=,starting,-,exited,-",
		"-,-,-,-,-,-,-,-,-,-,-,=,-,-,=,-",
		"-,-,-,-,-,-,-,-,-,-,-,=,-,-,=,-",
	}
	if len(states)*len(events) != 160 {
		t.Fatalf("coverage=%d", len(states)*len(events))
	}
	for i, state := range states {
		cells := splitCells(rows[i])
		if len(cells) != len(events) {
			t.Fatalf("state %s has %d cells", state, len(cells))
		}
		for j, event := range events {
			want := cells[j]
			got, allowed := Transition(state, event, TransitionContext{Generation: 1, ExitCode: 1, ClaudeSessionID: true, ObservedState: SessionIdle})
			if want == "-" {
				if allowed {
					t.Errorf("%s/%s allowed -> %s", state, event, got)
				}
				continue
			}
			if !allowed {
				t.Errorf("%s/%s rejected; want %s", state, event, want)
				continue
			}
			if want == "=" && got != state {
				t.Errorf("%s/%s -> %s, want unchanged %s", state, event, got, state)
			}
			if want != "=" && string(got) != want {
				t.Errorf("%s/%s -> %s, want %s", state, event, got, want)
			}
		}
	}
}

func splitCells(row string) []string {
	var cells []string
	start := 0
	for i := 0; i <= len(row); i++ {
		if i == len(row) || row[i] == ',' {
			cells = append(cells, row[start:i])
			start = i + 1
		}
	}
	return cells
}

func TestSessionIDValidation(t *testing.T) {
	for _, value := range []string{"alpha-1", "project-x-42"} {
		if _, err := NewSessionID(value); err != nil {
			t.Errorf("valid ID %q: %v", value, err)
		}
	}
	for _, value := range []string{"", "Alpha-1", "alpha-0", "alpha-one", "-1", "a/1", "alpha-01"} {
		if _, err := NewSessionID(value); err == nil {
			t.Errorf("invalid ID accepted: %q", value)
		}
	}
}

func TestConditionalTransitions(t *testing.T) {
	tests := []struct {
		name    string
		from    SupervisorSessionState
		event   TransitionEvent
		context TransitionContext
		want    SupervisorSessionState
		allowed bool
	}{
		{"pending send exit", SessionWorking, EventUnexpectedExit, TransitionContext{ExitCode: 1, HasPendingSend: true, ClaudeSessionID: true}, SessionStopped, true},
		{"clean exit", SessionWorking, EventUnexpectedExit, TransitionContext{ExitCode: 0, ClaudeSessionID: true}, SessionExited, true},
		{"missing resume id", SessionWorking, EventUnexpectedExit, TransitionContext{ExitCode: 1}, SessionStopped, true},
		{"restart count cap", SessionWorking, EventUnexpectedExit, TransitionContext{ExitCode: 1, ClaudeSessionID: true, RestartCount: 3}, SessionStopped, true},
		{"daily restart cap", SessionWorking, EventUnexpectedExit, TransitionContext{ExitCode: 1, ClaudeSessionID: true, DailyRestartCount: 10}, SessionStopped, true},
		{"restart recovery", SessionWorking, EventUnexpectedExit, TransitionContext{ExitCode: 1, ClaudeSessionID: true}, SessionRecovering, true},
		{"initial startup timeout", SessionStarting, EventStartupTimeout, TransitionContext{Generation: 1}, SessionFailed, true},
		{"respawn startup timeout", SessionStarting, EventStartupTimeout, TransitionContext{Generation: 2}, SessionStopped, true},
		{"rotation completion observed", SessionRotating, EventRotationDone, TransitionContext{ObservedState: SessionAwaitingPermission}, SessionAwaitingPermission, true},
		{"rotation completion undetermined", SessionRotating, EventRotationDone, TransitionContext{}, SessionRotating, true},
		{"resume child alive", SessionStopped, EventResume, TransitionContext{ChildAlive: true, ClaudeSessionID: true}, SessionWorking, true},
		{"resume missing id", SessionStopped, EventResume, TransitionContext{}, SessionStopped, false},
		{"resume id from previous generation", SessionStopped, EventResume, TransitionContext{HasPreviousClaudeSessionID: true}, SessionStarting, true},
		{"resume orphan alive", SessionStopped, EventResume, TransitionContext{OrphanChildAlive: true}, SessionStopped, false},
		{"resume rotation child alive", SessionStopped, EventResume, TransitionContext{HasRotation: true, ChildAlive: true}, SessionRotating, true},
		{"resume rotation exit timeout", SessionStopped, EventResume, TransitionContext{HasRotation: true, ChildAlive: true, RotationExitTimedOut: true}, SessionStopped, false},
		{"resume child idle", SessionStopped, EventResume, TransitionContext{ChildAlive: true, ClaudeSessionID: true, ObservedState: SessionIdle}, SessionIdle, true},
		{"resume child undetermined", SessionStopped, EventResume, TransitionContext{ChildAlive: true}, SessionWorking, true},
		{"resume rotation", SessionStopped, EventResume, TransitionContext{RotationStep: RotateSendRemoteControl}, SessionRotating, true},
		{"reconcile initial empty pid", SessionStarting, EventReconcile, TransitionContext{Generation: 1}, SessionExited, true},
		{"reconcile respawn", SessionStarting, EventReconcile, TransitionContext{Generation: 2}, SessionStarting, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Transition(tt.from, tt.event, tt.context)
			if ok != tt.allowed || got != tt.want {
				t.Fatalf("got (%s,%v), want (%s,%v)", got, ok, tt.want, tt.allowed)
			}
		})
	}
}

func TestResumeRefusalHasCanonicalReason(t *testing.T) {
	s := ManagedSession{State: SessionStopped, StopReason: StopSendNotConfirmed, StopScope: StopScopeSession, Generation: 1}
	err := s.Apply(EventResume, TransitionContext{OrphanChildAlive: true}, time.Now())
	if TransitionErrorReason(err) != "SUP_ORPHAN_CHILD_ALIVE" {
		t.Fatalf("orphan refusal reason=%v", err)
	}
	s = ManagedSession{State: SessionStopped, Generation: 1}
	err = s.Apply(EventResume, TransitionContext{}, time.Now())
	if TransitionErrorReason(err) != "SUP_CLAUDE_SESSION_ID_MISSING" || s.StopReason != StopClaudeSessionIDMissing || s.State != SessionStopped {
		t.Fatalf("missing ID resume: session=%+v err=%v", s, err)
	}
}

func TestGlobalResumeOnlyResumesGlobalStop(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := ManagedSession{State: SessionStopped, StopReason: StopSendNotConfirmed, StopScope: StopScopeGlobal, Generation: 1}
	if err := s.Apply(EventResume, TransitionContext{ClaudeSessionID: true, ResumeScope: StopScopeGlobal}, now); err != nil {
		t.Fatal(err)
	}
	if s.State != SessionStopped || s.StopReason != StopSendNotConfirmed || s.StopScope != StopScopeNone {
		t.Fatalf("global resume changed prior stop: %+v", s)
	}
	global := ManagedSession{State: SessionStopped, StopReason: StopGlobal, StopScope: StopScopeGlobal, Generation: 1}
	if err := global.Apply(EventResume, TransitionContext{ClaudeSessionID: true, ResumeScope: StopScopeGlobal}, now); err != nil {
		t.Fatal(err)
	}
	if global.State == SessionStopped || global.StopReason != "" {
		t.Fatalf("global stop did not resume: %+v", global)
	}
}

func TestEventKindRejectsUnlistedValues(t *testing.T) {
	for _, kind := range []SupervisorEventKind{"send_rc", "screen text", ""} {
		if ValidSupervisorEventKind(kind) {
			t.Errorf("accepted unlisted event kind %q", kind)
		}
	}
	if !ValidSupervisorEventKind(EventKindStateChanged) {
		t.Fatal("known event kind rejected")
	}
}

func TestApplyPreservesStoppedReasonAndReplacesReasonAfterResume(t *testing.T) {
	now := time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC)
	s := ManagedSession{State: SessionStopped, StopReason: StopSendNotConfirmed, StopScope: StopScopeSession, Generation: 1}
	if err := s.Apply(EventUnexpectedExit, TransitionContext{ExitCode: 0, ClaudeSessionID: true}, now); err != nil {
		t.Fatal(err)
	}
	if s.StopReason != StopSendNotConfirmed {
		t.Fatalf("stopped reason overwritten: %q", s.StopReason)
	}
	s = ManagedSession{State: SessionStopped, StopReason: StopGlobal, StopScope: StopScopeGlobal, Generation: 1}
	if err := s.Apply(EventResume, TransitionContext{ClaudeSessionID: true}, now); err != nil {
		t.Fatal(err)
	}
	if s.StopReason != "" || s.StopScope != StopScopeNone {
		t.Fatalf("stop data remained after resume: %+v", s)
	}
	s.State = SessionWorking
	if err := s.Apply(EventStop, TransitionContext{StopReason: StopSendNotConfirmed}, now); err != nil {
		t.Fatal(err)
	}
	if s.StopReason != StopSendNotConfirmed || s.State != SessionStopped {
		t.Fatalf("new stop reason not stored: %+v", s)
	}
}

type eventRecorderStub struct{ called int }

func (s *eventRecorderStub) RecordEvent(SessionID, SupervisorEventKind, SupervisorStopReason, SupervisorExitReason, SupervisorRotateFailReason, time.Time) {
	s.called++
}
func (*eventRecorderStub) RecordStateTransition(SessionID, SupervisorSessionState, SupervisorSessionState, time.Time) {
}
func (*eventRecorderStub) RecordReason(SessionID, SupervisorStopReason, time.Time) {}
func (*eventRecorderStub) RecordExit(SessionID, SupervisorExitReason, time.Time)   {}
func (*eventRecorderStub) RecordDetection(SessionID, DetectionBasis, time.Time)    {}

func TestRecordingEventRecorderRejectsUnlistedKind(t *testing.T) {
	stub := &eventRecorderStub{}
	recorder := RecordingEventRecorder{EventRecorder: stub}
	err := recorder.RecordEvent("alpha-1", "screen text", "", "", "", time.Now())
	if err == nil || !errors.Is(err, ErrInvalidSupervisorEventKind) || stub.called != 0 {
		t.Fatalf("err=%v calls=%d", err, stub.called)
	}
	if err := recorder.RecordEvent("alpha-1", EventKindStateChanged, "", "", "", time.Now()); err != nil || stub.called != 1 {
		t.Fatalf("valid event err=%v calls=%d", err, stub.called)
	}
}

func TestStoppedSessionStopIsIdempotent(t *testing.T) {
	now := time.Now().UTC()
	s := ManagedSession{State: SessionExited, ExitReason: ExitStopSessionRequested, StopScope: StopScopeNone}
	if err := s.Apply(EventStopSession, TransitionContext{}, now); err != nil {
		t.Fatal(err)
	}
	if s.ExitReason != ExitStopSessionRequested || s.StopScope != StopScopeNone {
		t.Fatalf("terminal STOP changed record: %+v", s)
	}
}

func TestRotationDoneReturnsObservedRdStates(t *testing.T) {
	for _, state := range []SupervisorSessionState{SessionIdle, SessionWorking, SessionAwaitingRecommendation, SessionExited} {
		got, ok := Transition(SessionRotating, EventRotationDone, TransitionContext{ObservedState: state})
		want, wantOK := state, true
		if state == SessionExited {
			want = SessionRotating
		}
		if got != want || ok != wantOK {
			t.Errorf("Rd observed %s = (%s,%v), want (%s,%v)", state, got, ok, want, wantOK)
		}
	}
}
