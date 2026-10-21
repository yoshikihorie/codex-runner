package supervisor

import "testing"

func TestSendGateStopPrecedesObservedState(t *testing.T) {
	s := ManagedSession{State: SessionIdle}
	if got := CheckSendGate(s, SessionWorking, SendKindRemoteControl, true, true, false); got != SendBlockedStop {
		t.Fatalf("reason=%q", got)
	}
}

func TestSCNsup0217SendGateKindStateMatrix(t *testing.T) {
	cases := []struct {
		state, observed SupervisorSessionState
		kind            SupervisorSendKind
		allowed         bool
	}{
		{SessionIdle, SessionIdle, SendKindRemoteControl, true},
		{SessionAwaitingRecommendation, SessionAwaitingRecommendation, SendKindRecommendationContinue, true},
		{SessionAwaitingRecommendation, SessionAwaitingRecommendation, SendKindHandoffEnd, true},
		{SessionAwaitingRecommendation, SessionAwaitingRecommendation, SendKindRemoteControl, true},
		{SessionAwaitingRecommendation, SessionAwaitingRecommendation, SendKindSessionStart, false},
		{SessionRotating, SessionIdle, SendKindHandoffEnd, false},
		{SessionRotating, SessionIdle, SendKindRecommendationContinue, false},
		{SessionStarting, SessionIdle, SendKindRemoteControl, false},
		{SessionIdle, SessionWorking, SendKindRemoteControl, false},
		{SessionStopped, SessionIdle, SendKindHandoffEnd, false},
		{SessionStopped, SessionIdle, SendKindRemoteControl, false},
		{SessionRecovering, SessionIdle, SendKindRemoteControl, false},
		{SessionFailed, SessionIdle, SendKindSessionStart, false},
		{SessionExited, SessionIdle, SendKindSessionStart, false},
		{SessionAwaitingPermission, SessionIdle, SendKindRecommendationContinue, false},
		{SessionWorking, SessionIdle, SendKindRecommendationContinue, false},
		{SessionIdle, SessionAwaitingPermission, SendKindRecommendationContinue, false},
		{SessionIdle, SessionStarting, SendKindSessionStart, false},
		{SessionIdle, SessionRecovering, SendKindRemoteControl, false},
		{SessionAwaitingRecommendation, SessionAwaitingRecommendation, SendKindParallelInstruction, false},
		{SessionAwaitingRecommendation, SessionAwaitingRecommendation, SendKindHandoffExit, false},
	}
	if !SendKindAllowed(SessionRotating, SessionIdle, SendKindHandoffEnd, SendOriginHandoff) {
		t.Fatal("handoff origin should permit rotating")
	}
	if CheckSendGate(ManagedSession{State: SessionRotating}, SessionIdle, SendKindHandoffEnd, false, true, true, SendOriginNormal) != SendBlockedState {
		t.Fatal("normal origin must not send while rotating")
	}
	for _, tc := range cases {
		if got := SendKindAllowed(tc.state, tc.observed, tc.kind); got != tc.allowed {
			t.Errorf("%s/%s/%s: got %v", tc.state, tc.observed, tc.kind, got)
		}
	}
}

func TestSCNsup0219SCNsup0238SCNsup0143SendGateReasons(t *testing.T) {
	s := ManagedSession{State: SessionIdle}
	check := func(stop bool, observed SupervisorSessionState, fixed bool, want SupervisorSendBlockedReason) {
		t.Helper()
		if got := CheckSendGate(s, observed, SendKindRemoteControl, stop, fixed, false); got != want {
			t.Fatalf("got %q want %q", got, want)
		}
	}
	check(false, SessionIdle, true, "")
	check(true, SessionIdle, true, SendBlockedStop)
	check(false, SessionWorking, true, SendBlockedState)
	s.SendGate.Pending = &PendingSend{SendID: "alpha-1-s1"}
	check(false, SessionIdle, true, SendBlockedPending)
	s.SendGate.Pending = nil
	s.SendGate.EverSent = true
	check(false, SessionIdle, true, SendBlockedUnconfirmed)
	s.SendGate.LastSendConfirmed = true
	check(false, SessionIdle, false, SendBlockedNotFixedText)
	s.SendGate.UnconfirmedDiscarded = true
	if got := CheckSendGate(s, SessionIdle, SendKindRemoteControl, false, true, false); got != SendBlockedUnconfirmed {
		t.Fatal(got)
	}
	if got := CheckSendGate(s, SessionIdle, SendKindRemoteControl, false, true, true); got != "" {
		t.Fatal(got)
	}
	s.SendGate.LastSendConfirmed = false
	if got := CheckSendGate(s, SessionIdle, SendKindRemoteControl, false, true, true); got != "" {
		t.Fatalf("fresh idle after discarded pending was blocked: %q", got)
	}
}
