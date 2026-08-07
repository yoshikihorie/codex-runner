package domain

import (
	"errors"
	"testing"
)

func TestPublishedEnumValues(t *testing.T) {
	if got := []TaskState{StateQueued, StateStarting, StateRunning, StateStalled, StateCompleted, StateFailed, StateTimeout, StateRecovering, StateRecovered, StateTimeoutLost, StateCancelling, StateKilled, StateAdopted, StateOrphaned, StateLost}; len(got) != 15 {
		t.Fatal("TaskState values changed")
	}
	if got := []ProtocolVerb{ProtocolVerbSubmit, ProtocolVerbStatus, ProtocolVerbCancel, ProtocolVerbTail, ProtocolVerbPing}; len(got) != 5 {
		t.Fatal("ProtocolVerb values changed")
	}
	if got := []Subcommand{SubcommandImpl, SubcommandReview, SubcommandPlan, SubcommandResearch, SubcommandRead, SubcommandStatus, SubcommandLogs, SubcommandCancel, SubcommandDoctor, SubcommandCleanup, SubcommandStats}; len(got) != 11 {
		t.Fatal("Subcommand values changed")
	}
	if got := []ExecutionRoute{ExecutionRouteDaemon, ExecutionRouteLegacy}; len(got) != 2 {
		t.Fatal("ExecutionRoute values changed")
	}
	if got := []ExecutionRouteReason{ExecutionRouteReasonNone, ExecutionRouteReasonConnectRefused, ExecutionRouteReasonConnectTimeout, ExecutionRouteReasonPingTimeout, ExecutionRouteReasonVersionUnknown, ExecutionRouteReasonStageDisabled, ExecutionRouteReasonClientUnavailable}; len(got) != 7 {
		t.Fatal("ExecutionRouteReason values changed")
	}
	if got := []ExitCodeClass{ExitCodeClassSuccess, ExitCodeClassFailure, ExitCodeClassTimeout, ExitCodeClassCancelled}; len(got) != 4 {
		t.Fatal("ExitCodeClass values changed")
	}
	if got := []RecoveryOrigin{RecoveryOriginTimeout, RecoveryOriginOrphan}; len(got) != 2 {
		t.Fatal("RecoveryOrigin values changed")
	}
}

func TestSentinelErrorsAndTerminalStates(t *testing.T) {
	for _, err := range []error{ErrInvalidStateTransition, ErrTaskAlreadyTerminal, ErrSessionNotResumable} {
		if !errors.Is(err, err) {
			t.Errorf("errors.Is failed for %v", err)
		}
	}
	for _, state := range []TaskState{StateCompleted, StateFailed, StateRecovered, StateTimeoutLost, StateKilled, StateLost} {
		if !state.terminal() {
			t.Errorf("%s is not terminal", state)
		}
	}
}
