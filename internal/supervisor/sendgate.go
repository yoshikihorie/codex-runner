package supervisor

import "time"

// Canonical values: specifications/10-shared/validation-rules.md §3.
const SupervisorSendConfirmTimeout = 60 * time.Second
const SupervisorExitGrace = 60 * time.Second // SUPERVISOR_EXIT_GRACE_SECONDS, validation-rules.md §3.
const SupervisorSendEnterDelay = 200 * time.Millisecond

// Canonical values: specifications/10-shared/published-language/enums.md SupervisorSendBlockedReason.
type SupervisorSendBlockedReason string

type SendOrigin uint8

const (
	SendOriginNormal SendOrigin = iota
	SendOriginHandoff
)

const (
	SendBlockedStop         SupervisorSendBlockedReason = "stop"
	SendBlockedState        SupervisorSendBlockedReason = "state"
	SendBlockedPending      SupervisorSendBlockedReason = "pending"
	SendBlockedUnconfirmed  SupervisorSendBlockedReason = "unconfirmed"
	SendBlockedNotFixedText SupervisorSendBlockedReason = "not_fixed_text"
	SendBlockedRateLimited  SupervisorSendBlockedReason = "rate_limited"
)

// SendKindAllowed applies FD-sup-02 §5.1's state and kind matrix.
func SendKindAllowed(state, observed SupervisorSessionState, kind SupervisorSendKind, origins ...SendOrigin) bool {
	if kind == SendKindOperatorInput {
		return state == SessionIdle && observed == SessionIdle
	}
	if state != SessionIdle && state != SessionAwaitingRecommendation && state != SessionRotating {
		return false
	}
	if state == SessionRotating && (len(origins) == 0 || origins[0] != SendOriginHandoff) {
		return false
	}
	if observed != SessionIdle && observed != SessionAwaitingRecommendation {
		return false
	}
	if state == SessionRotating && kind == SendKindRecommendationContinue {
		return false
	}
	if observed == SessionAwaitingRecommendation {
		return kind == SendKindRecommendationContinue || kind == SendKindHandoffEnd || kind == SendKindRemoteControl
	}
	return true
}

// CheckSendGate checks conditions 0–5 in their canonical priority order.
func CheckSendGate(s ManagedSession, observed SupervisorSessionState, kind SupervisorSendKind, stopped, fixed, freshAfterDiscard bool, origins ...SendOrigin) SupervisorSendBlockedReason {
	if s.State != SessionIdle && s.State != SessionAwaitingRecommendation && (s.State != SessionRotating || len(origins) == 0 || origins[0] != SendOriginHandoff) {
		return SendBlockedState
	}
	if stopped {
		return SendBlockedStop
	}
	if !SendKindAllowed(s.State, observed, kind, origins...) {
		return SendBlockedState
	}
	if s.SendGate.Pending != nil {
		return SendBlockedPending
	}
	if s.SendGate.UnconfirmedDiscarded && !freshAfterDiscard || !s.SendGate.UnconfirmedDiscarded && s.SendGate.EverSent && !s.SendGate.LastSendConfirmed {
		return SendBlockedUnconfirmed
	}
	if !fixed {
		return SendBlockedNotFixedText
	}
	return ""
}
