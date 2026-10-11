package supervisor

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

type SessionID string

var sessionIDPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*-[1-9][0-9]*$`)

func NewSessionID(value string) (SessionID, error) {
	if !sessionIDPattern.MatchString(value) {
		return "", fmt.Errorf("invalid session id")
	}
	return SessionID(value), nil
}
func (id SessionID) String() string               { return string(id) }
func (id SessionID) MarshalJSON() ([]byte, error) { return json.Marshal(string(id)) }
func (id *SessionID) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := NewSessionID(s)
	if err == nil {
		*id = v
	}
	return err
}

type SupervisorSessionState string

const (
	SessionStarting               SupervisorSessionState = "starting"
	SessionIdle                   SupervisorSessionState = "idle"
	SessionWorking                SupervisorSessionState = "working"
	SessionAwaitingPermission     SupervisorSessionState = "awaiting_permission"
	SessionAwaitingRecommendation SupervisorSessionState = "awaiting_recommendation"
	SessionRotating               SupervisorSessionState = "rotating"
	SessionRecovering             SupervisorSessionState = "recovering"
	SessionStopped                SupervisorSessionState = "stopped"
	SessionExited                 SupervisorSessionState = "exited"
	SessionFailed                 SupervisorSessionState = "failed"
)

type SupervisorCli string

const (
	CLIClaude SupervisorCli = "claude"
	CLICodex  SupervisorCli = "codex"
)

type SupervisorStopReason string

const (
	StopGlobal                     SupervisorStopReason = "stop_global"
	StopSession                    SupervisorStopReason = "stop_session"
	StopSupervisorRestarted        SupervisorStopReason = "supervisor_restarted"
	StopSendNotConfirmed           SupervisorStopReason = "send_not_confirmed"
	StopSendAborted                SupervisorStopReason = "send_aborted"
	StopProcessExitWithPending     SupervisorStopReason = "process_exit_with_pending"
	StopRecommendationForbidden    SupervisorStopReason = "recommendation_forbidden"
	StopRecommendationUndetermined SupervisorStopReason = "recommendation_undetermined"
	StopRecommendationRepeated     SupervisorStopReason = "recommendation_repeated"
	StopAutoContinueLimit          SupervisorStopReason = "auto_continue_limit"
	StopRotateStepFailed           SupervisorStopReason = "rotate_step_failed"
	StopRespawnFailed              SupervisorStopReason = "respawn_failed"
	StopRecoveryStartTimeout       SupervisorStopReason = "recovery_start_timeout"
	StopClaudeSessionIDMissing     SupervisorStopReason = "claude_session_id_missing"
	StopRestartLimitConsecutive    SupervisorStopReason = "restart_limit_consecutive"
	StopRestartLimitDaily          SupervisorStopReason = "restart_limit_daily"
	StopOrphanChildAlive           SupervisorStopReason = "orphan_child_alive"
	StopStateFileCorrupt           SupervisorStopReason = "state_file_corrupt"
)

type SupervisorExitReason string

const ExitStopSessionRequested SupervisorExitReason = "stop_session_requested"

type SendGateRecord struct {
	NextSeq              uint64       `json:"next_seq"`
	EverSent             bool         `json:"ever_sent"`
	LastSendConfirmed    bool         `json:"last_send_confirmed"`
	LastConfirmedAt      *time.Time   `json:"last_confirmed_at,omitempty"`
	UnconfirmedDiscarded bool         `json:"unconfirmed_discarded"`
	Pending              *PendingSend `json:"pending,omitempty"`
}
type SupervisorSendKind string

const (
	SendKindRecommendationContinue SupervisorSendKind = "recommendation_continue"
	SendKindRemoteControl          SupervisorSendKind = "remote_control"
	SendKindHandoffEnd             SupervisorSendKind = "handoff_end"
	SendKindSessionStart           SupervisorSendKind = "session_start"
	SendKindParallelInstruction    SupervisorSendKind = "parallel_instruction"
	SendKindHandoffExit            SupervisorSendKind = "handoff_exit"
)

type PendingSend struct {
	SendID   string             `json:"send_id"`
	Kind     SupervisorSendKind `json:"kind"`
	IssuedAt time.Time          `json:"issued_at"`
}
type LastSendRecord struct {
	SendID    string             `json:"send_id"`
	Kind      SupervisorSendKind `json:"kind"`
	IssuedAt  time.Time          `json:"issued_at"`
	Confirmed bool               `json:"confirmed"`
}
type RotationCheckpointSnapshot struct {
	Exists   bool       `json:"exists"`
	Mtime    *time.Time `json:"mtime,omitempty"`
	HeadHash string     `json:"head_hash,omitempty"`
}
type SupervisorRotateStep string

const (
	RotateWaitIdle                SupervisorRotateStep = "wait_idle"
	RotateSendEnd                 SupervisorRotateStep = "send_end"
	RotateConfirmHandoff          SupervisorRotateStep = "confirm_handoff"
	RotateSendExit                SupervisorRotateStep = "send_exit"
	RotateRestart                 SupervisorRotateStep = "restart"
	RotateSendStart               SupervisorRotateStep = "send_start"
	RotateSendParallelInstruction SupervisorRotateStep = "send_parallel_instruction"
	RotateSendRemoteControl       SupervisorRotateStep = "send_remote_control"
)

type SupervisorRotateFailReason string

const (
	RotateFailIdleWaitTimeout         SupervisorRotateFailReason = "idle_wait_timeout"
	RotateFailSendNotConfirmed        SupervisorRotateFailReason = "send_not_confirmed"
	RotateFailMtimeNotUpdated         SupervisorRotateFailReason = "mtime_not_updated"
	RotateFailHeadGenerationUnchanged SupervisorRotateFailReason = "head_generation_unchanged"
	RotateFailNotIdleAfterHandoff     SupervisorRotateFailReason = "not_idle_after_handoff"
	RotateFailExitTimeout             SupervisorRotateFailReason = "exit_timeout"
	RotateFailSpawnFailed             SupervisorRotateFailReason = "spawn_failed"
	RotateFailStartupTimeout          SupervisorRotateFailReason = "startup_timeout"
	RotateFailStartNotSettled         SupervisorRotateFailReason = "start_not_settled"
	RotateFailChildAbsent             SupervisorRotateFailReason = "child_absent"
	RotateFailRestartLimit            SupervisorRotateFailReason = "restart_limit"
)

type RotationRecord struct {
	Step               SupervisorRotateStep       `json:"step"`
	StartedAt          time.Time                  `json:"started_at"`
	StepStartedAt      *time.Time                 `json:"step_started_at,omitempty"`
	EndIssuedAt        *time.Time                 `json:"end_issued_at,omitempty"`
	CheckpointSnapshot RotationCheckpointSnapshot `json:"checkpoint_snapshot,omitempty"`
	RespawnWaitUntil   *time.Time                 `json:"respawn_wait_until,omitempty"`
	FailReason         SupervisorRotateFailReason `json:"fail_reason,omitempty"`
}
type SupervisorStopScope string

const (
	StopScopeNone    SupervisorStopScope = "none"
	StopScopeSession SupervisorStopScope = "session"
	StopScopeGlobal  SupervisorStopScope = "global"
)

type SupervisorFailureReason string

const (
	FailurePathEscape               SupervisorFailureReason = "path_escape"
	FailureStagedOutOfScope         SupervisorFailureReason = "staged_out_of_scope"
	FailureOutOfScopeUncommitted    SupervisorFailureReason = "out_of_scope_uncommitted"
	FailureRenamePartial            SupervisorFailureReason = "rename_partial"
	FailureGitConfigChanged         SupervisorFailureReason = "git_config_changed"
	FailureGitattributesFilter      SupervisorFailureReason = "gitattributes_filter"
	FailureGitConfigInclude         SupervisorFailureReason = "git_config_include"
	FailureTreeMismatchBeforeCommit SupervisorFailureReason = "tree_mismatch_before_commit"
	FailureCommitRejected           SupervisorFailureReason = "commit_rejected"
	FailureGitTimeout               SupervisorFailureReason = "git_timeout"
	FailureGitUnavailable           SupervisorFailureReason = "git_unavailable"
	FailureVerifyBinaryChanged      SupervisorFailureReason = "verify_binary_changed"
)

type AutoContinueDisabledReason string

const AutoContinueDisabledPermissionModeUnsafe AutoContinueDisabledReason = "permission_mode_unsafe"

type ManagedSession struct {
	SchemaVersion              int                        `json:"schema_version"`
	SessionID                  SessionID                  `json:"session_id"`
	ProjectID                  string                     `json:"project_id"`
	CLI                        SupervisorCli              `json:"cli"`
	Generation                 int                        `json:"generation"`
	State                      SupervisorSessionState     `json:"state"`
	StateSince                 time.Time                  `json:"state_since"`
	StopScope                  SupervisorStopScope        `json:"stop_scope"`
	StopReason                 SupervisorStopReason       `json:"stop_reason,omitempty"`
	ExitReason                 SupervisorExitReason       `json:"exit_reason,omitempty"`
	ClaudeSessionID            string                     `json:"claude_session_id,omitempty"`
	PreviousClaudeSessionID    string                     `json:"previous_claude_session_id,omitempty"`
	ChildPID                   int                        `json:"child_pid,omitempty"`
	ChildStartedAt             *time.Time                 `json:"child_started_at,omitempty"`
	SendGate                   SendGateRecord             `json:"send_gate"`
	LastSend                   *LastSendRecord            `json:"last_send,omitempty"`
	RestartCount               int                        `json:"restart_count"`
	RestartTimes               []time.Time                `json:"restart_times"`
	Rotation                   *RotationRecord            `json:"rotation,omitempty"`
	AutoContinueDisabledReason AutoContinueDisabledReason `json:"auto_continue_disabled_reason,omitempty"`
	PendingRemoteControl       bool                       `json:"pending_remote_control,omitempty"`
	PendingRemoteControlSince  *time.Time                 `json:"pending_remote_control_since,omitempty"`
	PreviousState              SupervisorSessionState     `json:"previous_state,omitempty"`
	LastVerifiedFingerprint    string                     `json:"last_verified_fingerprint,omitempty"`
	LastVerifiedResult         string                     `json:"last_verified_result,omitempty"`
	LastFailureFingerprint     string                     `json:"last_failure_fingerprint,omitempty"`
	LastFailureReason          SupervisorFailureReason    `json:"last_failure_reason,omitempty"`
	LastPushFailedHead         string                     `json:"last_push_failed_head,omitempty"`
	GitConfigFingerprint       string                     `json:"git_config_fingerprint,omitempty"`
}

type TransitionEvent string

const (
	EventDetectorIdle           TransitionEvent = "Di"
	EventDetectorWorking        TransitionEvent = "Dw"
	EventDetectorPermission     TransitionEvent = "Dp"
	EventDetectorRecommendation TransitionEvent = "Dr"
	EventDetectorUndetermined   TransitionEvent = "Du"
	EventUnexpectedExit         TransitionEvent = "Xe"
	EventHandoffExit            TransitionEvent = "Xi"
	EventStartupTimeout         TransitionEvent = "To"
	EventContextRotate          TransitionEvent = "Cr"
	EventRotationDone           TransitionEvent = "Rd"
	EventRotationFailed         TransitionEvent = "Rf"
	EventStop                   TransitionEvent = "St"
	EventResume                 TransitionEvent = "Rs"
	EventRecoveryWaitDone       TransitionEvent = "Rw"
	EventStopSession            TransitionEvent = "Ss"
	EventReconcile              TransitionEvent = "Rc"
)

// Canonical thresholds copied from specifications/10-shared/validation-rules.md.
const supervisorRestartMaxCount = 3
const supervisorRestartMaxPerDay = 10

type TransitionContext struct {
	Generation                 int
	ChildPID                   int
	ExitCode                   int
	HasPendingSend             bool
	ClaudeSessionID            bool
	HasPreviousClaudeSessionID bool
	ResumeScope                SupervisorStopScope
	RestartCount               int
	DailyRestartCount          int
	ObservedState              SupervisorSessionState
	StopReason                 SupervisorStopReason
	ChildAlive                 bool
	RotationStep               SupervisorRotateStep
	RotationExitTimedOut       bool
	HasRotation                bool
	OrphanChildAlive           bool
}

var transitionEvents = []TransitionEvent{EventDetectorIdle, EventDetectorWorking, EventDetectorPermission, EventDetectorRecommendation, EventDetectorUndetermined, EventUnexpectedExit, EventHandoffExit, EventStartupTimeout, EventContextRotate, EventRotationDone, EventRotationFailed, EventStop, EventResume, EventRecoveryWaitDone, EventStopSession, EventReconcile}

// Transition applies the specification state table. Conditions come from FD-sup-01 §5.2.
// Transition evaluates one event against the supervisor state transition rules.
func Transition(from SupervisorSessionState, event TransitionEvent, c TransitionContext) (SupervisorSessionState, bool) {
	if event == EventDetectorUndetermined {
		switch from {
		case SessionStarting, SessionIdle, SessionWorking, SessionAwaitingPermission, SessionAwaitingRecommendation, SessionRotating, SessionRecovering, SessionStopped:
			return from, true
		}
	}
	if event == EventDetectorIdle || event == EventDetectorWorking || event == EventDetectorPermission || event == EventDetectorRecommendation {
		targets := map[TransitionEvent]SupervisorSessionState{EventDetectorIdle: SessionIdle, EventDetectorWorking: SessionWorking, EventDetectorPermission: SessionAwaitingPermission, EventDetectorRecommendation: SessionAwaitingRecommendation}
		switch from {
		case SessionStarting, SessionIdle, SessionWorking, SessionAwaitingPermission, SessionAwaitingRecommendation:
			if from == SessionStarting && event == EventDetectorWorking {
				return from, true
			}
			if from == SessionStarting && event == EventDetectorRecommendation {
				return "", false
			}
			return targets[event], true
		case SessionRotating, SessionRecovering, SessionStopped:
			return from, true
		}
	}
	switch from {
	case SessionStarting:
		switch event {
		case EventUnexpectedExit:
			return classifyExit(c)
		case EventStartupTimeout:
			if c.Generation >= 2 {
				return SessionStopped, true
			}
			return SessionFailed, true
		case EventStop:
			return SessionStopped, true
		case EventStopSession:
			return SessionExited, true
		case EventReconcile:
			if c.Generation == 1 && c.ChildPID == 0 {
				return SessionExited, true
			}
			return from, false
		}
	case SessionIdle, SessionWorking, SessionAwaitingPermission, SessionAwaitingRecommendation:
		switch event {
		case EventUnexpectedExit:
			return classifyExit(c)
		case EventContextRotate:
			if from == SessionIdle || from == SessionAwaitingRecommendation {
				return SessionRotating, true
			}
			return from, true
		case EventStop:
			return SessionStopped, true
		case EventStopSession:
			if c.OrphanChildAlive || c.ChildAlive {
				return from, false
			}
			return SessionExited, true
		}
	case SessionRotating:
		switch event {
		case EventUnexpectedExit:
			return from, true
		case EventHandoffExit:
			return from, true
		case EventContextRotate:
			return from, true
		case EventRotationDone:
			if c.ObservedState == SessionIdle || c.ObservedState == SessionWorking || c.ObservedState == SessionAwaitingPermission || c.ObservedState == SessionAwaitingRecommendation {
				return c.ObservedState, true
			}
			return from, true
		case EventRotationFailed, EventStop:
			return SessionStopped, true
		case EventStopSession:
			if c.OrphanChildAlive || c.ChildAlive {
				return from, false
			}
			return SessionExited, true
		}
	case SessionRecovering:
		switch event {
		case EventStop:
			return SessionStopped, true
		case EventRecoveryWaitDone:
			return SessionStarting, true
		case EventStopSession:
			return SessionExited, true
		}
	case SessionStopped:
		switch event {
		case EventStop:
			return from, true
		case EventUnexpectedExit:
			return from, true
		case EventHandoffExit:
			return from, true
		case EventResume:
			if c.OrphanChildAlive {
				return from, false
			}
			if c.HasRotation || c.RotationStep != "" {
				if c.RotationExitTimedOut && c.ChildAlive {
					return from, false
				}
				return SessionRotating, true
			}
			if c.ChildAlive {
				switch c.ObservedState {
				case SessionIdle, SessionWorking, SessionAwaitingPermission, SessionAwaitingRecommendation:
					return c.ObservedState, true
				default:
					return SessionWorking, true
				}
			}
			if !c.ClaudeSessionID && !c.HasPreviousClaudeSessionID {
				return from, false
			}
			return SessionStarting, true
		case EventStopSession:
			return SessionExited, true
		}
	case SessionExited, SessionFailed:
		if event == EventStop || event == EventStopSession {
			return from, true
		}
	}
	return from, false
}

// TransitionError reports a rejected transition with its caller-facing canonical code.
type TransitionError struct{ Code string }

func (e *TransitionError) Error() string { return e.Code }
func TransitionErrorReason(err error) string {
	var te *TransitionError
	if errors.As(err, &te) {
		return te.Code
	}
	return ""
}

func TransitionWithError(from SupervisorSessionState, event TransitionEvent, c TransitionContext) (SupervisorSessionState, error) {
	to, ok := Transition(from, event, c)
	if ok {
		return to, nil
	}
	code := "SUP_ILLEGAL_TRANSITION"
	if from == SessionStopped && event == EventResume {
		switch {
		case c.OrphanChildAlive:
			code = "SUP_ORPHAN_CHILD_ALIVE"
		case c.RotationExitTimedOut && c.ChildAlive:
			code = "SUP_ROTATE_STEP_FAILED"
		case !c.ClaudeSessionID && !c.HasPreviousClaudeSessionID && !c.ChildAlive && !c.HasRotation:
			code = "SUP_CLAUDE_SESSION_ID_MISSING"
		}
	}
	return from, &TransitionError{Code: code}
}

func exitStopReason(c TransitionContext) SupervisorStopReason {
	if c.HasPendingSend {
		return StopProcessExitWithPending
	}
	if c.ExitCode == 0 {
		return ""
	}
	if !c.ClaudeSessionID {
		return StopClaudeSessionIDMissing
	}
	if c.RestartCount >= supervisorRestartMaxCount {
		return StopRestartLimitConsecutive
	}
	if c.DailyRestartCount >= supervisorRestartMaxPerDay {
		return StopRestartLimitDaily
	}
	return ""
}
func classifyExit(c TransitionContext) (SupervisorSessionState, bool) {
	if c.HasPendingSend {
		return SessionStopped, true
	}
	if c.ExitCode == 0 {
		return SessionExited, true
	}
	if !c.ClaudeSessionID {
		return SessionStopped, true
	}
	if c.RestartCount >= supervisorRestartMaxCount || c.DailyRestartCount >= supervisorRestartMaxPerDay {
		return SessionStopped, true
	}
	return SessionRecovering, true
}
func CanTransition(from SupervisorSessionState, event TransitionEvent) bool {
	_, ok := Transition(from, event, TransitionContext{Generation: 1, ClaudeSessionID: true, ObservedState: SessionIdle})
	return ok
}

// Apply evaluates an event and updates the managed session when it is allowed.
func (s *ManagedSession) Apply(event TransitionEvent, c TransitionContext, now time.Time) error {
	if event == EventResume && c.ResumeScope == StopScopeGlobal && (s.StopScope != StopScopeGlobal || s.StopReason != StopGlobal) {
		s.StopScope = StopScopeNone
		return nil
	}
	to, transitionErr := TransitionWithError(s.State, event, c)
	ok := transitionErr == nil
	if event == EventStop && s.State != SessionStopped && c.StopReason == "" {
		return fmt.Errorf("SUP_ILLEGAL_TRANSITION: stop reason required")
	}
	if event == EventStop && c.StopReason != "" && !validSupervisorStopReason(c.StopReason) {
		return fmt.Errorf("SUP_ILLEGAL_TRANSITION: invalid stop reason")
	}
	if !ok {
		if s.State == SessionStopped && event == EventResume && c.OrphanChildAlive {
			return transitionErr
		}
		if s.State == SessionStopped && event == EventResume && c.RotationExitTimedOut && c.ChildAlive {
			return &TransitionError{Code: "SUP_ROTATE_STEP_FAILED"}
		}
		if s.State == SessionStopped && event == EventResume && !c.ClaudeSessionID && !c.HasPreviousClaudeSessionID && !c.ChildAlive && !c.HasRotation {
			s.StopReason = StopClaudeSessionIDMissing
			return transitionErr
		}
		return transitionErr
	}
	previous := s.State
	if to != s.State {
		s.State = to
		s.StateSince = now
	}
	if previous == SessionStopped && to != SessionStopped {
		s.AutoContinueDisabledReason = ""
		s.StopReason = ""
		s.StopScope = StopScopeNone
	}
	if to == SessionExited && previous != SessionExited {
		s.StopReason = ""
		s.StopScope = StopScopeNone
	}
	if to == SessionStopped && to != previous {
		s.PreviousState = previous
	}
	if event == EventStop && (to == SessionStopped || to == SessionExited || to == SessionFailed) {
		if to != SessionStopped {
			if c.StopReason == StopGlobal {
				s.StopScope = StopScopeGlobal
			} else if c.StopReason == StopSession {
				s.StopScope = StopScopeSession
			}
			return nil
		}
		if previous != SessionStopped && previous != SessionExited && previous != SessionFailed {
			s.StopReason = c.StopReason
		}
		if c.StopReason == StopGlobal {
			s.StopScope = StopScopeGlobal
		} else if c.StopReason == StopSession {
			s.StopScope = StopScopeSession
		}
	}
	if event == EventUnexpectedExit && to == SessionStopped && previous != SessionStopped {
		s.StopReason = exitStopReason(c)
	}
	if event == EventStartupTimeout && to == SessionStopped {
		s.StopReason = StopRecoveryStartTimeout
	}
	if event == EventRotationFailed && to == SessionStopped {
		s.StopReason = StopRotateStepFailed
	}
	if event == EventStopSession && to == SessionExited && previous != SessionExited && previous != SessionFailed {
		s.ExitReason = ExitStopSessionRequested
	}
	return nil
}

func validSupervisorStopReason(reason SupervisorStopReason) bool {
	switch reason {
	case StopGlobal, StopSession, StopSupervisorRestarted, StopSendNotConfirmed, StopSendAborted, StopProcessExitWithPending, StopRecommendationForbidden, StopRecommendationUndetermined, StopRecommendationRepeated, StopAutoContinueLimit, StopRotateStepFailed, StopRespawnFailed, StopRecoveryStartTimeout, StopClaudeSessionIDMissing, StopRestartLimitConsecutive, StopRestartLimitDaily, StopOrphanChildAlive, StopStateFileCorrupt:
		return true
	}
	return false
}

// RecordingEventRecorder validates enum values before forwarding events to persistence.
type RecordingEventRecorder struct{ EventRecorder }

var ErrInvalidSupervisorEventKind = errors.New("invalid supervisor event kind")

func (r RecordingEventRecorder) RecordEvent(id SessionID, kind SupervisorEventKind, stop SupervisorStopReason, exit SupervisorExitReason, failure SupervisorRotateFailReason, at time.Time) error {
	if !ValidSupervisorEventKind(kind) {
		return fmt.Errorf("%w: %q", ErrInvalidSupervisorEventKind, kind)
	}
	r.EventRecorder.RecordEvent(id, kind, stop, exit, failure, at)
	return nil
}

type DetectionBasis string

const (
	DetectionScreenPattern  DetectionBasis = "screen_pattern"
	DetectionOutputQuiet    DetectionBasis = "output_quiet"
	DetectionProcessExit    DetectionBasis = "process_exit"
	DetectionStructuredInfo DetectionBasis = "structured_info"
)

type SupervisorEventKind string

const (
	EventKindSessionStarted                  SupervisorEventKind = "session_started"
	EventKindSessionStartFailed              SupervisorEventKind = "session_start_failed"
	EventKindSessionStartInterrupted         SupervisorEventKind = "session_start_interrupted"
	EventKindStateChanged                    SupervisorEventKind = "state_changed"
	EventKindIllegalTransition               SupervisorEventKind = "illegal_transition"
	EventKindSessionStopped                  SupervisorEventKind = "session_stopped"
	EventKindSessionResumed                  SupervisorEventKind = "session_resumed"
	EventKindResumeRefused                   SupervisorEventKind = "resume_refused"
	EventKindSessionFailed                   SupervisorEventKind = "session_failed"
	EventKindSessionExited                   SupervisorEventKind = "session_exited"
	EventKindSessionRestarted                SupervisorEventKind = "session_restarted"
	EventKindRestartCountersReset            SupervisorEventKind = "restart_counters_reset"
	EventKindExitWhileStopped                SupervisorEventKind = "exit_while_stopped"
	EventKindRestartLimitExceeded            SupervisorEventKind = "restart_limit_exceeded"
	EventKindOrphanChildAlive                SupervisorEventKind = "orphan_child_alive"
	EventKindStateFileCorrupt                SupervisorEventKind = "state_file_corrupt"
	EventKindUndeterminedLong                SupervisorEventKind = "undetermined_long"
	EventKindStopSet                         SupervisorEventKind = "stop_set"
	EventKindStopCleared                     SupervisorEventKind = "stop_cleared"
	EventKindStopFileRemoved                 SupervisorEventKind = "stop_file_removed"
	EventKindSendIssued                      SupervisorEventKind = "send_issued"
	EventKindSendConfirmed                   SupervisorEventKind = "send_confirmed"
	EventKindSendUnconfirmed                 SupervisorEventKind = "send_unconfirmed"
	EventKindSendBlocked                     SupervisorEventKind = "send_blocked"
	EventKindSendAborted                     SupervisorEventKind = "send_aborted"
	EventKindSendPendingDiscarded            SupervisorEventKind = "send_pending_discarded"
	EventKindRecommendationBlocked           SupervisorEventKind = "recommendation_blocked"
	EventKindPermissionPending               SupervisorEventKind = "permission_pending"
	EventKindPermissionModeUnsafe            SupervisorEventKind = "permission_mode_unsafe"
	EventKindNoChanges                       SupervisorEventKind = "no_changes"
	EventKindSensitivePathBlocked            SupervisorEventKind = "sensitive_path_blocked"
	EventKindVerifyFailed                    SupervisorEventKind = "verify_failed"
	EventKindTreeChangedDuringVerify         SupervisorEventKind = "tree_changed_during_verify"
	EventKindCommitCreated                   SupervisorEventKind = "commit_created"
	EventKindCommitFailed                    SupervisorEventKind = "commit_failed"
	EventKindCommitSuppressedSameFingerprint SupervisorEventKind = "commit_suppressed_same_fingerprint"
	EventKindRepoBusy                        SupervisorEventKind = "repo_busy"
	EventKindPushBlocked                     SupervisorEventKind = "push_blocked"
	EventKindPushSucceeded                   SupervisorEventKind = "push_succeeded"
	EventKindPushFailed                      SupervisorEventKind = "push_failed"
	EventKindPushSuppressedSameHead          SupervisorEventKind = "push_suppressed_same_head"
	EventKindRotatePending                   SupervisorEventKind = "rotate_pending"
	EventKindRotateStepDone                  SupervisorEventKind = "rotate_step_done"
	EventKindRotateResumed                   SupervisorEventKind = "rotate_resumed"
	EventKindRotateCompleted                 SupervisorEventKind = "rotate_completed"
	EventKindRotateFailed                    SupervisorEventKind = "rotate_failed"
	EventKindRotationNotApplicable           SupervisorEventKind = "rotation_not_applicable"
	EventKindRemoteControlPending            SupervisorEventKind = "remote_control_pending"
	EventKindRemoteControlExpired            SupervisorEventKind = "remote_control_expired"
	EventKindContextUnknown                  SupervisorEventKind = "context_unknown"
	EventKindNotifyFailed                    SupervisorEventKind = "notify_failed"
	EventKindNotifyDropped                   SupervisorEventKind = "notify_dropped"
	EventKindSupervisorStarted               SupervisorEventKind = "supervisor_started"
	EventKindLunaImplNotAllowed              SupervisorEventKind = "luna_impl_not_allowed"
	EventKindRotateIdleWaitTimeout           SupervisorEventKind = "rotate_idle_wait_timeout"
)

func ValidSupervisorEventKind(value SupervisorEventKind) bool {
	switch value {
	case EventKindSessionStarted:
		return true
	case EventKindSessionStartFailed:
		return true
	case EventKindSessionStartInterrupted:
		return true
	case EventKindStateChanged:
		return true
	case EventKindIllegalTransition:
		return true
	case EventKindSessionStopped:
		return true
	case EventKindSessionResumed:
		return true
	case EventKindResumeRefused:
		return true
	case EventKindSessionFailed:
		return true
	case EventKindSessionExited:
		return true
	case EventKindSessionRestarted:
		return true
	case EventKindRestartCountersReset:
		return true
	case EventKindExitWhileStopped:
		return true
	case EventKindRestartLimitExceeded:
		return true
	case EventKindOrphanChildAlive:
		return true
	case EventKindStateFileCorrupt:
		return true
	case EventKindUndeterminedLong:
		return true
	case EventKindStopSet:
		return true
	case EventKindStopCleared:
		return true
	case EventKindStopFileRemoved:
		return true
	case EventKindSendIssued:
		return true
	case EventKindSendConfirmed:
		return true
	case EventKindSendUnconfirmed:
		return true
	case EventKindSendBlocked:
		return true
	case EventKindSendAborted:
		return true
	case EventKindSendPendingDiscarded:
		return true
	case EventKindRecommendationBlocked:
		return true
	case EventKindPermissionPending:
		return true
	case EventKindPermissionModeUnsafe:
		return true
	case EventKindNoChanges:
		return true
	case EventKindSensitivePathBlocked:
		return true
	case EventKindVerifyFailed:
		return true
	case EventKindTreeChangedDuringVerify:
		return true
	case EventKindCommitCreated:
		return true
	case EventKindCommitFailed:
		return true
	case EventKindCommitSuppressedSameFingerprint:
		return true
	case EventKindRepoBusy:
		return true
	case EventKindPushBlocked:
		return true
	case EventKindPushSucceeded:
		return true
	case EventKindPushFailed:
		return true
	case EventKindPushSuppressedSameHead:
		return true
	case EventKindRotatePending:
		return true
	case EventKindRotateStepDone:
		return true
	case EventKindRotateResumed:
		return true
	case EventKindRotateCompleted:
		return true
	case EventKindRotateFailed:
		return true
	case EventKindRotationNotApplicable:
		return true
	case EventKindRemoteControlPending:
		return true
	case EventKindRemoteControlExpired:
		return true
	case EventKindContextUnknown:
		return true
	case EventKindNotifyFailed:
		return true
	case EventKindNotifyDropped:
		return true
	case EventKindSupervisorStarted:
		return true
	case EventKindLunaImplNotAllowed:
		return true
	case EventKindRotateIdleWaitTimeout:
		return true
	}
	return false
}

type EventRecorder interface {
	RecordEvent(SessionID, SupervisorEventKind, SupervisorStopReason, SupervisorExitReason, SupervisorRotateFailReason, time.Time)
	RecordStateTransition(SessionID, SupervisorSessionState, SupervisorSessionState, time.Time)
	RecordReason(SessionID, SupervisorStopReason, time.Time)
	RecordExit(SessionID, SupervisorExitReason, time.Time)
	RecordDetection(SessionID, DetectionBasis, time.Time)
}
