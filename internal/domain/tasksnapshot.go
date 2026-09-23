package domain

import (
	"fmt"
	"time"
)

const taskSnapshotSchemaVersion = 4

const workingDirTaskSnapshotSchemaVersion = 3

const previousTaskSnapshotSchemaVersion = 2

func copyOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// NewInitialTaskSnapshot creates the base metadata for a task before its first persistence.
func NewInitialTaskSnapshot(route ExecutionRoute, reasoningEffort *string, sandboxMode string, workingDir *string) TaskSnapshot {
	return TaskSnapshot{
		Route:           route,
		ReasoningEffort: copyOptionalString(reasoningEffort),
		SandboxMode:     sandboxMode,
		WorkingDir:      copyOptionalString(workingDir),
		SchemaVersion:   taskSnapshotSchemaVersion,
	}
}

// NewTaskSnapshotFromAdmission creates the first persisted snapshot for an admitted task.
func NewTaskSnapshotFromAdmission(task *Task, resolvedTimeout Timeout, model string, reasoningEffort *string, sandboxMode string, workingDir *string, route ExecutionRoute, stateUpdatedAt time.Time) (TaskSnapshot, error) {
	if task == nil {
		return TaskSnapshot{}, fmt.Errorf("task is nil")
	}
	var requestedTimeoutCopy *int
	if requested := task.requestedTimeout; requested != nil {
		value := *requested
		requestedTimeoutCopy = &value
	}
	snapshot := TaskSnapshot{
		TaskID:                  task.ID(),
		Subcommand:              task.Subcommand(),
		ResolvedTimeoutSeconds:  resolvedTimeout.ResolvedSeconds(),
		RequestedTimeoutSeconds: requestedTimeoutCopy,
		Model:                   model,
		ReasoningEffort:         copyOptionalString(reasoningEffort),
		SandboxMode:             sandboxMode,
		WorkingDir:              copyOptionalString(workingDir),
		RequestedAt:             task.requestedAt,
		Route:                   route,
		State:                   task.State(),
		StateUpdatedAt:          stateUpdatedAt,
		Recovered:               false,
		AdoptedAfterRestart:     false,
		SchemaVersion:           taskSnapshotSchemaVersion,
	}
	if err := snapshot.Validate(); err != nil {
		return TaskSnapshot{}, err
	}
	return snapshot, nil
}

type TaskSnapshot struct {
	TaskID                  TaskID          `json:"task_id"`
	Subcommand              Subcommand      `json:"subcommand"`
	PID                     *int            `json:"pid"`
	ProcessStartedAt        *time.Time      `json:"process_started_at"`
	ResolvedTimeoutSeconds  int             `json:"resolved_timeout_seconds"`
	RequestedTimeoutSeconds *int            `json:"requested_timeout_seconds,omitempty"`
	Model                   string          `json:"model"`
	ReasoningEffort         *string         `json:"reasoning_effort"`
	SandboxMode             string          `json:"sandbox_mode"`
	WorkingDir              *string         `json:"working_dir,omitempty"`
	RequestedAt             time.Time       `json:"requested_at"`
	Route                   ExecutionRoute  `json:"route"`
	State                   TaskState       `json:"state"`
	StateUpdatedAt          time.Time       `json:"state_updated_at"`
	SessionRef              *SessionRef     `json:"session_ref"`
	LastEventAt             *time.Time      `json:"last_event_at"`
	ExitCode                *ExitCode       `json:"exit_code"`
	Recovered               bool            `json:"recovered"`
	AdoptedAfterRestart     bool            `json:"adopted_after_restart"`
	RecoveryOrigin          *RecoveryOrigin `json:"recovery_origin"`
	FailureCode             *string         `json:"failure_code,omitempty"`
	SchemaVersion           int             `json:"schema_version"`
}

// SupportsWorkingDir reports whether the snapshot schema owns the working_dir field.
func (s TaskSnapshot) SupportsWorkingDir() bool {
	return s.SchemaVersion >= workingDirTaskSnapshotSchemaVersion
}

// SupportsFailureCode reports whether the snapshot schema owns the failure_code field.
func (s TaskSnapshot) SupportsFailureCode() bool {
	return s.SchemaVersion == taskSnapshotSchemaVersion
}

func isKnownTaskState(s TaskState) bool {
	switch s {
	case StateQueued, StateStarting, StateRunning, StateStalled, StateCompleted, StateFailed, StateTimeout, StateRecovering, StateRecovered, StateTimeoutLost, StateCancelling, StateKilled, StateAdopted, StateOrphaned, StateLost:
		return true
	}
	return false
}
func (s TaskSnapshot) Validate() error {
	bad := func(f string, a ...any) error { return fmt.Errorf("task snapshot invalid: "+f, a...) }
	if s.TaskID.String() == "" {
		return bad("task id is empty")
	}
	if !IsSubmittable(s.Subcommand) {
		return bad("unknown subcommand %q", s.Subcommand)
	}
	if s.Model == "" {
		return bad("model is empty")
	}
	if !IsValidSandboxMode(s.SandboxMode) {
		return bad("unknown sandbox mode %q", s.SandboxMode)
	}
	if !isKnownTaskState(s.State) {
		return bad("unknown state %q", s.State)
	}
	if s.Route != ExecutionRouteDaemon {
		return bad("route must be daemon")
	}
	if (s.PID == nil) != (s.ProcessStartedAt == nil) {
		return bad("pid and process_started_at must be set together")
	}
	if s.PID != nil && *s.PID <= 0 {
		return bad("pid must be positive")
	}
	if s.ProcessStartedAt != nil && s.ProcessStartedAt.IsZero() {
		return bad("process_started_at is zero")
	}
	if s.RequestedTimeoutSeconds != nil && *s.RequestedTimeoutSeconds < timeoutMinSeconds {
		return bad("requested timeout below minimum")
	}
	if s.ResolvedTimeoutSeconds < timeoutMinSeconds {
		return bad("resolved timeout below minimum")
	}
	if s.RecoveryOrigin != nil && *s.RecoveryOrigin != RecoveryOriginTimeout && *s.RecoveryOrigin != RecoveryOriginOrphan {
		return bad("unknown recovery origin")
	}
	requires := map[TaskState]bool{StateRunning: true, StateStalled: true, StateTimeout: true, StateRecovering: true, StateRecovered: true, StateTimeoutLost: true, StateLost: true, StateCompleted: true}
	processInfoMayBeAbsentAfterOrphanFinalization := s.State.terminal() && s.ExitCode != nil
	if requires[s.State] && s.PID == nil && !s.AdoptedAfterRestart && !processInfoMayBeAbsentAfterOrphanFinalization {
		return bad("state requires process info")
	}
	if s.ExitCode != nil && !s.State.terminal() {
		return bad("exit code for non-terminal state")
	}
	if s.Recovered != (s.State == StateRecovered) {
		return bad("recovered inconsistent")
	}
	lineage := s.State == StateRecovering || s.State == StateRecovered || s.State == StateTimeoutLost || s.State == StateLost
	if (s.RecoveryOrigin != nil) != lineage {
		return bad("recovery origin inconsistent")
	}
	if s.RequestedAt.IsZero() || s.StateUpdatedAt.IsZero() {
		return bad("timestamp is zero")
	}
	if s.SchemaVersion != previousTaskSnapshotSchemaVersion && s.SchemaVersion != workingDirTaskSnapshotSchemaVersion && s.SchemaVersion != taskSnapshotSchemaVersion {
		return bad("unsupported schema version")
	}
	if s.FailureCode != nil {
		if s.SchemaVersion != taskSnapshotSchemaVersion || s.State != StateFailed || s.PID != nil {
			return bad("failure_code requires schema version 4, failed state, and no process")
		}
		switch *s.FailureCode {
		case "LIVENESS_LOCK_IO_ERROR", "CONTRACT_WRITE_FAILED", "WORKTREE_CREATE_FAILED", "PTY_ALLOCATION_FAILED", "CHILD_PROCESS_LAUNCH_FAILED":
		default:
			return bad("unknown failure_code %q", *s.FailureCode)
		}
	}
	if s.SchemaVersion == previousTaskSnapshotSchemaVersion {
		if s.WorkingDir != nil {
			return bad("schema version 2 must not contain working_dir")
		}
		return nil
	}
	if s.WorkingDir == nil {
		if !s.allowsUnresolvedWorkingDir() {
			return bad("working_dir is required")
		}
		return nil
	}
	if _, err := NewNormalizedPath(*s.WorkingDir); err != nil {
		return bad("working_dir: %v", err)
	}
	return nil
}

func (s TaskSnapshot) allowsUnresolvedWorkingDir() bool {
	if s.PID != nil || s.ProcessStartedAt != nil {
		return false
	}
	switch s.State {
	case StateQueued, StateCancelling, StateKilled, StateFailed:
		return true
	default:
		return false
	}
}
func (s TaskSnapshot) Restore() (*Task, error) {
	if e := s.Validate(); e != nil {
		return nil, e
	}
	sl, e := slugFromTaskID(s.TaskID)
	if e != nil {
		return nil, e
	}
	to, e := NewTimeout(s.RequestedTimeoutSeconds, s.ResolvedTimeoutSeconds)
	if e != nil {
		return nil, e
	}
	t := &Task{id: s.TaskID, subcommand: s.Subcommand, slug: sl, requestedTimeout: s.RequestedTimeoutSeconds, requestedAt: s.RequestedAt, state: s.State, timeout: to, model: s.Model, sessionRef: s.SessionRef, adoptedAfterRestart: s.AdoptedAfterRestart, exitCode: s.ExitCode}
	if s.PID != nil {
		p, e := NewProcessStartTime(*s.PID, *s.ProcessStartedAt)
		if e != nil {
			return nil, e
		}
		t.processStartTime = &p
	}
	if s.LastEventAt != nil {
		t.lastEventAt = *s.LastEventAt
	}
	if s.RecoveryOrigin != nil {
		t.recoveryOrigin = *s.RecoveryOrigin
	}
	return t, nil
}
func (s TaskSnapshot) WithTask(t *Task, at time.Time) (TaskSnapshot, error) {
	n := s
	n.TaskID = t.id
	n.Subcommand = t.subcommand
	n.State = t.state
	n.StateUpdatedAt = at
	n.Model = t.model
	n.RequestedTimeoutSeconds = t.timeout.RequestedSeconds()
	n.ResolvedTimeoutSeconds = t.timeout.ResolvedSeconds()
	n.RequestedAt = t.requestedAt
	n.PID, n.ProcessStartedAt = nil, nil
	if t.processStartTime != nil {
		p := t.processStartTime.PID()
		st := t.processStartTime.StartedAt()
		n.PID = &p
		n.ProcessStartedAt = &st
	}
	n.SessionRef = t.sessionRef
	n.LastEventAt = nil
	if !t.lastEventAt.IsZero() {
		x := t.lastEventAt
		n.LastEventAt = &x
	}
	n.ExitCode = t.exitCode
	n.AdoptedAfterRestart = t.adoptedAfterRestart
	n.RecoveryOrigin = nil
	if t.recoveryOrigin != "" {
		x := t.recoveryOrigin
		n.RecoveryOrigin = &x
	}
	n.Recovered = s.Recovered || t.state == StateRecovered
	if err := n.Validate(); err != nil {
		return TaskSnapshot{}, err
	}
	return n, nil
}

func slugFromTaskID(id TaskID) (Slug, error) {
	m := taskIDPattern.FindStringSubmatch(id.String())
	if m == nil {
		return Slug{}, fmt.Errorf("task id does not embed a slug: %s", id.String())
	}
	return NewSlug(m[5])
}
