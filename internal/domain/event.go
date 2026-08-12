package domain

import "time"

type Event interface {
	Type() string
	event()
}
type TaskQueued struct {
	TaskID                  TaskID     `json:"task_id"`
	Subcommand              Subcommand `json:"subcommand"`
	Slug                    Slug       `json:"slug"`
	RequestedTimeoutSeconds *int       `json:"requested_timeout_seconds,omitempty"`
	QueuePosition           int        `json:"queue_position"`
	OccurredAt              time.Time  `json:"occurred_at"`
}

func (TaskQueued) Type() string { return "TaskQueued" }
func (TaskQueued) event()       {}

type TaskStarted struct {
	TaskID                 TaskID           `json:"task_id"`
	ProcessStartTime       ProcessStartTime `json:"process_start_time"`
	ResolvedTimeoutSeconds int              `json:"resolved_timeout_seconds"`
	Model                  string           `json:"model"`
	ExecutionRoute         ExecutionRoute   `json:"execution_route"`
	OccurredAt             time.Time        `json:"occurred_at"`
}

func (TaskStarted) Type() string { return "TaskStarted" }
func (TaskStarted) event()       {}

type TaskEventObserved struct {
	TaskID        TaskID    `json:"task_id"`
	JSONEventType string    `json:"json_event_type"`
	ObservedAt    time.Time `json:"observed_at"`
}

func (TaskEventObserved) Type() string { return "TaskEventObserved" }
func (TaskEventObserved) event()       {}

type TaskStalled struct {
	TaskID      TaskID     `json:"task_id"`
	LastEventAt *time.Time `json:"last_event_at"`
	GapSeconds  int        `json:"gap_seconds"`
	OccurredAt  time.Time  `json:"occurred_at"`
}

func (TaskStalled) Type() string { return "TaskStalled" }
func (TaskStalled) event()       {}

type TaskExited struct {
	TaskID              TaskID    `json:"task_id"`
	ExitCode            ExitCode  `json:"exit_code"`
	LastMessagePresent  bool      `json:"last_message_present"`
	Estimated           bool      `json:"estimated"`
	AdoptedAfterRestart bool      `json:"adopted_after_restart"`
	OccurredAt          time.Time `json:"occurred_at"`
}

func (TaskExited) Type() string { return "TaskExited" }
func (TaskExited) event()       {}

type TaskCompleted struct {
	TaskID     TaskID    `json:"task_id"`
	ExitCode   ExitCode  `json:"exit_code"`
	OccurredAt time.Time `json:"occurred_at"`
}

func (TaskCompleted) Type() string { return "TaskCompleted" }
func (TaskCompleted) event()       {}

type TaskFailed struct {
	TaskID     TaskID    `json:"task_id"`
	ExitCode   ExitCode  `json:"exit_code"`
	Reason     string    `json:"reason"`
	OccurredAt time.Time `json:"occurred_at"`
}

func (TaskFailed) Type() string { return "TaskFailed" }
func (TaskFailed) event()       {}

type TaskTimedOut struct {
	TaskID                 TaskID      `json:"task_id"`
	ResolvedTimeoutSeconds int         `json:"resolved_timeout_seconds"`
	SessionRef             *SessionRef `json:"session_ref"`
	OccurredAt             time.Time   `json:"occurred_at"`
}

func (TaskTimedOut) Type() string { return "TaskTimedOut" }
func (TaskTimedOut) event()       {}

type RecoveryAttempted struct {
	TaskID     TaskID         `json:"task_id"`
	Origin     RecoveryOrigin `json:"origin"`
	SessionRef *SessionRef    `json:"session_ref"`
	OccurredAt time.Time      `json:"occurred_at"`
}

func (RecoveryAttempted) Type() string { return "RecoveryAttempted" }
func (RecoveryAttempted) event()       {}

type RecoverySucceeded struct {
	TaskID     TaskID    `json:"task_id"`
	ExitCode   ExitCode  `json:"exit_code"`
	OccurredAt time.Time `json:"occurred_at"`
}

func (RecoverySucceeded) Type() string { return "RecoverySucceeded" }
func (RecoverySucceeded) event()       {}

type RecoveryFailed struct {
	TaskID             TaskID         `json:"task_id"`
	Origin             RecoveryOrigin `json:"origin"`
	PartialOutputSaved bool           `json:"partial_output_saved"`
	OccurredAt         time.Time      `json:"occurred_at"`
}

func (RecoveryFailed) Type() string { return "RecoveryFailed" }
func (RecoveryFailed) event()       {}

type TaskAdopted struct {
	TaskID                TaskID    `json:"task_id"`
	LockAcquiredOnRestart bool      `json:"lock_acquired_on_restart"`
	OccurredAt            time.Time `json:"occurred_at"`
}

func (TaskAdopted) Type() string { return "TaskAdopted" }
func (TaskAdopted) event()       {}

type TaskOrphanDetected struct {
	TaskID         TaskID    `json:"task_id"`
	DetectedDuring string    `json:"detected_during"`
	OccurredAt     time.Time `json:"occurred_at"`
}

func (TaskOrphanDetected) Type() string { return "TaskOrphanDetected" }
func (TaskOrphanDetected) event()       {}

type TaskCancelRequested struct {
	TaskID       TaskID       `json:"task_id"`
	RequestedVia ProtocolVerb `json:"requested_via"`
	Force        bool         `json:"force"`
	OccurredAt   time.Time    `json:"occurred_at"`
}

func (TaskCancelRequested) Type() string { return "TaskCancelRequested" }
func (TaskCancelRequested) event()       {}

type TaskKilled struct {
	TaskID     TaskID    `json:"task_id"`
	ExitCode   ExitCode  `json:"exit_code"`
	Estimated  bool      `json:"estimated"`
	OccurredAt time.Time `json:"occurred_at"`
}

func (TaskKilled) Type() string { return "TaskKilled" }
func (TaskKilled) event()       {}
