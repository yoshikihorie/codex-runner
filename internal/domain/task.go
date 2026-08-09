package domain

import (
	"fmt"
	"time"
)

const (
	eventTaskQueued          = "TaskQueued"
	eventTaskStarted         = "TaskStarted"
	eventTaskEventObserved   = "TaskEventObserved"
	eventTaskStalled         = "TaskStalled"
	eventTaskExited          = "TaskExited"
	eventTaskTimedOut        = "TaskTimedOut"
	eventRecoveryAttempted   = "RecoveryAttempted"
	eventRecoverySucceeded   = "RecoverySucceeded"
	eventRecoveryFailed      = "RecoveryFailed"
	eventTaskAdopted         = "TaskAdopted"
	eventTaskOrphanDetected  = "TaskOrphanDetected"
	eventTaskCancelRequested = "TaskCancelRequested"
	eventTaskKilled          = "TaskKilled"
)

// Task is the in-memory aggregate for one submitted Codex command.
type Task struct {
	id                  TaskID
	subcommand          Subcommand
	slug                Slug
	requestedTimeout    *int
	requestedAt         time.Time
	state               TaskState
	queuePosition       int
	timeout             Timeout
	model               string
	processStartTime    *ProcessStartTime
	lastEventAt         time.Time
	sessionRef          *SessionRef
	recoveryOrigin      RecoveryOrigin
	adoptedAfterRestart bool
	exitCode            *ExitCode
}

// NewTask receives the one-based queue position calculated by the admitting use case.
func NewTask(id TaskID, subcommand Subcommand, slug Slug, requestedTimeout *int, requestedAt time.Time, initialQueuePosition int) (*Task, []Event, error) {
	if !IsSubmittable(subcommand) {
		return nil, nil, fmt.Errorf("subcommand is not submittable")
	}
	var requestedCopy *int
	if requestedTimeout != nil {
		value := *requestedTimeout
		requestedCopy = &value
	}
	task := &Task{
		id:               id,
		subcommand:       subcommand,
		slug:             slug,
		requestedTimeout: requestedCopy,
		requestedAt:      requestedAt,
		state:            StateQueued,
		queuePosition:    initialQueuePosition,
	}
	return task, []Event{TaskQueued{TaskID: id, Subcommand: subcommand, Slug: slug, RequestedTimeoutSeconds: requestedCopy, QueuePosition: initialQueuePosition, OccurredAt: requestedAt}}, nil
}

func (t *Task) ID() TaskID             { return t.id }
func (t *Task) Subcommand() Subcommand { return t.subcommand }
func (t *Task) State() TaskState       { return t.state }

// transition is the only state-writing operation.  The event names are the
// canonical domain event type names from the state-transition table.
func (t *Task) transition(event string, destination TaskState) error {
	if t.state.terminal() && event == eventTaskCancelRequested {
		return ErrTaskAlreadyTerminal
	}
	allowed := map[TaskState]map[string]bool{
		StateQueued:     {eventTaskQueued: true, eventTaskStarted: true, eventTaskCancelRequested: true},
		StateStarting:   {eventTaskExited: true, eventTaskAdopted: true, eventTaskOrphanDetected: true, eventTaskCancelRequested: true},
		StateRunning:    {eventTaskEventObserved: true, eventTaskStalled: true, eventTaskExited: true, eventTaskTimedOut: true, eventTaskAdopted: true, eventTaskOrphanDetected: true, eventTaskCancelRequested: true},
		StateStalled:    {eventTaskEventObserved: true, eventTaskStalled: true, eventTaskExited: true, eventTaskTimedOut: true, eventTaskAdopted: true, eventTaskOrphanDetected: true, eventTaskCancelRequested: true},
		StateTimeout:    {eventRecoveryAttempted: true},
		StateRecovering: {eventRecoverySucceeded: true, eventRecoveryFailed: true},
		StateCancelling: {eventTaskCancelRequested: true, eventTaskKilled: true},
		StateAdopted:    {eventTaskExited: true, eventTaskOrphanDetected: true, eventTaskCancelRequested: true},
		StateOrphaned:   {eventTaskExited: true, eventRecoveryAttempted: true, eventTaskCancelRequested: true},
	}
	if !allowed[t.state][event] {
		return ErrInvalidStateTransition
	}
	t.state = destination
	return nil
}

func (t *Task) Requeue(queuePosition int, occurredAt time.Time) ([]Event, error) {
	if err := t.transition(eventTaskQueued, StateQueued); err != nil {
		return nil, err
	}
	t.queuePosition = queuePosition
	return []Event{TaskQueued{TaskID: t.id, Subcommand: t.subcommand, Slug: t.slug, RequestedTimeoutSeconds: t.requestedTimeout, QueuePosition: queuePosition, OccurredAt: occurredAt}}, nil
}

func (t *Task) Start(timeout Timeout, model string, occurredAt time.Time) ([]Event, error) {
	if err := t.transition(eventTaskStarted, StateStarting); err != nil {
		return nil, err
	}
	t.timeout, t.model = timeout, model
	return nil, nil
}

func (t *Task) RecordProcessInfo(pid int, processStartedAt, occurredAt time.Time) ([]Event, error) {
	if t.state != StateStarting || t.processStartTime != nil {
		return nil, ErrInvalidStateTransition
	}
	process, err := NewProcessStartTime(pid, processStartedAt)
	if err != nil {
		return nil, err
	}
	t.processStartTime = &process
	return []Event{TaskStarted{TaskID: t.id, ProcessStartTime: process, ResolvedTimeoutSeconds: t.timeout.ResolvedSeconds(), Model: t.model, ExecutionRoute: ExecutionRouteDaemon, OccurredAt: occurredAt}}, nil
}

func (t *Task) ConfirmRunning(occurredAt time.Time) error {
	if t.state == StateStarting || t.state == StateAdopted {
		// The transition table has no event column for this liveness-confirmation
		// result, so it is an internal state confirmation rather than a domain
		// event transition. See the Task model specification §2 invariant 9.
		t.state = StateRunning
		return nil
	}
	return ErrInvalidStateTransition
}

func (t *Task) ObserveEvent(sequence int, eventType string, observedAt time.Time) ([]Event, error) {
	if err := t.transition(eventTaskEventObserved, StateRunning); err != nil {
		return nil, err
	}
	t.lastEventAt = observedAt
	return []Event{TaskEventObserved{TaskID: t.id, JSONEventType: eventType, ObservedAt: observedAt}}, nil
}

func (t *Task) MarkStalled(gapSeconds int, occurredAt time.Time) ([]Event, error) {
	if err := t.transition(eventTaskStalled, StateStalled); err != nil {
		return nil, err
	}
	if t.lastEventAt.IsZero() {
		t.lastEventAt = occurredAt.Add(-time.Duration(gapSeconds) * time.Second)
	}
	return []Event{TaskStalled{TaskID: t.id, LastEventAt: t.lastEventAt, GapSeconds: gapSeconds, OccurredAt: occurredAt}}, nil
}

func (t *Task) RecordExit(exitCode ExitCode, lastMessagePresent, estimated, adoptedAfterRestart bool, occurredAt time.Time) ([]Event, error) {
	if t.state == StateOrphaned && (!estimated || !adoptedAfterRestart) {
		return nil, fmt.Errorf("orphan exit must be estimated and adopted")
	}
	if t.adoptedAfterRestart {
		estimated, adoptedAfterRestart = true, true
	}
	destination := StateFailed
	if exitCode.Class() == ExitCodeClassSuccess && lastMessagePresent {
		destination = StateCompleted
	}
	if err := t.transition(eventTaskExited, destination); err != nil {
		return nil, err
	}
	t.exitCode = &exitCode
	events := []Event{TaskExited{TaskID: t.id, ExitCode: exitCode, LastMessagePresent: lastMessagePresent, Estimated: estimated, AdoptedAfterRestart: adoptedAfterRestart, OccurredAt: occurredAt}}
	if destination == StateCompleted {
		return append(events, TaskCompleted{TaskID: t.id, ExitCode: exitCode, OccurredAt: occurredAt}), nil
	}
	reason := ReasonNoOutput
	if exitCode.Class() != ExitCodeClassSuccess {
		reason = ReasonAbnormalExit
	}
	return append(events, TaskFailed{TaskID: t.id, ExitCode: exitCode, Reason: reason, OccurredAt: occurredAt}), nil
}

func (t *Task) MarkTimedOut(sessionRef *SessionRef, occurredAt time.Time) ([]Event, error) {
	if err := t.transition(eventTaskTimedOut, StateTimeout); err != nil {
		return nil, err
	}
	t.sessionRef = sessionRef
	return []Event{TaskTimedOut{TaskID: t.id, ResolvedTimeoutSeconds: t.timeout.ResolvedSeconds(), SessionRef: sessionRef, OccurredAt: occurredAt}}, nil
}

func (t *Task) BeginRecovery(sessionRef *SessionRef, occurredAt time.Time) ([]Event, error) {
	origin := RecoveryOriginTimeout
	if t.state == StateOrphaned {
		origin = RecoveryOriginOrphan
	}
	if err := t.transition(eventRecoveryAttempted, StateRecovering); err != nil {
		return nil, err
	}
	t.recoveryOrigin, t.sessionRef = origin, sessionRef
	return []Event{RecoveryAttempted{TaskID: t.id, Origin: origin, SessionRef: sessionRef, OccurredAt: occurredAt}}, nil
}

func (t *Task) CompleteRecovery(exitCode ExitCode, occurredAt time.Time) ([]Event, error) {
	if err := t.transition(eventRecoverySucceeded, StateRecovered); err != nil {
		return nil, err
	}
	t.exitCode = &exitCode
	return []Event{RecoverySucceeded{TaskID: t.id, ExitCode: exitCode, OccurredAt: occurredAt}}, nil
}

func (t *Task) FailRecovery(partialOutputSaved bool, occurredAt time.Time) ([]Event, error) {
	destination := StateLost
	if t.recoveryOrigin == RecoveryOriginTimeout {
		destination = StateTimeoutLost
	}
	if err := t.transition(eventRecoveryFailed, destination); err != nil {
		return nil, err
	}
	return []Event{RecoveryFailed{TaskID: t.id, Origin: t.recoveryOrigin, PartialOutputSaved: partialOutputSaved, OccurredAt: occurredAt}}, nil
}

func (t *Task) Adopt(lockAcquiredOnRestart bool, occurredAt time.Time) ([]Event, error) {
	if err := t.transition(eventTaskAdopted, StateAdopted); err != nil {
		return nil, err
	}
	t.adoptedAfterRestart = true
	events := []Event{TaskAdopted{TaskID: t.id, LockAcquiredOnRestart: lockAcquiredOnRestart, OccurredAt: occurredAt}}
	if lockAcquiredOnRestart {
		if err := t.transition(eventTaskOrphanDetected, StateOrphaned); err != nil {
			return nil, err
		}
		return append(events, TaskOrphanDetected{TaskID: t.id, DetectedDuring: "adopted", OccurredAt: occurredAt}), nil
	}
	// Returning to running after a live adoption is an internal liveness
	// confirmation, not a row in the 225-cell domain-event transition table.
	t.state = StateRunning
	return events, nil
}

func (t *Task) DetectOrphan(detectedDuring string, occurredAt time.Time) ([]Event, error) {
	if detectedDuring != "running" && detectedDuring != "starting" && detectedDuring != "adopted" {
		return nil, fmt.Errorf("invalid detected during")
	}
	if err := t.transition(eventTaskOrphanDetected, StateOrphaned); err != nil {
		return nil, err
	}
	return []Event{TaskOrphanDetected{TaskID: t.id, DetectedDuring: detectedDuring, OccurredAt: occurredAt}}, nil
}

func (t *Task) RequestCancel(force bool, occurredAt time.Time) ([]Event, error) {
	if err := t.transition(eventTaskCancelRequested, StateCancelling); err != nil {
		return nil, err
	}
	return []Event{TaskCancelRequested{TaskID: t.id, RequestedVia: ProtocolVerbCancel, Force: force, OccurredAt: occurredAt}}, nil
}

func (t *Task) ConfirmKilled(exitCode ExitCode, estimated bool, occurredAt time.Time) ([]Event, error) {
	if err := t.transition(eventTaskKilled, StateKilled); err != nil {
		return nil, err
	}
	t.exitCode = &exitCode
	return []Event{TaskKilled{TaskID: t.id, ExitCode: exitCode, Estimated: estimated, OccurredAt: occurredAt}}, nil
}
