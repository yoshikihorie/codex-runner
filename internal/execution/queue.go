package execution

import (
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// TaskAdmissionInput is the already validated information required to admit a task.
type TaskAdmissionInput struct {
	TaskID           domain.TaskID
	Subcommand       domain.Subcommand
	Slug             domain.Slug
	RequestedTimeout *int
	RequestedAt      time.Time
	PromptText       string
	NormalizedPaths  []domain.NormalizedPath
	ResolvedTimeout  domain.Timeout
	Model            string
	ReasoningEffort  *string
	SandboxMode      string
	SourceWorkingDir string
	WorktreeMode     domain.WorktreeMode
}

// TaskLaunchPayload carries all task data required by lifecycle orchestration.
type TaskLaunchPayload struct {
	Task             *domain.Task
	Model            string
	ReasoningEffort  *string
	PromptText       string
	NormalizedPaths  []domain.NormalizedPath
	ResolvedTimeout  domain.Timeout
	SandboxMode      string
	SourceWorkingDir string
	WorkingDir       *string
	WorktreeMode     domain.WorktreeMode
}

// TaskAdmissionResult distinguishes immediate admission from queued admission.
type TaskAdmissionResult struct {
	State         domain.TaskState
	QueuePosition *int
	Events        []domain.Event
	LaunchPayload *TaskLaunchPayload
}

// TaskQueue stores queued task launch payloads in FIFO order.
type TaskQueue interface {
	Enqueue(payload TaskLaunchPayload) (position int)
	Dequeue() (payload TaskLaunchPayload, found bool)
	Prepend(payload TaskLaunchPayload)
	Reindex(occurredAt time.Time) []domain.Event
	Len() int
	Remove(taskID domain.TaskID, occurredAt time.Time) (payload TaskLaunchPayload, index int, removed bool, events []domain.Event)
	Restore(payload TaskLaunchPayload, index int, occurredAt time.Time) []domain.Event
}

// TaskQueueReader is the status-read extension of TaskQueue. It is deliberately
// separate from TaskQueue so lifecycle callers do not depend on status queries.
type TaskQueueReader interface {
	TaskQueue
	Lookup(taskID domain.TaskID) (payload TaskLaunchPayload, index int, found bool)
	QueuePosition(taskID domain.TaskID) (position int, found bool, err error)
}

// ActiveTaskRegistry tracks execution-slot reservations by task ID and subcommand,
// allowing ImplSize to count reservations for impl tasks.
type ActiveTaskRegistry interface {
	Size() int
	ImplSize() int
	Add(taskID domain.TaskID, subcommand domain.Subcommand)
	Remove(taskID domain.TaskID)
	Reset(reservations map[domain.TaskID]domain.Subcommand)
}

type taskQueue struct {
	payloads []TaskLaunchPayload
}

var _ TaskQueueReader = (*taskQueue)(nil)

func NewTaskQueue() TaskQueueReader { return &taskQueue{} }

func (q *taskQueue) Enqueue(payload TaskLaunchPayload) int {
	validateQueuedPayload(payload)
	q.payloads = append(q.payloads, payload)
	return len(q.payloads)
}

func (q *taskQueue) Dequeue() (TaskLaunchPayload, bool) {
	if len(q.payloads) == 0 {
		return TaskLaunchPayload{}, false
	}
	payload := q.payloads[0]
	q.payloads[0] = TaskLaunchPayload{}
	q.payloads = q.payloads[1:]
	return payload, true
}

func (q *taskQueue) Prepend(payload TaskLaunchPayload) {
	validateQueuedPayload(payload)
	q.payloads = append([]TaskLaunchPayload{payload}, q.payloads...)
}

func (q *taskQueue) Reindex(occurredAt time.Time) []domain.Event {
	q.validateAll()
	return q.reindex(occurredAt)
}

func (q *taskQueue) Len() int { return len(q.payloads) }

// QueuePosition reports a task's one-based FIFO position without changing the queue.
func (q *taskQueue) QueuePosition(taskID domain.TaskID) (position int, found bool, err error) {
	_, index, found := q.Lookup(taskID)
	if !found {
		return 0, false, nil
	}
	return index + 1, true, nil
}

func (q *taskQueue) Lookup(taskID domain.TaskID) (TaskLaunchPayload, int, bool) {
	for index, payload := range q.payloads {
		if payload.Task != nil && payload.Task.ID() == taskID {
			return payload, index, true
		}
	}
	return TaskLaunchPayload{}, -1, false
}

func (q *taskQueue) Remove(taskID domain.TaskID, occurredAt time.Time) (TaskLaunchPayload, int, bool, []domain.Event) {
	q.validateAll()
	for index, payload := range q.payloads {
		if payload.Task.ID() != taskID {
			continue
		}
		copy(q.payloads[index:], q.payloads[index+1:])
		q.payloads[len(q.payloads)-1] = TaskLaunchPayload{}
		q.payloads = q.payloads[:len(q.payloads)-1]
		return payload, index, true, q.reindex(occurredAt)
	}
	return TaskLaunchPayload{}, -1, false, nil
}

func (q *taskQueue) Restore(payload TaskLaunchPayload, index int, occurredAt time.Time) []domain.Event {
	q.validateAll()
	validateQueuedPayload(payload)
	if index < 0 || index > len(q.payloads) {
		panic("task queue restore index out of range")
	}
	q.payloads = append(q.payloads, TaskLaunchPayload{})
	copy(q.payloads[index+1:], q.payloads[index:])
	q.payloads[index] = payload
	return q.reindex(occurredAt)
}

func (q *taskQueue) validateAll() {
	for _, payload := range q.payloads {
		validateQueuedPayload(payload)
	}
}

func (q *taskQueue) reindex(occurredAt time.Time) []domain.Event {
	events := make([]domain.Event, 0, len(q.payloads))
	for index, payload := range q.payloads {
		requeued, err := payload.Task.Requeue(index+1, occurredAt)
		if err != nil {
			panic(err)
		}
		events = append(events, requeued...)
	}
	return events
}

func validateQueuedPayload(payload TaskLaunchPayload) {
	if payload.Task == nil || payload.Task.State() != domain.StateQueued {
		panic("task queue payload must contain a queued task")
	}
}
