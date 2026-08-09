package execution

import (
	"reflect"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func queueTestPayload(t *testing.T, suffix string) TaskLaunchPayload {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260809-120000-a1b2-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	slug, err := domain.NewSlug(suffix)
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := domain.NewTask(id, domain.SubcommandImpl, slug, nil, time.Now(), 1)
	if err != nil {
		t.Fatal(err)
	}
	return TaskLaunchPayload{Task: task}
}

func TestTaskQueueFIFOAndReindex(t *testing.T) {
	queue := NewTaskQueue()
	first, second := queueTestPayload(t, "first"), queueTestPayload(t, "second")
	if got := queue.Enqueue(first); got != 1 {
		t.Fatalf("first position = %d", got)
	}
	queue.Enqueue(second)
	dequeued, found := queue.Dequeue()
	if !found || dequeued.Task.ID() != first.Task.ID() {
		t.Fatalf("dequeued = %#v, found=%t", dequeued, found)
	}
	events := queue.Reindex(time.Now())
	if len(events) != 1 || events[0].(domain.TaskQueued).QueuePosition != 1 {
		t.Fatalf("events = %#v", events)
	}
}

func TestTaskQueuePrependAndRemove(t *testing.T) {
	queue := NewTaskQueue()
	first, second := queueTestPayload(t, "first"), queueTestPayload(t, "second")
	queue.Enqueue(second)
	queue.Prepend(first)
	removed, events := queue.Remove(first.Task.ID(), time.Now())
	if !removed || len(events) != 1 {
		t.Fatalf("removed=%t events=%#v", removed, events)
	}
	if _, found := queue.Dequeue(); !found {
		t.Fatal("queue unexpectedly empty")
	}
}

func TestTaskQueueRejectsInvalidPayload(t *testing.T) {
	queue := NewTaskQueue()
	defer func() {
		if recover() == nil {
			t.Fatal("Enqueue did not panic")
		}
	}()
	queue.Enqueue(TaskLaunchPayload{})
}

func TestTaskQueueDequeueEmptyDoesNotMutate(t *testing.T) {
	queue := NewTaskQueue()
	payload, found := queue.Dequeue()
	if found || !reflect.DeepEqual(payload, TaskLaunchPayload{}) || queue.Len() != 0 {
		t.Fatalf("payload=%#v found=%t length=%d", payload, found, queue.Len())
	}
}

func TestTaskQueueRejectsInvalidPayloadWithoutMutation(t *testing.T) {
	for _, operation := range []struct {
		name string
		run  func(TaskQueue, TaskLaunchPayload)
	}{
		{"enqueue", func(q TaskQueue, p TaskLaunchPayload) { q.Enqueue(p) }},
		{"prepend", func(q TaskQueue, p TaskLaunchPayload) { q.Prepend(p) }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			queue := NewTaskQueue()
			existing := queueTestPayload(t, "existing-"+operation.name)
			queue.Enqueue(existing)
			for _, invalid := range []TaskLaunchPayload{{}, {Task: nonQueuedTask(t, "invalid-"+operation.name)}} {
				func() {
					defer func() {
						if recover() == nil {
							t.Fatal("operation did not panic")
						}
					}()
					operation.run(queue, invalid)
				}()
				if queue.Len() != 1 {
					t.Fatalf("length after panic = %d", queue.Len())
				}
				got, found := queue.Dequeue()
				if !found || got.Task.ID() != existing.Task.ID() {
					t.Fatalf("queue changed after panic: %#v found=%t", got, found)
				}
				queue.Enqueue(existing)
			}
		})
	}
}

func TestTaskQueueReindexAndRemovePreflightAreAtomic(t *testing.T) {
	for _, operation := range []struct {
		name string
		run  func(TaskQueue)
	}{
		{"reindex", func(q TaskQueue) { q.Reindex(time.Now()) }},
		{"remove", func(q TaskQueue) { q.Remove(queueTestPayload(t, "missing").Task.ID(), time.Now()) }},
	} {
		t.Run(operation.name, func(t *testing.T) {
			queue := NewTaskQueue()
			first := queueTestPayload(t, "atomic-first-"+operation.name)
			invalid := TaskLaunchPayload{Task: nonQueuedTask(t, "atomic-invalid-"+operation.name)}
			concrete := queue.(*taskQueue)
			concrete.payloads = []TaskLaunchPayload{first, invalid}
			before := append([]TaskLaunchPayload(nil), concrete.payloads...)
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("operation did not panic")
					}
				}()
				operation.run(queue)
			}()
			if !reflect.DeepEqual(concrete.payloads, before) {
				t.Fatalf("queue mutated during preflight failure: got=%#v want=%#v", concrete.payloads, before)
			}
		})
	}
}

func TestTaskQueueClearsRemovedBackingArraySlots(t *testing.T) {
	t.Run("dequeue", func(t *testing.T) {
		queue := NewTaskQueue().(*taskQueue)
		first, second := queueTestPayload(t, "clear-first"), queueTestPayload(t, "clear-second")
		queue.Enqueue(first)
		queue.Enqueue(second)
		backing := queue.payloads
		got, found := queue.Dequeue()
		if !found || got.Task.ID() != first.Task.ID() || !reflect.DeepEqual(backing[0], TaskLaunchPayload{}) || queue.payloads[0].Task.ID() != second.Task.ID() {
			t.Fatalf("dequeue did not preserve payloads and clear slot: got=%#v backing=%#v", got, backing)
		}
	})
	t.Run("remove", func(t *testing.T) {
		queue := NewTaskQueue().(*taskQueue)
		first, second := queueTestPayload(t, "remove-first"), queueTestPayload(t, "remove-second")
		queue.Enqueue(first)
		queue.Enqueue(second)
		backing := queue.payloads
		removed, _ := queue.Remove(first.Task.ID(), time.Now())
		if !removed || queue.payloads[0].Task.ID() != second.Task.ID() || !reflect.DeepEqual(backing[1], TaskLaunchPayload{}) {
			t.Fatalf("remove did not preserve payload and clear old tail: queue=%#v backing=%#v", queue.payloads, backing)
		}
	})
}

func nonQueuedTask(t *testing.T, suffix string) *domain.Task {
	t.Helper()
	payload := queueTestPayload(t, suffix)
	timeout, err := domain.NewTimeout(nil, 1800)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := payload.Task.Start(timeout, "model", time.Now()); err != nil {
		t.Fatal(err)
	}
	return payload.Task
}
