package execution

import (
	"reflect"
	"runtime"
	"strings"
	"sync"
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

func TestTaskQueueQueuePositionIsOneBasedAndNonDestructive(t *testing.T) {
	queue := NewTaskQueue().(*taskQueue)
	payloads := []TaskLaunchPayload{queueTestPayload(t, "position-first"), queueTestPayload(t, "position-middle"), queueTestPayload(t, "position-last")}
	for _, payload := range payloads {
		queue.Enqueue(payload)
	}
	before := append([]TaskLaunchPayload(nil), queue.payloads...)
	for index, payload := range payloads {
		position, found, err := queue.QueuePosition(payload.Task.ID())
		if err != nil || !found || position != index+1 {
			t.Fatalf("position=%d found=%t err=%v", position, found, err)
		}
	}
	missing := queueTestPayload(t, "position-missing")
	if position, found, err := queue.QueuePosition(missing.Task.ID()); err != nil || found || position != 0 {
		t.Fatalf("position=%d found=%t err=%v", position, found, err)
	}
	if !reflect.DeepEqual(queue.payloads, before) {
		t.Fatalf("queue changed: got=%#v want=%#v", queue.payloads, before)
	}
}

func TestTaskQueueReaderReturnsPayloadAndZeroBasedIndexWithoutExpandingTaskQueue(t *testing.T) {
	queue := NewTaskQueue().(*taskQueue)
	payloads := []TaskLaunchPayload{queueTestPayload(t, "lookup-first"), queueTestPayload(t, "lookup-second")}
	for _, payload := range payloads {
		queue.Enqueue(payload)
	}
	got, index, found := queue.Lookup(payloads[1].Task.ID())
	if !found || index != 1 || !reflect.DeepEqual(got, payloads[1]) {
		t.Fatalf("payload=%#v index=%d found=%t", got, index, found)
	}
	if _, found := reflect.TypeOf((*TaskQueue)(nil)).Elem().MethodByName("QueuePosition"); found {
		t.Fatal("TaskQueue must not expose QueuePosition")
	}
}

func TestTaskQueuePrependAndRemove(t *testing.T) {
	queue := NewTaskQueue()
	first, second := queueTestPayload(t, "first"), queueTestPayload(t, "second")
	queue.Enqueue(second)
	queue.Prepend(first)
	_, _, removed, events := queue.Remove(first.Task.ID(), time.Now())
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
		_, _, removed, _ := queue.Remove(first.Task.ID(), time.Now())
		if !removed || queue.payloads[0].Task.ID() != second.Task.ID() || !reflect.DeepEqual(backing[1], TaskLaunchPayload{}) {
			t.Fatalf("remove did not preserve payload and clear old tail: queue=%#v backing=%#v", queue.payloads, backing)
		}
	})
}

func TestTaskQueueRemove_ReturnsPayloadIndexAndEvents(t *testing.T) {
	queue := NewTaskQueue()
	first := queueTestPayload(t, "remove-contract-first")
	second := queueTestPayload(t, "remove-contract-second")
	second.Model, second.PromptText, second.SandboxMode = "gpt-5", "prompt", "workspace-write"
	queue.Enqueue(first)
	queue.Enqueue(second)

	got, index, removed, events := queue.Remove(second.Task.ID(), time.Now())
	if !removed || index != 1 || !reflect.DeepEqual(got, second) || len(events) != 1 {
		t.Fatalf("payload=%#v index=%d removed=%t events=%#v", got, index, removed, events)
	}
}

func TestTaskQueueRemove_NotFoundReturnsZeroPayloadAndMinusOne(t *testing.T) {
	queue := NewTaskQueue()
	queue.Enqueue(queueTestPayload(t, "missing-present"))
	missing := queueTestPayload(t, "missing-absent")
	payload, index, removed, events := queue.Remove(missing.Task.ID(), time.Now())
	if removed || index != -1 || !reflect.DeepEqual(payload, TaskLaunchPayload{}) || events != nil || queue.Len() != 1 {
		t.Fatalf("payload=%#v index=%d removed=%t events=%#v len=%d", payload, index, removed, events, queue.Len())
	}
}

func TestTaskQueueRemove_ReturnsOriginalIndex(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target int
	}{{"first", 0}, {"middle", 1}, {"last", 2}} {
		t.Run(tc.name, func(t *testing.T) {
			queue := NewTaskQueue()
			payloads := []TaskLaunchPayload{queueTestPayload(t, tc.name+"-a"), queueTestPayload(t, tc.name+"-b"), queueTestPayload(t, tc.name+"-c")}
			for _, payload := range payloads {
				queue.Enqueue(payload)
			}
			_, index, removed, _ := queue.Remove(payloads[tc.target].Task.ID(), time.Now())
			if !removed || index != tc.target {
				t.Fatalf("removed=%t index=%d", removed, index)
			}
		})
	}
}

func TestTaskQueueRestore_ReinsertsAtOriginalIndex(t *testing.T) {
	queue := NewTaskQueue()
	payloads := []TaskLaunchPayload{queueTestPayload(t, "restore-a"), queueTestPayload(t, "restore-b"), queueTestPayload(t, "restore-c")}
	for _, payload := range payloads {
		queue.Enqueue(payload)
	}
	removed, index, ok, _ := queue.Remove(payloads[1].Task.ID(), time.Now())
	if !ok {
		t.Fatal("remove failed")
	}
	queue.Restore(removed, index, time.Now())
	for i, want := range payloads {
		got, found := queue.Dequeue()
		if !found || got.Task != want.Task {
			t.Fatalf("position %d = %#v found=%t", i, got, found)
		}
	}
}

func TestTaskQueueRemoveRestoreHoldsSharedMutexUntilRollbackCompletes(t *testing.T) {
	queue := NewTaskQueue()
	first, second := queueTestPayload(t, "rollback-lock-first"), queueTestPayload(t, "rollback-lock-second")
	queue.Enqueue(first)
	queue.Enqueue(second)
	var queueMu sync.Mutex
	queueMu.Lock()
	removed, index, ok, _ := queue.Remove(first.Task.ID(), time.Now())
	if !ok {
		t.Fatal("remove failed")
	}
	attempting := make(chan struct{})
	dequeued := make(chan TaskLaunchPayload, 1)
	go func() {
		close(attempting)
		queueMu.Lock()
		dequeuedPayload, found := queue.Dequeue()
		queueMu.Unlock()
		if found {
			dequeued <- dequeuedPayload
		}
	}()
	<-attempting
	runtime.Gosched()
	select {
	case payload := <-dequeued:
		t.Fatalf("dequeue passed mutex before rollback: %#v", payload)
	default:
	}
	queue.Restore(removed, index, time.Now())
	queueMu.Unlock()
	select {
	case payload := <-dequeued:
		if payload.Task.ID() != first.Task.ID() {
			t.Fatalf("dequeued=%#v; rollback did not restore FIFO head", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("dequeue remained blocked after rollback completed")
	}
}

func TestTaskQueueRestore_PreservesCompletePayload(t *testing.T) {
	queue := NewTaskQueue()
	payload := queueTestPayload(t, "restore-complete")
	effort, working := "high", "/absolute/worktree"
	payload.Model, payload.PromptText, payload.ReasoningEffort, payload.WorkingDir = "gpt-5", "full prompt", &effort, &working
	queue.Enqueue(payload)
	removed, index, _, _ := queue.Remove(payload.Task.ID(), time.Now())
	queue.Restore(removed, index, time.Now())
	got, found := queue.Dequeue()
	if !found || !reflect.DeepEqual(got, payload) {
		t.Fatalf("payload=%#v want=%#v", got, payload)
	}
}

func TestTaskQueueRestore_ReindexesWholeQueue(t *testing.T) {
	queue := NewTaskQueue()
	payloads := []TaskLaunchPayload{queueTestPayload(t, "reindex-a"), queueTestPayload(t, "reindex-b"), queueTestPayload(t, "reindex-c")}
	for _, payload := range payloads {
		queue.Enqueue(payload)
	}
	removed, index, _, _ := queue.Remove(payloads[1].Task.ID(), time.Now())
	events := queue.Restore(removed, index, time.Now())
	if len(events) != 3 {
		t.Fatalf("events=%#v", events)
	}
	for i, event := range events {
		queued, ok := event.(domain.TaskQueued)
		if !ok || queued.QueuePosition != i+1 {
			t.Fatalf("event %d=%#v", i, event)
		}
	}
}

func TestTaskQueueRestore_InvalidInputPanicsWithoutMutation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		payload TaskLaunchPayload
		index   int
	}{
		{"nil task", TaskLaunchPayload{}, 0},
		{"non queued", TaskLaunchPayload{Task: nonQueuedTask(t, "restore-invalid")}, 0},
		{"negative index", queueTestPayload(t, "restore-negative"), -1},
		{"past end", queueTestPayload(t, "restore-past-end"), 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queue := NewTaskQueue().(*taskQueue)
			existing := queueTestPayload(t, "restore-existing-"+strings.ReplaceAll(tc.name, " ", "-"))
			queue.Enqueue(existing)
			before := append([]TaskLaunchPayload(nil), queue.payloads...)
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("Restore did not panic")
					}
				}()
				queue.Restore(tc.payload, tc.index, time.Now())
			}()
			if !reflect.DeepEqual(queue.payloads, before) {
				t.Fatalf("queue mutated: %#v", queue.payloads)
			}
		})
	}
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
