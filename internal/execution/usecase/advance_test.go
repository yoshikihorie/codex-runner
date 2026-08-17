package usecase

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

type advanceQueueFake struct {
	payloads     []execution.TaskLaunchPayload
	prependCalls int
	reindexCalls int
}

func (q *advanceQueueFake) Enqueue(payload execution.TaskLaunchPayload) int {
	q.payloads = append(q.payloads, payload)
	return len(q.payloads)
}
func (q *advanceQueueFake) Dequeue() (execution.TaskLaunchPayload, bool) {
	if len(q.payloads) == 0 {
		return execution.TaskLaunchPayload{}, false
	}
	payload := q.payloads[0]
	q.payloads = q.payloads[1:]
	return payload, true
}
func (q *advanceQueueFake) Prepend(payload execution.TaskLaunchPayload) {
	q.prependCalls++
	q.payloads = append([]execution.TaskLaunchPayload{payload}, q.payloads...)
}
func (q *advanceQueueFake) Reindex(time.Time) []domain.Event { q.reindexCalls++; return nil }
func (q *advanceQueueFake) Len() int                         { return len(q.payloads) }
func (q *advanceQueueFake) Remove(taskID domain.TaskID, _ time.Time) (execution.TaskLaunchPayload, int, bool, []domain.Event) {
	for index, payload := range q.payloads {
		if payload.Task.ID() == taskID {
			q.payloads = append(q.payloads[:index], q.payloads[index+1:]...)
			return payload, index, true, nil
		}
	}
	return execution.TaskLaunchPayload{}, -1, false, nil
}
func (q *advanceQueueFake) Restore(payload execution.TaskLaunchPayload, index int, _ time.Time) []domain.Event {
	q.payloads = append(q.payloads, execution.TaskLaunchPayload{})
	copy(q.payloads[index+1:], q.payloads[index:])
	q.payloads[index] = payload
	return nil
}

type advanceRegistryFake struct {
	ids         map[domain.TaskID]struct{}
	addCalls    int
	removeCalls int
}

func (r *advanceRegistryFake) Size() int               { return len(r.ids) }
func (r *advanceRegistryFake) Add(id domain.TaskID)    { r.addCalls++; r.ids[id] = struct{}{} }
func (r *advanceRegistryFake) Remove(id domain.TaskID) { r.removeCalls++; delete(r.ids, id) }
func (r *advanceRegistryFake) Reset(ids []domain.TaskID) {
	r.ids = make(map[domain.TaskID]struct{}, len(ids))
	for _, id := range ids {
		r.ids[id] = struct{}{}
	}
}

func TestAdvanceQueueUseCaseDequeuesFIFOAndReleasesSlot(t *testing.T) {
	queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
	launching := execution.NewLaunchingTaskRegistry()
	admit := NewAdmitTaskUseCase(queue, registry, launching, mutex, 1, 2)
	active := testAdmissionInput(t, domain.SubcommandImpl, "active")
	if _, err := admit.Execute(context.Background(), active); err != nil {
		t.Fatal(err)
	}
	waiting := testAdmissionInput(t, domain.SubcommandReview, "waiting")
	if _, err := admit.Execute(context.Background(), waiting); err != nil {
		t.Fatal(err)
	}
	payload, id, found, err := NewAdvanceQueueUseCase(queue, registry, launching, mutex, 1).Execute(context.Background(), active.TaskID, time.Now())
	if err != nil || !found || id == 0 || payload.Task.ID() != waiting.TaskID || registry.Size() != 1 {
		t.Fatalf("payload=%#v id=%d found=%t err=%v size=%d", payload, id, found, err, registry.Size())
	}
	if snapshot, registered := launching.Lookup(waiting.TaskID); !registered || snapshot.TaskID != waiting.TaskID || snapshot.State != domain.StateQueued || snapshot.Model != waiting.Model || snapshot.ResolvedTimeoutSeconds != waiting.ResolvedTimeout.ResolvedSeconds() || snapshot.Route != domain.ExecutionRouteDaemon || !snapshot.RequestedAt.Equal(waiting.RequestedAt) || !snapshot.StateUpdatedAt.Equal(waiting.RequestedAt) {
		t.Fatalf("snapshot=%#v registered=%t", snapshot, registered)
	}
}

func TestAdvanceQueueUseCaseEmptyQueue(t *testing.T) {
	payload, id, found, err := NewAdvanceQueueUseCase(execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1).Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || found || id != 0 || payload.Task != nil || payload.Model != "" || payload.WorkingDir != nil {
		t.Fatalf("payload=%#v id=%d found=%t err=%v", payload, id, found, err)
	}
}

func TestAdvanceQueueRejectsDequeuedNonQueuedPayloadWithoutReindex(t *testing.T) {
	invalid := execution.TaskLaunchPayload{Task: advanceNonQueuedTask(t, "advance-invalid")}
	next := execution.TaskLaunchPayload{Task: queueTask(t, "advance-next")}
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{invalid, next}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]struct{}{}}
	payload, id, found, err := NewAdvanceQueueUseCase(queue, registry, execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1).Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != domain.ErrInvalidStateTransition || found || id != 0 || payload.Task != nil || queue.reindexCalls != 0 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != next.Task.ID() {
		t.Fatalf("payload=%#v id=%d found=%t err=%v queue=%#v reindex=%d", payload, id, found, err, queue.payloads, queue.reindexCalls)
	}
}

func TestAdvanceQueuePrependsWhenSlotBecomesUnavailable(t *testing.T) {
	payload := execution.TaskLaunchPayload{Task: queueTask(t, "advance-race")}
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]struct{}{}}
	// Size becomes full after the completed task is removed, simulating a competing reservation.
	registry.ids[queueTask(t, "advance-other").ID()] = struct{}{}
	got, id, found, err := NewAdvanceQueueUseCase(queue, registry, execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1).Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || found || id != 0 || got.Task != nil || queue.prependCalls != 1 || queue.reindexCalls != 0 || registry.addCalls != 0 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != payload.Task.ID() {
		t.Fatalf("payload=%#v id=%d found=%t err=%v queue=%#v", got, id, found, err, queue.payloads)
	}
}

func TestAdvanceQueueRemoveUnknownIDStillAdvances(t *testing.T) {
	input := testAdmissionInput(t, domain.SubcommandImpl, "advance-unknown")
	effort, workingDir := "high", input.SourceWorkingDir
	paths := []domain.NormalizedPath{mustNormalizedPath(t, "/private/tmp/advance-unknown")}
	payload := execution.TaskLaunchPayload{Task: queueTask(t, "advance-unknown"), Model: input.Model, ReasoningEffort: &effort, PromptText: input.PromptText, NormalizedPaths: paths, ResolvedTimeout: input.ResolvedTimeout, SandboxMode: input.SandboxMode, SourceWorkingDir: input.SourceWorkingDir, WorkingDir: &workingDir}
	remaining := execution.TaskLaunchPayload{Task: queueTask(t, "advance-remaining")}
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload, remaining}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]struct{}{queueTask(t, "advance-active").ID(): {}}}
	launching := execution.NewLaunchingTaskRegistry()
	got, id, found, err := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 2).Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || !found || id == 0 || got.Task.ID() != payload.Task.ID() || got.Model != payload.Model || got.PromptText != payload.PromptText || got.ReasoningEffort == nil || *got.ReasoningEffort != *payload.ReasoningEffort || len(got.NormalizedPaths) != 1 || got.NormalizedPaths[0] != payload.NormalizedPaths[0] || got.SandboxMode != payload.SandboxMode || got.SourceWorkingDir != payload.SourceWorkingDir || got.WorkingDir == nil || *got.WorkingDir != *payload.WorkingDir || registry.addCalls != 1 || queue.reindexCalls != 1 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != remaining.Task.ID() {
		t.Fatalf("payload=%#v id=%d found=%t err=%v add=%d reindex=%d", got, id, found, err, registry.addCalls, queue.reindexCalls)
	}
	if snapshot, registered := launching.Lookup(payload.Task.ID()); !registered || snapshot.Model != payload.Model || snapshot.ReasoningEffort == nil || *snapshot.ReasoningEffort != *payload.ReasoningEffort || snapshot.ResolvedTimeoutSeconds != payload.ResolvedTimeout.ResolvedSeconds() {
		t.Fatalf("snapshot=%#v registered=%t", snapshot, registered)
	}
}

func TestAdvanceQueueRestoresPayloadWhenSnapshotConstructionFails(t *testing.T) {
	payload := execution.TaskLaunchPayload{Task: queueTask(t, "advance-invalid-snapshot")}
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]struct{}{}}
	launching := execution.NewLaunchingTaskRegistry()
	got, id, found, err := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 1).Execute(context.Background(), domain.TaskID{}, time.Now())
	if err == nil || found || id != 0 || got.Task != nil || queue.prependCalls != 1 || queue.reindexCalls != 0 || registry.addCalls != 0 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != payload.Task.ID() {
		t.Fatalf("payload=%#v id=%d found=%t err=%v queue=%#v", got, id, found, err, queue.payloads)
	}
	if snapshot, registered := launching.Lookup(payload.Task.ID()); registered || snapshot != (domain.TaskSnapshot{}) {
		t.Fatalf("snapshot=%#v registered=%t", snapshot, registered)
	}
}

func TestAdvanceQueueCommitStartRemovesPromotionAndMakesCompensationANoop(t *testing.T) {
	payload := advanceQueuedPayload(t, "advance-commit")
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]struct{}{}}
	launching := execution.NewLaunchingTaskRegistry()
	useCase := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 1)

	got, id, found, err := useCase.Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || !found || id == 0 {
		t.Fatalf("payload=%#v id=%d found=%t err=%v", got, id, found, err)
	}
	useCase.CommitStart(got, id)
	if _, promoted := useCase.promotions[got.Task.ID()]; promoted {
		t.Fatal("promotion remained after start was committed")
	}
	useCase.CompensateRejectedStart(got, id, time.Now())
	if queue.prependCalls != 0 || registry.Size() != 1 {
		t.Fatalf("prepend=%d registry=%d", queue.prependCalls, registry.Size())
	}
	if _, registered := launching.Lookup(got.Task.ID()); !registered {
		t.Fatal("launching task was removed by stale compensation")
	}
}

func TestAdvanceQueueIgnoresUnregisteredZeroPromotionID(t *testing.T) {
	payload := advanceQueuedPayload(t, "advance-unregistered-zero-promotion")
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]struct{}{}}
	launching := execution.NewLaunchingTaskRegistry()
	useCase := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 1)

	got, id, found, err := useCase.Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || !found || id == 0 {
		t.Fatalf("payload=%#v id=%d found=%t err=%v", got, id, found, err)
	}
	delete(useCase.promotions, got.Task.ID())

	useCase.CompensateRejectedStart(got, 0, time.Now())
	useCase.CommitStart(got, 0)

	if queue.prependCalls != 0 || registry.Size() != 1 {
		t.Fatalf("prepend=%d registry=%d", queue.prependCalls, registry.Size())
	}
	if _, registered := launching.Lookup(got.Task.ID()); !registered {
		t.Fatal("launching task was removed by an unregistered zero promotion")
	}
}

func TestAdvanceQueueIgnoresStalePromotionCompensation(t *testing.T) {
	payload := advanceQueuedPayload(t, "advance-stale-promotion")
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]struct{}{}}
	launching := execution.NewLaunchingTaskRegistry()
	useCase := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 1)

	first, firstID, found, err := useCase.Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || !found || firstID == 0 {
		t.Fatalf("payload=%#v id=%d found=%t err=%v", first, firstID, found, err)
	}
	useCase.CompensateRejectedStart(first, firstID, time.Now())
	second, secondID, found, err := useCase.Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || !found || secondID == 0 || secondID == firstID {
		t.Fatalf("payload=%#v id=%d firstID=%d found=%t err=%v", second, secondID, firstID, found, err)
	}

	useCase.CompensateRejectedStart(first, firstID, time.Now())
	if queue.prependCalls != 1 || registry.Size() != 1 {
		t.Fatalf("prepend=%d registry=%d", queue.prependCalls, registry.Size())
	}
	if _, registered := launching.Lookup(second.Task.ID()); !registered {
		t.Fatal("launching task was removed by stale compensation")
	}
	if promoted := useCase.promotions[second.Task.ID()]; promoted != secondID {
		t.Fatalf("promotion=%d want=%d", promoted, secondID)
	}
}

func queueTask(t *testing.T, suffix string) *domain.Task { return testAdmissionTask(t, suffix) }

func advanceQueuedPayload(t *testing.T, suffix string) execution.TaskLaunchPayload {
	t.Helper()
	input := testAdmissionInput(t, domain.SubcommandImpl, suffix)
	return execution.TaskLaunchPayload{
		Task: taskFromAdmissionInput(t, input), Model: input.Model, ReasoningEffort: input.ReasoningEffort,
		PromptText: input.PromptText, NormalizedPaths: input.NormalizedPaths, ResolvedTimeout: input.ResolvedTimeout,
		SandboxMode: input.SandboxMode, SourceWorkingDir: input.SourceWorkingDir,
	}
}

func advanceNonQueuedTask(t *testing.T, suffix string) *domain.Task {
	task := testAdmissionTask(t, suffix)
	timeout, err := domain.NewTimeout(nil, 1800)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.Start(timeout, "model", time.Now()); err != nil {
		t.Fatal(err)
	}
	return task
}

func testAdmissionTask(t *testing.T, suffix string) *domain.Task {
	t.Helper()
	input := testAdmissionInput(t, domain.SubcommandImpl, suffix)
	return taskFromAdmissionInput(t, input)
}

func taskFromAdmissionInput(t *testing.T, input execution.TaskAdmissionInput) *domain.Task {
	t.Helper()
	task, _, err := domain.NewTask(input.TaskID, input.Subcommand, input.Slug, input.RequestedTimeout, input.RequestedAt, 1)
	if err != nil {
		t.Fatal(err)
	}
	return task
}
