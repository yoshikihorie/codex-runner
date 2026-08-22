package usecase

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

type advanceQueueFake struct {
	payloads     []execution.TaskLaunchPayload
	dequeueCalls int
	prependCalls int
	reindexCalls int
	dequeueHook  func()
}

func (q *advanceQueueFake) Enqueue(payload execution.TaskLaunchPayload) int {
	q.payloads = append(q.payloads, payload)
	return len(q.payloads)
}
func (q *advanceQueueFake) Dequeue() (execution.TaskLaunchPayload, bool) {
	q.dequeueCalls++
	if len(q.payloads) == 0 {
		return execution.TaskLaunchPayload{}, false
	}
	payload := q.payloads[0]
	q.payloads = q.payloads[1:]
	if q.dequeueHook != nil {
		q.dequeueHook()
	}
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
	ids         map[domain.TaskID]domain.Subcommand
	addCalls    int
	removeCalls int
	addHook     func()
	removeHook  func()
}

func (r *advanceRegistryFake) Size() int { return len(r.ids) }
func (r *advanceRegistryFake) ImplSize() int {
	count := 0
	for _, subcommand := range r.ids {
		if subcommand == domain.SubcommandImpl {
			count++
		}
	}
	return count
}
func (r *advanceRegistryFake) Add(id domain.TaskID, subcommand domain.Subcommand) {
	r.addCalls++
	r.ids[id] = subcommand
	if r.addHook != nil {
		r.addHook()
	}
}
func (r *advanceRegistryFake) Remove(id domain.TaskID) {
	r.removeCalls++
	delete(r.ids, id)
	if r.removeHook != nil {
		r.removeHook()
	}
}
func (r *advanceRegistryFake) Reset(reservations map[domain.TaskID]domain.Subcommand) {
	r.ids = make(map[domain.TaskID]domain.Subcommand, len(reservations))
	for id, subcommand := range reservations {
		r.ids[id] = subcommand
	}
}

type advanceLaunchingFake struct {
	snapshots       map[domain.TaskID]domain.TaskSnapshot
	registerCalls   int
	unregisterCalls int
	registerHook    func()
}

func (r *advanceLaunchingFake) Register(id domain.TaskID, snapshot domain.TaskSnapshot) {
	r.registerCalls++
	r.snapshots[id] = snapshot
	if r.registerHook != nil {
		r.registerHook()
	}
}
func (r *advanceLaunchingFake) Unregister(id domain.TaskID) {
	r.unregisterCalls++
	delete(r.snapshots, id)
}
func (r *advanceLaunchingFake) Lookup(id domain.TaskID) (domain.TaskSnapshot, bool) {
	snapshot, ok := r.snapshots[id]
	return snapshot, ok
}

func TestAdvanceQueueUseCaseDequeuesFIFOAndReleasesSlot(t *testing.T) {
	queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
	launching := execution.NewLaunchingTaskRegistry()
	admit := NewAdmitTaskUseCase(queue, registry, launching, mutex, 1, 1, 2)
	active := testAdmissionInput(t, domain.SubcommandImpl, "active")
	if _, err := admit.Execute(context.Background(), active); err != nil {
		t.Fatal(err)
	}
	waiting := testAdmissionInput(t, domain.SubcommandReview, "waiting")
	if _, err := admit.Execute(context.Background(), waiting); err != nil {
		t.Fatal(err)
	}
	payload, found, err := NewAdvanceQueueUseCase(queue, registry, launching, mutex, 1, 1).Execute(context.Background(), active.TaskID, time.Now())
	if err != nil || !found || payload.Task.ID() != waiting.TaskID || registry.Size() != 1 {
		t.Fatalf("payload=%#v found=%t err=%v size=%d", payload, found, err, registry.Size())
	}
	if snapshot, registered := launching.Lookup(waiting.TaskID); !registered || snapshot.TaskID != waiting.TaskID || snapshot.State != domain.StateQueued || snapshot.Model != waiting.Model || snapshot.ResolvedTimeoutSeconds != waiting.ResolvedTimeout.ResolvedSeconds() || snapshot.Route != domain.ExecutionRouteDaemon || !snapshot.RequestedAt.Equal(waiting.RequestedAt) || !snapshot.StateUpdatedAt.Equal(waiting.RequestedAt) {
		t.Fatalf("snapshot=%#v registered=%t", snapshot, registered)
	}
}

func TestAdvanceQueueUseCaseEmptyQueue(t *testing.T) {
	payload, found, err := NewAdvanceQueueUseCase(execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1, 1).Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || found || payload.Task != nil || payload.Model != "" || payload.WorkingDir != nil {
		t.Fatalf("payload=%#v found=%t err=%v", payload, found, err)
	}
}

func TestAdvanceQueueRejectsDequeuedNonQueuedPayloadWithoutReindex(t *testing.T) {
	invalid := execution.TaskLaunchPayload{Task: advanceNonQueuedTask(t, "advance-invalid")}
	next := execution.TaskLaunchPayload{Task: queueTask(t, "advance-next")}
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{invalid, next}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{}}
	payload, found, err := NewAdvanceQueueUseCase(queue, registry, execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1, 1).Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != domain.ErrInvalidStateTransition || found || payload.Task != nil || queue.reindexCalls != 0 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != next.Task.ID() {
		t.Fatalf("payload=%#v found=%t err=%v queue=%#v reindex=%d", payload, found, err, queue.payloads, queue.reindexCalls)
	}
}

func TestAdvanceQueuePrependsWhenSlotBecomesUnavailable(t *testing.T) {
	payload := execution.TaskLaunchPayload{Task: queueTask(t, "advance-race")}
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{}}
	// Size becomes full after the completed task is removed, simulating a competing reservation.
	registry.ids[queueTask(t, "advance-other").ID()] = domain.SubcommandReview
	got, found, err := NewAdvanceQueueUseCase(queue, registry, execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1, 1).Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || found || got.Task != nil || queue.prependCalls != 1 || queue.reindexCalls != 0 || registry.addCalls != 0 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != payload.Task.ID() {
		t.Fatalf("payload=%#v found=%t err=%v queue=%#v", got, found, err, queue.payloads)
	}
}

func TestAdvanceQueueDoesNotBypassImplHeadAtImplLimit(t *testing.T) {
	impl := advanceQueuedPayload(t, "impl-head-blocked")
	reviewInput := testAdmissionInput(t, domain.SubcommandReview, "review-behind-impl")
	review := execution.TaskLaunchPayload{Task: taskFromAdmissionInput(t, reviewInput), Model: reviewInput.Model, PromptText: reviewInput.PromptText, ResolvedTimeout: reviewInput.ResolvedTimeout, SandboxMode: reviewInput.SandboxMode, SourceWorkingDir: reviewInput.SourceWorkingDir}
	activeID := queueTask(t, "impl-active").ID()
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{impl, review}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{activeID: domain.SubcommandImpl}}
	payload, found, err := NewAdvanceQueueUseCase(queue, registry, execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 3, 1).Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || found || payload.Task != nil || queue.prependCalls != 1 || queue.reindexCalls != 0 || len(queue.payloads) != 2 || queue.payloads[0].Task.ID() != impl.Task.ID() || queue.payloads[1].Task.ID() != review.Task.ID() {
		t.Fatalf("payload=%#v found=%t err=%v queue=%#v prepend=%d reindex=%d", payload, found, err, queue.payloads, queue.prependCalls, queue.reindexCalls)
	}
}

func TestAdvanceQueueRemoveUnknownIDStillAdvances(t *testing.T) {
	input := testAdmissionInput(t, domain.SubcommandImpl, "advance-unknown")
	effort, workingDir := "high", input.SourceWorkingDir
	paths := []domain.NormalizedPath{mustNormalizedPath(t, "/private/tmp/advance-unknown")}
	payload := execution.TaskLaunchPayload{Task: queueTask(t, "advance-unknown"), Model: input.Model, ReasoningEffort: &effort, PromptText: input.PromptText, NormalizedPaths: paths, ResolvedTimeout: input.ResolvedTimeout, SandboxMode: input.SandboxMode, SourceWorkingDir: input.SourceWorkingDir, WorkingDir: &workingDir}
	remaining := execution.TaskLaunchPayload{Task: queueTask(t, "advance-remaining")}
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload, remaining}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{queueTask(t, "advance-active").ID(): domain.SubcommandReview}}
	launching := execution.NewLaunchingTaskRegistry()
	got, found, err := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 2, 2).Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || !found || got.Task.ID() != payload.Task.ID() || got.Model != payload.Model || got.PromptText != payload.PromptText || got.ReasoningEffort == nil || *got.ReasoningEffort != *payload.ReasoningEffort || len(got.NormalizedPaths) != 1 || got.NormalizedPaths[0] != payload.NormalizedPaths[0] || got.SandboxMode != payload.SandboxMode || got.SourceWorkingDir != payload.SourceWorkingDir || got.WorkingDir == nil || *got.WorkingDir != *payload.WorkingDir || registry.addCalls != 1 || queue.reindexCalls != 1 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != remaining.Task.ID() {
		t.Fatalf("payload=%#v found=%t err=%v add=%d reindex=%d", got, found, err, registry.addCalls, queue.reindexCalls)
	}
	if snapshot, registered := launching.Lookup(payload.Task.ID()); !registered || snapshot.Model != payload.Model || snapshot.ReasoningEffort == nil || *snapshot.ReasoningEffort != *payload.ReasoningEffort || snapshot.ResolvedTimeoutSeconds != payload.ResolvedTimeout.ResolvedSeconds() {
		t.Fatalf("snapshot=%#v registered=%t", snapshot, registered)
	}
}

func TestAdvanceQueueRestoresPayloadWhenSnapshotConstructionFails(t *testing.T) {
	payload := execution.TaskLaunchPayload{Task: queueTask(t, "advance-invalid-snapshot")}
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{}}
	launching := execution.NewLaunchingTaskRegistry()
	got, found, err := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 1, 1).Execute(context.Background(), domain.TaskID{}, time.Now())
	if err == nil || found || got.Task != nil || queue.prependCalls != 1 || queue.reindexCalls != 0 || registry.addCalls != 0 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != payload.Task.ID() {
		t.Fatalf("payload=%#v found=%t err=%v queue=%#v", got, found, err, queue.payloads)
	}
	if snapshot, registered := launching.Lookup(payload.Task.ID()); registered || snapshot != (domain.TaskSnapshot{}) {
		t.Fatalf("snapshot=%#v registered=%t", snapshot, registered)
	}
}

func TestAdvanceQueueUseCaseCancellationCompensatesEachPromotionCheckpoint(t *testing.T) {
	for _, tc := range []struct {
		name   string
		slug   string
		cancel func(context.CancelFunc, *advanceQueueFake, *advanceRegistryFake, *advanceLaunchingFake)
		check  func(*testing.T, execution.TaskLaunchPayload, *advanceQueueFake, *advanceRegistryFake, *advanceLaunchingFake)
	}{
		{
			name: "after-remove-before-dequeue",
			slug: "rm",
			cancel: func(cancel context.CancelFunc, _ *advanceQueueFake, registry *advanceRegistryFake, _ *advanceLaunchingFake) {
				registry.removeHook = cancel
			},
			check: func(t *testing.T, payload execution.TaskLaunchPayload, queue *advanceQueueFake, registry *advanceRegistryFake, launching *advanceLaunchingFake) {
				if queue.dequeueCalls != 0 || registry.addCalls != 0 || launching.registerCalls != 0 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != payload.Task.ID() {
					t.Fatalf("dequeue=%d add=%d register=%d queue=%#v", queue.dequeueCalls, registry.addCalls, launching.registerCalls, queue.payloads)
				}
			},
		},
		{
			name: "after-dequeue",
			slug: "dq",
			cancel: func(cancel context.CancelFunc, queue *advanceQueueFake, _ *advanceRegistryFake, _ *advanceLaunchingFake) {
				queue.dequeueHook = cancel
			},
			check: func(t *testing.T, payload execution.TaskLaunchPayload, queue *advanceQueueFake, registry *advanceRegistryFake, launching *advanceLaunchingFake) {
				if queue.prependCalls != 1 || queue.reindexCalls != 0 || registry.addCalls != 0 || launching.registerCalls != 0 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != payload.Task.ID() {
					t.Fatalf("prepend=%d reindex=%d add=%d register=%d queue=%#v", queue.prependCalls, queue.reindexCalls, registry.addCalls, launching.registerCalls, queue.payloads)
				}
			},
		},
		{
			name: "after-add",
			slug: "add",
			cancel: func(cancel context.CancelFunc, _ *advanceQueueFake, registry *advanceRegistryFake, _ *advanceLaunchingFake) {
				registry.addHook = cancel
			},
			check: func(t *testing.T, payload execution.TaskLaunchPayload, queue *advanceQueueFake, registry *advanceRegistryFake, launching *advanceLaunchingFake) {
				if queue.prependCalls != 1 || queue.reindexCalls != 1 || registry.removeCalls != 2 || registry.addCalls != 1 || launching.registerCalls != 0 || registry.Size() != 0 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != payload.Task.ID() {
					t.Fatalf("prepend=%d reindex=%d remove=%d add=%d register=%d registry=%d queue=%#v", queue.prependCalls, queue.reindexCalls, registry.removeCalls, registry.addCalls, launching.registerCalls, registry.Size(), queue.payloads)
				}
			},
		},
		{
			name: "after-register",
			slug: "reg",
			cancel: func(cancel context.CancelFunc, _ *advanceQueueFake, _ *advanceRegistryFake, launching *advanceLaunchingFake) {
				launching.registerHook = cancel
			},
			check: func(t *testing.T, payload execution.TaskLaunchPayload, queue *advanceQueueFake, registry *advanceRegistryFake, launching *advanceLaunchingFake) {
				if queue.prependCalls != 1 || queue.reindexCalls != 1 || registry.removeCalls != 2 || registry.addCalls != 1 || launching.registerCalls != 1 || launching.unregisterCalls != 1 || registry.Size() != 0 || len(launching.snapshots) != 0 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != payload.Task.ID() {
					t.Fatalf("prepend=%d reindex=%d remove=%d add=%d register=%d unregister=%d registry=%d launching=%#v queue=%#v", queue.prependCalls, queue.reindexCalls, registry.removeCalls, registry.addCalls, launching.registerCalls, launching.unregisterCalls, registry.Size(), launching.snapshots, queue.payloads)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := advanceQueuedPayload(t, "advance-cancel-"+tc.slug)
			queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
			registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{}}
			launching := &advanceLaunchingFake{snapshots: map[domain.TaskID]domain.TaskSnapshot{}}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			tc.cancel(cancel, queue, registry, launching)

			got, found, err := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 1, 1).Execute(ctx, domain.TaskID{}, time.Now())
			if !errors.Is(err, context.Canceled) || found || got.Task != nil {
				t.Fatalf("payload=%#v found=%t err=%v", got, found, err)
			}
			tc.check(t, payload, queue, registry, launching)
		})
	}
}

func TestAdvanceQueueCommitStartRemovesPromotionAndMakesCompensationANoop(t *testing.T) {
	payload := advanceQueuedPayload(t, "advance-commit")
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{}}
	launching := execution.NewLaunchingTaskRegistry()
	useCase := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 1, 1)

	got, found, err := useCase.Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || !found {
		t.Fatalf("payload=%#v found=%t err=%v", got, found, err)
	}
	useCase.CommitStart(got)
	if _, promoted := useCase.promotions[got.Task.ID()]; promoted {
		t.Fatal("promotion remained after start was committed")
	}
	useCase.CompensateRejectedStart(got, time.Now())
	if queue.prependCalls != 0 || registry.Size() != 1 {
		t.Fatalf("prepend=%d registry=%d", queue.prependCalls, registry.Size())
	}
	if _, registered := launching.Lookup(got.Task.ID()); !registered {
		t.Fatal("launching task was removed by stale compensation")
	}
}

func TestAdvanceQueueIgnoresUnregisteredPromotion(t *testing.T) {
	payload := advanceQueuedPayload(t, "advance-unregistered-zero-promotion")
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{}}
	launching := execution.NewLaunchingTaskRegistry()
	useCase := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 1, 1)

	got, found, err := useCase.Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || !found {
		t.Fatalf("payload=%#v found=%t err=%v", got, found, err)
	}
	delete(useCase.promotions, got.Task.ID())

	useCase.CompensateRejectedStart(got, time.Now())
	useCase.CommitStart(got)

	if queue.prependCalls != 0 || registry.Size() != 1 {
		t.Fatalf("prepend=%d registry=%d", queue.prependCalls, registry.Size())
	}
	if _, registered := launching.Lookup(got.Task.ID()); !registered {
		t.Fatal("launching task was removed by an unregistered promotion")
	}
}

func TestAdvanceQueueIgnoresStalePromotionCompensation(t *testing.T) {
	payload := advanceQueuedPayload(t, "advance-stale-promotion")
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{}}
	launching := execution.NewLaunchingTaskRegistry()
	useCase := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 1, 1)

	first, found, err := useCase.Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || !found {
		t.Fatalf("payload=%#v found=%t err=%v", first, found, err)
	}
	useCase.CompensateRejectedStart(first, time.Now())
	useCase.CompensateRejectedStart(first, time.Now())
	second, found, err := useCase.Execute(context.Background(), domain.TaskID{}, time.Now())
	if err != nil || !found {
		t.Fatalf("payload=%#v found=%t err=%v", second, found, err)
	}

	if queue.prependCalls != 1 || registry.Size() != 1 {
		t.Fatalf("prepend=%d registry=%d", queue.prependCalls, registry.Size())
	}
	if _, registered := launching.Lookup(second.Task.ID()); !registered {
		t.Fatal("launching task was removed by stale compensation")
	}
	if _, promoted := useCase.promotions[second.Task.ID()]; !promoted {
		t.Fatal("second promotion was not registered")
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
