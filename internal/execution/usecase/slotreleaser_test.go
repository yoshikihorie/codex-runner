package usecase

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

type lifecycleRunnerFake struct {
	called chan struct{}
	ctx    context.Context
	input  TaskLifecycleInput
}

type lifecycleStarterFake struct {
	started  []execution.TaskLaunchPayload
	accepted bool
}

func (f *lifecycleStarterFake) Start(payload execution.TaskLaunchPayload) bool {
	f.started = append(f.started, payload)
	return f.accepted
}

func TestSlotFinalizerCommitsAcceptedStart(t *testing.T) {
	payload := advanceQueuedPayload(t, "slot-commit")
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{}}
	launching := execution.NewLaunchingTaskRegistry()
	advance := NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 1, 1)
	starter := &lifecycleStarterFake{accepted: true}

	NewSlotReleaser(advance, starter).ReleaseAndAdvance(context.Background(), domain.TaskID{}, time.Now())
	if len(starter.started) != 1 {
		t.Fatalf("starts=%d", len(starter.started))
	}
	if _, promoted := advance.promotions[payload.Task.ID()]; promoted {
		t.Fatal("promotion remained after accepted start")
	}
}

func (f *lifecycleRunnerFake) Run(ctx context.Context, input TaskLifecycleInput) {
	f.ctx, f.input = ctx, input
	close(f.called)
}

func TestSlotFinalizerStartsLifecycleWithBaseContextChild(t *testing.T) {
	queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
	launching := execution.NewLaunchingTaskRegistry()
	admit := NewAdmitTaskUseCase(queue, registry, launching, mutex, 1, 1, 1)
	active := testAdmissionInput(t, domain.SubcommandImpl, "active-slot")
	if _, err := admit.Execute(context.Background(), active); err != nil {
		t.Fatal(err)
	}
	waiting := testAdmissionInput(t, domain.SubcommandReview, "waiting-slot")
	if _, err := admit.Execute(context.Background(), waiting); err != nil {
		t.Fatal(err)
	}
	baseCtx, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()
	runner := &lifecycleRunnerFake{called: make(chan struct{})}
	now := time.Now()
	starter := newLifecycleStarterForTest(t, runner, baseCtx, domain.ClockFunc(func() time.Time { return now }))
	releaser := NewSlotReleaser(NewAdvanceQueueUseCase(queue, registry, launching, mutex, 1, 1), starter)
	releaser.ReleaseAndAdvance(context.Background(), active.TaskID, now)
	select {
	case <-runner.called:
	case <-time.After(time.Second):
		t.Fatal("runner was not called")
	}
	if runner.ctx == baseCtx || runner.input.TaskDirPath != "/private/tmp/tasks/"+waiting.TaskID.String() || !runner.input.Now.Equal(now) {
		t.Fatalf("ctx=%v input=%#v", runner.ctx, runner.input)
	}
	select {
	case <-runner.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("task context was not cancelled")
	}
}

func TestSlotFinalizerPromotesMultipleTasksAfterImplSlotRelease(t *testing.T) {
	queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
	launching := execution.NewLaunchingTaskRegistry()
	admit := NewAdmitTaskUseCase(queue, registry, launching, mutex, 4, 2, 4)
	firstActive := testAdmissionInput(t, domain.SubcommandImpl, "scn25-first-active")
	secondActive := testAdmissionInput(t, domain.SubcommandImpl, "scn25-second-active")
	queuedImpl := testAdmissionInput(t, domain.SubcommandImpl, "scn25-queued-impl")
	queuedReviewFirst := testAdmissionInput(t, domain.SubcommandReview, "scn25-queued-review-first")
	queuedReviewSecond := testAdmissionInput(t, domain.SubcommandReview, "scn25-queued-review-second")
	for _, input := range []execution.TaskAdmissionInput{firstActive, secondActive, queuedImpl, queuedReviewFirst, queuedReviewSecond} {
		if _, err := admit.Execute(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}
	starter := &lifecycleStarterFake{accepted: true}
	NewSlotReleaser(NewAdvanceQueueUseCase(queue, registry, launching, mutex, 4, 2), starter).ReleaseAndAdvance(context.Background(), firstActive.TaskID, time.Now())

	if len(starter.started) != 3 || starter.started[0].Task.ID() != queuedImpl.TaskID || starter.started[1].Task.ID() != queuedReviewFirst.TaskID || starter.started[2].Task.ID() != queuedReviewSecond.TaskID {
		t.Fatalf("started=%#v", starter.started)
	}
	if registry.Size() != 4 || registry.ImplSize() != 2 {
		t.Fatalf("size=%d impl_size=%d", registry.Size(), registry.ImplSize())
	}
}

func TestSlotFinalizerDoesNotStartLifecycleWhenQueueIsEmpty(t *testing.T) {
	runner := &lifecycleRunnerFake{called: make(chan struct{})}
	releaser := NewSlotReleaser(NewAdvanceQueueUseCase(execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1, 1), newLifecycleStarterForTest(t, runner, context.Background(), domain.ClockFunc(time.Now)))
	releaser.ReleaseAndAdvance(context.Background(), domain.TaskID{}, time.Now())
	select {
	case <-runner.called:
		t.Fatal("runner was called for an empty queue")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSlotFinalizerLogsAdvanceErrorAndDoesNotStartLifecycle(t *testing.T) {
	invalidQueue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{{Task: advanceNonQueuedTask(t, "finalizer-invalid")}}}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	runner := &lifecycleRunnerFake{called: make(chan struct{})}
	releaser := NewSlotReleaser(NewAdvanceQueueUseCase(invalidQueue, &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{}}, execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1, 1), newLifecycleStarterForTest(t, runner, context.Background(), domain.ClockFunc(time.Now)), logger)
	taskID := testAdmissionInput(t, domain.SubcommandImpl, "finalizer-release").TaskID
	releaser.ReleaseAndAdvance(context.Background(), taskID, time.Now())
	if !bytes.Contains(logs.Bytes(), []byte("WARN")) || !bytes.Contains(logs.Bytes(), []byte(taskID.String())) || !bytes.Contains(logs.Bytes(), []byte(domain.ErrInvalidStateTransition.Error())) {
		t.Fatalf("logs=%q", logs.String())
	}
	select {
	case <-runner.called:
		t.Fatal("runner was called after advance error")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestSlotFinalizerDoesNotRetryAfterSnapshotFailure(t *testing.T) {
	payload := execution.TaskLaunchPayload{Task: queueTask(t, "finalizer-invalid-snapshot")}
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{}}
	starter := &lifecycleStarterFake{accepted: true}
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	taskID := testAdmissionInput(t, domain.SubcommandImpl, "finalizer-snapshot-failure").TaskID

	NewSlotReleaser(NewAdvanceQueueUseCase(queue, registry, execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1, 1), starter, logger).ReleaseAndAdvance(context.Background(), taskID, time.Now())

	if queue.dequeueCalls != 1 || queue.prependCalls != 1 || len(queue.payloads) != 1 || queue.payloads[0].Task.ID() != payload.Task.ID() || len(starter.started) != 0 || registry.addCalls != 0 {
		t.Fatalf("dequeue=%d prepend=%d queue=%#v starts=%d add=%d", queue.dequeueCalls, queue.prependCalls, queue.payloads, len(starter.started), registry.addCalls)
	}
	if !bytes.Contains(logs.Bytes(), []byte("WARN")) || !bytes.Contains(logs.Bytes(), []byte(taskID.String())) || !bytes.Contains(logs.Bytes(), []byte("task snapshot invalid")) {
		t.Fatalf("logs=%q", logs.String())
	}
}

func TestSlotFinalizerDoesNotStartLifecycleAfterAdvanceCancellation(t *testing.T) {
	payload := advanceQueuedPayload(t, "slot-cancel")
	queue := &advanceQueueFake{payloads: []execution.TaskLaunchPayload{payload}}
	registry := &advanceRegistryFake{ids: map[domain.TaskID]domain.Subcommand{}}
	launching := &advanceLaunchingFake{snapshots: map[domain.TaskID]domain.TaskSnapshot{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry.removeHook = cancel
	starter := &lifecycleStarterFake{accepted: true}

	NewSlotReleaser(NewAdvanceQueueUseCase(queue, registry, launching, &sync.Mutex{}, 1, 1), starter).ReleaseAndAdvance(ctx, domain.TaskID{}, time.Now())
	if len(starter.started) != 0 || queue.dequeueCalls != 0 || registry.addCalls != 0 || launching.registerCalls != 0 {
		t.Fatalf("starts=%d dequeue=%d add=%d register=%d", len(starter.started), queue.dequeueCalls, registry.addCalls, launching.registerCalls)
	}
}

func TestNewSlotReleaserRejectsNilStarter(t *testing.T) {
	advance := NewAdvanceQueueUseCase(execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1, 1)
	for _, tc := range []struct {
		name    string
		starter execution.TaskLifecycleStarter
	}{
		{"nil-interface", nil},
		{"typed-nil", (*lifecycleStarterFake)(nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if got := recover(); got != "slot releaser requires non-nil dependencies" {
					t.Fatalf("panic=%v", got)
				}
			}()
			NewSlotReleaser(advance, tc.starter)
		})
	}
}

func TestSlotFinalizerRunnerReceivesOnlyBaseContextValues(t *testing.T) {
	type key string
	baseCtx := context.WithValue(context.Background(), key("base"), "base-value")
	callCtx := context.WithValue(context.Background(), key("call"), "call-value")
	queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
	launching := execution.NewLaunchingTaskRegistry()
	admit := NewAdmitTaskUseCase(queue, registry, launching, mutex, 1, 1, 1)
	active := testAdmissionInput(t, domain.SubcommandImpl, "context-active")
	if _, err := admit.Execute(context.Background(), active); err != nil {
		t.Fatal(err)
	}
	if _, err := admit.Execute(context.Background(), testAdmissionInput(t, domain.SubcommandImpl, "context-waiting")); err != nil {
		t.Fatal(err)
	}
	runner := &lifecycleRunnerFake{called: make(chan struct{})}
	releaser := NewSlotReleaser(NewAdvanceQueueUseCase(queue, registry, launching, mutex, 1, 1), newLifecycleStarterForTest(t, runner, baseCtx, domain.ClockFunc(time.Now)))
	releaser.ReleaseAndAdvance(callCtx, active.TaskID, time.Now())
	select {
	case <-runner.called:
	case <-time.After(time.Second):
		t.Fatal("runner was not called")
	}
	if runner.ctx == baseCtx || runner.ctx == callCtx || runner.ctx.Value(key("base")) != "base-value" || runner.ctx.Value(key("call")) != nil {
		t.Fatalf("unexpected runner context")
	}
	select {
	case <-runner.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("runner context was not cancelled")
	}
}

func newLifecycleStarterForTest(t *testing.T, runner taskLifecycleRunner, baseCtx context.Context, clock domain.Clock) *taskLifecycleStarter {
	t.Helper()
	starter, err := NewTaskLifecycleStarter(runner, "/private/tmp/tasks", baseCtx, clock)
	if err != nil {
		t.Fatal(err)
	}
	return starter
}
