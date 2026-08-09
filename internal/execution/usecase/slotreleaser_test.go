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

func (f *lifecycleRunnerFake) Run(ctx context.Context, input TaskLifecycleInput) {
	f.ctx, f.input = ctx, input
	close(f.called)
}

func TestSlotFinalizerStartsLifecycleWithBaseContextChild(t *testing.T) {
	queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
	admit := NewAdmitTaskUseCase(queue, registry, mutex, 1, 1)
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
	releaser := NewSlotReleaser(NewAdvanceQueueUseCase(queue, registry, mutex, 1), runner, "/private/tmp/tasks", baseCtx, domain.ClockFunc(func() time.Time { return now }))
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

func TestSlotFinalizerDoesNotStartLifecycleWhenQueueIsEmpty(t *testing.T) {
	runner := &lifecycleRunnerFake{called: make(chan struct{})}
	releaser := NewSlotReleaser(NewAdvanceQueueUseCase(execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}, 1), runner, "/private/tmp/tasks", context.Background(), domain.ClockFunc(time.Now))
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
	releaser := NewSlotReleaser(NewAdvanceQueueUseCase(invalidQueue, &advanceRegistryFake{ids: map[domain.TaskID]struct{}{}}, &sync.Mutex{}, 1), runner, "/private/tmp/tasks", context.Background(), domain.ClockFunc(time.Now), logger)
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

func TestSlotFinalizerRunnerReceivesOnlyBaseContextValues(t *testing.T) {
	type key string
	baseCtx := context.WithValue(context.Background(), key("base"), "base-value")
	callCtx := context.WithValue(context.Background(), key("call"), "call-value")
	queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
	admit := NewAdmitTaskUseCase(queue, registry, mutex, 1, 1)
	active := testAdmissionInput(t, domain.SubcommandImpl, "context-active")
	if _, err := admit.Execute(context.Background(), active); err != nil {
		t.Fatal(err)
	}
	if _, err := admit.Execute(context.Background(), testAdmissionInput(t, domain.SubcommandImpl, "context-waiting")); err != nil {
		t.Fatal(err)
	}
	runner := &lifecycleRunnerFake{called: make(chan struct{})}
	releaser := NewSlotReleaser(NewAdvanceQueueUseCase(queue, registry, mutex, 1), runner, "/private/tmp/tasks", baseCtx, domain.ClockFunc(time.Now))
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
