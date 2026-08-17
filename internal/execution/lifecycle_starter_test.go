package execution

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func lifecycleStarterPayload(t *testing.T, suffix string) TaskLaunchPayload {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260817-120000-a1b2-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	slug, err := domain.NewSlug(suffix)
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := domain.NewTask(id, domain.SubcommandImpl, slug, nil, time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC), 1)
	if err != nil {
		t.Fatal(err)
	}
	return TaskLaunchPayload{Task: task}
}

func TestDefaultTaskLifecycleStarterRejectsStartAfterShutdown(t *testing.T) {
	started := make(chan struct{})
	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	starter := NewDefaultTaskLifecycleStarter(baseCtx, func(context.Context, TaskLaunchPayload) { close(started) })

	starter.Shutdown(context.Background())
	if starter.Start(lifecycleStarterPayload(t, "after-shutdown")) {
		t.Fatal("Start accepted a launch after shutdown")
	}
	select {
	case <-started:
		t.Fatal("delegate ran after shutdown")
	default:
	}
}

func TestDefaultTaskLifecycleStarterCancelsAcceptedLifecycleOnShutdown(t *testing.T) {
	started := make(chan struct{})
	released := make(chan struct{})
	var once sync.Once
	starter := NewDefaultTaskLifecycleStarter(context.Background(), func(ctx context.Context, _ TaskLaunchPayload) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		close(released)
	})

	if !starter.Start(lifecycleStarterPayload(t, "cancel-on-shutdown")) {
		t.Fatal("Start rejected an open gate")
	}
	<-started
	starter.Shutdown(context.Background())
	<-released
}
