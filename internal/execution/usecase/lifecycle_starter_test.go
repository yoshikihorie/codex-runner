package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestNewTaskLifecycleStarterRejectsInvalidDependencies(t *testing.T) {
	runner := &lifecycleRunnerFake{called: make(chan struct{})}
	clock := domain.ClockFunc(time.Now)
	for _, tc := range []struct {
		runner taskLifecycleRunner
		root   string
		ctx    context.Context
		clock  domain.Clock
	}{
		{nil, "/private/tmp/tasks", context.Background(), clock},
		{runner, "relative", context.Background(), clock},
		{runner, "/private/tmp/tasks", nil, clock},
		{runner, "/private/tmp/tasks", context.Background(), nil},
	} {
		if starter, err := NewTaskLifecycleStarter(tc.runner, tc.root, tc.ctx, tc.clock); err == nil || starter != nil {
			t.Fatal("invalid starter dependencies were accepted")
		}
	}
}
