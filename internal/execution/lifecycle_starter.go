package execution

import (
	"context"
	"sync"
)

// TaskLifecycleStarter owns asynchronous lifecycle execution startup.
type TaskLifecycleStarter interface {
	Start(TaskLaunchPayload) bool
}

// TaskLifecycleStartFunc performs the lifecycle work for an accepted launch.
type TaskLifecycleStartFunc func(context.Context, TaskLaunchPayload)

// DefaultTaskLifecycleStarter serializes shutdown with lifecycle startup.
// Its gate is deliberately the sole owner of the lifecycle base context.
type DefaultTaskLifecycleStarter struct {
	mu       sync.Mutex
	closed   bool
	baseCtx  context.Context
	cancel   context.CancelFunc
	delegate TaskLifecycleStartFunc
}

func NewDefaultTaskLifecycleStarter(baseCtx context.Context, delegate TaskLifecycleStartFunc) *DefaultTaskLifecycleStarter {
	if baseCtx == nil || delegate == nil {
		panic("default task lifecycle starter requires non-nil dependencies")
	}
	ownedCtx, cancel := context.WithCancel(baseCtx)
	return &DefaultTaskLifecycleStarter{baseCtx: ownedCtx, cancel: cancel, delegate: delegate}
}

// Start accepts a launch only while the shutdown gate is open.
func (s *DefaultTaskLifecycleStarter) Start(payload TaskLaunchPayload) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if payload.Task == nil {
		panic("task lifecycle starter requires a non-nil task")
	}
	if s.closed || s.baseCtx.Err() != nil {
		return false
	}
	ctx, cancel := context.WithCancel(s.baseCtx)
	go func() {
		defer cancel()
		s.delegate(ctx, payload)
	}()
	return true
}

// Shutdown closes the start gate before cancelling every accepted lifecycle.
func (s *DefaultTaskLifecycleStarter) Shutdown(_ context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	s.cancel()
}

var _ TaskLifecycleStarter = (*DefaultTaskLifecycleStarter)(nil)
