package usecase

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

// RecoverSessionUseCase completes an abnormal-exit recovery after the policy delay.
type RecoverSessionUseCase struct {
	Store            SessionRecordStore
	Stop             *StopState
	Respawn          func(context.Context, supervisor.SessionID, RespawnKind) error
	Now              func() time.Time
	Wait             func(context.Context, time.Duration) error
	RecordStructured func(store.Event) error
	Notify           func(supervisor.SupervisorEventKind, string)
	LifetimeContext  context.Context
	Running          *sync.WaitGroup
}

// Start tracks an asynchronous recovery until it finishes.
func (u RecoverSessionUseCase) Start(id supervisor.SessionID) {
	if u.Running != nil {
		u.Running.Add(1)
	}
	go func() {
		if u.Running != nil {
			defer u.Running.Done()
		}
		ctx := u.LifetimeContext
		if ctx == nil {
			ctx = context.Background()
		}
		_ = u.Execute(ctx, id)
	}()
}

func (u RecoverSessionUseCase) WaitForCompletion() {
	if u.Running != nil {
		u.Running.Wait()
	}
}

func (u RecoverSessionUseCase) Execute(ctx context.Context, id supervisor.SessionID) error {
	if u.LifetimeContext != nil {
		ctx = u.LifetimeContext
	}
	if u.Store == nil || u.Respawn == nil {
		return errors.New("recovery store and respawn are required")
	}
	now := time.Now
	if u.Now != nil {
		now = u.Now
	}
	s, err := u.Store.Load(id)
	if err != nil {
		return err
	}
	if s.State != supervisor.SessionRecovering {
		return nil
	}
	if s.CLI != supervisor.CLIClaude {
		return nil
	}
	if s.ClaudeSessionID == "" {
		return u.stop(id, supervisor.StopClaudeSessionIDMissing, now().UTC())
	}
	if reason := supervisor.RestartLimitReason(s.RestartCount, s.RestartTimes, now().UTC()); reason != "" {
		return u.stop(id, reason, now().UTC())
	}
	wait := u.Wait
	if wait == nil {
		wait = func(ctx context.Context, delay time.Duration) error {
			return waitRecoveryDelay(ctx, delay, func() bool { return u.Stop != nil && u.Stop.IsStopped(id) })
		}
	}
	if err := wait(ctx, supervisor.RestartBackoff(s.RestartCount)); err != nil {
		if errors.Is(err, ErrStopActive) {
			reason := supervisor.StopSession
			if u.Stop != nil && u.Stop.IsGlobalStopped() {
				reason = supervisor.StopGlobal
			}
			return u.stop(id, reason, now().UTC())
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			_ = u.stop(id, supervisor.StopSupervisorRestarted, now().UTC())
		}
		return err
	}
	if err := ctx.Err(); err != nil {
		_ = u.stop(id, supervisor.StopSupervisorRestarted, now().UTC())
		return err
	}
	ready := false
	var stoppedReason supervisor.SupervisorStopReason
	err = UpdateSession(u.Store, id, func(current *supervisor.ManagedSession) error {
		if current.State != supervisor.SessionRecovering {
			return nil
		}
		if u.Stop != nil && u.Stop.IsStopped(id) {
			reason := supervisor.StopSession
			if u.Stop.IsGlobalStopped() {
				reason = supervisor.StopGlobal
			}
			stoppedReason = reason
			return current.Apply(supervisor.EventStop, supervisor.TransitionContext{StopReason: reason}, now().UTC())
		}
		if current.ClaudeSessionID == "" {
			stoppedReason = supervisor.StopClaudeSessionIDMissing
			return current.Apply(supervisor.EventStop, supervisor.TransitionContext{StopReason: stoppedReason}, now().UTC())
		}
		if reason := supervisor.RestartLimitReason(current.RestartCount, current.RestartTimes, now().UTC()); reason != "" {
			stoppedReason = reason
			return current.Apply(supervisor.EventStop, supervisor.TransitionContext{StopReason: reason}, now().UTC())
		}
		if err := current.Apply(supervisor.EventRecoveryWaitDone, supervisor.TransitionContext{}, now().UTC()); err != nil {
			return err
		}
		ready = true
		return nil
	})
	if err != nil {
		return err
	}
	if stoppedReason != "" {
		u.stoppedEvent(id, stoppedReason)
		return nil
	}
	if !ready {
		return err
	}
	if u.RecordStructured != nil {
		_ = u.RecordStructured(store.Event{SessionID: id.String(), Kind: supervisor.EventKindStateChanged, FromState: supervisor.SessionRecovering, ToState: supervisor.SessionStarting})
	}
	return u.Respawn(ctx, id, RespawnRecovery)
}

func (u RecoverSessionUseCase) stop(id supervisor.SessionID, reason supervisor.SupervisorStopReason, now time.Time) error {
	changed := false
	err := UpdateSession(u.Store, id, func(s *supervisor.ManagedSession) error {
		if s.State != supervisor.SessionRecovering {
			return nil
		}
		if err := s.Apply(supervisor.EventStop, supervisor.TransitionContext{StopReason: reason}, now); err != nil {
			return err
		}
		changed = true
		return nil
	})
	if err != nil || !changed {
		return err
	}
	u.stoppedEvent(id, reason)
	return nil
}

func (u RecoverSessionUseCase) stoppedEvent(id supervisor.SessionID, reason supervisor.SupervisorStopReason) {
	kind := supervisor.EventKindSessionStopped
	if reason == supervisor.StopRestartLimitConsecutive || reason == supervisor.StopRestartLimitDaily {
		kind = supervisor.EventKindRestartLimitExceeded
	}
	if u.RecordStructured != nil {
		_ = u.RecordStructured(store.Event{SessionID: id.String(), Kind: supervisor.EventKindStateChanged, FromState: supervisor.SessionRecovering, ToState: supervisor.SessionStopped})
		code := ""
		if kind == supervisor.EventKindRestartLimitExceeded {
			code = "SUP_RESTART_LIMIT_EXCEEDED"
		}
		_ = u.RecordStructured(store.Event{SessionID: id.String(), Kind: kind, Reason: store.EventReason(reason), Code: code})
	}
	if u.Notify != nil {
		u.Notify(kind, id.String())
	}
}

func waitRecoveryDelay(ctx context.Context, delay time.Duration, stopped func() bool) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	ticker := time.NewTicker(defaultObserveInterval)
	defer ticker.Stop()
	for {
		if stopped() {
			return ErrStopActive
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		case <-ticker.C:
		}
	}
}
