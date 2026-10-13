package usecase

import (
	"context"
	"errors"
	"os"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

// SUPERVISOR_STOP_SESSION_EXIT_CONFIRM_SECONDS in validation-rules.md.
const stopSessionExitConfirm = 10 * time.Second

// StopSessionExitConfirmTimeout is the longest stop-session confirmation wait.
func StopSessionExitConfirmTimeout() time.Duration { return stopSessionExitConfirm }

type StopSessionOutput struct {
	SessionID supervisor.SessionID
	State     supervisor.SupervisorSessionState
	Reason    supervisor.SupervisorStopReason
}

type StopSessionUseCase struct {
	Sessions         SessionRecordStore
	PTYs             *PTYRegistry
	ConfirmTimeout   time.Duration
	PollInterval     time.Duration
	ChildAlive       func(int) bool
	RecordStructured func(store.Event) error
	Notify           func(supervisor.SupervisorEventKind, string)
	Now              func() time.Time
}

func (u StopSessionUseCase) Execute(ctx context.Context, id supervisor.SessionID) (StopSessionOutput, error) {
	if u.Sessions == nil || u.PTYs == nil {
		return StopSessionOutput{}, errors.New("stop-session dependencies are required")
	}
	session, err := u.Sessions.Load(id)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return StopSessionOutput{}, ErrSessionNotFound
		}
		return StopSessionOutput{}, err
	}
	if session.SessionID != id {
		return StopSessionOutput{}, ErrSessionNotFound
	}
	pty := u.PTYs.MarkStopping(id)
	defer u.PTYs.ClearStopping(id, pty)
	if pty != nil {
		if err := pty.Close(); err != nil {
			return StopSessionOutput{}, err
		}
	}
	alive := u.ChildAlive
	if alive == nil {
		alive = func(pid int) bool { return pid > 0 && syscall.Kill(pid, 0) == nil }
	}
	limit := u.ConfirmTimeout
	if limit <= 0 {
		limit = stopSessionExitConfirm
	}
	interval := u.PollInterval
	if interval <= 0 {
		interval = 100 * time.Millisecond
	}
	deadline := time.NewTimer(limit)
	defer deadline.Stop()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		current, err := u.Sessions.Load(id)
		if err != nil {
			return StopSessionOutput{}, err
		}
		if current.State == supervisor.SessionExited {
			return StopSessionOutput{SessionID: id, State: current.State}, nil
		}
		if pty == nil && !alive(current.ChildPID) {
			return u.finish(id, false, pty)
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			current, err := u.Sessions.Load(id)
			if err != nil {
				return StopSessionOutput{}, err
			}
			return u.finish(id, alive(current.ChildPID), pty)
		case <-ctx.Done():
			return StopSessionOutput{}, ctx.Err()
		}
	}
}

func (u StopSessionUseCase) finish(id supervisor.SessionID, timedOut bool, pty ManagedPTY) (StopSessionOutput, error) {
	changed := false
	childPID := 0
	var resulting supervisor.ManagedSession
	now := time.Now().UTC()
	if u.Now != nil {
		now = u.Now().UTC()
	}
	err := UpdateSession(u.Sessions, id, func(s *supervisor.ManagedSession) error {
		if s.State == supervisor.SessionExited {
			resulting = *s
			return nil
		}
		if s.State == supervisor.SessionFailed {
			resulting = *s
			return nil
		}
		if timedOut {
			if err := s.Apply(supervisor.EventStop, supervisor.TransitionContext{StopReason: supervisor.StopOrphanChildAlive}, now); err != nil {
				return err
			}
			// P2: an existing stopped reason is replaced after exit confirmation times out.
			s.StopReason = supervisor.StopOrphanChildAlive
		} else {
			childPID = s.ChildPID
			if err := s.Apply(supervisor.EventStopSession, supervisor.TransitionContext{ChildAlive: false}, now); err != nil {
				return err
			}
			s.ChildPID = 0
			s.ChildStartedAt = nil
		}
		changed = true
		resulting = *s
		return nil
	})
	if err != nil {
		return StopSessionOutput{}, err
	}
	if changed {
		if timedOut {
			if pty != nil {
				u.PTYs.RemoveManaged(id, pty)
			}
			u.record(id, supervisor.EventKindOrphanChildAlive, store.Event{})
		} else {
			ownedChildren.Remove(id, childPID)
			u.record(id, supervisor.EventKindSessionExited, store.Event{Reason: store.ReasonStopSessionRequested})
		}
	}
	return StopSessionOutput{SessionID: id, State: resulting.State, Reason: resulting.StopReason}, nil
}

func (u StopSessionUseCase) record(id supervisor.SessionID, kind supervisor.SupervisorEventKind, event store.Event) {
	if u.RecordStructured != nil {
		event.SessionID = id.String()
		event.Kind = kind
		_ = u.RecordStructured(event)
	}
	if u.Notify != nil {
		u.Notify(kind, id.String())
	}
}
