package usecase

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

var ErrResumeForbidden = errors.New("SUP_RESUME_FORBIDDEN")
var ErrStopActive = errors.New("SUP_STOP_ACTIVE")

type SessionRepository interface {
	List(context.Context) ([]supervisor.ManagedSession, error)
	Save(context.Context, supervisor.ManagedSession) error
}
type StopEventRecorder interface {
	RecordEvent(supervisor.SessionID, supervisor.SupervisorEventKind, supervisor.SupervisorStopReason, supervisor.SupervisorExitReason, supervisor.SupervisorRotateFailReason, time.Time)
}
type CheckedStopEventRecorder interface {
	RecordEventChecked(supervisor.SessionID, supervisor.SupervisorEventKind, supervisor.SupervisorStopReason, supervisor.SupervisorExitReason, supervisor.SupervisorRotateFailReason, time.Time) error
}
type StopEventFailureLogger interface {
	LogEventFailure(supervisor.SupervisorEventKind, error)
}
type StopNotifier interface {
	Notify(context.Context, supervisor.SupervisorEventKind, string) error
}
type ResumeSuppression interface {
	Clear(context.Context, supervisor.SessionID) error
}
type ResumeMaintenance interface {
	DiscardPending(context.Context, supervisor.SessionID) error
	RefreshGitConfigFingerprint(context.Context, supervisor.SessionID) error
	ResetRestartCounters(context.Context, supervisor.SessionID) error
}
type ResumeTransitionContext func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error)
type ResumeOutput struct {
	Resumed      []supervisor.SessionID
	StillStopped []StillStopped
}
type StillStopped struct {
	SessionID supervisor.SessionID
	Reason    string
}
type StopOutput struct{ Stopped []supervisor.SessionID }
type StopControlUseCase struct {
	mu              sync.Mutex
	stops           *StopState
	sessions        SessionRepository
	events          StopEventRecorder
	notifier        StopNotifier
	now             func() time.Time
	rotationResumer func(context.Context, supervisor.ManagedSession) error
	recoveryStarter func(context.Context, supervisor.ManagedSession) error
	suppression     ResumeSuppression
	maintenance     ResumeMaintenance
	transitionCtx   ResumeTransitionContext
}

func (u *StopControlUseCase) SetResumeSuppression(s ResumeSuppression) { u.suppression = s }
func (u *StopControlUseCase) SetResumeMaintenance(m ResumeMaintenance) { u.maintenance = m }
func (u *StopControlUseCase) SetResumeTransitionContext(provider ResumeTransitionContext) {
	u.transitionCtx = provider
}

func NewStopControlUseCase(stops *StopState, sessions SessionRepository, events StopEventRecorder, notifier StopNotifier) *StopControlUseCase {
	u := &StopControlUseCase{stops: stops, sessions: sessions, events: events, notifier: notifier, now: time.Now}
	stops.SetRemovalHandler(func(id supervisor.SessionID) {
		u.record(id, supervisor.EventKindStopFileRemoved, "")
		u.notify(context.Background(), supervisor.EventKindStopFileRemoved, id)
	})
	return u
}

// SetResumeContinuations wires T4-13 handoff and T4-10 recovery steps without coupling this package to them.
func (u *StopControlUseCase) SetResumeContinuations(rotation, recovery func(context.Context, supervisor.ManagedSession) error) {
	u.rotationResumer = rotation
	u.recoveryStarter = recovery
}
func (u *StopControlUseCase) Stop(ctx context.Context, id *supervisor.SessionID) (StopOutput, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	var out StopOutput
	if id == nil {
		sessions, err := u.sessions.List(ctx)
		if err != nil {
			return out, err
		}
		wasStopped := u.stops.IsGlobalStopped()
		if err := u.stops.EnableGlobal(); err != nil {
			return out, err
		}
		if !wasStopped {
			u.record("", supervisor.EventKindStopSet, supervisor.StopGlobal)
		}
		for _, s := range sessions {
			before := s
			changed := false
			transitioned := false
			if s.State == supervisor.SessionStopped {
				changed = s.StopScope != supervisor.StopScopeGlobal
				s.StopScope = supervisor.StopScopeGlobal
			} else if !terminal(s.State) {
				s.PreviousState = s.State
				if err = s.Apply(supervisor.EventStop, supervisor.TransitionContext{StopReason: supervisor.StopGlobal}, u.now()); err != nil {
					return out, err
				}
				s.StopScope = supervisor.StopScopeGlobal
				changed = true
				transitioned = true
			}
			if !changed {
				continue
			}
			if err = u.persistSessionPatch(ctx, before, s); err != nil {
				return out, err
			}
			if transitioned {
				out.Stopped = append(out.Stopped, s.SessionID)
				u.record(s.SessionID, supervisor.EventKindSessionStopped, s.StopReason)
				u.notify(ctx, supervisor.EventKindSessionStopped, s.SessionID)
			}
		}
		return out, nil
	}
	if _, err := supervisor.NewSessionID(id.String()); err != nil {
		return out, err
	}
	s, err := u.find(ctx, *id)
	if err != nil {
		return out, err
	}
	before := s
	wasStopped := u.stops.IsStopped(*id)
	if err := u.stops.EnableSession(*id); err != nil {
		return out, err
	}
	if !wasStopped {
		u.record(*id, supervisor.EventKindStopSet, supervisor.StopSession)
	}
	changed := false
	transitioned := false
	if s.State == supervisor.SessionStopped {
		changed = s.StopScope != supervisor.StopScopeSession
		s.StopScope = supervisor.StopScopeSession
	} else if !terminal(s.State) {
		s.PreviousState = s.State
		if err = s.Apply(supervisor.EventStop, supervisor.TransitionContext{StopReason: supervisor.StopSession}, u.now()); err != nil {
			return out, err
		}
		s.StopScope = supervisor.StopScopeSession
		changed = true
		transitioned = true
	}
	if !changed {
		return out, nil
	}
	if err = u.persistSessionPatch(ctx, before, s); err != nil {
		return out, err
	}
	if transitioned {
		out.Stopped = append(out.Stopped, *id)
		u.record(*id, supervisor.EventKindSessionStopped, s.StopReason)
		u.notify(ctx, supervisor.EventKindSessionStopped, *id)
	}
	return out, nil
}

// Resume enforces the caller-supplied tty and supervised-marker checks before taking the exclusive lock.
func (u *StopControlUseCase) Resume(ctx context.Context, id *supervisor.SessionID, isatty, supervised bool) (ResumeOutput, error) {
	var out ResumeOutput
	if !isatty || supervised {
		if id != nil {
			u.record(*id, supervisor.EventKindResumeRefused, "")
		} else {
			u.record("", supervisor.EventKindResumeRefused, "")
		}
		return out, ErrResumeForbidden
	}
	if id != nil {
		if _, err := supervisor.NewSessionID(id.String()); err != nil {
			return out, ErrSessionNotFound
		}
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if id == nil {
		wasStopped := u.stops.IsGlobalStopped()
		if err := u.stops.ClearGlobal(); err != nil {
			return out, err
		}
		if wasStopped {
			u.record("", supervisor.EventKindStopCleared, "")
		}
		sessions, err := u.sessions.List(ctx)
		if err != nil {
			return out, err
		}
		for _, s := range sessions {
			if s.State != supervisor.SessionStopped || s.StopScope != supervisor.StopScopeGlobal {
				continue
			}
			if s.StopReason != supervisor.StopGlobal {
				before := s
				s.StopScope = supervisor.StopScopeNone
				if err = u.persistSessionPatch(ctx, before, s); err != nil {
					return out, err
				}
				out.StillStopped = append(out.StillStopped, StillStopped{s.SessionID, "reason_not_resumed_by_global"})
				continue
			}
			if u.stops.IsSessionStopped(s.SessionID) {
				out.StillStopped = append(out.StillStopped, StillStopped{s.SessionID, "still_stopped_by_global"})
				continue
			}
			if err = u.resumeOne(ctx, &s, supervisor.StopScopeGlobal); err != nil {
				out.StillStopped = append(out.StillStopped, StillStopped{s.SessionID, resumeReason(s, err)})
				continue
			}
			out.Resumed = append(out.Resumed, s.SessionID)
		}
		return out, nil
	}
	s, err := u.find(ctx, *id)
	if err != nil {
		return out, err
	}
	if u.stops.IsGlobalStopped() {
		out.StillStopped = append(out.StillStopped, StillStopped{*id, "still_stopped_by_global"})
		return out, nil
	}
	if s.State != supervisor.SessionStopped {
		if err := u.stops.ClearSession(*id); err != nil {
			return out, err
		}
		return out, nil
	}
	if err = u.stops.ClearSession(*id); err != nil {
		return out, err
	}
	if u.stops.IsStopped(*id) {
		out.StillStopped = append(out.StillStopped, StillStopped{*id, "stop_recreated"})
		return out, nil
	}
	s.StopScope = supervisor.StopScopeNone
	if err = u.resumeOne(ctx, &s, supervisor.StopScopeSession); err != nil {
		out.StillStopped = append(out.StillStopped, StillStopped{*id, resumeReason(s, err)})
		return out, nil
	}
	out.Resumed = append(out.Resumed, *id)
	return out, nil
}
func (u *StopControlUseCase) resumeOne(ctx context.Context, s *supervisor.ManagedSession, scope supervisor.SupervisorStopScope) error {
	before := *s
	ctxTransition := supervisor.TransitionContext{ChildAlive: s.ChildPID > 0, OrphanChildAlive: s.StopReason == supervisor.StopOrphanChildAlive, HasRotation: s.Rotation != nil, ClaudeSessionID: s.ClaudeSessionID != "", HasPreviousClaudeSessionID: s.PreviousClaudeSessionID != "", ObservedState: s.PreviousState}
	if u.transitionCtx != nil {
		var err error
		ctxTransition, err = u.transitionCtx(ctx, *s)
		if err != nil {
			return err
		}
	}
	ctxTransition.ResumeScope = scope
	if s.Rotation != nil {
		ctxTransition.RotationStep = s.Rotation.Step
	}
	if err := u.stops.GuardResume(scope, s.SessionID, func() error {
		if err := s.Apply(supervisor.EventResume, ctxTransition, u.now()); err != nil {
			return err
		}
		return u.persistSessionPatch(ctx, before, *s)
	}); err != nil {
		return err
	}
	before = *s
	if scope == supervisor.StopScopeSession {
		u.record(s.SessionID, supervisor.EventKindStopCleared, "")
	}
	if s.SendGate.Pending != nil {
		s.SendGate.Pending = nil
		s.SendGate.UnconfirmedDiscarded = true
		u.record(s.SessionID, supervisor.EventKindSendPendingDiscarded, "")
	}
	if u.maintenance != nil {
		if err := u.maintenance.DiscardPending(ctx, s.SessionID); err != nil {
			return err
		}
		if err := u.maintenance.RefreshGitConfigFingerprint(ctx, s.SessionID); err != nil {
			return err
		}
	}
	if u.suppression != nil {
		if err := u.suppression.Clear(ctx, s.SessionID); err != nil {
			return err
		}
	}
	if s.ChildPID == 0 {
		s.RestartCount = 0
		s.RestartTimes = nil
		if u.maintenance != nil {
			if err := u.maintenance.ResetRestartCounters(ctx, s.SessionID); err != nil {
				return err
			}
		}
		u.record(s.SessionID, supervisor.EventKindRestartCountersReset, "")
	}
	if err := u.persistSessionPatch(ctx, before, *s); err != nil {
		return err
	}
	u.record(s.SessionID, supervisor.EventKindSessionResumed, "")
	u.notify(ctx, supervisor.EventKindSessionResumed, s.SessionID)
	if err := u.stops.GuardResume(scope, s.SessionID, func() error { return nil }); err != nil {
		return err
	}
	if s.Rotation != nil && u.rotationResumer != nil {
		return u.rotationResumer(ctx, *s)
	}
	if s.ChildPID == 0 && u.recoveryStarter != nil {
		return u.recoveryStarter(ctx, *s)
	}
	return nil
}

func (u *StopControlUseCase) persistSessionPatch(ctx context.Context, before, after supervisor.ManagedSession) error {
	return UpdateSessionPatch(sessionLocker, after.SessionID,
		func() (supervisor.ManagedSession, error) { return u.find(ctx, after.SessionID) },
		func(session supervisor.ManagedSession) error { return u.sessions.Save(ctx, session) }, before, after)
}
func resumeReason(s supervisor.ManagedSession, err error) string {
	message := err.Error()
	if strings.Contains(message, "stop_recreated") {
		return "stop_recreated"
	}
	if strings.Contains(message, "still_stopped_by_global") {
		return "still_stopped_by_global"
	}
	if strings.Contains(message, "SUP_ORPHAN_CHILD_ALIVE") || s.StopReason == supervisor.StopOrphanChildAlive {
		return "orphan_child_alive"
	}
	if strings.Contains(message, "SUP_ROTATE_STEP_FAILED") {
		return "exit_process_alive"
	}
	if s.ClaudeSessionID == "" && s.PreviousClaudeSessionID == "" && s.ChildPID == 0 && s.Rotation == nil {
		return "claude_session_id_missing"
	}
	return "exit_process_alive"
}
func (u *StopControlUseCase) find(ctx context.Context, id supervisor.SessionID) (supervisor.ManagedSession, error) {
	sessions, err := u.sessions.List(ctx)
	if err != nil {
		return supervisor.ManagedSession{}, err
	}
	for _, s := range sessions {
		if s.SessionID == id {
			return s, nil
		}
	}
	return supervisor.ManagedSession{}, ErrSessionNotFound
}
func (u *StopControlUseCase) record(id supervisor.SessionID, k supervisor.SupervisorEventKind, r supervisor.SupervisorStopReason) {
	if recorder, ok := u.events.(CheckedStopEventRecorder); ok {
		if err := recorder.RecordEventChecked(id, k, r, "", "", u.now()); err != nil {
			if logger, ok := u.events.(StopEventFailureLogger); ok {
				logger.LogEventFailure(k, err)
			} else {
				slog.Warn("supervisor event recording failed", "kind", k, "error", err)
			}
		}
		return
	}
	if u.events != nil {
		u.events.RecordEvent(id, k, r, "", "", u.now())
	}
}
func (u *StopControlUseCase) notify(ctx context.Context, k supervisor.SupervisorEventKind, id supervisor.SessionID) {
	if u.notifier != nil {
		value := id.String()
		if _, err := supervisor.NewSessionID(value); err != nil {
			value = ""
		}
		_ = u.notifier.Notify(ctx, k, value)
	}
}
func terminal(s supervisor.SupervisorSessionState) bool {
	return s == supervisor.SessionExited || s == supervisor.SessionFailed
}
