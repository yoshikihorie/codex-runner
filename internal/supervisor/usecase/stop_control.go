package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
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

// NewProcessResumeTransitionContext checks both PID liveness and process identity.
func NewProcessResumeTransitionContext(isAlive func(int) bool, processStartedAt func(int) (time.Time, bool)) ResumeTransitionContext {
	if isAlive == nil {
		isAlive = func(pid int) bool { return syscall.Kill(pid, 0) == nil }
	}
	if processStartedAt == nil {
		processStartedAt = proc.ProcessStartTime
	}
	return func(_ context.Context, s supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		alive := false
		orphan := false
		if ownedChildren.Owns(s.SessionID, s.ChildPID) {
			if s.StopReason == supervisor.StopOrphanChildAlive {
				orphan = isAlive(s.ChildPID)
			} else {
				alive = true
			}
		} else if s.ChildPID > 0 && isAlive(s.ChildPID) {
			if s.ChildStartedAt == nil {
				orphan = true
			} else if started, ok := processStartedAt(s.ChildPID); !ok || withinOneSecond(started, *s.ChildStartedAt) {
				orphan = true
			}
		}
		return supervisor.TransitionContext{ChildAlive: alive || orphan, OrphanChildAlive: orphan, HasRotation: s.Rotation != nil, RotationExitTimedOut: s.Rotation != nil && s.Rotation.FailReason == supervisor.RotateFailExitTimeout, ClaudeSessionID: s.ClaudeSessionID != "", HasPreviousClaudeSessionID: s.PreviousClaudeSessionID != "", ObservedState: s.PreviousState}, nil
	}
}

type ResumeOutput struct {
	Resumed      []supervisor.SessionID `json:"resumed"`
	StillStopped []StillStopped         `json:"still_stopped"`
}

func (o ResumeOutput) MarshalJSON() ([]byte, error) {
	if o.Resumed == nil {
		o.Resumed = []supervisor.SessionID{}
	}
	if o.StillStopped == nil {
		o.StillStopped = []StillStopped{}
	}
	type output ResumeOutput
	return json.Marshal(output(o))
}

type StillStopped struct {
	SessionID supervisor.SessionID `json:"session_id"`
	Reason    string               `json:"reason"`
}
type StopOutput struct {
	Stopped []supervisor.SessionID `json:"stopped"`
}
type StopControlUseCase struct {
	mu              sync.Mutex
	stops           *StopState
	sessions        SessionRepository
	events          StopEventRecorder
	notifier        StopNotifier
	now             func() time.Time
	rotationResumer func(context.Context, supervisor.ManagedSession) error
	rotation        *RotateSessionUseCase
	supervisorCtx   context.Context
	recoveryStarter func(context.Context, supervisor.ManagedSession) error
	suppression     ResumeSuppression
	maintenance     ResumeMaintenance
	transitionCtx   ResumeTransitionContext
	autoContinue    *AutoContinueUseCase
}

func (u *StopControlUseCase) SetResumeSuppression(s ResumeSuppression) { u.suppression = s }
func (u *StopControlUseCase) SetAutoContinue(a *AutoContinueUseCase)   { u.autoContinue = a }
func (u *StopControlUseCase) SetResumeMaintenance(m ResumeMaintenance) { u.maintenance = m }
func (u *StopControlUseCase) SetResumeTransitionContext(provider ResumeTransitionContext) {
	u.transitionCtx = provider
}

func NewStopControlUseCase(stops *StopState, sessions SessionRepository, events StopEventRecorder, notifier StopNotifier) *StopControlUseCase {
	u := &StopControlUseCase{stops: stops, sessions: sessions, events: events, notifier: notifier, now: time.Now, transitionCtx: NewProcessResumeTransitionContext(nil, nil)}
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

// SetRotationUseCase prepares the persisted resume before publishing rotating,
// then starts one worker using the supervisor lifetime.
func (u *StopControlUseCase) SetRotationUseCase(rotation *RotateSessionUseCase, supervisorCtx context.Context) {
	u.rotation = rotation
	u.supervisorCtx = supervisorCtx
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
			if u.autoContinue != nil && !wasStopped {
				u.autoContinue.ClearCandidate(s.SessionID)
			}
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
		if u.autoContinue != nil {
			u.autoContinue.ClearCandidate(*id)
		}
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
				if reason, known := resumeReason(s, err); known {
					out.StillStopped = append(out.StillStopped, StillStopped{s.SessionID, reason})
					continue
				}
				return out, err
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
	if err = u.resumeOne(ctx, &s, supervisor.StopScopeSession); err != nil {
		if reason, known := resumeReason(s, err); known {
			out.StillStopped = append(out.StillStopped, StillStopped{*id, reason})
			return out, nil
		}
		return out, err
	}
	out.Resumed = append(out.Resumed, *id)
	return out, nil
}
func (u *StopControlUseCase) resumeOne(ctx context.Context, s *supervisor.ManagedSession, scope supervisor.SupervisorStopScope) error {
	before := *s
	resumeStep := supervisor.SupervisorRotateStep("")
	if s.Rotation != nil {
		resumeStep = s.Rotation.Step
		rotationBefore := *s.Rotation
		before.Rotation = &rotationBefore
	}
	ctxTransition := supervisor.TransitionContext{}
	if u.transitionCtx != nil {
		var err error
		ctxTransition, err = u.transitionCtx(ctx, *s)
		if err != nil {
			return err
		}
	}
	if !ctxTransition.ChildAlive && s.ChildPID > 0 {
		s.ChildPID = 0
		s.ChildStartedAt = nil
	}
	ctxTransition.ResumeScope = scope
	if s.Rotation != nil {
		ctxTransition.RotationStep = s.Rotation.Step
		ctxTransition.RotationExitTimedOut = s.Rotation.FailReason == supervisor.RotateFailExitTimeout
		if s.Rotation.Step == supervisor.RotateSendExit && ctxTransition.ChildAlive && (ctxTransition.RotationExitTimedOut || s.SendGate.Pending != nil && s.SendGate.Pending.Kind == supervisor.SendKindHandoffExit || s.LastSend != nil && s.LastSend.Kind == supervisor.SendKindHandoffExit) {
			return errors.New("exit_process_alive")
		}
		if !ctxTransition.ChildAlive && (s.Rotation.Step == supervisor.RotateSendEnd || s.Rotation.Step == supervisor.RotateConfirmHandoff) && s.ClaudeSessionID == "" && s.PreviousClaudeSessionID == "" {
			return errors.New("claude_session_id_missing")
		}
	}
	if u.autoContinue != nil {
		u.autoContinue.Reset(s.SessionID)
	}
	if err := u.stops.GuardResume(scope, s.SessionID, func() error {
		if err := s.Apply(supervisor.EventResume, ctxTransition, u.now()); err != nil {
			if ctxTransition.OrphanChildAlive {
				u.record(s.SessionID, supervisor.EventKindResumeRefused, "")
				u.notify(ctx, supervisor.EventKindOrphanChildAlive, s.SessionID)
			}
			return err
		}
		if s.Rotation != nil && u.rotation != nil {
			if err := u.rotation.PrepareResume(s); err != nil {
				return err
			}
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
	if s.Rotation != nil {
		s.RestartCount = 0
	}
	if s.ChildPID == 0 && s.Rotation == nil && u.recoveryStarter != nil {
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
	if s.Rotation != nil && u.rotation != nil {
		if resumeStep != s.Rotation.Step {
			if err := u.rotation.record(s.SessionID, supervisor.EventKindRotateStepDone, resumeStep, "", ""); err != nil {
				return err
			}
		}
		if err := u.rotation.record(s.SessionID, supervisor.EventKindRotateResumed, resumeStep, "", ""); err != nil {
			return err
		}
	}
	u.record(s.SessionID, supervisor.EventKindSessionResumed, "")
	u.notify(ctx, supervisor.EventKindSessionResumed, s.SessionID)
	if err := u.stops.GuardResume(scope, s.SessionID, func() error { return nil }); err != nil {
		return err
	}
	if s.Rotation != nil && u.rotation != nil {
		lifetime := u.supervisorCtx
		if lifetime == nil {
			lifetime = ctx
		}
		go func() {
			if err := u.rotation.Run(lifetime, s.SessionID); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("rotation worker ended after resume", "reason", "run_failed")
			}
		}()
		return nil
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
func resumeReason(s supervisor.ManagedSession, err error) (string, bool) {
	switch err.Error() {
	case "stop_recreated", "still_stopped_by_global", "exit_process_alive", "claude_session_id_missing":
		return err.Error(), true
	case "SUP_ORPHAN_CHILD_ALIVE":
		return "orphan_child_alive", true
	case "SUP_CLAUDE_SESSION_ID_MISSING":
		return "claude_session_id_missing", true
	case "SUP_ROTATE_STEP_FAILED":
		if s.Rotation != nil && s.Rotation.Step == supervisor.RotateSendExit && s.Rotation.FailReason == supervisor.RotateFailExitTimeout {
			return "exit_process_alive", true
		}
	}
	return "", false
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
