package usecase

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/launch"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

// RespawnKind follows FD-sup-01 §5.4a.
type RespawnKind string

const (
	RespawnRecovery       RespawnKind = "recovery"
	RespawnResumeRecovery RespawnKind = "resume_recovery"
	RespawnHandoffStage5  RespawnKind = "handoff_stage5"
)

type RespawnSessionUseCase struct {
	Config             StartSessionConfig
	Projects           map[string]StartProject
	Store              SessionRecordStore
	Stop               *StopState
	Billing            launch.BillingGuard
	Starter            PTYStarter
	PTYs               *PTYRegistry
	Lock               func() (func(), error)
	Now                func() time.Time
	ParentEnv          func() []string
	RecordStructured   func(store.Event) error
	RecordStartFailure func(string, store.SupervisorStartFailReason)
	Notify             func(supervisor.SupervisorEventKind, string)
	Observe            func(context.Context, supervisor.ManagedSession, ManagedPTY)
	CleanupGrace       time.Duration
}

var respawnMutex sync.Mutex

func (u RespawnSessionUseCase) Execute(ctx context.Context, id supervisor.SessionID, kind RespawnKind) error {
	if kind != RespawnRecovery && kind != RespawnResumeRecovery && kind != RespawnHandoffStage5 {
		return fmt.Errorf("invalid respawn kind: %q", kind)
	}
	if u.Store == nil {
		return errors.New("session store is required")
	}
	respawnMutex.Lock()
	defer respawnMutex.Unlock()
	if u.Lock != nil {
		unlock, err := u.Lock()
		if err != nil {
			return err
		}
		defer unlock()
	}
	start := func() error { return u.executeLocked(ctx, id, kind) }
	if u.Stop != nil {
		err := u.Stop.GuardStart(start)
		if errors.Is(err, ErrStopActive) {
			return u.stopForStop(id)
		}
		return err
	}
	return start()
}

func (u RespawnSessionUseCase) now() time.Time {
	if u.Now != nil {
		return u.Now().UTC()
	}
	return time.Now().UTC()
}

func (u RespawnSessionUseCase) stopForStop(id supervisor.SessionID) error {
	reason, scope := supervisor.StopSession, supervisor.StopScopeSession
	if u.Stop != nil && u.Stop.IsGlobalStopped() {
		reason, scope = supervisor.StopGlobal, supervisor.StopScopeGlobal
	}
	changed := false
	var from supervisor.SupervisorSessionState
	err := UpdateSession(u.Store, id, func(s *supervisor.ManagedSession) error {
		if s.State == supervisor.SessionStopped || s.State == supervisor.SessionExited || s.State == supervisor.SessionFailed {
			return nil
		}
		changed = true
		from = s.State
		if err := s.Apply(supervisor.EventStop, supervisor.TransitionContext{StopReason: reason}, u.now()); err != nil {
			return err
		}
		s.StopScope = scope
		return nil
	})
	if err == nil && changed {
		u.stopped(id, from, reason)
	}
	return err
}

func (u RespawnSessionUseCase) executeLocked(ctx context.Context, id supervisor.SessionID, kind RespawnKind) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if u.Stop != nil && u.Stop.IsStopped(id) {
		return u.stopForStop(id)
	}
	session, err := u.Store.Load(id)
	if err != nil {
		return err
	}
	if kind == RespawnHandoffStage5 {
		if session.State != supervisor.SessionRotating {
			return nil
		}
	} else if session.State != supervisor.SessionStarting {
		return nil
	}
	project := u.Projects[session.ProjectID]
	// Respawn retains the project in the existing record; registration is not rechecked.
	if project.ID == "" {
		project.ID = session.ProjectID
	}
	projectID := session.ProjectID
	fail := func(reason store.SupervisorStartFailReason) { u.fail(id, projectID, reason) }
	preflight := launchPreflight{config: u.Config, billing: u.Billing, parentEnv: u.ParentEnv, failure: fail}
	env, err := preflight.check(project)
	if err != nil {
		return u.markFailed(id, err)
	}
	settingsPath, err := preflight.settings(id)
	if err != nil {
		return u.markFailed(id, err)
	}
	resumeID := session.ClaudeSessionID
	if resumeID == "" {
		resumeID = session.PreviousClaudeSessionID
	}
	if kind == RespawnHandoffStage5 {
		resumeID = ""
	} else if resumeID == "" {
		return u.stopWithReason(id, supervisor.StopClaudeSessionIDMissing)
	}
	argv, err := launch.BuildLaunchArgv(string(project.CLI), project.BinaryPath, settingsPath, resumeID)
	if err != nil {
		fail(store.StartFailLaunchArgvInvalid)
		return u.markFailed(id, err)
	}
	// Count and persist the attempt before spawn, so a failed launch still consumes it.
	var attempted supervisor.ManagedSession
	err = UpdateSession(u.Store, id, func(s *supervisor.ManagedSession) error {
		if (kind == RespawnHandoffStage5 && s.State != supervisor.SessionRotating) || (kind != RespawnHandoffStage5 && s.State != supervisor.SessionStarting) || (u.Stop != nil && u.Stop.IsStopped(id)) {
			return ErrStopActive
		}
		s.Generation++
		if kind != RespawnResumeRecovery {
			s.RestartCount++
			cutoff := u.now().Add(-supervisor.RestartWindow)
			kept := s.RestartTimes[:0]
			for _, at := range s.RestartTimes {
				if !at.Before(cutoff) {
					kept = append(kept, at)
				}
			}
			s.RestartTimes = kept
			s.RestartTimes = append(s.RestartTimes, u.now())
		}
		if s.ClaudeSessionID != "" {
			s.PreviousClaudeSessionID = s.ClaudeSessionID
		}
		s.ClaudeSessionID = ""
		s.ChildPID, s.ChildStartedAt = 0, nil
		attempted = *s
		return nil
	})
	if err != nil {
		if errors.Is(err, ErrStopActive) {
			if u.Stop != nil && u.Stop.IsStopped(id) {
				return u.stopForStop(id)
			}
			return nil
		}
		return err
	}
	starter := u.Starter
	if starter == nil {
		starter = defaultPTYStarter{}
	}
	pty, err := starter.Start(project.BinaryPath, argv, env, project.CWD, u.Config.Columns, u.Config.Rows)
	if err != nil {
		fail(store.StartFailSpawnFailed)
		return u.markFailed(id, err)
	}
	started := u.now()
	ownedChildren.Register(id, pty.Pid())
	if err := UpdateSession(u.Store, id, func(s *supervisor.ManagedSession) error {
		s.ChildPID, s.ChildStartedAt = pty.Pid(), &started
		return nil
	}); err != nil {
		ownedChildren.Remove(id, pty.Pid())
		_ = pty.Close()
		done := make(chan struct{})
		go func() { _, _ = pty.Wait(); close(done) }()
		grace := u.CleanupGrace
		if grace <= 0 {
			grace = timeoutKillGrace
		}
		select {
		case <-done:
		case <-time.After(grace):
			_ = pty.Kill(syscall.SIGKILL)
			<-done
		}
		fail(store.StartFailSpawnFailed)
		return u.markFailed(id, err)
	}
	if u.PTYs != nil {
		if writer, ok := pty.(PTYWriter); ok {
			u.PTYs.Register(id, writer)
		}
	}
	if u.Observe != nil {
		observed, err := u.Store.Load(id)
		if err != nil {
			// The child is already running; retain observation with the record just saved.
			observed = attempted
			observed.ChildPID, observed.ChildStartedAt = pty.Pid(), &started
		}
		go func() {
			defer ownedChildren.Remove(id, pty.Pid())
			defer func() {
				if u.PTYs != nil {
					if writer, ok := pty.(PTYWriter); ok {
						u.PTYs.Remove(id, writer)
					}
				}
			}()
			u.Observe(ctx, observed, pty)
		}()
	} else {
		go func() {
			_, _ = pty.Wait()
			ownedChildren.Remove(id, pty.Pid())
			if u.PTYs != nil {
				if writer, ok := pty.(PTYWriter); ok {
					u.PTYs.Remove(id, writer)
				}
			}
			_ = pty.Close()
		}()
	}
	if kind == RespawnRecovery && u.RecordStructured != nil {
		_ = u.RecordStructured(store.Event{Kind: supervisor.EventKindSessionRestarted, SessionID: id.String()})
	}
	if kind == RespawnRecovery && u.Notify != nil {
		u.Notify(supervisor.EventKindSessionRestarted, id.String())
	}
	return nil
}

func (u RespawnSessionUseCase) markFailed(id supervisor.SessionID, cause error) error {
	return u.stopWithReason(id, supervisor.StopRespawnFailed)
}

func (u RespawnSessionUseCase) stopWithReason(id supervisor.SessionID, reason supervisor.SupervisorStopReason) error {
	changed := false
	var from supervisor.SupervisorSessionState
	if err := UpdateSession(u.Store, id, func(s *supervisor.ManagedSession) error {
		if s.State == supervisor.SessionStopped {
			return nil
		}
		changed = true
		from = s.State
		return s.Apply(supervisor.EventStop, supervisor.TransitionContext{StopReason: reason}, u.now())
	}); err != nil {
		return err
	}
	if changed {
		u.stopped(id, from, reason)
	}
	return nil
}

func (u RespawnSessionUseCase) stopped(id supervisor.SessionID, from supervisor.SupervisorSessionState, reason supervisor.SupervisorStopReason) {
	if u.RecordStructured != nil {
		_ = u.RecordStructured(store.Event{Kind: supervisor.EventKindStateChanged, SessionID: id.String(), FromState: from, ToState: supervisor.SessionStopped})
		_ = u.RecordStructured(store.Event{Kind: supervisor.EventKindSessionStopped, SessionID: id.String(), ToState: supervisor.SessionStopped, Reason: store.EventReason(reason)})
	}
	if u.Notify != nil {
		u.Notify(supervisor.EventKindSessionStopped, id.String())
	}
}

func (u RespawnSessionUseCase) fail(id supervisor.SessionID, projectID string, reason store.SupervisorStartFailReason) {
	if u.RecordStartFailure != nil {
		u.RecordStartFailure(projectID, reason)
	} else if u.RecordStructured != nil {
		_ = u.RecordStructured(store.Event{Kind: supervisor.EventKindSessionStartFailed, ProjectID: projectID, Reason: store.EventReason(reason)})
	}
	if u.Notify != nil {
		u.Notify(supervisor.EventKindSessionStartFailed, "")
	}
}
