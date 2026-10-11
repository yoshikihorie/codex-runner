package usecase

import (
	"context"
	"errors"
	"io"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

const (
	defaultObserveInterval     = 500 * time.Millisecond
	defaultStartupTimeout      = 120 * time.Second
	undeterminedNotifyInterval = 300 * time.Second
)

type ObserveSessionUseCase struct {
	Detect                  DetectSessionStateUseCase
	Sessions                SessionRecordStore
	Stop                    *StopState
	RecordStructured        func(store.Event) error
	Observed                *ObservedStateStore
	Notify                  func(supervisor.SupervisorEventKind, string)
	Interval                time.Duration
	StartupTimeout          time.Duration
	UndeterminedNotifyAfter time.Duration
	Now                     func() time.Time
}

func (u ObserveSessionUseCase) Run(ctx context.Context, initial supervisor.ManagedSession, pty interface {
	Read([]byte) (int, error)
	Wait() (int, error)
	Close() error
}, vt *supervisor.VirtualTerminal) error {
	if pty == nil || vt == nil || u.Sessions == nil {
		return errors.New("observer dependencies are required")
	}
	if u.RecordStructured != nil || u.Notify != nil {
		bindQuarantine(u.Sessions, func(id supervisor.SessionID) { u.recordEvent(id, supervisor.EventKindStateFileCorrupt, store.Event{}) })
	}
	interval := u.Interval
	if interval <= 0 {
		interval = defaultObserveInterval
	}
	startupTimeout := u.StartupTimeout
	if startupTimeout <= 0 {
		startupTimeout = defaultStartupTimeout
	}
	notifyAfter := u.UndeterminedNotifyAfter
	if notifyAfter <= 0 {
		notifyAfter = undeterminedNotifyInterval
	}
	now := time.Now
	if u.Now != nil {
		now = u.Now
	}
	started := initial.StateSince
	if initial.ChildStartedAt != nil {
		started = *initial.ChildStartedAt
	}
	lastChange := now().UTC()
	readerCtx, stopReader := context.WithCancel(ctx)
	type readResult struct {
		data []byte
		err  error
	}
	reads := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := pty.Read(buf)
			item := readResult{data: append([]byte(nil), buf[:n]...), err: err}
			select {
			case reads <- item:
			case <-readerCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	waits := make(chan struct {
		code int
		err  error
	}, 1)
	go func() {
		code, err := pty.Wait()
		waits <- struct {
			code int
			err  error
		}{code, err}
	}()
	var exited bool
	defer func() {
		stopReader()
		if exited {
			_ = pty.Close()
		}
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	var exitCode *int
	var undeterminedSince time.Time
	undeterminedNotified := false
	lastIllegal := ""
	for {
		select {
		case waited := <-waits:
			exited = true
			code := waited.code
			if waited.err != nil {
				code = -1
			}
			exitCode = &code
			_ = pty.Close()
		case item := <-reads:
			if len(item.data) > 0 {
				before := vt.Screen()
				_, _ = vt.Write(item.data)
				after := vt.Screen()
				if !sameRows(before, after) {
					lastChange = now().UTC()
				}
			}
			if item.err != nil && !errors.Is(item.err, io.EOF) && !exited {
				// Wait remains the source of process-exit truth.
			}
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		}
		current, err := u.Sessions.Load(initial.SessionID)
		if err != nil {
			return err
		}
		result, err := u.Detect.Execute(ctx, current, vt, lastChange, exited, exitCode)
		if err != nil {
			return err
		}
		if u.Observed != nil {
			u.Observed.Set(initial.SessionID, result.State)
		}
		state := supervisor.SessionWorking
		switch result.State {
		case detect.StateIdle:
			state = supervisor.SessionIdle
		case detect.StateWorking:
			state = supervisor.SessionWorking
		case detect.StateAwaitingPermission:
			state = supervisor.SessionAwaitingPermission
		case detect.StateAwaitingRecommendation:
			state = supervisor.SessionAwaitingRecommendation
		case detect.StateExited:
			state = supervisor.SessionExited
		}
		var nextState supervisor.SupervisorSessionState
		var illegal string
		var unsafeSet bool
		stoppedByControl := u.Stop != nil && u.Stop.IsStopped(initial.SessionID)
		wasStopped := current.State == supervisor.SessionStopped
		var fromState supervisor.SupervisorSessionState
		err = UpdateSession(u.Sessions, initial.SessionID, func(s *supervisor.ManagedSession) error {
			fromState = s.State
			if result.PermissionMode == detect.PermissionUnsafe && s.AutoContinueDisabledReason == "" {
				s.AutoContinueDisabledReason = supervisor.AutoContinueDisabledPermissionModeUnsafe
				unsafeSet = true
			}
			if exited {
				// Xe remains active while stopped; no exited child may retain a PID.
				s.ChildPID = 0
				s.ChildStartedAt = nil
				event := supervisor.EventUnexpectedExit
				if s.State == supervisor.SessionRotating {
					event = supervisor.EventHandoffExit
				}
				previous := s.State
				err := s.Apply(event, supervisor.TransitionContext{Generation: s.Generation, ChildPID: s.ChildPID, ExitCode: valueOrZero(exitCode), HasPendingSend: s.SendGate.Pending != nil, ClaudeSessionID: s.ClaudeSessionID != "", HasPreviousClaudeSessionID: s.PreviousClaudeSessionID != "", RestartCount: s.RestartCount, DailyRestartCount: restartCountSince(s.RestartTimes, now().Add(-24*time.Hour)), ObservedState: state}, now().UTC())
				if err == nil && previous != s.State {
					nextState = s.State
				}
				return nil
			}
			if stoppedByControl || s.State == supervisor.SessionStopped {
				return nil
			}
			if result.State != detect.StateUndetermined {
				event := map[supervisor.SupervisorSessionState]supervisor.TransitionEvent{supervisor.SessionIdle: supervisor.EventDetectorIdle, supervisor.SessionWorking: supervisor.EventDetectorWorking, supervisor.SessionAwaitingPermission: supervisor.EventDetectorPermission, supervisor.SessionAwaitingRecommendation: supervisor.EventDetectorRecommendation}[state]
				if event != "" {
					previous := s.State
					if err := s.Apply(event, supervisor.TransitionContext{Generation: s.Generation, ChildPID: s.ChildPID, ObservedState: state}, now().UTC()); err != nil {
						illegal = string(s.State) + "/" + string(state)
					} else if previous != s.State {
						nextState = s.State
					}
				}
			}
			if s.State == supervisor.SessionStarting && !stoppedByControl && now().Sub(started) >= startupTimeout && result.State != detect.StateIdle && result.State != detect.StateAwaitingPermission && result.State != detect.StateAwaitingRecommendation {
				if err := s.Apply(supervisor.EventStartupTimeout, supervisor.TransitionContext{Generation: s.Generation}, now().UTC()); err != nil {
					return err
				}
				nextState = s.State
			}
			return nil
		})
		if err != nil {
			return err
		}
		if unsafeSet {
			u.recordEvent(initial.SessionID, supervisor.EventKindPermissionModeUnsafe, store.Event{Code: "SUP_PERMISSION_MODE_UNSAFE"})
		}
		if exited && wasStopped {
			u.recordEvent(initial.SessionID, supervisor.EventKindExitWhileStopped, store.Event{})
		}
		if illegal != "" && illegal != lastIllegal {
			u.recordEvent(initial.SessionID, supervisor.EventKindIllegalTransition, store.Event{Code: "SUP_ILLEGAL_TRANSITION"})
		}
		if illegal != "" || nextState != "" {
			lastIllegal = illegal
		}
		if nextState != "" {
			u.recordEvent(initial.SessionID, supervisor.EventKindStateChanged, store.Event{FromState: fromState, ToState: nextState})
		}
		if exited {
			current, err = u.Sessions.Load(initial.SessionID)
			if err != nil {
				return err
			}
			if current.State == supervisor.SessionExited {
				u.recordEvent(initial.SessionID, supervisor.EventKindSessionExited, store.Event{Reason: store.EventReason(current.ExitReason)})
			} else if current.State == supervisor.SessionStopped && !wasStopped {
				u.recordEvent(initial.SessionID, supervisor.EventKindSessionStopped, store.Event{Reason: store.EventReason(current.StopReason)})
			}
		}
		if result.State == detect.StateUndetermined {
			if undeterminedSince.IsZero() {
				undeterminedSince = now().UTC()
			}
			if !undeterminedNotified && now().Sub(undeterminedSince) >= notifyAfter {
				u.recordEvent(initial.SessionID, supervisor.EventKindUndeterminedLong, store.Event{})
				undeterminedNotified = true
			}
		} else {
			undeterminedSince = time.Time{}
			undeterminedNotified = false
		}
		if nextState == supervisor.SessionFailed || nextState == supervisor.SessionStopped && result.State != detect.StateExited {
			current, err = u.Sessions.Load(initial.SessionID)
			if err != nil {
				return err
			}
			reason := "SUP_SPAWN_FAILED"
			kind := supervisor.EventKindSessionFailed
			if current.Generation >= 2 {
				reason = string(supervisor.StopRecoveryStartTimeout)
				kind = supervisor.EventKindSessionStopped
			}
			event := store.Event{}
			if kind == supervisor.EventKindSessionFailed {
				event.Code = "SUP_SPAWN_FAILED"
			} else {
				event.Reason = store.EventReason(reason)
			}
			u.recordEvent(initial.SessionID, kind, event)
			if nextState == supervisor.SessionFailed {
				if killer, ok := pty.(interface{ Kill(syscall.Signal) error }); ok {
					_ = killer.Kill(syscall.SIGTERM)
					select {
					case <-waits:
					case <-time.After(timeoutKillGrace):
						_ = killer.Kill(syscall.SIGKILL)
						<-waits
					}
				} else {
					_ = pty.Close()
					<-waits
				}
				exited = true
				_ = pty.Close()
				if err := UpdateSession(u.Sessions, initial.SessionID, func(s *supervisor.ManagedSession) error {
					s.ChildPID = 0
					s.ChildStartedAt = nil
					return nil
				}); err != nil {
					return err
				}
				if u.Observed != nil {
					u.Observed.Delete(initial.SessionID)
				}
				return nil
			}
		}
		if exited {
			if u.Observed != nil {
				u.Observed.Delete(initial.SessionID)
			}
			return nil
		}
	}
}

func restartCountSince(times []time.Time, since time.Time) int {
	count := 0
	for _, at := range times {
		if !at.Before(since) {
			count++
		}
	}
	return count
}
func (u ObserveSessionUseCase) recordEvent(id supervisor.SessionID, kind supervisor.SupervisorEventKind, event store.Event) {
	if u.RecordStructured != nil {
		event.SessionID = id.String()
		event.Kind = kind
		_ = u.RecordStructured(event)
	}
	if u.Notify != nil {
		u.Notify(kind, id.String())
	}
}
func sameRows(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func valueOrZero(v *int) int {
	if v == nil {
		return 0
	}
	return *v
}
