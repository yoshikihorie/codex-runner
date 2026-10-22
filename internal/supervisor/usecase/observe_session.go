package usecase

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
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
	Recover                 *RecoverSessionUseCase
	Respawn                 *RespawnSessionUseCase
	SendInput               *SendInputUseCase
	InitialRemoteControl    *InitialRemoteControl
	Rotation                *RotateSessionUseCase
	AutoContinue            *AutoContinueUseCase
	PermissionPending       *PermissionPendingUseCase
	CommitVerified          *CommitVerifiedChangesUseCase
	EvaluateCommit          func(context.Context, supervisor.SessionID) error
	PlanRotation            PlanRotationUseCase
	PendingRemoteControl    *PendingRemoteControlUseCase
	RotationStatuslineAt    func(supervisor.SessionID) *time.Time
	SupervisorContext       context.Context
	CompletionSelection     func([]string) (bool, string)
	RecordStructured        func(store.Event) error
	Observed                *ObservedStateStore
	ContextPercent          *ObservedContextStore
	Notify                  func(supervisor.SupervisorEventKind, string)
	Interval                time.Duration
	StartupTimeout          time.Duration
	UndeterminedNotifyAfter time.Duration
	Now                     func() time.Time
}

var rotationObservationWakes sync.Map
var nextScreenOwner atomic.Uint64

func rotationWakeChannel(id supervisor.SessionID) <-chan struct{} {
	value, _ := rotationObservationWakes.LoadOrStore(id, make(chan struct{}, 1))
	return value.(chan struct{})
}

func signalRotationObservation(id supervisor.SessionID) {
	value, _ := rotationObservationWakes.LoadOrStore(id, make(chan struct{}, 1))
	ch := value.(chan struct{})
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (u ObserveSessionUseCase) rotationStatuslineAt(id supervisor.SessionID) *time.Time {
	if u.RotationStatuslineAt != nil {
		return u.RotationStatuslineAt(id)
	}
	if u.Detect.StateDir == "" {
		return nil
	}
	record, err := readStatusline(filepath.Join(u.Detect.StateDir, "statusline", id.String()+".json"))
	if err != nil {
		return nil
	}
	return &record.ObservedAt
}

func (u ObserveSessionUseCase) Run(ctx context.Context, initial supervisor.ManagedSession, pty interface {
	Read([]byte) (int, error)
	Wait() (int, error)
	Close() error
}, vt *supervisor.VirtualTerminal) (runErr error) {
	var exitEventErr error
	// Report a confirmed-event failure after the normal exit cleanup, including
	// when a later operation also fails. The durable result remains confirmed.
	defer func() {
		if exitEventErr != nil {
			runErr = errors.Join(runErr, exitEventErr)
		}
	}()
	if pty == nil || vt == nil || u.Sessions == nil {
		return errors.New("observer dependencies are required")
	}
	screenOwner := nextScreenOwner.Add(1)
	if u.ContextPercent != nil {
		u.ContextPercent.Delete(initial.SessionID)
		defer u.ContextPercent.Delete(initial.SessionID)
	}
	gitPhaseDone := make(chan time.Time, 1)
	commitEvaluationDone := make(chan struct{}, 1)
	var gitPhaseReported sync.Map
	if u.CommitVerified != nil {
		unregister := u.CommitVerified.RegisterGitPhase(initial.SessionID, func(since time.Time) {
			gitPhaseReported.Store(since, true)
			current, err := u.Sessions.Load(initial.SessionID)
			if err != nil || !current.StateSince.Equal(since) {
				return
			}
			select {
			case gitPhaseDone <- since:
			default:
			}
		})
		defer unregister()
		u.EvaluateCommit = u.CommitVerified.Execute
	}
	if u.AutoContinue != nil {
		var stopped func(supervisor.SessionID) bool
		if u.Stop != nil {
			stopped = u.Stop.IsStopped
		}
		u.AutoContinue.Bind(u.Sessions, u.SendInput, stopped, u.RecordStructured, u.Notify)
		if u.SendInput != nil {
			u.SendInput.BindRecommendationIssued(u.AutoContinue.RecordIssued)
		}
	}
	if u.PermissionPending != nil {
		u.PermissionPending.Bind(func(id supervisor.SessionID, kind supervisor.SupervisorEventKind) error {
			if u.RecordStructured != nil {
				return u.RecordStructured(store.Event{Kind: kind, SessionID: id.String()})
			}
			return nil
		}, u.Notify)
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
	var observation uint64
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
	var exitedAt time.Time
	defer func() {
		stopReader()
		if u.SendInput != nil {
			if screens, ok := u.SendInput.Screens.(interface {
				DeleteOwned(supervisor.SessionID, uint64)
			}); ok {
				screens.DeleteOwned(initial.SessionID, screenOwner)
			} else if screens, ok := u.SendInput.Screens.(interface{ Delete(supervisor.SessionID) }); ok {
				screens.Delete(initial.SessionID)
			}
		}
		if current, err := u.Sessions.Load(initial.SessionID); err == nil && terminal(current.State) {
			rotationObservationWakes.Delete(initial.SessionID)
			if u.Rotation != nil {
				u.Rotation.EndSession(initial.SessionID)
			}
		}
		if u.SendInput != nil && u.SendInput.PTYs != nil {
			if writer, ok := pty.(PTYWriter); ok {
				u.SendInput.PTYs.Remove(initial.SessionID, writer)
			}
		}
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
	var rotationPendingSince *time.Time
	rotationIdleWaitNotified := false
	var rotationDecisionState supervisor.SupervisorSessionState
	var rotationDecisionSince time.Time
	rotationContextUnknownRecorded := false
	rotationNotApplicableRecorded := false
	pendingDone := make(chan struct{}, 1)
	pendingInFlight := false
	var commitSince time.Time
	var recommendationSince time.Time
	commitStarted := false
	commitInFlight := false
	autoAttempted := false
	contextWasKnown := false
	var permissionWarnedSince time.Time
	var lastDetected detect.DetectedState
	armed := false
	for {
		select {
		case <-pendingDone:
			pendingInFlight = false
		default:
		}
		var screenUpdatedAt time.Time
		var gitPhaseCompleted time.Time
		select {
		case gitPhaseCompleted = <-gitPhaseDone:
		case <-commitEvaluationDone:
			commitInFlight = false
		case waited := <-waits:
			exited = true
			exitedAt = now().UTC()
			if u.SendInput != nil && u.SendInput.PTYs != nil {
				registry := u.SendInput.PTYs
				if registry.IsShuttingDown() {
					return nil
				}
				if wrapped, ok := pty.(observedPTY); ok {
					managed := wrapped.ManagedPTY
					if registry.GetManaged(initial.SessionID) != managed {
						ownedChildren.Remove(initial.SessionID, initial.ChildPID)
						return nil
					}
					if registry.IsStopping(initial.SessionID, managed) {
						changed := false
						err := UpdateSession(u.Sessions, initial.SessionID, func(s *supervisor.ManagedSession) error {
							if s.State == supervisor.SessionExited {
								return nil
							}
							if err := s.Apply(supervisor.EventStopSession, supervisor.TransitionContext{ChildAlive: false}, now().UTC()); err != nil {
								return err
							}
							s.ChildPID = 0
							s.ChildStartedAt = nil
							changed = true
							return nil
						})
						if err != nil {
							return err
						}
						ownedChildren.Remove(initial.SessionID, initial.ChildPID)
						if changed {
							u.recordEvent(initial.SessionID, supervisor.EventKindSessionExited, store.Event{Reason: store.ReasonStopSessionRequested})
						}
						return nil
					}
				}
			}
			ownedChildren.Remove(initial.SessionID, initial.ChildPID)
			code := waited.code
			if waited.err != nil {
				code = -1
			}
			exitCode = &code
			_ = pty.Close()
		case item := <-reads:
			if len(item.data) > 0 {
				observation++
				screenUpdatedAt = now().UTC()
				before := vt.Screen()
				var beforeCells [][]supervisor.Cell
				if u.SendInput != nil {
					beforeCells = make([][]supervisor.Cell, len(before))
					for i := range before {
						beforeCells[i] = vt.Cells(i)
					}
				}
				_, _ = vt.Write(item.data)
				after := vt.Screen()
				cellsChanged := false
				if u.SendInput != nil && sameRows(before, after) {
					afterCells := make([][]supervisor.Cell, len(after))
					for i := range after {
						afterCells[i] = vt.Cells(i)
					}
					cellsChanged = !reflect.DeepEqual(beforeCells, afterCells)
				}
				if !sameRows(before, after) || cellsChanged {
					lastChange = now().UTC()
					screenUpdatedAt = lastChange
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
		if !exited {
			if err := u.resetStableRestartCount(initial.SessionID, current, now().UTC()); err != nil {
				return err
			}
		}
		result, err := u.Detect.Execute(ctx, current, vt, lastChange, exited, exitCode)
		if err != nil {
			return err
		}
		if result.ProcessingIndicator {
			armed = true
		}
		if !armed && result.State == detect.StateAwaitingRecommendation {
			result.State = detect.StateIdle
		}
		if u.AutoContinue != nil && current.State == supervisor.SessionStopped && lastDetected == detect.StateWorking && result.State == detect.StateAwaitingRecommendation {
			u.AutoContinue.entered(initial.SessionID, now().UTC())
		}
		lastDetected = result.State
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
		var exitSave *exitCommit
		var exitPending *supervisor.PendingSend
		updateObserved := func(s *supervisor.ManagedSession, tx *exitCommit) error {
			fromState = s.State
			if result.PermissionMode == detect.PermissionUnsafe && s.AutoContinueDisabledReason == "" {
				s.AutoContinueDisabledReason = supervisor.AutoContinueDisabledPermissionModeUnsafe
				unsafeSet = true
			}
			if exited {
				if s.SendGate.Pending != nil {
					pending := *s.SendGate.Pending
					exitPending = &pending
				}
				if exitPending != nil && exitPending.Kind == supervisor.SendKindHandoffExit && u.SendInput != nil {
					u.SendInput.observeExit(s, *exitPending, handoffPTYOwner(pty), tx)
				}
				// Xe remains active while stopped; no exited child may retain a PID.
				s.ChildPID = 0
				s.ChildStartedAt = nil
				event := supervisor.EventUnexpectedExit
				if s.State == supervisor.SessionRotating {
					event = supervisor.EventHandoffExit
				}
				previous := s.State
				err := s.Apply(event, supervisor.TransitionContext{CLI: s.CLI, Generation: s.Generation, ChildPID: s.ChildPID, ExitCode: valueOrZero(exitCode), HasPendingSend: s.SendGate.Pending != nil, ClaudeSessionID: s.ClaudeSessionID != "", HasPreviousClaudeSessionID: s.PreviousClaudeSessionID != "", RestartCount: s.RestartCount, DailyRestartCount: supervisor.RestartCountInWindow(s.RestartTimes, now().UTC()), ObservedState: state}, now().UTC())
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
		}
		if u.SendInput != nil {
			exitSave, err = u.SendInput.updateExitSession(u.Sessions, initial.SessionID, updateObserved)
		} else {
			err = UpdateSession(u.Sessions, initial.SessionID, func(s *supervisor.ManagedSession) error { return updateObserved(s, &exitCommit{}) })
		}
		if err != nil {
			return err
		}
		if exitSave != nil && exitSave.event != nil && u.RecordStructured != nil {
			exitEventErr = u.RecordStructured(*exitSave.event)
		}
		if u.SendInput != nil {
			if exited {
				if exitPending != nil {
					u.SendInput.NotifyConfirmation(SendConfirmation{SessionID: initial.SessionID, SendID: exitPending.SendID, ProcessExited: true, ExitedAt: exitedAt})
				}
			} else {
				rows := vt.TextRows()
				cells := make([][]supervisor.Cell, len(rows))
				for i := range rows {
					cells[i] = vt.Cells(i)
				}
				menu, selected := false, ""
				if u.CompletionSelection != nil {
					menu, selected = u.CompletionSelection(rows)
				}
				shot := ScreenSnapshot{Observation: observation, Rows: rows, Cells: cells, State: result.State, Processing: result.State == detect.StateWorking && slices.Contains(result.Basis, supervisor.DetectionScreenPattern), CompletionMenu: menu, SelectedCompletion: selected, ObservedAt: now().UTC(), ChangedAt: lastChange}
				if screens, ok := u.SendInput.Screens.(interface {
					SetSnapshotOwned(supervisor.SessionID, ScreenSnapshot, uint64)
				}); ok {
					screens.SetSnapshotOwned(initial.SessionID, shot, screenOwner)
				} else if screens, ok := u.SendInput.Screens.(interface {
					SetSnapshot(supervisor.SessionID, ScreenSnapshot)
				}); ok {
					screens.SetSnapshot(initial.SessionID, shot)
				}
				current, err = u.Sessions.Load(initial.SessionID)
				if err != nil {
					return err
				}
				if current.SendGate.Pending != nil && !screenUpdatedAt.IsZero() {
					u.SendInput.NotifyConfirmation(SendConfirmation{SessionID: initial.SessionID, SendID: current.SendGate.Pending.SendID, Screen: shot})
				}
			}
		}
		current, err = u.Sessions.Load(initial.SessionID)
		if err != nil {
			return err
		}
		if u.ContextPercent != nil {
			validated := result.ContextPercent
			decision := u.PlanRotation.Evaluate(RotationPlanInput{Session: current, Detected: result, StatuslineObservedAt: u.rotationStatuslineAt(initial.SessionID), ObservedState: result.State, Now: now().UTC()})
			if exited || decision.ContextUnknown || decision.NotApplicable {
				validated = nil
			}
			u.ContextPercent.Set(initial.SessionID, validated)
		}
		if current.State != rotationDecisionState || !current.StateSince.Equal(rotationDecisionSince) {
			rotationDecisionState = current.State
			rotationDecisionSince = current.StateSince
			rotationContextUnknownRecorded = false
			rotationNotApplicableRecorded = false
		}
		if u.Rotation != nil {
			if current.Rotation != nil && (current.State == supervisor.SessionRotating || current.State == supervisor.SessionStopped && current.PreviousState == supervisor.SessionRotating && (current.StopReason == supervisor.StopGlobal || current.StopReason == supervisor.StopSession)) && !u.Rotation.IsRunning(initial.SessionID) {
				if err := u.Rotation.validateCore(); err != nil {
					return err
				}
				lifetime := u.SupervisorContext
				if lifetime == nil {
					lifetime = ctx
				}
				go func() {
					if err := u.Rotation.Run(lifetime, initial.SessionID); err != nil && !errors.Is(err, context.Canceled) {
						slog.Warn("rotation worker ended", "reason", "run_failed")
					}
				}()
			}
			if !exited && !u.Rotation.IsRunning(initial.SessionID) && current.Rotation == nil {
				decision := u.PlanRotation.Evaluate(RotationPlanInput{Session: current, Detected: result, StatuslineObservedAt: u.rotationStatuslineAt(initial.SessionID), ObservedState: result.State, PendingSince: rotationPendingSince, Stopped: stoppedByControl, Now: now().UTC()})
				if decision.ContextUnknown && !rotationContextUnknownRecorded {
					rotationContextUnknownRecorded = true
					u.recordEvent(initial.SessionID, supervisor.EventKindContextUnknown, store.Event{})
				}
				if decision.NotApplicable && !rotationNotApplicableRecorded {
					rotationNotApplicableRecorded = true
					u.recordEvent(initial.SessionID, supervisor.EventKindRotationNotApplicable, store.Event{})
				}
				if decision.Start {
					if err := u.Rotation.validateCore(); err != nil {
						return err
					}
					rotationPendingSince = nil
					rotationIdleWaitNotified = false
					lifetime := u.SupervisorContext
					if lifetime == nil {
						lifetime = ctx
					}
					go func() {
						if err := u.Rotation.Execute(lifetime, initial.SessionID, nil); err != nil && !errors.Is(err, context.Canceled) {
							slog.Warn("rotation worker ended", "reason", "execute_failed")
						}
					}()
				} else if decision.Pending {
					if rotationPendingSince == nil {
						pending := now().UTC()
						rotationPendingSince = &pending
						u.recordEvent(initial.SessionID, supervisor.EventKindRotatePending, store.Event{})
					} else if decision.IdleWaitTimeout && !rotationIdleWaitNotified {
						rotationIdleWaitNotified = true
						u.recordEvent(initial.SessionID, supervisor.EventKindRotateIdleWaitTimeout, store.Event{Reason: store.ReasonIdleWaitTimeout})
					}
				} else if result.State != detect.StateAwaitingPermission {
					rotationPendingSince = nil
					rotationIdleWaitNotified = false
				}
			}
			signalRotationObservation(initial.SessionID)
		}
		if u.PermissionPending != nil {
			if current.State != supervisor.SessionAwaitingPermission {
				permissionWarnedSince = time.Time{}
			}
			if err := u.PermissionPending.Observe(initial.SessionID, current.State, current.StateSince, now().UTC()); err != nil {
				if !permissionWarnedSince.Equal(current.StateSince) {
					permissionWarnedSince = current.StateSince
					slog.Warn("permission pending observation failed", "error", err)
				}
			}
		}
		if !exited && armed && (current.State == supervisor.SessionIdle || current.State == supervisor.SessionAwaitingRecommendation) {
			if !current.StateSince.Equal(commitSince) {
				commitSince = current.StateSince
				commitStarted = false
			}
			settled := now().Sub(current.StateSince) >= commitSettle
			if settled && u.EvaluateCommit != nil && !commitStarted && !commitInFlight {
				commitStarted = true
				commitInFlight = true
				staySince := current.StateSince
				go func() {
					if err := u.EvaluateCommit(ctx, initial.SessionID); err != nil {
						slog.Warn("commit evaluation failed", "error", err)
					}
					if _, reported := gitPhaseReported.Load(staySince); !reported {
						current, err := u.Sessions.Load(initial.SessionID)
						if err == nil && current.StateSince.Equal(staySince) {
							select {
							case gitPhaseDone <- staySince:
							default:
							}
						}
					}
					select {
					case commitEvaluationDone <- struct{}{}:
					case <-ctx.Done():
					}
				}()
			}
		}
		if u.AutoContinue != nil && !exited && current.State == supervisor.SessionAwaitingRecommendation {
			enteredFromWorking := nextState == supervisor.SessionAwaitingRecommendation && fromState == supervisor.SessionWorking
			if !current.StateSince.Equal(recommendationSince) {
				recommendationSince = current.StateSince
				autoAttempted = false
				contextWasKnown = false
			}
			if enteredFromWorking {
				u.AutoContinue.entered(initial.SessionID, current.StateSince)
			}
			statuslineAt := u.rotationStatuslineAt(initial.SessionID)
			decision := u.PlanRotation.Evaluate(RotationPlanInput{Session: current, Detected: result, StatuslineObservedAt: statuslineAt, ObservedState: result.State, Now: now().UTC()})
			if decision.ContextUnknown && !rotationContextUnknownRecorded {
				rotationContextUnknownRecorded = true
				u.recordEvent(initial.SessionID, supervisor.EventKindContextUnknown, store.Event{})
			}
			settled := now().Sub(current.StateSince) >= commitSettle
			contextPercent := result.ContextPercent
			if decision.ContextUnknown {
				contextPercent = nil
			}
			contextKnown := contextPercent != nil
			if settled && (gitPhaseCompleted.Equal(current.StateSince) || contextKnown && !contextWasKnown && autoAttempted || !autoAttempted && u.EvaluateCommit == nil) {
				u.AutoContinue.EvaluateAsync(ctx, AutoContinueInput{
					SessionID: initial.SessionID, Rows: vt.TextRows(), ContextPercent: contextPercent,
					RequireSettle: true, Settled: true, StaySince: current.StateSince,
				}, nil)
				autoAttempted = true
			}
			contextWasKnown = contextKnown
		}
		if u.PendingRemoteControl != nil && current.PendingRemoteControl && !pendingInFlight {
			pendingInFlight = true
			lifetime := u.SupervisorContext
			if lifetime == nil {
				lifetime = ctx
			}
			go func() {
				_ = u.PendingRemoteControl.OnObserved(lifetime, initial.SessionID, result.State)
				pendingDone <- struct{}{}
			}()
		}
		if !exited && nextState != "" && u.InitialRemoteControl != nil && (fromState == supervisor.SessionStarting || nextState == supervisor.SessionIdle) {
			current, err = u.Sessions.Load(initial.SessionID)
			if err != nil {
				return err
			}
			requestCtx := u.SupervisorContext
			if requestCtx == nil {
				requestCtx = context.WithoutCancel(ctx)
			}
			if nextState == supervisor.SessionIdle {
				go func(s supervisor.ManagedSession) {
					if err := u.InitialRemoteControl.OnStateChange(requestCtx, fromState, s); err != nil {
						u.InitialRemoteControl.warnFailure(s)
					}
				}(current)
			} else {
				if err := u.InitialRemoteControl.OnStateChange(requestCtx, fromState, current); err != nil {
					u.InitialRemoteControl.warnFailure(current)
				}
			}
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
			if u.SendInput != nil && u.SendInput.PTYs != nil {
				writer := u.SendInput.PTYs.Get(initial.SessionID)
				if writer != nil {
					u.SendInput.PTYs.Remove(initial.SessionID, writer)
				}
			}
			if u.SendInput != nil {
				u.SendInput.EndSession(initial.SessionID, screenOwner)
			}
			if u.InitialRemoteControl != nil {
				u.InitialRemoteControl.EndSession(initial.SessionID)
			}
			current, err = u.Sessions.Load(initial.SessionID)
			if err != nil {
				return err
			}
			if current.PendingRemoteControl && u.PendingRemoteControl != nil {
				if err := UpdateSession(u.Sessions, initial.SessionID, func(s *supervisor.ManagedSession) error {
					s.PendingRemoteControl = false
					s.PendingRemoteControlSince = nil
					return nil
				}); err != nil {
					return err
				}
				u.recordEvent(initial.SessionID, supervisor.EventKindRemoteControlExpired, store.Event{})
				current.PendingRemoteControl = false
			}
			u.recordExitOutcome(initial.SessionID, current, wasStopped)
			if current.Rotation == nil {
				u.startRecovery(initial.SessionID, current, nextState)
			} else {
				signalRotationObservation(initial.SessionID)
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
				ownedChildren.Remove(initial.SessionID, initial.ChildPID)
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
				if u.ContextPercent != nil {
					u.ContextPercent.Delete(initial.SessionID)
				}
				return nil
			}
		}
		if exited {
			if u.Observed != nil {
				u.Observed.Delete(initial.SessionID)
			}
			if u.ContextPercent != nil {
				u.ContextPercent.Delete(initial.SessionID)
			}
			return nil
		}
	}
}

func (u ObserveSessionUseCase) resetStableRestartCount(id supervisor.SessionID, current supervisor.ManagedSession, now time.Time) error {
	if current.Generation < 2 || current.RestartCount == 0 || current.ChildPID == 0 || current.ChildStartedAt == nil || !supervisor.RestartStable(*current.ChildStartedAt, now) {
		return nil
	}
	return UpdateSession(u.Sessions, id, func(s *supervisor.ManagedSession) error {
		if s.Generation == current.Generation && s.ChildPID == current.ChildPID && s.RestartCount > 0 && s.ChildStartedAt != nil && supervisor.RestartStable(*s.ChildStartedAt, now) {
			s.RestartCount = 0
		}
		return nil
	})
}

func (u ObserveSessionUseCase) recordExitOutcome(id supervisor.SessionID, current supervisor.ManagedSession, wasStopped bool) {
	switch {
	case current.State == supervisor.SessionExited:
		u.recordEvent(id, supervisor.EventKindSessionExited, store.Event{Reason: store.EventReason(current.ExitReason)})
	case current.State == supervisor.SessionStopped && !wasStopped:
		if current.StopReason == supervisor.StopRestartLimitConsecutive || current.StopReason == supervisor.StopRestartLimitDaily {
			u.recordEvent(id, supervisor.EventKindRestartLimitExceeded, store.Event{Reason: store.EventReason(current.StopReason), Code: "SUP_RESTART_LIMIT_EXCEEDED"})
		} else {
			u.recordEvent(id, supervisor.EventKindSessionStopped, store.Event{Reason: store.EventReason(current.StopReason)})
		}
	}
}

type observedPTY struct {
	ManagedPTY
	io.Reader
}

// StartObserved owns ManagedPTY registration and starts one observation loop.
func (u ObserveSessionUseCase) StartObserved(ctx context.Context, session supervisor.ManagedSession, pty ManagedPTY, columns, rows uint16) <-chan error {
	done := make(chan error, 1)
	reader, ok := pty.(io.Reader)
	if !ok {
		_ = pty.Close()
		_, _ = pty.Wait()
		done <- errors.New("PTY reader is required")
		return done
	}
	var registry *PTYRegistry
	if u.SendInput != nil {
		registry = u.SendInput.PTYs
	}
	if registry != nil {
		registry.RegisterManaged(session.SessionID, pty)
		if registry.IsStopping(session.SessionID, pty) || registry.IsShuttingDown() {
			_ = pty.Close()
		}
	}
	go func() {
		if registry != nil {
			defer registry.RemoveManaged(session.SessionID, pty)
			if writer, ok := pty.(PTYWriter); ok {
				defer registry.Remove(session.SessionID, writer)
			}
		}
		done <- u.Run(ctx, session, observedPTY{ManagedPTY: pty, Reader: reader}, supervisor.NewVirtualTerminal(int(columns), int(rows)))
	}()
	return done
}

// ObservedRespawn wires every child generation to the same observation loop.
// Resume and automatic recovery must both use this function.
func (u ObserveSessionUseCase) ObservedRespawn() func(context.Context, supervisor.SessionID, RespawnKind) error {
	if u.Respawn == nil {
		return nil
	}
	respawn := *u.Respawn
	if u.SendInput != nil {
		respawn.PTYs = u.SendInput.PTYs
	}
	respawn.Observe = func(_ context.Context, session supervisor.ManagedSession, pty ManagedPTY) {
		lifetime := u.SupervisorContext
		if lifetime == nil {
			lifetime = context.Background()
		}
		<-u.StartObserved(lifetime, session, pty, respawn.Config.Columns, respawn.Config.Rows)
	}
	return respawn.Execute
}

// ResumeRecovery is the continuation passed to StopControl after resume.
func (u ObserveSessionUseCase) ResumeRecovery(ctx context.Context, session supervisor.ManagedSession) error {
	respawn := u.ObservedRespawn()
	if respawn == nil {
		return errors.New("respawn is required")
	}
	return respawn(ctx, session.SessionID, RespawnResumeRecovery)
}

func (u ObserveSessionUseCase) startRecovery(id supervisor.SessionID, current supervisor.ManagedSession, next supervisor.SupervisorSessionState) {
	if next == supervisor.SessionRecovering && current.State == supervisor.SessionRecovering && u.Recover != nil {
		recovery := *u.Recover
		if recovery.LifetimeContext == nil {
			recovery.LifetimeContext = u.SupervisorContext
		}
		if respawn := u.ObservedRespawn(); respawn != nil {
			recovery.Respawn = respawn
		}
		recovery.Start(id)
	}
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
