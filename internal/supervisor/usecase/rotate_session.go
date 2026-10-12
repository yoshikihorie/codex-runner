package usecase

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

// Canonical values: specifications/10-shared/validation-rules.md §3.
const (
	rotationStepTimeout    = 900 * time.Second // SUPERVISOR_ROTATE_STEP_TIMEOUT_SECONDS
	rotationStartupTimeout = 120 * time.Second // SUPERVISOR_STARTUP_TIMEOUT_SECONDS
)

type rotationInputSender interface {
	Execute(context.Context, SendInput) (SendResult, error)
}
type rotationRespawner interface {
	Execute(context.Context, supervisor.SessionID, RespawnKind) error
}

// RotateSessionUseCase runs one handoff per session. A caller starts Run in a
// goroutine and wakes it after each observation; it must never run in an observer.
type RotateSessionUseCase struct {
	Sessions             SessionRecordStore
	Send                 rotationInputSender
	Respawn              rotationRespawner
	Stop                 *StopState
	IsStopped            func(supervisor.SessionID) bool
	Observed             func(supervisor.SessionID) supervisor.SupervisorSessionState
	ChildAlive           func(supervisor.SessionID) bool
	Inspect              func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error)
	CheckpointCWD        func(supervisor.ManagedSession) string
	ResumeReady          func(supervisor.SessionID, time.Time) bool
	PendingRemoteControl *PendingRemoteControlUseCase
	AutoContinue         *AutoContinueUseCase
	RecordStructured     func(store.Event) error
	Notify               func(supervisor.SupervisorEventKind, string)
	Now                  func() time.Time
	Wake                 func(supervisor.SessionID) <-chan struct{}
	mu                   sync.Mutex
	running              map[supervisor.SessionID]bool
	stepLocks            map[supervisor.SessionID]*sync.Mutex
	resumeSent           map[supervisor.SessionID]time.Time
}

// Execute is the worker entrypoint. Call it in one goroutine per session;
// resumeFrom must match the stage retained in the session record.
func (u *RotateSessionUseCase) Execute(ctx context.Context, id supervisor.SessionID, resumeFrom *supervisor.SupervisorRotateStep) error {
	if !u.beginRunning(id) {
		return errors.New("rotation already running")
	}
	defer u.endRunning(id)
	if resumeFrom == nil {
		if err := u.Start(ctx, id); err != nil {
			return err
		}
	} else {
		if u.Sessions == nil {
			return errors.New("session store is required")
		}
		s, err := u.Sessions.Load(id)
		if err != nil {
			return err
		}
		if s.Rotation == nil || s.Rotation.Step != *resumeFrom {
			return errors.New("rotation resume step changed")
		}
		if err := u.Resume(ctx, s); err != nil {
			return err
		}
	}
	return u.run(ctx, id)
}

func (u *RotateSessionUseCase) IsRunning(id supervisor.SessionID) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.running[id]
}

func (u *RotateSessionUseCase) beginRunning(id supervisor.SessionID) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.running == nil {
		u.running = make(map[supervisor.SessionID]bool)
	}
	if u.running[id] {
		return false
	}
	u.running[id] = true
	return true
}

func (u *RotateSessionUseCase) endRunning(id supervisor.SessionID) {
	u.mu.Lock()
	delete(u.running, id)
	u.mu.Unlock()
}

// EndSession releases transient per-session worker state after terminal exit.
func (u *RotateSessionUseCase) EndSession(id supervisor.SessionID) {
	u.mu.Lock()
	delete(u.running, id)
	delete(u.stepLocks, id)
	delete(u.resumeSent, id)
	u.mu.Unlock()
}

func (u *RotateSessionUseCase) stepLock(id supervisor.SessionID) *sync.Mutex {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.stepLocks == nil {
		u.stepLocks = make(map[supervisor.SessionID]*sync.Mutex)
	}
	if u.stepLocks[id] == nil {
		u.stepLocks[id] = &sync.Mutex{}
	}
	return u.stepLocks[id]
}

func (u *RotateSessionUseCase) resumeReady(id supervisor.SessionID) (bool, error) {
	s, err := u.Sessions.Load(id)
	if err != nil {
		return false, err
	}
	if s.Rotation == nil || s.State != supervisor.SessionRotating || s.PreviousState != supervisor.SessionRotating {
		return true, nil
	}
	u.mu.Lock()
	sentAt, sent := u.resumeSent[id]
	u.mu.Unlock()
	if sent && sentAt.Equal(s.StateSince) {
		return true, nil
	}
	if u.ResumeReady == nil {
		return false, errors.New("resume readiness detector is required")
	}
	return u.ResumeReady(id, s.StateSince), nil
}

func (u *RotateSessionUseCase) sentAfterResume(id supervisor.SessionID) error {
	s, err := u.Sessions.Load(id)
	if err != nil {
		return err
	}
	if s.Rotation == nil || s.State != supervisor.SessionRotating || s.PreviousState != supervisor.SessionRotating {
		return nil
	}
	u.mu.Lock()
	if u.resumeSent == nil {
		u.resumeSent = make(map[supervisor.SessionID]time.Time)
	}
	u.resumeSent[id] = s.StateSince
	u.mu.Unlock()
	return nil
}

func (u *RotateSessionUseCase) now() time.Time {
	if u.Now != nil {
		return u.Now().UTC()
	}
	return time.Now().UTC()
}

func (u *RotateSessionUseCase) validateCore() error {
	if u.Sessions == nil || u.Send == nil || u.Observed == nil || u.ChildAlive == nil ||
		u.Inspect == nil && u.CheckpointCWD == nil || u.Stop == nil && u.IsStopped == nil ||
		u.Wake == nil || u.PendingRemoteControl == nil || u.ResumeReady == nil {
		return errors.New("rotation dependencies are required")
	}
	return nil
}
func (u *RotateSessionUseCase) stopped(id supervisor.SessionID) bool {
	return u.Stop != nil && u.Stop.IsStopped(id) || u.IsStopped != nil && u.IsStopped(id)
}
func (u *RotateSessionUseCase) observed(id supervisor.SessionID) supervisor.SupervisorSessionState {
	if u.Observed != nil {
		return u.Observed(id)
	}
	return ""
}
func rotationIdle(state supervisor.SupervisorSessionState) bool {
	return state == supervisor.SessionIdle || state == supervisor.SessionAwaitingRecommendation
}
func (u *RotateSessionUseCase) alive(s supervisor.ManagedSession) bool {
	if u.ChildAlive != nil {
		return u.ChildAlive(s.SessionID)
	}
	return s.ChildPID > 0
}
func (u *RotateSessionUseCase) inspect(s supervisor.ManagedSession) supervisor.RotationCheckpointSnapshot {
	if u.Inspect != nil {
		shot, err := u.Inspect(s)
		if err != nil {
			return supervisor.RotationCheckpointSnapshot{Undeterminable: true}
		}
		return shot
	}
	if u.CheckpointCWD == nil {
		return supervisor.RotationCheckpointSnapshot{Undeterminable: true}
	}
	shot, err := (CheckpointInspector{CWD: u.CheckpointCWD(s)}).Inspect()
	if err != nil {
		return supervisor.RotationCheckpointSnapshot{Undeterminable: true}
	}
	return shot
}
func (u *RotateSessionUseCase) record(id supervisor.SessionID, kind supervisor.SupervisorEventKind, step supervisor.SupervisorRotateStep, reason supervisor.SupervisorRotateFailReason, code string) error {
	if u.RecordStructured == nil {
		return nil
	}
	return u.RecordStructured(store.Event{Kind: kind, SessionID: id.String(), At: u.now(), Step: string(step), Reason: store.EventReason(reason), Code: code})
}

// Start enters stage 2. The caller has already applied PlanRotationUseCase.
func (u *RotateSessionUseCase) Start(ctx context.Context, id supervisor.SessionID) error {
	if err := u.validateCore(); err != nil {
		return err
	}
	if u.stopped(id) {
		return ErrStopActive
	}
	now := u.now()
	wasPending := false
	err := UpdateSession(u.Sessions, id, func(s *supervisor.ManagedSession) error {
		if s.Rotation != nil || s.State != supervisor.SessionIdle && s.State != supervisor.SessionAwaitingRecommendation {
			return errors.New("rotation start state changed")
		}
		if s.GitPhase != supervisor.GitPhaseNone && s.GitPhase != "" {
			return errors.New("rotation start blocked by git phase")
		}
		if err := s.Apply(supervisor.EventContextRotate, supervisor.TransitionContext{}, now); err != nil {
			return err
		}
		s.PreviousState = ""
		s.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendEnd, StartedAt: now, StepStartedAt: &now}
		wasPending = s.PendingRemoteControl
		s.PendingRemoteControl = false
		s.PendingRemoteControlSince = nil
		return nil
	})
	if err != nil {
		return err
	}
	if wasPending {
		if err := u.record(id, supervisor.EventKindRemoteControlExpired, "", "", ""); err != nil {
			return err
		}
	}
	return u.Step(ctx, id)
}

// Run serializes the active worker for this session. Wake must be signalled by
// the observer after persisting screen, checkpoint, or process facts.
func (u *RotateSessionUseCase) Run(ctx context.Context, id supervisor.SessionID) error {
	if err := u.validateCore(); err != nil {
		return err
	}
	if !u.beginRunning(id) {
		return errors.New("rotation already running")
	}
	defer u.endRunning(id)
	return u.run(ctx, id)
}

func (u *RotateSessionUseCase) run(ctx context.Context, id supervisor.SessionID) error {
	for {
		if err := u.Step(ctx, id); err != nil {
			return err
		}
		s, err := u.Sessions.Load(id)
		if err != nil {
			return err
		}
		if s.Rotation == nil || s.State != supervisor.SessionRotating {
			return nil
		}
		if err := u.wait(ctx, s); err != nil {
			return err
		}
	}
}

func (u *RotateSessionUseCase) wait(ctx context.Context, s supervisor.ManagedSession) error {
	var deadline time.Time
	r := s.Rotation
	if r.StepStartedAt != nil {
		switch r.Step {
		case supervisor.RotateSendEnd:
			if r.EndIssuedAt != nil && !r.ResumeEnd {
				deadline = r.EndIssuedAt.Add(supervisor.SupervisorSendConfirmTimeout)
			}
		case supervisor.RotateConfirmHandoff, supervisor.RotateSendStart:
			deadline = r.StepStartedAt.Add(rotationStepTimeout)
		case supervisor.RotateSendParallelInstruction:
			if s.LastSend == nil || s.LastSend.Kind != supervisor.SendKindParallelInstruction || !s.LastSend.Confirmed {
				deadline = r.StepStartedAt.Add(rotationStepTimeout)
			}
		case supervisor.RotateRestart:
			if s.ChildPID > 0 {
				deadline = r.StepStartedAt.Add(rotationStartupTimeout)
			}
		}
	}
	if r.RespawnWaitUntil != nil {
		deadline = *r.RespawnWaitUntil
	}
	if r.ResumeEnd {
		deadline = time.Time{}
	}
	if !deadline.IsZero() && !deadline.After(u.now()) {
		return nil
	}
	var timer *time.Timer
	var tick <-chan time.Time
	if !deadline.IsZero() {
		timer = time.NewTimer(deadline.Sub(u.now()))
		tick = timer.C
		defer timer.Stop()
	}
	var wake <-chan struct{}
	if u.Wake != nil {
		wake = u.Wake(s.SessionID)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wake:
		return nil
	case <-tick:
		return nil
	}
}

// Step advances only while evidence for the current stage is present. It is
// also useful for injected-clock callers that trigger one observation at a time.
func (u *RotateSessionUseCase) Step(ctx context.Context, id supervisor.SessionID) error {
	if err := u.validateCore(); err != nil {
		return err
	}
	lock := u.stepLock(id)
	lock.Lock()
	defer lock.Unlock()
	if u.Sessions == nil {
		return errors.New("session store is required")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		s, err := u.Sessions.Load(id)
		if err != nil {
			return err
		}
		if s.Rotation == nil {
			return nil
		}
		if s.Rotation.ResumeSpawn && s.State == supervisor.SessionRotating {
			if u.Respawn == nil {
				return errors.New("respawn use case is required")
			}
			if err := u.Respawn.Execute(ctx, id, RespawnHandoffResume); err != nil {
				return u.fail(id, s.Rotation.Step, supervisor.RotateFailSpawnFailed)
			}
			if err := UpdateSession(u.Sessions, id, func(v *supervisor.ManagedSession) error {
				if v.Rotation != nil {
					v.Rotation.ResumeSpawn = false
				}
				return nil
			}); err != nil {
				return err
			}
			continue
		}
		if s.State == supervisor.SessionStopped && s.PreviousState == supervisor.SessionRotating && (s.StopReason == supervisor.StopGlobal || s.StopReason == supervisor.StopSession) {
			return u.confirmWhileStopped(s)
		}
		if s.State != supervisor.SessionRotating {
			return nil
		}
		step := s.Rotation.Step
		if u.stopped(id) && step != supervisor.RotateConfirmHandoff && !(step == supervisor.RotateSendEnd && s.Rotation.EndIssuedAt != nil) {
			return u.stop(id)
		}
		var advanced bool
		switch step {
		case supervisor.RotateSendEnd:
			advanced, err = u.sendEnd(ctx, s)
		case supervisor.RotateConfirmHandoff:
			advanced, err = u.confirmHandoff(s)
		case supervisor.RotateSendExit:
			advanced, err = u.sendExit(ctx, s)
		case supervisor.RotateRestart:
			advanced, err = u.restart(ctx, s)
		case supervisor.RotateSendStart:
			advanced, err = u.sendStart(ctx, s)
		case supervisor.RotateSendParallelInstruction:
			return u.sendParallel(ctx, s)
		default:
			return fmt.Errorf("invalid rotation step: %q", step)
		}
		if err != nil || !advanced {
			return err
		}
		if u.stopped(id) {
			return u.stop(id)
		}
	}
}

func (u *RotateSessionUseCase) confirmWhileStopped(s supervisor.ManagedSession) error {
	switch s.Rotation.Step {
	case supervisor.RotateSendEnd:
		if s.Rotation.EndIssuedAt == nil {
			return nil
		}
		shot := u.inspect(s)
		if u.observed(s.SessionID) == supervisor.SessionWorking || shot.Exists && !shot.Undeterminable && shot.Mtime != nil && shot.Mtime.After(*s.Rotation.EndIssuedAt) {
			return u.advance(s.SessionID, supervisor.RotateSendEnd, supervisor.RotateConfirmHandoff)
		}
	case supervisor.RotateConfirmHandoff:
		shot := u.inspect(s)
		r := s.Rotation
		if r.EndIssuedAt != nil && r.CheckpointSnapshot != nil && CheckpointUpdated(*r.CheckpointSnapshot, shot, *r.EndIssuedAt) && rotationIdle(u.observed(s.SessionID)) && u.alive(s) {
			return u.advance(s.SessionID, supervisor.RotateConfirmHandoff, supervisor.RotateSendExit)
		}
	case supervisor.RotateSendExit:
		if s.ChildPID == 0 && s.LastSend != nil && s.LastSend.Kind == supervisor.SendKindHandoffExit && s.LastSend.Confirmed {
			return u.advance(s.SessionID, supervisor.RotateSendExit, supervisor.RotateRestart)
		}
	case supervisor.RotateRestart:
		if s.ChildPID > 0 && u.alive(s) && u.observed(s.SessionID) == supervisor.SessionIdle {
			return u.advance(s.SessionID, supervisor.RotateRestart, supervisor.RotateSendStart)
		}
	case supervisor.RotateSendStart:
		if s.LastSend != nil && s.LastSend.Kind == supervisor.SendKindSessionStart && s.LastSend.Confirmed && rotationIdle(u.observed(s.SessionID)) {
			return u.advance(s.SessionID, supervisor.RotateSendStart, supervisor.RotateSendParallelInstruction)
		}
	case supervisor.RotateSendParallelInstruction:
		if s.LastSend != nil && s.LastSend.Kind == supervisor.SendKindParallelInstruction && s.LastSend.Confirmed && !s.Rotation.Stage7ConfirmedWhileStopped {
			recorded := false
			if err := UpdateSession(u.Sessions, s.SessionID, func(v *supervisor.ManagedSession) error {
				if v.Rotation != nil && !v.Rotation.Stage7ConfirmedWhileStopped {
					v.Rotation.Stage7ConfirmedWhileStopped = true
					recorded = true
				}
				return nil
			}); err != nil {
				return err
			}
			if recorded {
				return u.record(s.SessionID, supervisor.EventKindRotateStepDone, supervisor.RotateSendParallelInstruction, "", "")
			}
		}
	}
	return nil
}

func (u *RotateSessionUseCase) sendEnd(ctx context.Context, s supervisor.ManagedSession) (bool, error) {
	r := s.Rotation
	resume := r.ResumeEnd
	if r.EndIssuedAt != nil && !resume {
		shot := u.inspect(s)
		if u.observed(s.SessionID) == supervisor.SessionWorking || shot.Exists && !shot.Undeterminable && shot.Mtime != nil && shot.Mtime.After(*r.EndIssuedAt) {
			return true, u.advance(s.SessionID, supervisor.RotateSendEnd, supervisor.RotateConfirmHandoff)
		}
		if u.stopped(s.SessionID) {
			return false, u.stop(s.SessionID)
		}
		if !u.now().Before(r.EndIssuedAt.Add(supervisor.SupervisorSendConfirmTimeout)) {
			return false, u.fail(s.SessionID, supervisor.RotateSendEnd, supervisor.RotateFailSendNotConfirmed)
		}
		return false, nil
	}
	if !u.alive(s) {
		return false, u.fail(s.SessionID, supervisor.RotateSendEnd, supervisor.RotateFailChildAbsent)
	}
	if resume {
		ready, err := u.resumeReady(s.SessionID)
		if err != nil || !ready {
			return false, err
		}
	}
	if ready, err := u.resumeReady(s.SessionID); err != nil || !ready {
		return false, err
	}
	if r.CheckpointSnapshot == nil {
		shot := u.inspect(s)
		if err := UpdateSession(u.Sessions, s.SessionID, func(v *supervisor.ManagedSession) error { v.Rotation.RecordCheckpoint(shot); return nil }); err != nil {
			return false, err
		}
	}
	previousIssued := r.EndIssuedAt
	result, err := u.Send.Execute(ctx, SendInput{SessionID: s.SessionID, Kind: supervisor.SendKindHandoffEnd, Origin: supervisor.SendOriginHandoff})
	issued := u.now()
	if result.SendID != "" {
		if err2 := UpdateSession(u.Sessions, s.SessionID, func(v *supervisor.ManagedSession) error {
			if v.Rotation.EndIssuedAt == nil || resume && previousIssued != nil && v.Rotation.EndIssuedAt.Equal(*previousIssued) {
				v.Rotation.EndIssuedAt = &issued
			}
			return nil
		}); err2 != nil {
			return false, err2
		}
	}
	if err != nil || !result.Confirmed {
		if !u.alive(s) {
			return false, u.deadDuringSend(s, supervisor.RotateSendEnd, result.SendID)
		}
		if u.stopped(s.SessionID) {
			return false, u.stop(s.SessionID)
		}
		return false, u.fail(s.SessionID, supervisor.RotateSendEnd, supervisor.RotateFailSendNotConfirmed)
	}
	if err := u.sentAfterResume(s.SessionID); err != nil {
		return false, err
	}
	if err := UpdateSession(u.Sessions, s.SessionID, func(v *supervisor.ManagedSession) error {
		v.Rotation.ResumeEnd = false
		return nil
	}); err != nil {
		return false, err
	}
	current, err := u.Sessions.Load(s.SessionID)
	if err != nil {
		return false, err
	}
	return u.sendEnd(ctx, current)
}

func (u *RotateSessionUseCase) confirmHandoff(s supervisor.ManagedSession) (bool, error) {
	if !u.alive(s) {
		return false, u.fail(s.SessionID, supervisor.RotateConfirmHandoff, supervisor.RotateFailChildAbsent)
	}
	r := s.Rotation
	shot := u.inspect(s)
	mtime := r.EndIssuedAt != nil && r.CheckpointSnapshot != nil && !r.CheckpointSnapshot.Undeterminable && !shot.Undeterminable && shot.Exists && shot.Mtime != nil && shot.Mtime.After(*r.EndIssuedAt)
	head := mtime && (!r.CheckpointSnapshot.Exists || r.CheckpointSnapshot.HeadHash != shot.HeadHash)
	idle := rotationIdle(u.observed(s.SessionID))
	if mtime && head && idle {
		return true, u.advance(s.SessionID, supervisor.RotateConfirmHandoff, supervisor.RotateSendExit)
	}
	if u.stopped(s.SessionID) {
		return false, u.stop(s.SessionID)
	}
	if r.StepStartedAt == nil || u.now().Before(r.StepStartedAt.Add(rotationStepTimeout)) {
		return false, nil
	}
	reason := supervisor.RotateFailMtimeNotUpdated
	if mtime && !head {
		reason = supervisor.RotateFailHeadGenerationUnchanged
	}
	if mtime && head && !idle {
		reason = supervisor.RotateFailNotIdleAfterHandoff
	}
	return false, u.fail(s.SessionID, supervisor.RotateConfirmHandoff, reason)
}

func (u *RotateSessionUseCase) sendExit(ctx context.Context, s supervisor.ManagedSession) (bool, error) {
	if !u.alive(s) {
		return false, u.fail(s.SessionID, supervisor.RotateSendExit, supervisor.RotateFailChildAbsent)
	}
	if u.Send == nil {
		return false, errors.New("send input use case is required")
	}
	if ready, err := u.resumeReady(s.SessionID); err != nil || !ready {
		return false, err
	}
	result, err := u.Send.Execute(ctx, SendInput{SessionID: s.SessionID, Kind: supervisor.SendKindHandoffExit, Origin: supervisor.SendOriginHandoff, ConfirmTimeout: supervisor.SupervisorExitGrace})
	if err != nil || !result.Confirmed {
		if !u.alive(s) {
			return false, u.deadDuringSend(s, supervisor.RotateSendExit, result.SendID)
		}
		if u.stopped(s.SessionID) {
			return false, u.stop(s.SessionID)
		}
		return false, u.fail(s.SessionID, supervisor.RotateSendExit, supervisor.RotateFailExitTimeout)
	}
	if err := u.sentAfterResume(s.SessionID); err != nil {
		return false, err
	}
	if err := UpdateSession(u.Sessions, s.SessionID, func(v *supervisor.ManagedSession) error { v.ChildPID = 0; v.ChildStartedAt = nil; return nil }); err != nil {
		return false, err
	}
	return true, u.advance(s.SessionID, supervisor.RotateSendExit, supervisor.RotateRestart)
}

func (u *RotateSessionUseCase) restart(ctx context.Context, s supervisor.ManagedSession) (bool, error) {
	r := s.Rotation
	if r.RespawnWaitUntil != nil && u.now().Before(*r.RespawnWaitUntil) {
		return false, nil
	}
	if s.ChildPID == 0 && r.Stage5Started {
		return false, u.childAbsent(s)
	}
	if s.ChildPID == 0 {
		if u.Respawn == nil {
			return false, errors.New("respawn use case is required")
		}
		if err := u.Respawn.Execute(ctx, s.SessionID, RespawnHandoffStage5); err != nil {
			return false, u.fail(s.SessionID, supervisor.RotateRestart, supervisor.RotateFailSpawnFailed)
		}
		if err := UpdateSession(u.Sessions, s.SessionID, func(v *supervisor.ManagedSession) error {
			if v.State != supervisor.SessionRotating || v.Rotation == nil {
				return nil
			}
			v.Rotation.Advance(supervisor.RotateRestart, u.now())
			v.Rotation.Stage5Started = true
			v.Rotation.RespawnWaitUntil = nil
			return nil
		}); err != nil {
			return false, err
		}
		return false, nil
	}
	if !u.alive(s) {
		return false, u.childAbsent(s)
	}
	if u.observed(s.SessionID) == supervisor.SessionIdle {
		return true, u.advance(s.SessionID, supervisor.RotateRestart, supervisor.RotateSendStart)
	}
	if r.StepStartedAt != nil && !u.now().Before(r.StepStartedAt.Add(rotationStartupTimeout)) {
		return false, u.fail(s.SessionID, supervisor.RotateRestart, supervisor.RotateFailStartupTimeout)
	}
	return false, nil
}

func (u *RotateSessionUseCase) sendStart(ctx context.Context, s supervisor.ManagedSession) (bool, error) {
	if !u.alive(s) {
		return false, u.childAbsent(s)
	}
	if s.LastSend != nil && s.LastSend.Kind == supervisor.SendKindSessionStart && s.LastSend.Confirmed {
		if rotationIdle(u.observed(s.SessionID)) {
			return true, u.advance(s.SessionID, supervisor.RotateSendStart, supervisor.RotateSendParallelInstruction)
		}
		if s.Rotation.StepStartedAt != nil && !u.now().Before(s.Rotation.StepStartedAt.Add(rotationStepTimeout)) {
			return false, u.fail(s.SessionID, supervisor.RotateSendStart, supervisor.RotateFailStartNotSettled)
		}
		return false, nil
	}
	if u.observed(s.SessionID) != supervisor.SessionIdle {
		if s.Rotation.StepStartedAt != nil && !u.now().Before(s.Rotation.StepStartedAt.Add(rotationStepTimeout)) {
			return false, u.fail(s.SessionID, supervisor.RotateSendStart, supervisor.RotateFailStartNotSettled)
		}
		return false, nil
	}
	if ready, err := u.resumeReady(s.SessionID); err != nil || !ready {
		return false, err
	}
	result, err := u.Send.Execute(ctx, SendInput{SessionID: s.SessionID, Kind: supervisor.SendKindSessionStart, Origin: supervisor.SendOriginHandoff})
	if err != nil || !result.Confirmed {
		if !u.alive(s) {
			return false, u.deadDuringSend(s, supervisor.RotateSendStart, result.SendID)
		}
		if u.stopped(s.SessionID) {
			return false, u.stop(s.SessionID)
		}
		return false, u.fail(s.SessionID, supervisor.RotateSendStart, supervisor.RotateFailSendNotConfirmed)
	}
	if err := u.sentAfterResume(s.SessionID); err != nil {
		return false, err
	}
	if rotationIdle(u.observed(s.SessionID)) {
		return true, u.advance(s.SessionID, supervisor.RotateSendStart, supervisor.RotateSendParallelInstruction)
	}
	return false, nil
}

func (u *RotateSessionUseCase) sendParallel(ctx context.Context, s supervisor.ManagedSession) error {
	if !u.alive(s) {
		return u.childAbsent(s)
	}
	alreadyConfirmed := s.LastSend != nil && s.LastSend.Kind == supervisor.SendKindParallelInstruction && s.LastSend.Confirmed
	if !alreadyConfirmed {
		if u.observed(s.SessionID) != supervisor.SessionIdle {
			if s.Rotation.StepStartedAt != nil && !u.now().Before(s.Rotation.StepStartedAt.Add(rotationStepTimeout)) {
				return u.fail(s.SessionID, supervisor.RotateSendParallelInstruction, supervisor.RotateFailSendNotConfirmed)
			}
			return nil
		}
		if ready, err := u.resumeReady(s.SessionID); err != nil || !ready {
			return err
		}
		result, err := u.Send.Execute(ctx, SendInput{SessionID: s.SessionID, Kind: supervisor.SendKindParallelInstruction, Origin: supervisor.SendOriginHandoff})
		if err != nil || !result.Confirmed {
			if !u.alive(s) {
				return u.deadDuringSend(s, supervisor.RotateSendParallelInstruction, result.SendID)
			}
			if u.stopped(s.SessionID) {
				return u.stop(s.SessionID)
			}
			return u.fail(s.SessionID, supervisor.RotateSendParallelInstruction, supervisor.RotateFailSendNotConfirmed)
		}
		if err := u.sentAfterResume(s.SessionID); err != nil {
			return err
		}
		if u.stopped(s.SessionID) {
			recorded := false
			if err := UpdateSession(u.Sessions, s.SessionID, func(v *supervisor.ManagedSession) error {
				if v.Rotation != nil && !v.Rotation.Stage7ConfirmedWhileStopped {
					v.Rotation.Stage7ConfirmedWhileStopped = true
					recorded = true
				}
				return nil
			}); err != nil {
				return err
			}
			if recorded {
				if err := u.record(s.SessionID, supervisor.EventKindRotateStepDone, supervisor.RotateSendParallelInstruction, "", ""); err != nil {
					return err
				}
			}
			return u.stop(s.SessionID)
		}
	}
	// Completion, record removal, and /rc registration form a single session write.
	observed := u.observed(s.SessionID)
	if observed != supervisor.SessionIdle && observed != supervisor.SessionWorking && observed != supervisor.SessionAwaitingPermission && observed != supervisor.SessionAwaitingRecommendation {
		return nil
	}
	if u.PendingRemoteControl != nil {
		lock := u.PendingRemoteControl.sessionLock(s.SessionID)
		lock.Lock()
		defer lock.Unlock()
	}
	var pendingGeneration int
	if err := UpdateSession(u.Sessions, s.SessionID, func(v *supervisor.ManagedSession) error {
		if err := v.Apply(supervisor.EventRotationDone, supervisor.TransitionContext{ObservedState: observed}, u.now()); err != nil {
			return err
		}
		v.Rotation = nil
		v.PendingRemoteControl = true
		now := u.now()
		v.PendingRemoteControlSince = &now
		pendingGeneration = v.Generation
		return nil
	}); err != nil {
		return err
	}
	if u.PendingRemoteControl != nil {
		u.PendingRemoteControl.registerGeneration(s.SessionID, pendingGeneration)
	}
	if u.AutoContinue != nil {
		u.AutoContinue.Reset(s.SessionID)
	}
	if !alreadyConfirmed {
		if err := u.record(s.SessionID, supervisor.EventKindRotateStepDone, supervisor.RotateSendParallelInstruction, "", ""); err != nil {
			return err
		}
	}
	if err := u.record(s.SessionID, supervisor.EventKindRotateCompleted, "", "", ""); err != nil {
		return err
	}
	return u.record(s.SessionID, supervisor.EventKindRemoteControlPending, "", "", "")
}

func (u *RotateSessionUseCase) advance(id supervisor.SessionID, done, next supervisor.SupervisorRotateStep) error {
	if err := UpdateSession(u.Sessions, id, func(s *supervisor.ManagedSession) error { s.Rotation.Advance(next, u.now()); return nil }); err != nil {
		return err
	}
	return u.record(id, supervisor.EventKindRotateStepDone, done, "", "")
}
func (u *RotateSessionUseCase) fail(id supervisor.SessionID, step supervisor.SupervisorRotateStep, reason supervisor.SupervisorRotateFailReason) error {
	if err := UpdateSession(u.Sessions, id, func(s *supervisor.ManagedSession) error {
		s.Rotation.ResumeSpawn = false
		s.Rotation.ResumeEnd = false
		s.Rotation.Fail(step, reason)
		return s.Apply(supervisor.EventRotationFailed, supervisor.TransitionContext{}, u.now())
	}); err != nil {
		return err
	}
	code := "SUP_ROTATE_STEP_FAILED"
	if reason == supervisor.RotateFailRestartLimit {
		code = "SUP_RESTART_LIMIT_EXCEEDED"
	}
	if err := u.record(id, supervisor.EventKindRotateFailed, step, reason, code); err != nil {
		return err
	}
	if u.Notify != nil {
		// Wording and placeholders: specifications/10-shared/message-catalog.md error.supervisor.rotateStepFailed.
		u.Notify(supervisor.EventKindRotateFailed, fmt.Sprintf("セッション「%s」の引き継ぎが段階「%s」で確認できず、停止しました（理由: %s）。状況を確認してから再開してください。", id, step, reason))
	}
	return nil
}
func (u *RotateSessionUseCase) stop(id supervisor.SessionID) error {
	return UpdateSession(u.Sessions, id, func(s *supervisor.ManagedSession) error {
		if s.State != supervisor.SessionRotating {
			return nil
		}
		reason := supervisor.StopSession
		if u.Stop != nil && u.Stop.IsGlobalStopped() {
			reason = supervisor.StopGlobal
		}
		return s.Apply(supervisor.EventStop, supervisor.TransitionContext{StopReason: reason}, u.now())
	})
}

func (u *RotateSessionUseCase) childAbsent(s supervisor.ManagedSession) error {
	if s.Rotation.Step == supervisor.RotateSendEnd || s.Rotation.Step == supervisor.RotateConfirmHandoff {
		return u.fail(s.SessionID, s.Rotation.Step, supervisor.RotateFailChildAbsent)
	}
	if supervisor.RestartLimitReason(s.RestartCount, s.RestartTimes, u.now()) != "" {
		return u.fail(s.SessionID, s.Rotation.Step, supervisor.RotateFailRestartLimit)
	}
	return UpdateSession(u.Sessions, s.SessionID, func(v *supervisor.ManagedSession) error {
		wait := u.now().Add(supervisor.RestartBackoff(v.RestartCount))
		v.Rotation.Advance(supervisor.RotateRestart, u.now())
		v.Rotation.Stage5Started = false
		v.Rotation.RespawnWaitUntil = &wait
		v.ChildPID = 0
		v.ChildStartedAt = nil
		return nil
	})
}

func (u *RotateSessionUseCase) deadDuringSend(s supervisor.ManagedSession, step supervisor.SupervisorRotateStep, sendID string) error {
	if err := UpdateSession(u.Sessions, s.SessionID, func(v *supervisor.ManagedSession) error {
		v.SendGate.Pending = nil
		v.ChildPID = 0
		v.ChildStartedAt = nil
		return nil
	}); err != nil {
		return err
	}
	if step == supervisor.RotateSendExit {
		if gate, ok := u.Send.(interface {
			CRWritten(supervisor.SessionID, string) bool
		}); ok && sendID != "" && gate.CRWritten(s.SessionID, sendID) {
			return u.advance(s.SessionID, step, supervisor.RotateRestart)
		}
		return u.fail(s.SessionID, step, supervisor.RotateFailChildAbsent)
	}
	current, err := u.Sessions.Load(s.SessionID)
	if err != nil {
		return err
	}
	return u.childAbsent(current)
}

// PrepareResume updates the in-memory record before StopControl publishes Rs.
func (u *RotateSessionUseCase) PrepareResume(s *supervisor.ManagedSession) error {
	if u.ResumeReady == nil {
		return errors.New("rotation resume dependencies are required")
	}
	if err := u.validateCore(); err != nil {
		return err
	}
	if s.Rotation == nil || s.State != supervisor.SessionRotating {
		return errors.New("rotation is not resumed")
	}
	if s.Rotation.Step == supervisor.RotateSendExit && u.alive(*s) && (s.Rotation.FailReason == supervisor.RotateFailExitTimeout || s.SendGate.Pending != nil && s.SendGate.Pending.Kind == supervisor.SendKindHandoffExit || s.LastSend != nil && s.LastSend.Kind == supervisor.SendKindHandoffExit) {
		return errors.New("exit_process_alive")
	}
	step := s.Rotation.Step
	resumedAt := s.StateSince
	if resumedAt.IsZero() {
		resumedAt = u.now()
	}
	s.Rotation.ResumeEnd = false
	s.Rotation.ResumeSpawn = false
	u.mu.Lock()
	delete(u.resumeSent, s.SessionID)
	u.mu.Unlock()
	if !u.alive(*s) && (step == supervisor.RotateSendEnd || step == supervisor.RotateConfirmHandoff) {
		if s.ClaudeSessionID == "" && s.PreviousClaudeSessionID == "" {
			return errors.New("claude_session_id_missing")
		}
		if u.Respawn == nil {
			return errors.New("respawn use case is required")
		}
		s.Rotation.ResumeSpawn = true
	}
	if step == supervisor.RotateSendEnd && s.Rotation.EndIssuedAt != nil {
		if s.Rotation.CheckpointSnapshot != nil && CheckpointUpdated(*s.Rotation.CheckpointSnapshot, u.inspect(*s), *s.Rotation.EndIssuedAt) {
			s.Rotation.Advance(supervisor.RotateConfirmHandoff, resumedAt)
		} else {
			s.Rotation.ResumeEnd = true
		}
	}
	if step == supervisor.RotateSendExit && !u.alive(*s) {
		if s.LastSend != nil && s.LastSend.Kind == supervisor.SendKindHandoffExit && s.LastSend.Confirmed {
			s.Rotation.Advance(supervisor.RotateRestart, resumedAt)
		} else {
			if s.ClaudeSessionID == "" && s.PreviousClaudeSessionID == "" {
				return errors.New("claude_session_id_missing")
			}
			if u.Respawn == nil {
				return errors.New("respawn use case is required")
			}
			s.Rotation.ResumeSpawn = true
		}
	}
	s.Rotation.Advance(s.Rotation.Step, resumedAt)
	if s.Rotation.RespawnWaitUntil != nil {
		wait := resumedAt.Add(supervisor.RestartBackoff(s.RestartCount))
		s.Rotation.RespawnWaitUntil = &wait
	}
	s.PreviousState = supervisor.SessionRotating
	return nil
}

// Resume is retained for callers that prepare an already resumed record.
func (u *RotateSessionUseCase) Resume(_ context.Context, s supervisor.ManagedSession) error {
	if u.stopped(s.SessionID) {
		return ErrStopActive
	}
	step := s.Rotation.Step
	if err := UpdateSession(u.Sessions, s.SessionID, func(v *supervisor.ManagedSession) error {
		v.StateSince = u.now()
		return u.PrepareResume(v)
	}); err != nil {
		return err
	}
	current, err := u.Sessions.Load(s.SessionID)
	if err != nil {
		return err
	}
	if current.Rotation != nil && current.Rotation.Step != step {
		if err := u.record(s.SessionID, supervisor.EventKindRotateStepDone, step, "", ""); err != nil {
			return err
		}
	}
	return u.record(s.SessionID, supervisor.EventKindRotateResumed, step, "", "")
}
