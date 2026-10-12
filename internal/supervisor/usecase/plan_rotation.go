package usecase

import (
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
)

// Values copied from validation-rules.md: SUPERVISOR_CONTEXT_ROTATE_PERCENT
// and SUPERVISOR_ROTATE_IDLE_WAIT_SECONDS.
const (
	rotationContextPercent = 60
	rotationIdleWait       = 7200 * time.Second
)

type RotationPlanInput struct {
	Session              supervisor.ManagedSession
	Detected             detect.DetectResult
	StatuslineObservedAt *time.Time
	ObservedState        detect.DetectedState
	PendingSince         *time.Time
	Stopped              bool
	Resume               bool
	Now                  time.Time
}

type RotationPlanDecision struct {
	Start           bool
	Pending         bool
	IdleWaitTimeout bool
	ContextUnknown  bool
	NotApplicable   bool
}

// PlanRotationUseCase decides whether the caller may enter stage 2. Event and
// notification emission belong to the caller, which owns per-state deduplication.
type PlanRotationUseCase struct{}

func (PlanRotationUseCase) Evaluate(in RotationPlanInput) RotationPlanDecision {
	var d RotationPlanDecision
	s := in.Session
	if s.CLI != supervisor.CLIClaude {
		d.NotApplicable = true
		return d
	}
	started := s.StateSince
	if s.ChildStartedAt != nil {
		started = *s.ChildStartedAt
	}
	if in.Detected.ContextPercent == nil || in.StatuslineObservedAt == nil || in.StatuslineObservedAt.IsZero() ||
		!in.StatuslineObservedAt.After(started) ||
		(s.SendGate.LastConfirmedAt != nil && in.StatuslineObservedAt.Before(*s.SendGate.LastConfirmedAt)) {
		d.ContextUnknown = true
		return d
	}
	if *in.Detected.ContextPercent <= rotationContextPercent || in.Stopped {
		return d
	}
	if s.State != supervisor.SessionIdle && s.State != supervisor.SessionAwaitingRecommendation && s.State != supervisor.SessionWorking {
		return d
	}
	if s.Rotation != nil && !in.Resume {
		return d
	}
	if s.State == supervisor.SessionWorking {
		if in.ObservedState == detect.StateWorking {
			d.Pending = true
			if in.PendingSince != nil && in.Now.Sub(*in.PendingSince) > rotationIdleWait {
				d.IdleWaitTimeout = true
			}
		}
		return d
	}
	if in.ObservedState != detect.StateIdle && in.ObservedState != detect.StateAwaitingRecommendation {
		if in.ObservedState == detect.StateWorking {
			d.Pending = true
			if in.PendingSince != nil && in.Now.Sub(*in.PendingSince) > rotationIdleWait {
				d.IdleWaitTimeout = true
			}
		}
		return d
	}
	if s.GitPhase != supervisor.GitPhaseNone && s.GitPhase != "" {
		return d
	}
	d.Start = true
	return d
}
