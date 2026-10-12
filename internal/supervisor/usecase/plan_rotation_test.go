package usecase

import (
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
)

func rotationInput(percent *int) RotationPlanInput {
	now := time.Now().UTC()
	started := now.Add(-time.Second)
	return RotationPlanInput{Session: supervisor.ManagedSession{SessionID: "rotation-plan", CLI: supervisor.CLIClaude, Generation: 1, ChildStartedAt: &started, State: supervisor.SessionIdle}, Detected: detect.DetectResult{ContextPercent: percent}, StatuslineObservedAt: &now, ObservedState: detect.StateIdle, Now: now}
}

func TestPlanRotation_SCN0402_Threshold(t *testing.T) {
	for _, tc := range []struct {
		percent int
		start   bool
	}{{59, false}, {60, false}, {61, true}, {100, true}} {
		in := rotationInput(&tc.percent)
		if got := (PlanRotationUseCase{}).Evaluate(in); got.Start != tc.start {
			t.Fatalf("%d: %+v", tc.percent, got)
		}
	}
}

func TestPlanRotation_SCN0403_WaitForIdle(t *testing.T) {
	p := 62
	in := rotationInput(&p)
	in.ObservedState = detect.StateWorking
	got := (PlanRotationUseCase{}).Evaluate(in)
	if got.Start || !got.Pending {
		t.Fatalf("working: %+v", got)
	}
	pendingSince := in.Now
	in.PendingSince = &pendingSince
	in.Now = in.Now.Add(rotationIdleWait + time.Second)
	got = (PlanRotationUseCase{}).Evaluate(in)
	if got.Start || !got.IdleWaitTimeout {
		t.Fatalf("timeout: %+v", got)
	}
	in.ObservedState = detect.StateIdle
	if !(PlanRotationUseCase{}).Evaluate(in).Start {
		t.Fatal("idle did not start")
	}
}

func TestPlanRotation_SCN0404_StaleContext(t *testing.T) {
	in := rotationInput(nil)
	if !(PlanRotationUseCase{}).Evaluate(in).ContextUnknown {
		t.Fatal("missing context")
	}
	p := 80
	in.Detected.ContextPercent = &p
	old := in.StatuslineObservedAt.Add(-time.Second)
	in.StatuslineObservedAt = &old
	if !(PlanRotationUseCase{}).Evaluate(in).ContextUnknown {
		t.Fatal("old generation")
	}
	in.StatuslineObservedAt = in.Session.ChildStartedAt
	in.Session.SendGate.LastConfirmedAt = &in.Now
	later := in.Now.Add(time.Second)
	in.Session.SendGate.LastConfirmedAt = &later
	if !(PlanRotationUseCase{}).Evaluate(in).ContextUnknown {
		t.Fatal("old send")
	}
}

func TestPlanRotation_SCN0427_NewGenerationNeedsNewContextObservation(t *testing.T) {
	percent := 61
	in := rotationInput(&percent)
	if !(PlanRotationUseCase{}).Evaluate(in).Start {
		t.Fatal("first generation did not start")
	}
	in.Session.Generation++
	newStarted := in.Now.Add(time.Second)
	in.Session.ChildStartedAt = &newStarted
	if got := (PlanRotationUseCase{}).Evaluate(in); got.Start || !got.ContextUnknown {
		t.Fatalf("old context reused in new generation: %+v", got)
	}
	newStatus := newStarted.Add(time.Nanosecond)
	in.StatuslineObservedAt = &newStatus
	if !(PlanRotationUseCase{}).Evaluate(in).Start {
		t.Fatal("new generation context was rejected")
	}
}

func TestPlanRotation_SCN0424_RecommendationCLIAndGit(t *testing.T) {
	p := 61
	in := rotationInput(&p)
	in.ObservedState = detect.StateAwaitingRecommendation
	if !(PlanRotationUseCase{}).Evaluate(in).Start {
		t.Fatal("recommendation did not start")
	}
	in.Session.CLI = supervisor.CLICodex
	if !(PlanRotationUseCase{}).Evaluate(in).NotApplicable {
		t.Fatal("codex applicable")
	}
	in.Session.CLI = supervisor.CLIClaude
	in.Session.GitPhase = supervisor.GitPhaseEvaluating
	if (PlanRotationUseCase{}).Evaluate(in).Start {
		t.Fatal("git phase start")
	}
	in.Session.GitPhase = supervisor.GitPhaseNone
	if !(PlanRotationUseCase{}).Evaluate(in).Start {
		t.Fatal("completed git phase was blocked")
	}
}

func TestPlanRotation_SCN0402_OnlyIdleSessionStatesStart(t *testing.T) {
	p := 61
	for _, state := range []supervisor.SupervisorSessionState{
		supervisor.SessionWorking, supervisor.SessionAwaitingPermission,
		supervisor.SessionRotating, supervisor.SessionStopped,
		supervisor.SessionRecovering, supervisor.SessionStarting,
		supervisor.SessionFailed, supervisor.SessionExited,
	} {
		in := rotationInput(&p)
		in.Session.State = state
		if got := (PlanRotationUseCase{}).Evaluate(in); got.Start {
			t.Errorf("state %s started: %+v", state, got)
		}
	}
}

func TestPlanRotation_SCN0426_StopPermissionAndExistingRotation(t *testing.T) {
	p := 61
	in := rotationInput(&p)
	in.Stopped = true
	if (PlanRotationUseCase{}).Evaluate(in).Start {
		t.Fatal("STOP allowed start")
	}
	in.Stopped = false
	in.ObservedState = detect.StateAwaitingPermission
	if (PlanRotationUseCase{}).Evaluate(in).Start {
		t.Fatal("permission allowed start")
	}
	in.ObservedState = detect.StateIdle
	in.Session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateConfirmHandoff}
	if (PlanRotationUseCase{}).Evaluate(in).Start {
		t.Fatal("existing rotation allowed start")
	}
	in.Resume = true
	if !(PlanRotationUseCase{}).Evaluate(in).Start {
		t.Fatal("resume rejected")
	}
}
