package recovery

import (
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestRecoverViaResumeInput_ZeroValueAndFieldAssignment(t *testing.T) {
	taskID, err := domain.NewTaskID("impl-20260810-120000-abcd-resume-types")
	if err != nil {
		t.Fatalf("NewTaskID: %v", err)
	}
	sessionRef, err := domain.NewSessionRef("00112233-4455-6677-8899-aabbccddeeff", time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC), false)
	if err != nil {
		t.Fatalf("NewSessionRef: %v", err)
	}

	var input RecoverViaResumeInput
	input.TaskID = taskID
	input.SessionRef = &sessionRef
	input.Origin = domain.RecoveryOriginTimeout
	input.OccurredAt = time.Date(2026, time.August, 10, 12, 1, 0, 0, time.UTC)
	input.SessionRef = nil
	if input.SessionRef != nil {
		t.Fatal("SessionRef must allow nil")
	}
}

func TestRecoverViaResumeOutput_ZeroValueAndFieldAssignment(t *testing.T) {
	var output RecoverViaResumeOutput
	output.Succeeded = true
	output.ExitCode = domain.NewExitCode(0)
	output.PartialOutputSaved = false
	output.FinalState = domain.StateRecovered
}
