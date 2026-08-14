package metrics

import (
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestRecordTaskMetricsInput_FieldAssignments(t *testing.T) {
	taskID := domain.TaskID{}
	finalState := domain.StateCompleted
	estimated := false
	occurredAt := time.Date(2026, time.August, 13, 0, 0, 0, 0, time.UTC)
	stalledTotalMs := 0

	var input RecordTaskMetricsInput
	input.TaskID = taskID
	input.FinalState = finalState
	input.Estimated = estimated
	input.OccurredAt = occurredAt
	input.StalledTotalMs = stalledTotalMs
}

func TestRecordTaskMetricsOutput_FieldAssignment(t *testing.T) {
	recorded := false

	var output RecordTaskMetricsOutput
	output.Recorded = recorded
}

func TestRecordTaskMetricsTypes_PositionalLiterals(t *testing.T) {
	taskID := domain.TaskID{}
	finalState := domain.StateCompleted
	estimated := false
	occurredAt := time.Date(2026, time.August, 13, 0, 0, 0, 0, time.UTC)
	stalledTotalMs := 0
	recorded := false

	_ = RecordTaskMetricsInput{
		taskID,
		finalState,
		estimated,
		occurredAt,
		stalledTotalMs,
	}
	_ = RecordTaskMetricsOutput{
		recorded,
	}
}
