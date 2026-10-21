package supervisor

import "time"

// Advance records entry into the next handoff step. Resume uses the same operation
// to restart the step's waiting period without changing the handoff start time.
func (r *RotationRecord) Advance(step SupervisorRotateStep, at time.Time) {
	r.Step = step
	t := at.UTC()
	r.StepStartedAt = &t
	r.FailReason = ""
}

// Fail preserves the failed step and its checkpoint evidence for resume.
func (r *RotationRecord) Fail(step SupervisorRotateStep, reason SupervisorRotateFailReason) {
	r.Step = step
	r.FailReason = reason
}

// RecordCheckpoint retains only usable comparison metadata.
func (r *RotationRecord) RecordCheckpoint(snapshot RotationCheckpointSnapshot) {
	if snapshot.Undeterminable {
		snapshot = RotationCheckpointSnapshot{Undeterminable: true}
	}
	r.CheckpointSnapshot = &snapshot
}
