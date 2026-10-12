package supervisor

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRotationRecord_SCN0441_UndeterminableSnapshot(t *testing.T) {
	r := RotationRecord{}
	now := time.Now().UTC()
	r.RecordCheckpoint(RotationCheckpointSnapshot{Exists: true, Mtime: &now, HeadHash: "stale", Undeterminable: true})
	data, err := json.Marshal(r.CheckpointSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if r.CheckpointSnapshot.Exists || r.CheckpointSnapshot.Mtime != nil || r.CheckpointSnapshot.HeadHash != "" || string(data) != `{"exists":false,"undeterminable":true}` {
		t.Fatalf("snapshot=%+v json=%s", r.CheckpointSnapshot, data)
	}
}

func TestRotationRecord_SCN0406_AdvanceAndFail(t *testing.T) {
	now := time.Now().UTC()
	r := RotationRecord{Step: RotateSendEnd, StartedAt: now}
	r.Advance(RotateConfirmHandoff, now.Add(time.Second))
	if r.Step != RotateConfirmHandoff || r.StepStartedAt == nil || !r.StepStartedAt.Equal(now.Add(time.Second)) || r.FailReason != "" {
		t.Fatalf("advance: %+v", r)
	}
	r.Fail(RotateConfirmHandoff, RotateFailMtimeNotUpdated)
	if r.Step != RotateConfirmHandoff || r.FailReason != RotateFailMtimeNotUpdated {
		t.Fatalf("fail: %+v", r)
	}
	r.Advance(RotateSendExit, now.Add(2*time.Second))
	if r.FailReason != "" || r.Step != RotateSendExit {
		t.Fatalf("resume: %+v", r)
	}
}
