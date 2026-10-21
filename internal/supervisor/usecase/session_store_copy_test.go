package usecase

import (
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

func TestRegressionSessionStoreCopiesNestedRecords(t *testing.T) {
	now := time.Now().UTC()
	wantTime := now
	id := supervisor.SessionID("copy-boundary-1")
	input := supervisor.ManagedSession{
		SessionID:      id,
		ChildStartedAt: &now,
		SendGate:       supervisor.SendGateRecord{LastConfirmedAt: &now, Pending: &supervisor.PendingSend{SendID: "pending"}},
		LastSend:       &supervisor.LastSendRecord{SendID: "last"},
		RestartTimes:   []time.Time{now},
		Rotation: &supervisor.RotationRecord{Step: supervisor.RotateSendEnd, StepStartedAt: &now, EndIssuedAt: &now,
			CheckpointSnapshot: &supervisor.RotationCheckpointSnapshot{Mtime: &now, HeadHash: "head"}, RespawnWaitUntil: &now},
		PendingRemoteControlSince: &now,
	}
	s := &regressionSessionStore{}
	if err := s.Save(input); err != nil {
		t.Fatal(err)
	}
	input.Rotation.Step = supervisor.RotateRestart
	input.Rotation.CheckpointSnapshot.HeadHash = "changed"
	input.SendGate.Pending.SendID = "changed"
	input.RestartTimes[0] = now.Add(time.Hour)
	*input.ChildStartedAt = now.Add(time.Hour)
	stored, err := s.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Rotation.Step != supervisor.RotateSendEnd || stored.Rotation.CheckpointSnapshot.HeadHash != "head" || stored.SendGate.Pending.SendID != "pending" || !stored.RestartTimes[0].Equal(wantTime) || !stored.ChildStartedAt.Equal(wantTime) {
		t.Fatalf("Save retained caller references: %+v", stored)
	}
	stored.Rotation.Step = supervisor.RotateRestart
	stored.Rotation.CheckpointSnapshot.HeadHash = "changed"
	stored.SendGate.Pending.SendID = "changed"
	stored.LastSend.SendID = "changed"
	stored.RestartTimes[0] = now.Add(time.Hour)
	*stored.PendingRemoteControlSince = now.Add(time.Hour)
	again, err := s.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if again.Rotation.Step != supervisor.RotateSendEnd || again.Rotation.CheckpointSnapshot.HeadHash != "head" || again.SendGate.Pending.SendID != "pending" || again.LastSend.SendID != "last" || !again.RestartTimes[0].Equal(wantTime) || !again.PendingRemoteControlSince.Equal(wantTime) {
		t.Fatalf("Load exposed stored references: %+v", again)
	}
}
