package store

import (
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestRoundTripLoadRestoreTransitionSave(t *testing.T) {
	root := t.TempDir()
	store, err := NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	id, err := domain.NewTaskID("impl-20260806-120000-a1b2-roundtrip")
	if err != nil {
		t.Fatal(err)
	}
	requested := 1860
	requestedAt := time.Date(2026, time.August, 6, 12, 0, 0, 0, time.UTC)
	snapshot := domain.TaskSnapshot{
		TaskID: id, Subcommand: domain.SubcommandImpl, ResolvedTimeoutSeconds: 1920,
		RequestedTimeoutSeconds: &requested, Model: "gpt-5", RequestedAt: requestedAt,
		Route: domain.ExecutionRouteDaemon, State: domain.StateQueued,
		StateUpdatedAt: requestedAt, SchemaVersion: 1,
	}
	if err := store.Reserve(id); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(id, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	task, err := loaded.Restore()
	if err != nil {
		t.Fatal(err)
	}

	timeout, err := domain.NewTimeout(&requested, 1920)
	if err != nil {
		t.Fatal(err)
	}
	startingAt := requestedAt.Add(time.Minute)
	if _, err := task.Start(timeout, "gpt-5", startingAt); err != nil {
		t.Fatal(err)
	}
	snapshot, err = loaded.WithTask(task, startingAt)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != domain.StateStarting || snapshot.PID != nil || snapshot.ExitCode != nil {
		t.Fatalf("starting snapshot = %#v", snapshot)
	}
	if err := store.Save(id, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	task, err = loaded.Restore()
	if err != nil {
		t.Fatal(err)
	}

	processStartedAt := startingAt.Add(time.Second)
	if _, err := task.RecordProcessInfo(1234, processStartedAt, processStartedAt); err != nil {
		t.Fatal(err)
	}
	snapshot, err = loaded.WithTask(task, processStartedAt)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != domain.StateStarting || snapshot.PID == nil || *snapshot.PID != 1234 || snapshot.ProcessStartedAt == nil {
		t.Fatalf("started snapshot = %#v", snapshot)
	}
	if err := store.Save(id, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	task, err = loaded.Restore()
	if err != nil {
		t.Fatal(err)
	}
	if err := task.ConfirmRunning(processStartedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := task.ObserveEvent("message", processStartedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	runningAt := processStartedAt.Add(3 * time.Second)
	snapshot, err = loaded.WithTask(task, runningAt)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != domain.StateRunning || snapshot.PID == nil || *snapshot.PID != 1234 || snapshot.ProcessStartedAt == nil || !snapshot.ProcessStartedAt.Equal(processStartedAt) || snapshot.LastEventAt == nil {
		t.Fatalf("running snapshot = %#v", snapshot)
	}
	if err := store.Save(id, snapshot); err != nil {
		t.Fatal(err)
	}

	loaded, err = store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	task, err = loaded.Restore()
	if err != nil {
		t.Fatal(err)
	}
	completedAt := runningAt.Add(time.Second)
	if _, err := task.RecordExit(domain.NewExitCode(0), true, false, false, completedAt); err != nil {
		t.Fatal(err)
	}
	snapshot, err = loaded.WithTask(task, completedAt)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.State != domain.StateCompleted || snapshot.ExitCode == nil || snapshot.ExitCode.Raw() != 0 || snapshot.PID == nil || snapshot.LastEventAt == nil || !snapshot.StateUpdatedAt.Equal(completedAt) {
		t.Fatalf("completed snapshot = %#v", snapshot)
	}
	if err := store.Save(id, snapshot); err != nil {
		t.Fatal(err)
	}
	final, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if final.State != domain.StateCompleted || final.ExitCode == nil || final.ExitCode.Raw() != 0 || final.PID == nil || *final.PID != 1234 || final.LastEventAt == nil {
		t.Fatalf("final snapshot = %#v", final)
	}
}
