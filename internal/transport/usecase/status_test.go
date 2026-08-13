package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

type statusProviderStub struct {
	snapshot      domain.TaskSnapshot
	err           error
	position      int
	found         bool
	positionErr   error
	snapshotCalls int
	positionCalls int
}

func (p *statusProviderStub) Snapshot(domain.TaskID) (domain.TaskSnapshot, error) {
	p.snapshotCalls++
	return p.snapshot, p.err
}
func (p *statusProviderStub) QueuePosition(domain.TaskID) (int, bool, error) {
	p.positionCalls++
	return p.position, p.found, p.positionErr
}

func statusSnapshot(t *testing.T, state domain.TaskState) domain.TaskSnapshot {
	t.Helper()
	id, err := domain.NewTaskID("review-20260813-120000-a1b2-status-view")
	if err != nil {
		t.Fatal(err)
	}
	requestedAt := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	pid := 7
	return domain.TaskSnapshot{TaskID: id, Subcommand: domain.SubcommandReview, PID: &pid, ProcessStartedAt: &requestedAt, ResolvedTimeoutSeconds: 1800, Model: "gpt-5", RequestedAt: requestedAt, Route: domain.ExecutionRouteDaemon, State: state, StateUpdatedAt: requestedAt, SchemaVersion: 1}
}

func TestGetTaskStatusExecuteMapsQueuedSnapshotAndPosition(t *testing.T) {
	snapshot := statusSnapshot(t, domain.StateQueued)
	provider := &statusProviderStub{snapshot: snapshot, position: 2, found: true}
	uc := NewGetTaskStatusUseCase(provider, domain.ClockFunc(func() time.Time { return snapshot.RequestedAt }))
	out, err := uc.Execute(context.Background(), GetTaskStatusInput{TaskID: snapshot.TaskID})
	if err != nil || out.NotFound || out.View.TaskID != snapshot.TaskID || out.View.QueuePosition == nil || *out.View.QueuePosition != 2 || out.View.ReasoningEffort != nil || out.View.LivenessConfirmed != true || out.View.StateBasis != daemonAuthoritativeStateBasis {
		t.Fatalf("out=%#v err=%v", out, err)
	}
	if provider.snapshotCalls != 1 || provider.positionCalls != 1 {
		t.Fatalf("snapshot calls=%d position calls=%d", provider.snapshotCalls, provider.positionCalls)
	}
}

func TestGetTaskStatusExecuteMapsNotFoundAndReadFailure(t *testing.T) {
	snapshot := statusSnapshot(t, domain.StateRunning)
	clock := domain.ClockFunc(func() time.Time { return snapshot.RequestedAt })
	notFound := NewGetTaskStatusUseCase(&statusProviderStub{err: domain.ErrTaskNotFound}, clock)
	out, err := notFound.Execute(context.Background(), GetTaskStatusInput{TaskID: snapshot.TaskID})
	if err != nil || !out.NotFound {
		t.Fatalf("out=%#v err=%v", out, err)
	}
	readErr := errors.New("read failed")
	if _, err := NewGetTaskStatusUseCase(&statusProviderStub{err: readErr}, clock).Execute(context.Background(), GetTaskStatusInput{TaskID: snapshot.TaskID}); !errors.Is(err, readErr) {
		t.Fatalf("err=%v", err)
	}
}

func TestGetTaskStatusHandleMapsErrorsAndExcludesInternalFields(t *testing.T) {
	snapshot := statusSnapshot(t, domain.StateRunning)
	provider := &statusProviderStub{snapshot: snapshot}
	uc := NewGetTaskStatusUseCase(provider, domain.ClockFunc(func() time.Time { return snapshot.RequestedAt }))
	invalid := uc.Handle(transportRequest("invalid"))
	if invalid.OK || invalid.Error == nil || invalid.Error.Code != "TASK_ID_INVALID_FORMAT" || invalid.Error.Detail["task_id"] != "invalid" || provider.snapshotCalls != 0 {
		t.Fatalf("invalid=%#v", invalid)
	}
	response := uc.Handle(transportRequest(snapshot.TaskID.String()))
	if !response.OK || response.Error != nil {
		t.Fatalf("response=%#v", response)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(response.Result, &object); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"pid", "session_ref", "requested_timeout_seconds", "recovery_origin", "schema_version"} {
		if _, found := object[name]; found {
			t.Fatalf("internal field %q leaked", name)
		}
	}
}

func TestGetTaskStatusExecuteStalledGapUsesLastEventOrProcessStart(t *testing.T) {
	snapshot := statusSnapshot(t, domain.StateStalled)
	lastEventAt := snapshot.RequestedAt.Add(time.Minute)
	snapshot.LastEventAt = &lastEventAt
	now := snapshot.RequestedAt.Add(5 * time.Minute)
	uc := NewGetTaskStatusUseCase(&statusProviderStub{snapshot: snapshot}, domain.ClockFunc(func() time.Time { return now }))
	out, err := uc.Execute(context.Background(), GetTaskStatusInput{TaskID: snapshot.TaskID})
	if err != nil || out.View.GapSeconds == nil || *out.View.GapSeconds != 240 {
		t.Fatalf("out=%#v err=%v", out, err)
	}
	snapshot.LastEventAt = nil
	out, err = NewGetTaskStatusUseCase(&statusProviderStub{snapshot: snapshot}, domain.ClockFunc(func() time.Time { return now })).Execute(context.Background(), GetTaskStatusInput{TaskID: snapshot.TaskID})
	if err != nil || out.View.GapSeconds == nil || *out.View.GapSeconds != 300 {
		t.Fatalf("out=%#v err=%v", out, err)
	}
}

func TestGetTaskStatusExecuteStalledGapDoesNotBecomeNegative(t *testing.T) {
	snapshot := statusSnapshot(t, domain.StateStalled)
	future := snapshot.RequestedAt.Add(time.Minute)
	snapshot.LastEventAt = &future
	out, err := NewGetTaskStatusUseCase(&statusProviderStub{snapshot: snapshot}, domain.ClockFunc(func() time.Time { return snapshot.RequestedAt })).Execute(context.Background(), GetTaskStatusInput{TaskID: snapshot.TaskID})
	if err != nil || out.View.GapSeconds == nil || *out.View.GapSeconds != 0 {
		t.Fatalf("out=%#v err=%v", out, err)
	}
}

func TestGetTaskStatusExecuteQueuedPositionNotFoundIsNull(t *testing.T) {
	snapshot := statusSnapshot(t, domain.StateQueued)
	provider := &statusProviderStub{snapshot: snapshot, found: false}
	out, err := NewGetTaskStatusUseCase(provider, domain.ClockFunc(func() time.Time { return snapshot.RequestedAt })).Execute(context.Background(), GetTaskStatusInput{TaskID: snapshot.TaskID})
	if err != nil || out.View.QueuePosition != nil || provider.positionCalls != 1 {
		t.Fatalf("out=%#v err=%v calls=%d", out, err, provider.positionCalls)
	}
}

func transportRequest(taskID string) transport.Request {
	return transport.Request{RequestID: "request", TaskID: taskID}
}
