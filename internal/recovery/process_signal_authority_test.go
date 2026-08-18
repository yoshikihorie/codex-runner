package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type processSignalAuthorityStoreFake struct {
	snapshot  domain.TaskSnapshot
	loadErr   error
	loadCalls int
}

func (f *processSignalAuthorityStoreFake) Load(domain.TaskID) (domain.TaskSnapshot, error) {
	f.loadCalls++
	return f.snapshot, f.loadErr
}
func (*processSignalAuthorityStoreFake) Save(domain.TaskID, domain.TaskSnapshot) error { return nil }
func (*processSignalAuthorityStoreFake) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	return nil, nil
}
func (*processSignalAuthorityStoreFake) Reserve(domain.TaskID) error            { return nil }
func (*processSignalAuthorityStoreFake) Release(domain.TaskID) error            { return nil }
func (*processSignalAuthorityStoreFake) IsReserved(domain.TaskID) (bool, error) { return true, nil }

type processSignalAuthorityOwnershipFake struct {
	executed   bool
	err        error
	calls      int
	taskID     domain.TaskID
	generation domain.LifecycleGeneration
}

func (f *processSignalAuthorityOwnershipFake) WithCurrent(taskID domain.TaskID, generation domain.LifecycleGeneration, action func() error) (bool, error) {
	f.calls++
	f.taskID, f.generation = taskID, generation
	if !f.executed {
		return false, f.err
	}
	return true, action()
}

func processSignalAuthorityID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260818-120000-a1b2-authority")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func processSignalAuthorityDifferentID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260818-120001-a1b2-mismatch")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestProcessSignalAuthorityValidatorValidate(t *testing.T) {
	id := processSignalAuthorityID(t)
	started := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	generation := domain.LifecycleGeneration(7)
	authority := ProcessSignalAuthority{TaskID: id, PID: 4321, ProcessStartedAt: started, ExpectedState: domain.StateTimeout, LifecycleGeneration: &generation}
	snapshot := domain.TaskSnapshot{TaskID: id, PID: &authority.PID, ProcessStartedAt: &started, State: domain.StateTimeout}

	for _, tc := range []struct {
		name               string
		authority          ProcessSignalAuthority
		snapshot           domain.TaskSnapshot
		loadErr            error
		ownershipExecuted  bool
		ownershipErr       error
		actionErr          error
		wantExecuted       bool
		wantLoadCalls      int
		wantOwnershipCalls int
		wantActionCalls    int
	}{
		{name: "valid normal generation", authority: authority, snapshot: snapshot, ownershipExecuted: true, wantExecuted: true, wantLoadCalls: 1, wantOwnershipCalls: 1, wantActionCalls: 1},
		{name: "zero authority", authority: ProcessSignalAuthority{}, snapshot: snapshot, wantLoadCalls: 0},
		{name: "task ID mismatch", authority: authority, snapshot: domain.TaskSnapshot{TaskID: processSignalAuthorityDifferentID(t), PID: &authority.PID, ProcessStartedAt: &started, State: domain.StateTimeout}, wantLoadCalls: 1},
		{name: "PID mismatch", authority: authority, snapshot: domain.TaskSnapshot{TaskID: id, PID: func() *int { pid := 4322; return &pid }(), ProcessStartedAt: &started, State: domain.StateTimeout}, wantLoadCalls: 1},
		{name: "process start mismatch", authority: authority, snapshot: domain.TaskSnapshot{TaskID: id, PID: &authority.PID, ProcessStartedAt: func() *time.Time { at := started.Add(time.Second); return &at }(), State: domain.StateTimeout}, wantLoadCalls: 1},
		{name: "state mismatch", authority: authority, snapshot: domain.TaskSnapshot{TaskID: id, PID: &authority.PID, ProcessStartedAt: &started, State: domain.StateCancelling}, wantLoadCalls: 1},
		{name: "snapshot load error", authority: authority, snapshot: snapshot, loadErr: errors.New("load"), wantLoadCalls: 1},
		{name: "owner absent", authority: authority, snapshot: snapshot, wantLoadCalls: 1, wantOwnershipCalls: 1},
		{name: "stale generation", authority: authority, snapshot: snapshot, ownershipErr: errors.New("stale"), wantLoadCalls: 1, wantOwnershipCalls: 1},
		{name: "action error preserves identity", authority: authority, snapshot: snapshot, ownershipExecuted: true, actionErr: errors.New("signal"), wantExecuted: true, wantLoadCalls: 1, wantOwnershipCalls: 1, wantActionCalls: 1},
		{name: "valid adoption skips generation verification", authority: ProcessSignalAuthority{TaskID: id, PID: 4321, ProcessStartedAt: started, ExpectedState: domain.StateTimeout}, snapshot: snapshot, wantExecuted: true, wantLoadCalls: 1, wantActionCalls: 1},
		{name: "adoption state mismatch fails closed", authority: ProcessSignalAuthority{TaskID: id, PID: 4321, ProcessStartedAt: started, ExpectedState: domain.StateTimeout}, snapshot: domain.TaskSnapshot{TaskID: id, PID: &authority.PID, ProcessStartedAt: &started, State: domain.StateCancelling}, wantLoadCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storeFake := &processSignalAuthorityStoreFake{snapshot: tc.snapshot, loadErr: tc.loadErr}
			ownership := &processSignalAuthorityOwnershipFake{executed: tc.ownershipExecuted, err: tc.ownershipErr}
			validator := NewProcessSignalAuthorityValidator(storeFake, store.NewTaskMutex(), ownership)
			actionCalls := 0
			executed, err := validator.Validate(context.Background(), tc.authority, func(pid int) error {
				actionCalls++
				if pid != tc.authority.PID {
					t.Fatalf("action pid=%d want=%d", pid, tc.authority.PID)
				}
				return tc.actionErr
			})
			if executed != tc.wantExecuted || storeFake.loadCalls != tc.wantLoadCalls || ownership.calls != tc.wantOwnershipCalls || actionCalls != tc.wantActionCalls {
				t.Fatalf("executed=%v load=%d ownership=%d action=%d err=%v", executed, storeFake.loadCalls, ownership.calls, actionCalls, err)
			}
			if tc.actionErr != nil && !errors.Is(err, tc.actionErr) {
				t.Fatalf("error=%v, want action error identity", err)
			}
			if tc.loadErr != nil && !errors.Is(err, tc.loadErr) {
				t.Fatalf("error=%v, want load error identity", err)
			}
		})
	}
}

func TestProcessSignalAuthorityValidatorValidateRunsNormalActionInsideWithCurrent(t *testing.T) {
	id := processSignalAuthorityID(t)
	started := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	generation := domain.LifecycleGeneration(1)
	pid := 4321
	storeFake := &processSignalAuthorityStoreFake{snapshot: domain.TaskSnapshot{TaskID: id, PID: &pid, ProcessStartedAt: &started, State: domain.StateCancelling}}
	ownership := &processSignalAuthorityOwnershipFake{executed: true}
	validator := NewProcessSignalAuthorityValidator(storeFake, store.NewTaskMutex(), ownership)
	_, _ = validator.Validate(context.Background(), ProcessSignalAuthority{TaskID: id, PID: pid, ProcessStartedAt: started, ExpectedState: domain.StateCancelling, LifecycleGeneration: &generation}, func(int) error { return nil })
	if ownership.calls != 1 || ownership.taskID != id || ownership.generation != generation {
		t.Fatalf("WithCurrent calls=%d task=%s generation=%d", ownership.calls, ownership.taskID, ownership.generation)
	}
}
