package usecase

import (
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

type recoveryReconcileStore struct{ regressionSessionStore }

func (s *recoveryReconcileStore) List() ([]supervisor.ManagedSession, []supervisor.SessionID, []supervisor.SessionID, error) {
	v, _ := s.Load(s.session.SessionID)
	return []supervisor.ManagedSession{v}, nil, nil, nil
}

func TestSCNsup0525StartingWithDeadRecordedPIDStops(t *testing.T) {
	now := time.Now().UTC()
	s := &recoveryReconcileStore{regressionSessionStore{session: supervisor.ManagedSession{SessionID: "alpha-1", State: supervisor.SessionStarting, StateSince: now, Generation: 1, ChildPID: 123, ChildStartedAt: &now}}}
	rows, err := (ReconcileOnStartupUseCase{Store: s, IsAlive: func(int) bool { return false }, Now: func() time.Time { return now }}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].State != supervisor.SessionStopped || rows[0].StopReason != supervisor.StopSupervisorRestarted || rows[0].ChildPID != 0 {
		t.Fatalf("reconciled: %+v", rows[0])
	}
}

func TestSCNsup0525StoppedOrphanKeepsOriginalReason(t *testing.T) {
	now := time.Now().UTC()
	s := &recoveryReconcileStore{regressionSessionStore{session: supervisor.ManagedSession{SessionID: "alpha-1", State: supervisor.SessionStopped, StateSince: now, Generation: 1, StopReason: supervisor.StopSendNotConfirmed, ChildPID: 123, ChildStartedAt: &now}}}
	rows, err := (ReconcileOnStartupUseCase{Store: s, IsAlive: func(int) bool { return true }, ProcessStartedAt: func(int) (time.Time, bool) { return now, true }}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].StopReason != supervisor.StopSendNotConfirmed || rows[0].ChildPID != 123 {
		t.Fatalf("reconciled: %+v", rows[0])
	}
}

func TestReconcileOrphanUsesSupervisorRestartedReason(t *testing.T) {
	now := time.Now().UTC()
	s := &recoveryReconcileStore{regressionSessionStore{session: supervisor.ManagedSession{SessionID: "alpha-1", State: supervisor.SessionIdle, StateSince: now, Generation: 1, ChildPID: 123, ChildStartedAt: &now}}}
	rows, err := (ReconcileOnStartupUseCase{Store: s, IsAlive: func(int) bool { return true }, ProcessStartedAt: func(int) (time.Time, bool) { return now, true }}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].State != supervisor.SessionStopped || rows[0].StopReason != supervisor.StopSupervisorRestarted || rows[0].ChildPID != 123 {
		t.Fatalf("reconciled: %+v", rows[0])
	}
}
