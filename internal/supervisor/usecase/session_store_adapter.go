package usecase

import (
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

// SessionStoreAdapter connects the durable store's corrupt-ID result to usecases.
type SessionStoreAdapter struct{ Store *store.SessionStore }

func (a SessionStoreAdapter) SetQuarantineHandler(report func(supervisor.SessionID)) {
	a.Store.SetQuarantineHandler(report)
}

func (a SessionStoreAdapter) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	return a.Store.Load(id)
}
func (a SessionStoreAdapter) Save(session supervisor.ManagedSession) error {
	return a.Store.Save(session)
}
func (a SessionStoreAdapter) Create(session supervisor.ManagedSession) error {
	return a.Store.Create(session)
}
func (a SessionStoreAdapter) UpdateSession(id supervisor.SessionID, update func(*supervisor.ManagedSession) error) error {
	return a.Store.UpdateSession(id, update)
}
func (a SessionStoreAdapter) SetGitPhase(id supervisor.SessionID, phase supervisor.GitPhase) {
	a.Store.SetGitPhase(id, phase)
}
func (a SessionStoreAdapter) SetGitPhaseIfEligible(id supervisor.SessionID, phase supervisor.GitPhase) (bool, error) {
	return a.Store.SetGitPhaseIfEligible(id, phase)
}
func (a SessionStoreAdapter) Delete(id supervisor.SessionID) error { return a.Store.Delete(id) }
func (a SessionStoreAdapter) List() ([]supervisor.ManagedSession, error) {
	sessions, _, err := a.ListWithCorrupt()
	return sessions, err
}
func (a SessionStoreAdapter) ListWithCorrupt() ([]supervisor.ManagedSession, []supervisor.SessionID, error) {
	sessions, newIDs, oldIDs, err := a.Store.List()
	if err != nil {
		return nil, nil, err
	}
	now := time.Now().UTC()
	for _, id := range append(newIDs, oldIDs...) {
		found := false
		for _, session := range sessions {
			if session.SessionID == id {
				found = true
				break
			}
		}
		if !found {
			sessions = append(sessions, supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StateSince: now, StopScope: supervisor.StopScopeNone, StopReason: supervisor.StopStateFileCorrupt, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}})
		}
	}
	return sessions, newIDs, nil
}

type ReconcileSessionStoreAdapter struct{ Store *store.SessionStore }

func (a ReconcileSessionStoreAdapter) SetQuarantineHandler(report func(supervisor.SessionID)) {
	a.Store.SetQuarantineHandler(report)
}

func (a ReconcileSessionStoreAdapter) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	return a.Store.Load(id)
}
func (a ReconcileSessionStoreAdapter) Save(session supervisor.ManagedSession) error {
	return a.Store.Save(session)
}
func (a ReconcileSessionStoreAdapter) List() ([]supervisor.ManagedSession, []supervisor.SessionID, []supervisor.SessionID, error) {
	return a.Store.List()
}
