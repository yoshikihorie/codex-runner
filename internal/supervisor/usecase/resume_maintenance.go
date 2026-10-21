package usecase

import (
	"context"
	"errors"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// SessionResumeMaintenance performs only the parts of FD-sup-05 §5.1 step 4
// that resumeOne and SessionResumeSuppression do not already perform.
type SessionResumeMaintenance struct {
	sessions    SessionRecordStore
	fingerprint func(context.Context, string) (string, error)
}

func NewSessionResumeMaintenance(sessions SessionRecordStore, fingerprint func(context.Context, string) (string, error)) *SessionResumeMaintenance {
	return &SessionResumeMaintenance{sessions: sessions, fingerprint: fingerprint}
}

func (m *SessionResumeMaintenance) DiscardPending(ctx context.Context, id supervisor.SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.sessions == nil {
		return errors.New("session store is required")
	}
	return UpdateSession(m.sessions, id, func(s *supervisor.ManagedSession) error {
		s.AutoContinueDisabledReason = ""
		return nil
	})
}

func (m *SessionResumeMaintenance) RefreshGitConfigFingerprint(ctx context.Context, id supervisor.SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.sessions == nil {
		return errors.New("session store is required")
	}
	return UpdateSession(m.sessions, id, func(s *supervisor.ManagedSession) error {
		fingerprint := ""
		if m.fingerprint != nil {
			if value, err := m.fingerprint(ctx, s.ProjectID); err == nil {
				fingerprint = value
			}
		}
		s.GitConfigFingerprint = fingerprint
		return nil
	})
}

// resumeOne already persists the reset and records restart_counters_reset.
func (m *SessionResumeMaintenance) ResetRestartCounters(_ context.Context, _ supervisor.SessionID) error {
	return nil
}
