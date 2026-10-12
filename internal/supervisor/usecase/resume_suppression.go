package usecase

import (
	"context"
	"fmt"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// SessionResumeSuppression clears retry suppression after resume has been accepted.
type SessionResumeSuppression struct {
	sessions SessionRecordStore
	push     *PushToAllowedRemoteUseCase
}

func NewSessionResumeSuppression(sessions SessionRecordStore, push *PushToAllowedRemoteUseCase) *SessionResumeSuppression {
	return &SessionResumeSuppression{sessions: sessions, push: push}
}

func (r *SessionResumeSuppression) Clear(ctx context.Context, id supervisor.SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.sessions == nil {
		return fmt.Errorf("session store is required")
	}
	if err := UpdateSession(r.sessions, id, func(s *supervisor.ManagedSession) error {
		s.LastPushFailedHead = ""
		s.LastFailureFingerprint = ""
		s.LastFailureReason = ""
		// "failed" is the last_verified_result value in FD-sup-03 §16.
		if s.LastVerifiedResult == "failed" {
			s.LastVerifiedFingerprint = ""
			s.LastVerifiedResult = ""
		}
		return nil
	}); err != nil {
		return err
	}
	if r.push != nil {
		r.push.clearSuppressed(id)
	}
	return nil
}

var _ ResumeSuppression = (*SessionResumeSuppression)(nil)
