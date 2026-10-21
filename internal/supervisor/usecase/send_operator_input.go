package usecase

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/textpolicy"
)

// Canonical value: specifications/10-shared/validation-rules.md.
const SupervisorOperatorInputMinInterval = time.Second

type OperatorInputError struct {
	Code   string
	Reason supervisor.OperatorInputRefuseReason
	SendID string
}

func (e *OperatorInputError) Error() string { return e.Code }

type SendOperatorInputUseCase struct {
	Sessions interface {
		Load(supervisor.SessionID) (supervisor.ManagedSession, error)
	}
	Send             *SendInputUseCase
	RecordStructured func(store.Event) error
	Now              func() time.Time
	mu               sync.Mutex
	last             map[supervisor.SessionID]time.Time
}

func (u *SendOperatorInputUseCase) now() time.Time {
	if u.Now != nil {
		return u.Now()
	}
	return time.Now()
}

func (u *SendOperatorInputUseCase) refuse(id supervisor.SessionID, reason supervisor.OperatorInputRefuseReason) (SendResult, error) {
	if u.RecordStructured != nil {
		if err := u.RecordStructured(store.Event{Kind: supervisor.EventKindOperatorInputRefused, SessionID: id.String(), Reason: store.EventReason(reason)}); err != nil {
			return SendResult{}, err
		}
	}
	return SendResult{}, &OperatorInputError{Code: supervisor.ErrorCodeOperatorInputForbidden, Reason: reason}
}

// Execute checks the caller and text before entering the sole PTY send gate.
func (u *SendOperatorInputUseCase) Execute(ctx context.Context, id supervisor.SessionID, text supervisor.OperatorText, supervised bool) (SendResult, error) {
	if text.Len() == 0 {
		return SendResult{}, errors.New("SUP_REQUEST_MALFORMED")
	}
	if supervised {
		return u.refuse(id, supervisor.OperatorInputRefuseSupervisedCaller)
	}
	if u.Sessions == nil || u.Send == nil {
		return SendResult{}, errors.New("SUP_CONFIG_INVALID")
	}
	session, err := u.Sessions.Load(id)
	if errors.Is(err, os.ErrNotExist) || err == nil && session.SessionID != id {
		return SendResult{}, ErrSessionNotFound
	}
	if err != nil {
		return SendResult{}, err
	}
	categories, unknown := textpolicy.Classify(text.PTYText())
	if unknown || len(categories) > 0 {
		return u.refuse(id, supervisor.OperatorInputRefuseForbiddenText)
	}
	in := SendInput{SessionID: id, Kind: supervisor.SendKindOperatorInput, OperatorText: text}
	u.mu.Lock()
	if u.last == nil {
		u.last = make(map[supervisor.SessionID]time.Time)
	}
	now := u.now()
	if previous, ok := u.last[id]; ok && now.Sub(previous) < SupervisorOperatorInputMinInterval {
		u.mu.Unlock()
		return u.Send.blocked(id, supervisor.SendBlockedRateLimited, in)
	}
	u.last[id] = now
	u.mu.Unlock()
	result, err := u.Send.Execute(ctx, in)
	if err == nil {
		return result, nil
	}
	current, loadErr := u.Sessions.Load(id)
	if loadErr == nil {
		if current.StopReason == supervisor.StopSendAborted {
			return result, &OperatorInputError{Code: "SUP_SEND_ABORTED", SendID: result.SendID}
		}
		if current.StopReason == supervisor.StopSendNotConfirmed {
			return result, &OperatorInputError{Code: "SUP_SEND_NOT_CONFIRMED", SendID: result.SendID}
		}
	}
	return result, err
}
