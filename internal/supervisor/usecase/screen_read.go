package usecase

import (
	"errors"
	"os"
	"strings"
	"time"
	"unicode"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type ScreenReadResult struct {
	SessionID  supervisor.SessionID `json:"session_id"`
	Rows       []string             `json:"rows"`
	RowCount   int                  `json:"row_count"`
	ObservedAt time.Time            `json:"observed_at"`
}

type ScreenReadUseCase struct {
	Sessions interface {
		Load(supervisor.SessionID) (supervisor.ManagedSession, error)
	}
	Screens          ScreenReader
	RecordStructured func(store.Event) error
}

func (u ScreenReadUseCase) Execute(id supervisor.SessionID) (ScreenReadResult, error) {
	if u.Sessions == nil || u.Screens == nil || u.RecordStructured == nil {
		return ScreenReadResult{}, errors.New("SUP_CONFIG_INVALID")
	}
	session, err := u.Sessions.Load(id)
	if errors.Is(err, os.ErrNotExist) {
		return ScreenReadResult{}, ErrSessionNotFound
	}
	if err != nil {
		return ScreenReadResult{}, err
	}
	if session.SessionID != id {
		return ScreenReadResult{}, ErrSessionNotFound
	}
	if session.State == supervisor.SessionExited || session.State == supervisor.SessionFailed {
		return ScreenReadResult{}, ErrScreenUnavailable
	}
	shot := u.Screens.Snapshot(id)
	if shot.ObservedAt.IsZero() {
		return ScreenReadResult{}, ErrScreenUnavailable
	}
	session, err = u.Sessions.Load(id)
	if err != nil {
		return ScreenReadResult{}, err
	}
	if session.State == supervisor.SessionExited || session.State == supervisor.SessionFailed {
		return ScreenReadResult{}, ErrScreenUnavailable
	}
	rows := make([]string, len(shot.Rows))
	for i, row := range shot.Rows {
		rows[i] = strings.TrimRightFunc(row, unicode.IsSpace)
	}
	count := len(rows)
	if err := u.RecordStructured(store.Event{Kind: supervisor.EventKindScreenRead, SessionID: string(id), RowCount: &count}); err != nil {
		return ScreenReadResult{}, err
	}
	return ScreenReadResult{SessionID: id, Rows: rows, RowCount: count, ObservedAt: shot.ObservedAt.UTC()}, nil
}
