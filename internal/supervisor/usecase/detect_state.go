package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
)

type statuslineRecord struct {
	SchemaVersion   int       `json:"schema_version"`
	ObservedAt      time.Time `json:"observed_at"`
	UsedPercentage  *int      `json:"used_percentage"`
	ClaudeSessionID string    `json:"claude_session_id"`
}

type SessionLoader interface {
	Load(supervisor.SessionID) (supervisor.ManagedSession, error)
}
type SessionSaver interface {
	Save(supervisor.ManagedSession) error
}
type StateDetector interface {
	Detect(context.Context, detect.DetectInput) (detect.DetectResult, error)
}

type DetectSessionStateUseCase struct {
	StateDir string
	Sessions SessionSaver
	Detector StateDetector
	Now      func() time.Time
}

var claudeSessionUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func (u DetectSessionStateUseCase) Execute(ctx context.Context, session supervisor.ManagedSession, vt *supervisor.VirtualTerminal, changed time.Time, exited bool, exitCode *int) (detect.DetectResult, error) {
	if u.Detector == nil {
		return detect.DetectResult{}, errors.New("detector is required")
	}
	now := time.Now().UTC()
	if u.Now != nil {
		now = u.Now().UTC()
	}
	input := detect.DetectInput{SessionID: session.SessionID, CLI: session.CLI, ScreenRows: vt.Screen(), LastScreenChangeAt: changed, ProcessExited: exited, ExitCode: exitCode, LastSendConfirmedAt: session.SendGate.LastConfirmedAt, Now: now}
	if input.ScreenRows == nil {
		input.ScreenRows = []string{}
	}
	input.ScreenCells = make([][]supervisor.Cell, len(input.ScreenRows))
	input.ScreenDim = make([][]bool, len(input.ScreenRows))
	for i := range input.ScreenRows {
		input.ScreenCells[i] = vt.Cells(i)
		input.ScreenDim[i] = make([]bool, len(input.ScreenCells[i]))
		for j, cell := range input.ScreenCells[i] {
			input.ScreenDim[i][j] = cell.Faint
		}
	}
	var unknownUsageSessionID string
	if u.StateDir != "" {
		path := filepath.Join(u.StateDir, "statusline", session.SessionID.String()+".json")
		if rec, err := readStatusline(path); err == nil && (rec.UsedPercentage == nil || *rec.UsedPercentage >= 0 && *rec.UsedPercentage <= 100) {
			info := &detect.StructuredInfo{ObservedAt: rec.ObservedAt, SessionID: rec.ClaudeSessionID}
			if !claudeSessionUUID.MatchString(info.SessionID) {
				info.SessionID = ""
			}
			startedAt := session.StateSince
			if session.ChildStartedAt != nil {
				startedAt = *session.ChildStartedAt
			}
			if !info.ObservedAt.Before(startedAt) && !info.ObservedAt.After(now.Add(5*time.Second)) && statuslineIsFresh(input, info) {
				if rec.UsedPercentage != nil {
					info.UsedPercentage = *rec.UsedPercentage
					input.Structured = info
				} else {
					// FD-sup-01 §5.3b: retain the ID without presenting missing usage as 0%.
					unknownUsageSessionID = info.SessionID
				}
			}
		}
	}
	result, err := u.Detector.Detect(ctx, input)
	if err != nil {
		return result, err
	}
	if unknownUsageSessionID != "" {
		result.SessionID = unknownUsageSessionID
	}
	if result.SessionID != "" && result.SessionID != session.ClaudeSessionID {
		if u.Sessions != nil {
			recordStore, ok := u.Sessions.(SessionRecordStore)
			if !ok {
				return result, errors.New("session store must support atomic updates")
			}
			if err := UpdateSession(recordStore, session.SessionID, func(current *supervisor.ManagedSession) error {
				current.ClaudeSessionID = result.SessionID
				current.PreviousClaudeSessionID = ""
				return nil
			}); err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

func readStatusline(path string) (statuslineRecord, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return statuslineRecord{}, err
	}
	if !info.Mode().IsRegular() {
		return statuslineRecord{}, errors.New("statusline record is not a regular file")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return statuslineRecord{}, err
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !opened.Mode().IsRegular() {
		return statuslineRecord{}, errors.New("statusline record is not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, 64*1024+1))
	if err != nil {
		return statuslineRecord{}, err
	}
	if len(data) > 64*1024 {
		return statuslineRecord{}, errors.New("statusline record exceeds size limit")
	}
	var rec statuslineRecord
	if err = json.Unmarshal(data, &rec); err != nil {
		return statuslineRecord{}, err
	}
	if rec.SchemaVersion != 1 || rec.ObservedAt.IsZero() {
		return statuslineRecord{}, errors.New("invalid statusline record")
	}
	return rec, nil
}
func statuslineIsFresh(input detect.DetectInput, info *detect.StructuredInfo) bool {
	if info == nil || info.UsedPercentage < 0 || info.UsedPercentage > 100 || info.ObservedAt.IsZero() {
		return false
	}
	if input.LastSendConfirmedAt == nil {
		return true
	}
	return !info.ObservedAt.Before(*input.LastSendConfirmedAt)
}
