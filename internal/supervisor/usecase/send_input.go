package usecase

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type SendInput struct {
	SessionID      supervisor.SessionID
	Kind           supervisor.SupervisorSendKind
	Origin         supervisor.SendOrigin
	ConfirmTimeout time.Duration
}
type SendResult struct {
	SendID    string
	Blocked   supervisor.SupervisorSendBlockedReason
	Confirmed bool
}

var errSendStateChanged = errors.New("session state changed before issue")
var errSendPendingAppeared = errors.New("pending send appeared before issue")

// Canonical value: specifications/10-shared/validation-rules.md SUPERVISOR_PARALLEL_INSTRUCTION_DEFAULT.
const defaultParallelInstruction = "自動作業計画モード 並列で作業して。実装は ~/.claude/scripts/codex.sh impl --model gpt-6-luna を明示して委譲する"

type ScreenSnapshot struct {
	Rows                  []string
	Cells                 [][]supervisor.Cell
	State                 detect.DetectedState
	Processing            bool
	CompletionMenu        bool
	SelectedCompletion    string
	ObservedAt, ChangedAt time.Time
}
type ScreenReader interface {
	Snapshot(supervisor.SessionID) ScreenSnapshot
}
type ObservedScreenStore struct {
	mu    sync.RWMutex
	shots map[supervisor.SessionID]ScreenSnapshot
}

func (s *ObservedScreenStore) Snapshot(id supervisor.SessionID) ScreenSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.shots[id]
}
func (s *ObservedScreenStore) SetSnapshot(id supervisor.SessionID, shot ScreenSnapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shots == nil {
		s.shots = make(map[supervisor.SessionID]ScreenSnapshot)
	}
	s.shots[id] = shot
}
func (s *ObservedScreenStore) Delete(id supervisor.SessionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.shots, id)
}

type PTYWriter interface {
	WriteLine(context.Context, string, time.Duration, func() bool) error
}
type SendConfirmation struct {
	SessionID     supervisor.SessionID
	SendID        string
	Screen        ScreenSnapshot
	ProcessExited bool
	ExitedAt      time.Time
}

// SendInputUseCase is the sole PTY input path. NotifyConfirmation is called by the observer after screen updates.
type SendInputUseCase struct {
	Sessions             SessionRecordStore
	Screens              ScreenReader
	PTY                  PTYWriter
	PTYs                 *PTYRegistry
	IsStopped            func(supervisor.SessionID) bool
	StopState            *StopState
	ParallelInstruction  func(supervisor.SessionID) string
	ProjectInstructions  map[string]string
	RecordStructured     func(store.Event) error
	OperationalLog       *store.SupervisorLog
	Notify               func(supervisor.SupervisorEventKind, string)
	LogBlocked           func(supervisor.SessionID, supervisor.SupervisorSendBlockedReason)
	RecommendationIssued func(supervisor.SessionID, [32]byte, time.Time)
	Now                  func() time.Time
	ConfirmTimeout       time.Duration
	issuedBindOnce       sync.Once
	mu                   sync.Mutex
	locks                map[supervisor.SessionID]*sync.Mutex
	confirm              map[supervisor.SessionID]chan SendConfirmation
	crWritten            map[supervisor.SessionID]string
	crAt                 map[supervisor.SessionID]time.Time
	ended                map[supervisor.SessionID]bool
}

func (u *SendInputUseCase) sessionLock(id supervisor.SessionID) *sync.Mutex {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.locks == nil {
		u.locks = make(map[supervisor.SessionID]*sync.Mutex)
	}
	if u.locks[id] == nil {
		u.locks[id] = &sync.Mutex{}
	}
	return u.locks[id]
}
func (u *SendInputUseCase) confirmationChannel(id supervisor.SessionID) chan SendConfirmation {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.confirm == nil {
		u.confirm = make(map[supervisor.SessionID]chan SendConfirmation)
	}
	if u.confirm[id] == nil {
		u.confirm[id] = make(chan SendConfirmation, 1)
	}
	return u.confirm[id]
}
func (u *SendInputUseCase) NotifyConfirmation(c SendConfirmation) {
	u.mu.Lock()
	if u.ended[c.SessionID] {
		u.mu.Unlock()
		return
	}
	ch := u.confirm[c.SessionID]
	u.mu.Unlock()
	if ch == nil {
		return
	}
	select {
	case ch <- c:
	default:
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- c:
		default:
		}
	}
}
func (u *SendInputUseCase) now() time.Time {
	if u.Now != nil {
		return u.Now().UTC()
	}
	return time.Now().UTC()
}

func (u *SendInputUseCase) ResumeReady(id supervisor.SessionID, resumedAt time.Time) bool {
	if u.Screens == nil {
		return false
	}
	shot := u.Screens.Snapshot(id)
	return shot.State == detect.StateIdle &&
		shot.ObservedAt.After(resumedAt) &&
		shot.ChangedAt.After(resumedAt) &&
		u.now().Sub(shot.ChangedAt) >= detect.QuietPeriod &&
		screenReadyForWrite(shot)
}
func (u *SendInputUseCase) record(e store.Event) error {
	if u.RecordStructured != nil {
		return u.RecordStructured(e)
	}
	return nil
}
func (u *SendInputUseCase) blocked(id supervisor.SessionID, reason supervisor.SupervisorSendBlockedReason) (SendResult, error) {
	var logErr error
	if reason == supervisor.SendBlockedNotFixedText && u.OperationalLog != nil {
		logErr = u.OperationalLog.Record(store.SupervisorLogEntry{Level: store.LogLevelError, Kind: supervisor.EventKindSendBlocked, SessionID: id.String()})
	}
	if reason == supervisor.SendBlockedNotFixedText && u.LogBlocked != nil {
		u.LogBlocked(id, reason)
	}
	err := u.record(store.Event{Kind: supervisor.EventKindSendBlocked, SessionID: id.String(), Reason: store.EventReason(reason), Code: "SUP_SEND_BLOCKED"})
	return SendResult{Blocked: reason}, errors.Join(logErr, err)
}
func (u *SendInputUseCase) stopped(id supervisor.SessionID) bool {
	if u.IsStopped == nil || u.StopState == nil {
		return true
	}
	if u.IsStopped(id) {
		return true
	}
	if u.StopState.IsStopped(id) {
		return true
	}
	for _, name := range []string{"", id.String()} {
		_, err := os.Stat(u.StopState.filePath(name))
		if err == nil || !errors.Is(err, os.ErrNotExist) {
			return true
		}
	}
	return false
}

func (u *SendInputUseCase) resetConfirmation(id supervisor.SessionID) chan SendConfirmation {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.confirm == nil {
		u.confirm = make(map[supervisor.SessionID]chan SendConfirmation)
	}
	ch := make(chan SendConfirmation, 1)
	u.confirm[id] = ch
	delete(u.ended, id)
	return ch
}

func (u *SendInputUseCase) markCR(id supervisor.SessionID, sendID string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.crWritten == nil {
		u.crWritten = make(map[supervisor.SessionID]string)
	}
	u.crWritten[id] = sendID
	if u.crAt == nil {
		u.crAt = make(map[supervisor.SessionID]time.Time)
	}
	u.crAt[id] = u.now()
}

func (u *SendInputUseCase) CRWritten(id supervisor.SessionID, sendID string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.crWritten[id] == sendID
}

func (u *SendInputUseCase) CRWrittenAt(id supervisor.SessionID, sendID string) time.Time {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.crWritten[id] != sendID {
		return time.Time{}
	}
	return u.crAt[id]
}

func (u *SendInputUseCase) EndSession(id supervisor.SessionID) {
	u.mu.Lock()
	if u.ended == nil {
		u.ended = make(map[supervisor.SessionID]bool)
	}
	u.ended[id] = true
	delete(u.confirm, id)
	delete(u.crWritten, id)
	delete(u.crAt, id)
	delete(u.locks, id)
	u.mu.Unlock()
	if screens, ok := u.Screens.(interface{ Delete(supervisor.SessionID) }); ok {
		screens.Delete(id)
	}
}

func snapshotObservedAt(s ScreenSnapshot) time.Time {
	return s.ObservedAt
}
func (u *SendInputUseCase) BindRecommendationIssued(record func(supervisor.SessionID, [32]byte, time.Time)) {
	u.issuedBindOnce.Do(func() { u.RecommendationIssued = record })
}

func snapshotChangedAt(s ScreenSnapshot) time.Time {
	return s.ChangedAt
}

func (u *SendInputUseCase) Execute(ctx context.Context, in SendInput) (SendResult, error) {
	if u.Sessions == nil || u.Screens == nil || (u.PTY == nil && u.PTYs == nil) {
		return SendResult{}, errors.New("send dependencies are required")
	}
	lock := u.sessionLock(in.SessionID)
	if !lock.TryLock() {
		return u.blocked(in.SessionID, supervisor.SendBlockedPending)
	}
	defer lock.Unlock()
	s, err := u.Sessions.Load(in.SessionID)
	if err != nil {
		return SendResult{}, err
	}
	parallel := u.parallelInstructionFor(s)
	message, fixed := supervisor.FixedText(in.Kind, parallel)
	shot := u.Screens.Snapshot(in.SessionID)
	fresh := !s.SendGate.UnconfirmedDiscarded || shot.State == detect.StateIdle && snapshotObservedAt(shot).After(s.StateSince) && !snapshotChangedAt(shot).IsZero() && u.now().Sub(snapshotChangedAt(shot)) >= detect.QuietPeriod && screenReadyForWrite(shot)
	reason := supervisor.CheckSendGate(s, supervisor.SupervisorSessionState(shot.State), in.Kind, u.stopped(in.SessionID), fixed, fresh, in.Origin)
	if reason != "" {
		return u.blocked(in.SessionID, reason)
	}
	second := u.Screens.Snapshot(in.SessionID)
	if u.stopped(in.SessionID) {
		return u.blocked(in.SessionID, supervisor.SendBlockedStop)
	}
	if !supervisor.SendKindAllowed(s.State, supervisor.SupervisorSessionState(second.State), in.Kind, in.Origin) || !screenReadyForWrite(second) {
		return u.blocked(in.SessionID, supervisor.SendBlockedState)
	}
	pty := u.PTY
	if u.PTYs != nil {
		pty = u.PTYs.Get(in.SessionID)
	}
	if pty == nil {
		return u.blocked(in.SessionID, supervisor.SendBlockedState)
	}
	prev := s.SendGate.LastSendConfirmed
	prevEver := s.SendGate.EverSent
	var pending supervisor.PendingSend
	ch := u.resetConfirmation(in.SessionID)
	err = UpdateSession(u.Sessions, in.SessionID, func(v *supervisor.ManagedSession) error {
		if !supervisor.SendKindAllowed(v.State, supervisor.SupervisorSessionState(second.State), in.Kind, in.Origin) {
			return errSendStateChanged
		}
		if v.SendGate.Pending != nil {
			return errSendPendingAppeared
		}
		seq := v.SendGate.NextSeq
		if seq == 0 {
			seq = 1
		}
		pending = supervisor.PendingSend{SendID: fmt.Sprintf("%s-s%d", in.SessionID, seq), Kind: in.Kind, IssuedAt: u.now()}
		v.SendGate.NextSeq = seq + 1
		v.SendGate.Pending = &pending
		if in.Kind == supervisor.SendKindHandoffEnd && in.Origin == supervisor.SendOriginHandoff && v.Rotation != nil {
			issued := pending.IssuedAt
			v.Rotation.EndIssuedAt = &issued
		}
		v.SendGate.LastSendConfirmed = false
		v.SendGate.EverSent = true
		return nil
	})
	if err != nil {
		if errors.Is(err, errSendStateChanged) {
			return u.blocked(in.SessionID, supervisor.SendBlockedState)
		}
		if errors.Is(err, errSendPendingAppeared) {
			return u.blocked(in.SessionID, supervisor.SendBlockedPending)
		}
		return SendResult{}, err
	}
	result := SendResult{SendID: pending.SendID}
	if err = u.record(store.Event{Kind: supervisor.EventKindSendIssued, SessionID: in.SessionID.String(), SendID: pending.SendID, SendKind: in.Kind}); err != nil {
		return result, errors.Join(err, u.blockBeforeText(in.SessionID, pending, prevEver, prev, supervisor.SendBlockedState))
	}
	if in.Kind == supervisor.SendKindRecommendationContinue && u.RecommendationIssued != nil {
		if hash, ok := ctx.Value(recommendationHashContextKey{}).([32]byte); ok {
			u.RecommendationIssued(in.SessionID, hash, pending.IssuedAt)
		}
	}
	if u.stopped(in.SessionID) {
		result.Blocked = supervisor.SendBlockedStop
		return result, u.blockBeforeText(in.SessionID, pending, prevEver, prev, supervisor.SendBlockedStop)
	}
	var crAt time.Time
	var beforeCalled, beforeAllowed bool
	before := func() bool {
		beforeCalled = true
		if u.stopped(in.SessionID) || !screenReadyForEnter(u.Screens.Snapshot(in.SessionID), message) {
			return false
		}
		crAt = u.now()
		u.markCR(in.SessionID, pending.SendID)
		beforeAllowed = true
		return true
	}
	err = pty.WriteLine(ctx, message, supervisor.SupervisorSendEnterDelay, before)
	if err != nil {
		if beforeAllowed {
			return result, errors.Join(err, u.unconfirmed(in.SessionID, pending, in.Origin))
		}
		if !beforeCalled && (strings.ContainsAny(message, "\r\n") || supervisor.SupervisorSendEnterDelay < 0 || err.Error() == "line text must not contain CR or LF" || err.Error() == "delay must not be negative") {
			return result, errors.Join(err, u.blockBeforeText(in.SessionID, pending, prevEver, prev, supervisor.SendBlockedState))
		}
		return result, errors.Join(err, u.abortBeforeCR(in.SessionID, pending, prevEver, prev, in.Origin))
	}
	timeout := in.ConfirmTimeout
	if timeout <= 0 && in.Kind == supervisor.SendKindHandoffExit && in.Origin == supervisor.SendOriginHandoff {
		timeout = supervisor.SupervisorExitGrace
	}
	if timeout <= 0 {
		timeout = u.ConfirmTimeout
	}
	if timeout <= 0 {
		timeout = supervisor.SupervisorSendConfirmTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case c := <-ch:
			if c.SendID != pending.SendID {
				continue
			}
			if c.ProcessExited {
				if in.Kind == supervisor.SendKindHandoffExit {
					if !c.ExitedAt.After(crAt) {
						continue
					}
					// The observer confirms handoff_exit in the same write as the exit transition.
					current, err := u.Sessions.Load(in.SessionID)
					if err != nil {
						return result, err
					}
					if current.SendGate.Pending == nil && current.LastSend != nil && current.LastSend.SendID == pending.SendID && current.LastSend.Confirmed {
						return SendResult{SendID: pending.SendID, Confirmed: true}, nil
					}
					if !u.CRWritten(in.SessionID, pending.SendID) {
						continue
					}
					return u.confirmSend(in.SessionID, pending)
				}
				current, err := u.Sessions.Load(in.SessionID)
				if err != nil {
					return result, err
				}
				if current.State == supervisor.SessionStopped && current.StopReason == supervisor.StopProcessExitWithPending {
					return result, errors.New("process exited with pending send")
				}
				if in.Origin == supervisor.SendOriginHandoff {
					return result, errors.New("process exited with pending send")
				}
				if err := UpdateSession(u.Sessions, in.SessionID, func(v *supervisor.ManagedSession) error {
					v.State = supervisor.SessionStopped
					v.StopReason = supervisor.StopProcessExitWithPending
					return nil
				}); err != nil {
					return result, err
				}
				if u.Notify != nil {
					u.Notify(supervisor.EventKindSessionStopped, in.SessionID.String())
				}
				return result, errors.New("process exited with pending send")
			}
			if !snapshotChangedAt(c.Screen).After(crAt) {
				continue
			}
			if in.Kind == supervisor.SendKindHandoffExit {
				continue
			}
			blank := detect.IsInputBlank(detect.DefaultPatternSet(), c.Screen.Rows, c.Screen.Cells)
			if !blank && (in.Kind == supervisor.SendKindRemoteControl || !c.Screen.Processing) {
				continue
			}
			return u.confirmSend(in.SessionID, pending)
		case <-timer.C:
			if err := u.unconfirmed(in.SessionID, pending, in.Origin); err != nil {
				return result, err
			}
			return result, errors.New("SUP_SEND_NOT_CONFIRMED")
		case <-ctx.Done():
			return result, errors.Join(ctx.Err(), u.unconfirmed(in.SessionID, pending, in.Origin))
		}
	}
}

func (u *SendInputUseCase) parallelInstructionFor(s supervisor.ManagedSession) string {
	if u.ParallelInstruction != nil {
		return u.ParallelInstruction(s.SessionID)
	}
	if value := u.ProjectInstructions[s.ProjectID]; value != "" {
		return value
	}
	return defaultParallelInstruction
}

func (u *SendInputUseCase) abortBeforeCR(id supervisor.SessionID, p supervisor.PendingSend, ever, confirmed bool, origin supervisor.SendOrigin) error {
	if err := UpdateSession(u.Sessions, id, func(v *supervisor.ManagedSession) error {
		v.SendGate.Pending = nil
		v.SendGate.EverSent = ever
		v.SendGate.LastSendConfirmed = confirmed
		if origin != supervisor.SendOriginHandoff && v.State != supervisor.SessionStopped {
			v.State = supervisor.SessionStopped
			v.StopReason = supervisor.StopSendAborted
		}
		return nil
	}); err != nil {
		return err
	}
	if err := u.record(store.Event{Kind: supervisor.EventKindSendAborted, SessionID: id.String(), SendID: p.SendID, SendKind: p.Kind, Code: "SUP_SEND_ABORTED"}); err != nil {
		return err
	}
	if origin != supervisor.SendOriginHandoff && u.Notify != nil {
		u.Notify(supervisor.EventKindSendAborted, id.String())
	}
	return nil
}

func (u *SendInputUseCase) blockBeforeText(id supervisor.SessionID, p supervisor.PendingSend, ever, confirmed bool, reason supervisor.SupervisorSendBlockedReason) error {
	if err := UpdateSession(u.Sessions, id, func(v *supervisor.ManagedSession) error {
		v.SendGate.Pending = nil
		v.SendGate.EverSent = ever
		v.SendGate.LastSendConfirmed = confirmed
		return nil
	}); err != nil {
		return err
	}
	_, err := u.blocked(id, reason)
	return err
}

func (u *SendInputUseCase) unconfirmed(id supervisor.SessionID, p supervisor.PendingSend, origin supervisor.SendOrigin) error {
	if err := UpdateSession(u.Sessions, id, func(v *supervisor.ManagedSession) error {
		if origin != supervisor.SendOriginHandoff && v.State != supervisor.SessionStopped {
			v.State = supervisor.SessionStopped
			v.StopReason = supervisor.StopSendNotConfirmed
		}
		return nil
	}); err != nil {
		return err
	}
	if err := u.record(store.Event{Kind: supervisor.EventKindSendUnconfirmed, SessionID: id.String(), SendID: p.SendID, SendKind: p.Kind, Code: "SUP_SEND_NOT_CONFIRMED"}); err != nil {
		return err
	}
	if origin != supervisor.SendOriginHandoff && u.Notify != nil {
		u.Notify(supervisor.EventKindSendUnconfirmed, id.String())
	}
	return nil
}
func (u *SendInputUseCase) confirmSend(id supervisor.SessionID, p supervisor.PendingSend) (SendResult, error) {
	now := u.now()
	err := UpdateSession(u.Sessions, id, func(v *supervisor.ManagedSession) error {
		v.SendGate.Pending = nil
		v.SendGate.EverSent = true
		v.SendGate.LastSendConfirmed = true
		v.SendGate.LastConfirmedAt = &now
		v.SendGate.UnconfirmedDiscarded = false
		v.LastSend = &supervisor.LastSendRecord{SendID: p.SendID, Kind: p.Kind, IssuedAt: p.IssuedAt, Confirmed: true}
		return nil
	})
	if err != nil {
		return SendResult{SendID: p.SendID}, err
	}
	err = u.record(store.Event{Kind: supervisor.EventKindSendConfirmed, SessionID: id.String(), SendID: p.SendID, SendKind: p.Kind})
	return SendResult{SendID: p.SendID, Confirmed: true}, err
}
func screenReadyForWrite(s ScreenSnapshot) bool {
	if s.Processing || s.State != detect.StateIdle && s.State != detect.StateAwaitingRecommendation {
		return false
	}
	if !detect.IsInputBlank(detect.DefaultPatternSet(), s.Rows, s.Cells) {
		return false
	}
	return !hasSelectionMenu(s.Rows)
}
func screenReadyForEnter(s ScreenSnapshot, want string) bool {
	if s.Processing || s.State == detect.StateAwaitingPermission || s.State == detect.StateExited {
		return false
	}
	if hasSelectionMenu(s.Rows) && !(strings.HasPrefix(want, "/") && s.CompletionMenu && completionCandidateMatches(s.SelectedCompletion, want) && !hasPermissionDialog(s.Rows)) {
		return false
	}
	for i := len(s.Rows) - 1; i >= 0; i-- {
		row := s.Rows[i]
		if strings.HasPrefix(row, "❯\u00a0") {
			return strings.TrimSpace(strings.TrimPrefix(row, "❯\u00a0")) == want
		}
	}
	return false
}
func hasPermissionDialog(rows []string) bool {
	for _, row := range rows {
		if strings.Contains(row, "Enter to confirm") || strings.Contains(row, "Esc to cancel") {
			return true
		}
	}
	return false
}
func hasSelectionMenu(rows []string) bool {
	if hasPermissionDialog(rows) {
		return true
	}
	for _, row := range rows {
		if strings.HasPrefix(row, " ") && strings.HasPrefix(strings.TrimSpace(row), "❯ ") {
			return true
		}
	}
	return false
}
