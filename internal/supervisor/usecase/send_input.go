package usecase

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
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
	OperatorText   supervisor.OperatorText
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
	// Observation advances once per output chunk processed by the observer.
	Observation           uint64
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
	mu     sync.RWMutex
	shots  map[supervisor.SessionID]ScreenSnapshot
	owners map[supervisor.SessionID]uint64
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

// SetSnapshotOwned rejects updates from an observation loop superseded by a newer loop.
func (s *ObservedScreenStore) SetSnapshotOwned(id supervisor.SessionID, shot ScreenSnapshot, owner uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owners == nil {
		s.owners = make(map[supervisor.SessionID]uint64)
	}
	if owner < s.owners[id] {
		return
	}
	if s.shots == nil {
		s.shots = make(map[supervisor.SessionID]ScreenSnapshot)
	}
	s.owners[id] = owner
	s.shots[id] = shot
}
func (s *ObservedScreenStore) Delete(id supervisor.SessionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.shots, id)
}

func (s *ObservedScreenStore) DeleteOwned(id supervisor.SessionID, owner uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.owners[id] == owner {
		delete(s.shots, id)
	}
}

type PTYWriter interface {
	WriteLine(context.Context, string, time.Duration, func() bool) error
}
type SendConfirmation struct {
	// earlier preserves intervening screens when the notification channel is full.
	earlier       []SendConfirmation
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
	Observed             *ObservedStateStore
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
	EchoWait             time.Duration
	issuedBindOnce       sync.Once
	mu                   sync.Mutex
	locks                map[supervisor.SessionID]*sync.Mutex
	confirm              map[supervisor.SessionID]chan SendConfirmation
	crWritten            map[supervisor.SessionID]string
	crAt                 map[supervisor.SessionID]time.Time
	ended                map[supervisor.SessionID]bool
	exitSends            map[supervisor.SessionID]*handoffExitSend
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
	defer u.mu.Unlock()
	if u.ended[c.SessionID] {
		return
	}
	ch := u.confirm[c.SessionID]
	if ch == nil {
		return
	}
	select {
	case ch <- c:
	default:
		select {
		case previous := <-ch:
			c.earlier = previous.earlier
			previous.earlier = nil
			c.earlier = append(c.earlier, previous)
		default:
		}
		ch <- c
	}
}

// Stop retaining observations when no sender is waiting (including after timeout).
func (u *SendInputUseCase) releaseConfirmation(id supervisor.SessionID, ch chan SendConfirmation) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.confirm[id] == ch {
		delete(u.confirm, id)
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
func (u *SendInputUseCase) blocked(id supervisor.SessionID, reason supervisor.SupervisorSendBlockedReason, inputs ...SendInput) (SendResult, error) {
	var logErr error
	if reason == supervisor.SendBlockedNotFixedText && u.OperationalLog != nil {
		logErr = u.OperationalLog.Record(store.SupervisorLogEntry{Level: store.LogLevelError, Kind: supervisor.EventKindSendBlocked, SessionID: id.String()})
	}
	if reason == supervisor.SendBlockedNotFixedText && u.LogBlocked != nil {
		u.LogBlocked(id, reason)
	}
	event := store.Event{Kind: supervisor.EventKindSendBlocked, SessionID: id.String(), Reason: store.EventReason(reason), Code: "SUP_SEND_BLOCKED"}
	if len(inputs) != 0 && inputs[0].Kind == supervisor.SendKindOperatorInput {
		event.SendKind = supervisor.SendKindOperatorInput
		length := inputs[0].OperatorText.Len()
		event.TextLength = &length
	}
	err := u.record(event)
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

func (u *SendInputUseCase) EndSession(id supervisor.SessionID, owner ...uint64) {
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
	if len(owner) != 0 {
		if screens, ok := u.Screens.(interface {
			DeleteOwned(supervisor.SessionID, uint64)
		}); ok {
			screens.DeleteOwned(id, owner[0])
		} else if screens, ok := u.Screens.(interface{ Delete(supervisor.SessionID) }); ok {
			screens.Delete(id)
		}
	} else if screens, ok := u.Screens.(interface{ Delete(supervisor.SessionID) }); ok {
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
		return u.blocked(in.SessionID, supervisor.SendBlockedPending, in)
	}
	defer lock.Unlock()
	s, err := u.Sessions.Load(in.SessionID)
	if err != nil {
		return SendResult{}, err
	}
	parallel := u.parallelInstructionFor(s)
	message, fixed := supervisor.FixedText(in.Kind, parallel)
	if in.Kind == supervisor.SendKindOperatorInput {
		message, fixed = in.OperatorText.PTYText(), in.OperatorText.Len() > 0
	} else if in.OperatorText.Len() != 0 {
		fixed = false
	}
	shot := u.Screens.Snapshot(in.SessionID)
	fresh := !s.SendGate.UnconfirmedDiscarded || shot.State == detect.StateIdle && snapshotObservedAt(shot).After(s.StateSince) && !snapshotChangedAt(shot).IsZero() && u.now().Sub(snapshotChangedAt(shot)) >= detect.QuietPeriod && screenReadyForWrite(shot)
	reason := supervisor.CheckSendGate(s, supervisor.SupervisorSessionState(shot.State), in.Kind, u.stopped(in.SessionID), fixed, fresh, in.Origin)
	if reason != "" {
		return u.blocked(in.SessionID, reason, in)
	}
	if in.Kind == supervisor.SendKindOperatorInput && !operatorScreenReadyForWrite(shot) {
		return u.blocked(in.SessionID, supervisor.SendBlockedState, in)
	}
	second := u.Screens.Snapshot(in.SessionID)
	if u.stopped(in.SessionID) {
		return u.blocked(in.SessionID, supervisor.SendBlockedStop, in)
	}
	if !supervisor.SendKindAllowed(s.State, supervisor.SupervisorSessionState(second.State), in.Kind, in.Origin) || !screenReadyForWrite(second) || in.Kind == supervisor.SendKindOperatorInput && !operatorScreenReadyForWrite(second) {
		return u.blocked(in.SessionID, supervisor.SendBlockedState, in)
	}
	pty := u.PTY
	if u.PTYs != nil {
		pty = u.PTYs.Get(in.SessionID)
	}
	if pty == nil {
		return u.blocked(in.SessionID, supervisor.SendBlockedState, in)
	}
	prev := s.SendGate.LastSendConfirmed
	prevEver := s.SendGate.EverSent
	var pending supervisor.PendingSend
	var exitSend *handoffExitSend
	defer func() { u.collectExit(in.SessionID, exitSend) }()
	ch := u.resetConfirmation(in.SessionID)
	defer u.releaseConfirmation(in.SessionID, ch)
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
		if in.Kind == supervisor.SendKindHandoffExit {
			exitSend = &handoffExitSend{pending: pending, in: in, owner: handoffPTYOwner(pty)}
			u.mu.Lock()
			if u.exitSends == nil {
				u.exitSends = make(map[supervisor.SessionID]*handoffExitSend)
			}
			u.exitSends[in.SessionID] = exitSend
			u.mu.Unlock()
		}
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
			return u.blocked(in.SessionID, supervisor.SendBlockedState, in)
		}
		if errors.Is(err, errSendPendingAppeared) {
			return u.blocked(in.SessionID, supervisor.SendBlockedPending, in)
		}
		return SendResult{}, err
	}
	result := SendResult{SendID: pending.SendID}
	if err = u.record(sendEvent(supervisor.EventKindSendIssued, in, pending, "")); err != nil {
		return result, errors.Join(err, u.blockBeforeText(in, pending, prevEver, prev, supervisor.SendBlockedState))
	}
	if in.Kind == supervisor.SendKindRecommendationContinue && u.RecommendationIssued != nil {
		if hash, ok := ctx.Value(recommendationHashContextKey{}).([32]byte); ok {
			u.RecommendationIssued(in.SessionID, hash, pending.IssuedAt)
		}
	}
	if u.stopped(in.SessionID) {
		result.Blocked = supervisor.SendBlockedStop
		return result, u.blockBeforeText(in, pending, prevEver, prev, supervisor.SendBlockedStop)
	}
	if in.Kind == supervisor.SendKindOperatorInput && !operatorScreenReadyForWrite(u.Screens.Snapshot(in.SessionID)) {
		result.Blocked = supervisor.SendBlockedState
		return result, u.blockBeforeText(in, pending, prevEver, prev, supervisor.SendBlockedState)
	}
	var crAt time.Time
	var beforeCalled, beforeAllowed bool
	before := func() bool {
		beforeCalled = true
		if !u.waitForEnter(ctx, in, message, pending.SendID, ch) {
			return false
		}
		if exitSend != nil && !u.beginExitCR(in.SessionID, exitSend) {
			return false
		}
		beforeAllowed = true
		return true
	}
	err = pty.WriteLine(ctx, message, supervisor.SupervisorSendEnterDelay, before)
	if err != nil {
		u.endExitCR(in.SessionID, exitSend, false)
		if in.Kind == supervisor.SendKindOperatorInput && u.OperationalLog != nil {
			kind := supervisor.EventKindSendAborted
			if beforeAllowed {
				kind = supervisor.EventKindSendUnconfirmed
			}
			if logErr := u.logOperatorSendOutcome(in, pending, kind); logErr != nil {
				err = errors.Join(err, logErr)
			}
		}
		if beforeAllowed {
			return result, errors.Join(err, u.unconfirmed(in, pending))
		}
		if !beforeCalled && (strings.ContainsAny(message, "\r\n") || supervisor.SupervisorSendEnterDelay < 0 || err.Error() == "line text must not contain CR or LF" || err.Error() == "delay must not be negative") {
			return result, errors.Join(err, u.blockBeforeText(in, pending, prevEver, prev, supervisor.SendBlockedState))
		}
		return result, errors.Join(err, u.abortBeforeCR(in, pending, prevEver, prev))
	}
	// WriteLine returns only after the separate CR write completes.
	u.endExitCR(in.SessionID, exitSend, true)
	u.markCR(in.SessionID, pending.SendID)
	crAt = u.CRWrittenAt(in.SessionID, pending.SendID)
	if exitSend != nil {
		resolved, resolveErr := u.resolveExit(in, pending, exitSend.owner, false)
		if resolveErr != nil || resolved.Confirmed {
			return resolved, resolveErr
		}
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
	var lastObservation uint64
	consecutive := 0
	for {
		select {
		case notification := <-ch:
			for _, c := range append(notification.earlier, notification) {
				if c.SendID != pending.SendID {
					continue
				}
				if c.ProcessExited {
					if in.Kind == supervisor.SendKindHandoffExit {
						resolved, resolveErr := u.resolveExit(in, pending, exitSend.owner, false)
						if resolveErr != nil || resolved.Confirmed {
							return resolved, resolveErr
						}
						continue
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
				if in.Kind == supervisor.SendKindHandoffExit {
					continue
				}
				if c.Screen.Observation == 0 {
					consecutive = 0
					continue
				}
				if c.Screen.Observation <= lastObservation {
					continue
				}
				adjacent := lastObservation != 0 && c.Screen.Observation-lastObservation == 1
				lastObservation = c.Screen.Observation
				blank := detect.IsInputBlank(detect.DefaultPatternSet(), c.Screen.Rows, c.Screen.Cells)
				if !snapshotChangedAt(c.Screen).After(crAt) || !blank && (in.Kind == supervisor.SendKindRemoteControl || !c.Screen.Processing) {
					consecutive = 0
					continue
				}
				if !adjacent {
					consecutive = 0
				}
				consecutive++
				// FD-sup-02 §5.2 requires two consecutive, distinct observations.
				if consecutive >= 2 {
					return u.confirmSend(in, pending)
				}
			}

		case <-timer.C:
			if exitSend != nil {
				return u.expireExit(in, pending, exitSend.owner, errors.New("SUP_SEND_NOT_CONFIRMED"))
			}
			return result, errors.Join(errors.New("SUP_SEND_NOT_CONFIRMED"), u.unconfirmed(in, pending), u.logOperatorSendOutcome(in, pending, supervisor.EventKindSendUnconfirmed))
		case <-ctx.Done():
			if exitSend != nil {
				return u.expireExit(in, pending, exitSend.owner, ctx.Err())
			}
			return result, errors.Join(ctx.Err(), u.unconfirmed(in, pending), u.logOperatorSendOutcome(in, pending, supervisor.EventKindSendUnconfirmed))
		}
	}
}

func (u *SendInputUseCase) logOperatorSendOutcome(in SendInput, p supervisor.PendingSend, kind supervisor.SupervisorEventKind) error {
	if in.Kind != supervisor.SendKindOperatorInput || u.OperationalLog == nil {
		return nil
	}
	return u.OperationalLog.Record(store.SupervisorLogEntry{Level: store.LogLevelWarn, Kind: kind, SessionID: in.SessionID.String(), SendID: p.SendID})
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

func (u *SendInputUseCase) abortBeforeCR(in SendInput, p supervisor.PendingSend, ever, confirmed bool) error {
	id, origin := in.SessionID, in.Origin
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
	if err := u.record(sendEvent(supervisor.EventKindSendAborted, in, p, "SUP_SEND_ABORTED")); err != nil {
		return err
	}
	if origin != supervisor.SendOriginHandoff && u.Notify != nil {
		u.Notify(supervisor.EventKindSendAborted, id.String())
	}
	return nil
}

func (u *SendInputUseCase) blockBeforeText(in SendInput, p supervisor.PendingSend, ever, confirmed bool, reason supervisor.SupervisorSendBlockedReason) error {
	id := in.SessionID
	if err := UpdateSession(u.Sessions, id, func(v *supervisor.ManagedSession) error {
		v.SendGate.Pending = nil
		v.SendGate.EverSent = ever
		v.SendGate.LastSendConfirmed = confirmed
		return nil
	}); err != nil {
		return err
	}
	_, err := u.blocked(id, reason, in)
	return err
}

func (u *SendInputUseCase) unconfirmed(in SendInput, p supervisor.PendingSend) error {
	if in.Kind == supervisor.SendKindHandoffExit {
		_, err := u.resolveExit(in, p, u.exitOwner(in.SessionID, p.SendID), true)
		return err
	}
	id, origin := in.SessionID, in.Origin
	if err := UpdateSession(u.Sessions, id, func(v *supervisor.ManagedSession) error {
		if origin != supervisor.SendOriginHandoff && v.State != supervisor.SessionStopped {
			v.State = supervisor.SessionStopped
			v.StopReason = supervisor.StopSendNotConfirmed
		}
		return nil
	}); err != nil {
		return err
	}
	if err := u.record(sendEvent(supervisor.EventKindSendUnconfirmed, in, p, "SUP_SEND_NOT_CONFIRMED")); err != nil {
		return err
	}
	if origin != supervisor.SendOriginHandoff && u.Notify != nil {
		u.Notify(supervisor.EventKindSendUnconfirmed, id.String())
	}
	return nil
}
func (u *SendInputUseCase) confirmSend(in SendInput, p supervisor.PendingSend) (SendResult, error) {
	if in.Kind == supervisor.SendKindHandoffExit {
		return u.resolveExit(in, p, u.exitOwner(in.SessionID, p.SendID), false)
	}
	id := in.SessionID
	now := u.now()
	err := UpdateSession(u.Sessions, id, func(v *supervisor.ManagedSession) error {
		applySendConfirmation(v, p, now)
		return nil
	})
	if err != nil {
		return SendResult{SendID: p.SendID}, err
	}
	err = u.record(sendEvent(supervisor.EventKindSendConfirmed, in, p, ""))
	return SendResult{SendID: p.SendID, Confirmed: true}, err
}

func sendEvent(kind supervisor.SupervisorEventKind, in SendInput, p supervisor.PendingSend, code string) store.Event {
	event := store.Event{Kind: kind, SessionID: in.SessionID.String(), SendID: p.SendID, SendKind: p.Kind, Code: code}
	if in.Kind == supervisor.SendKindOperatorInput {
		length := in.OperatorText.Len()
		event.TextLength = &length
	}
	return event
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
func operatorScreenReadyForWrite(s ScreenSnapshot) bool {
	return s.State == detect.StateIdle && !s.CompletionMenu && screenReadyForWrite(s)
}

// waitForEnter only waits for a readable prefix. The first snapshot is taken after
// WriteLine's text write and fixed delay; using its observation as the baseline
// also excludes all observations from before the text write completed.
func (u *SendInputUseCase) waitForEnter(ctx context.Context, in SendInput, message, sendID string, ch <-chan SendConfirmation) bool {
	shot := u.Screens.Snapshot(in.SessionID)
	operator := in.Kind == supervisor.SendKindOperatorInput
	check := func(shot ScreenSnapshot) (match, prefix bool) {
		if ctx.Err() != nil || u.stopped(in.SessionID) || !screenSafeForEnter(shot, message) {
			return false, false
		}
		current, err := u.Sessions.Load(in.SessionID)
		if err != nil || current.State == supervisor.SessionStopped {
			return false, false
		}
		if operator {
			if shot.State != detect.StateIdle && shot.State != detect.StateWorking || shot.CompletionMenu || current.State != supervisor.SessionIdle && current.State != supervisor.SessionWorking {
				return false, false
			}
			if u.Observed != nil {
				state, ok := u.Observed.Get(in.SessionID)
				if !ok || state != detect.StateIdle && state != detect.StateWorking {
					return false, false
				}
			}
		}
		body, want, ok := enterInputText(shot, message, operator)
		return ok && body == want, ok && strings.HasPrefix(want, body)
	}
	match, prefix := check(shot)
	if match || !prefix {
		return match
	}
	limit := u.EchoWait
	if limit <= 0 {
		limit = supervisor.SupervisorSendEchoWait
	}
	deadline := time.Now().Add(limit)
	timer := time.NewTimer(limit)
	defer timer.Stop()
	poll := time.NewTicker(supervisor.SupervisorStopPollInterval)
	defer poll.Stop()
	baseline, last := shot.Observation, shot.Observation
	for {
		expired := false
		select {
		case <-ctx.Done():
			return false
		case <-poll.C:
		case <-timer.C:
			expired = true
		case notification := <-ch:
			for _, c := range append(notification.earlier, notification) {
				if c.SessionID != in.SessionID || c.SendID != sendID {
					continue
				}
				if c.ProcessExited {
					return false
				}
				if c.Screen.Observation <= last {
					continue
				}
				_, prefix = check(c.Screen)
				if !prefix {
					return false
				}
				shot, last = c.Screen, c.Screen.Observation
			}
		}
		// A stream of notifications must not postpone the deadline.
		expired = expired || !time.Now().Before(deadline)
		// Recheck STOP and session state even with no output. At the deadline,
		// read the latest screen once more, including updates not yet notified.
		latest := u.Screens.Snapshot(in.SessionID)
		if latest.Observation >= last {
			shot, last = latest, latest.Observation
		}
		match, prefix = check(shot)
		if !prefix {
			return false
		}
		if match && shot.Observation > baseline {
			return true
		}
		if expired {
			return false
		}
	}
}

func screenSafeForEnter(s ScreenSnapshot, want string) bool {
	if s.Processing || s.State == detect.StateAwaitingPermission || s.State == detect.StateExited {
		return false
	}
	return !hasSelectionMenu(s.Rows) || strings.HasPrefix(want, "/") && s.CompletionMenu && completionCandidateMatches(s.SelectedCompletion, want) && !hasPermissionDialog(s.Rows)
}

func enterInputText(s ScreenSnapshot, want string, operator bool) (string, string, bool) {
	for i := len(s.Rows) - 1; i >= 0; i-- {
		if strings.HasPrefix(s.Rows[i], "❯\u00a0") {
			body := strings.TrimPrefix(s.Rows[i], "❯\u00a0")
			if operator {
				// Preserve leading spaces: a changed operator text must abort.
				return strings.TrimRight(body, " "), strings.TrimRight(want, " "), true
			}
			return strings.TrimSpace(body), want, true
		}
	}
	return "", want, false
}

func screenReadyForEnter(s ScreenSnapshot, want string, operatorInput ...bool) bool {
	if !screenSafeForEnter(s, want) {
		return false
	}
	body, expected, ok := enterInputText(s, want, len(operatorInput) != 0 && operatorInput[0])
	return ok && body == expected
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

// handoffExitSend is transient. EndSession intentionally leaves it for the sender
// to collect. The owner is the managed PTY itself, never a screen timestamp.
type exitWriteState uint8

const (
	exitBeforeCR exitWriteState = iota
	exitWritingCR
	exitCRSucceeded
	exitCRFailed
)

type exitResultState uint8

const (
	exitUndecided exitResultState = iota
	exitConfirmed
	exitUnconfirmed
)

type handoffExitSend struct {
	pending       supervisor.PendingSend
	in            SendInput
	owner         any
	marked        bool
	write         exitWriteState
	exited        bool
	exitAfterMark bool
	result        exitResultState
	saving        *exitCommit
}
type exitCommit struct {
	record    *handoffExitSend
	done      chan struct{}
	result    exitResultState
	confirmed bool
	event     *store.Event
}

var errExitSavePending = errors.New("handoff exit result save pending")

func handoffPTYOwner(pty any) any {
	if observed, ok := pty.(observedPTY); ok {
		pty = observed.ManagedPTY
	}
	// Managed PTYs are pointers. Refuse ownerless/value-only stand-ins rather than
	// accidentally treating equal values as the same child generation.
	if pty == nil || reflect.ValueOf(pty).Kind() != reflect.Ptr || reflect.ValueOf(pty).IsNil() {
		return nil
	}
	return pty
}
func (u *SendInputUseCase) collectExit(id supervisor.SessionID, r *handoffExitSend) {
	if r == nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.exitSends[id] == r && u.exitSends[id].owner == r.owner {
		delete(u.exitSends, id)
	}
}
func (u *SendInputUseCase) beginExitCR(id supervisor.SessionID, r *handoffExitSend) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.exitSends[id] != r || r.owner == nil || r.exited || r.write != exitBeforeCR {
		return false
	}
	r.marked = true
	r.write = exitWritingCR
	return true
}
func (u *SendInputUseCase) endExitCR(id supervisor.SessionID, r *handoffExitSend, success bool) {
	if r == nil {
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.exitSends[id] != r || !r.marked {
		return
	}
	r.write = exitCRFailed
	if success {
		r.write = exitCRSucceeded
	}
}

// updateExitSession keeps store-lock -> u.mu ordering. A reservation is not a
// published result. A concurrent updater first waits for the session write, then
// retries after its owner has completed bookkeeping outside the store lock.
func (u *SendInputUseCase) updateExitSession(sessions SessionRecordStore, id supervisor.SessionID, update func(*supervisor.ManagedSession, *exitCommit) error) (*exitCommit, error) {
	for {
		tx := &exitCommit{}
		var waiting <-chan struct{}
		err := UpdateSession(sessions, id, func(v *supervisor.ManagedSession) error {
			u.mu.Lock()
			if r := u.exitSends[id]; r != nil && r.saving != nil {
				waiting = r.saving.done
			}
			u.mu.Unlock()
			if waiting != nil {
				return errExitSavePending
			}
			return update(v, tx)
		})
		if errors.Is(err, errExitSavePending) && waiting != nil {
			<-waiting
			continue
		}
		if tx.record != nil {
			u.mu.Lock()
			if err == nil {
				tx.record.result = tx.result
			}
			tx.record.saving = nil
			u.mu.Unlock()
			close(tx.done)
		}
		if err != nil {
			tx.confirmed = false
			tx.event = nil
		}
		return tx, err
	}
}

// decideExit runs only inside a session update. The caller supplies a PTY owner
// and send ID on both registration and retrieval; notifications alone are hints.
func (u *SendInputUseCase) decideExit(v *supervisor.ManagedSession, p supervisor.PendingSend, owner any, expired bool, tx *exitCommit) {
	at := u.now()
	if v.LastSend != nil && v.LastSend.SendID == p.SendID && v.LastSend.Kind == p.Kind && v.LastSend.Confirmed {
		tx.confirmed = true
		return
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	r := u.exitSends[v.SessionID]
	if r == nil || owner == nil || r.owner != owner || r.pending.SendID != p.SendID {
		return
	}
	if r.result != exitUndecided {
		return
	}
	if r.marked && r.write == exitCRSucceeded {
		// LastSend's presence for this exact ID/kind is the durable CR-success proof.
		v.LastSend = &supervisor.LastSendRecord{SendID: p.SendID, Kind: p.Kind, IssuedAt: p.IssuedAt, Confirmed: false}
	}
	confirmed := !expired && r.marked && r.write == exitCRSucceeded && r.exited && r.exitAfterMark
	if !confirmed && !expired {
		return
	}
	tx.record = r
	tx.done = make(chan struct{})
	tx.result = exitUnconfirmed
	kind, code := supervisor.EventKindSendUnconfirmed, "SUP_SEND_NOT_CONFIRMED"
	if confirmed {
		tx.result = exitConfirmed
		tx.confirmed = true
		kind, code = supervisor.EventKindSendConfirmed, ""
		applySendConfirmation(v, p, at)
	} else if r.in.Origin != supervisor.SendOriginHandoff && v.State != supervisor.SessionStopped {
		v.State = supervisor.SessionStopped
		v.StopReason = supervisor.StopSendNotConfirmed
	}
	event := sendEvent(kind, r.in, p, code)
	tx.event = &event
	r.saving = tx
}
func (u *SendInputUseCase) observeExit(v *supervisor.ManagedSession, p supervisor.PendingSend, owner any, tx *exitCommit) {
	u.mu.Lock()
	r := u.exitSends[v.SessionID]
	if r == nil || owner == nil || r.owner != owner || r.pending.SendID != p.SendID {
		u.mu.Unlock()
		return
	}
	if !r.exited {
		r.exited = true
		r.exitAfterMark = r.marked
	}
	u.mu.Unlock()
	u.decideExit(v, p, owner, false, tx)
}
func (u *SendInputUseCase) resolveExit(in SendInput, p supervisor.PendingSend, owner any, expired bool) (SendResult, error) {
	tx, err := u.updateExitSession(u.Sessions, in.SessionID, func(v *supervisor.ManagedSession, tx *exitCommit) error {
		u.decideExit(v, p, owner, expired, tx)
		return nil
	})
	result := SendResult{SendID: p.SendID, Confirmed: tx.confirmed}
	if err != nil {
		return result, err
	}
	if tx.event != nil {
		err = u.record(*tx.event)
		if !tx.confirmed && in.Origin != supervisor.SendOriginHandoff && u.Notify != nil {
			u.Notify(supervisor.EventKindSendUnconfirmed, in.SessionID.String())
		}
	}
	return result, err
}
func (u *SendInputUseCase) expireExit(in SendInput, p supervisor.PendingSend, owner any, cause error) (SendResult, error) {
	result, err := u.resolveExit(in, p, owner, true)
	if result.Confirmed {
		return result, err
	}
	return result, errors.Join(cause, err)
}

func applySendConfirmation(v *supervisor.ManagedSession, p supervisor.PendingSend, at time.Time) {
	v.SendGate.Pending = nil
	v.SendGate.EverSent = true
	v.SendGate.LastSendConfirmed = true
	v.SendGate.LastConfirmedAt = &at
	v.SendGate.UnconfirmedDiscarded = false
	if p.Kind == supervisor.SendKindRemoteControl {
		v.RemoteControlConfirmed = true
	}
	v.LastSend = &supervisor.LastSendRecord{SendID: p.SendID, Kind: p.Kind, IssuedAt: p.IssuedAt, Confirmed: true}
}

func (u *SendInputUseCase) exitOwner(id supervisor.SessionID, sendID string) any {
	u.mu.Lock()
	defer u.mu.Unlock()
	if r := u.exitSends[id]; r != nil && r.pending.SendID == sendID {
		return r.owner
	}
	return nil
}
