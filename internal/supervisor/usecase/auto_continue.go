package usecase

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/textpolicy"
)

// Canonical value: SUPERVISOR_AUTO_CONTINUE_MAX_CONSECUTIVE in validation-rules.md.
const autoContinueMaxConsecutive = 5

type recommendationHashContextKey struct{}

type autoContinueSender interface {
	Execute(context.Context, SendInput) (SendResult, error)
}
type AutoContinueInput struct {
	SessionID          supervisor.SessionID
	Rows               []string
	ContextPercent     *int
	EnteredFromWorking bool
	RequireSettle      bool
	Settled            bool
	StaySince          time.Time
}
type autoContinueState struct {
	hash          [32]byte
	hasHash       bool
	count         int
	issuedAt      time.Time
	eligibleSince time.Time
	resetAt       time.Time
	generation    uint64
}

// AutoContinueUseCase owns only process-memory duplicate and count state.
type AutoContinueUseCase struct {
	Sessions         SessionRecordStore
	Send             autoContinueSender
	Extractor        supervisor.RecommendationExtractor
	IsStopped        func(supervisor.SessionID) bool
	RecordStructured func(store.Event) error
	Notify           func(supervisor.SupervisorEventKind, string)
	bindOnce         sync.Once
	mu               sync.Mutex
	states           map[supervisor.SessionID]autoContinueState
	inflight         map[supervisor.SessionID]bool
	pending          map[supervisor.SessionID]autoContinueRequest
}

type autoContinueRequest struct {
	ctx    context.Context
	in     AutoContinueInput
	before func(context.Context, supervisor.SessionID) error
}

func (u *AutoContinueUseCase) Bind(sessions SessionRecordStore, send autoContinueSender, stopped func(supervisor.SessionID) bool, record func(store.Event) error, notify func(supervisor.SupervisorEventKind, string)) {
	u.bindOnce.Do(func() {
		if u.Sessions == nil {
			u.Sessions = sessions
		}
		if u.Send == nil {
			u.Send = send
		}
		if u.IsStopped == nil {
			u.IsStopped = stopped
		}
		if u.RecordStructured == nil {
			u.RecordStructured = record
		}
		if u.Notify == nil {
			u.Notify = notify
		}
	})
}

func (u *AutoContinueUseCase) state(id supervisor.SessionID) autoContinueState {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.states[id]
}
func (u *AutoContinueUseCase) Count(id supervisor.SessionID) int { return u.state(id).count }
func (u *AutoContinueUseCase) ClearCandidate(id supervisor.SessionID) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.states == nil {
		return
	}
	s := u.states[id]
	s.eligibleSince = time.Time{}
	u.states[id] = s
}
func (u *AutoContinueUseCase) Reset(id supervisor.SessionID) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.states == nil {
		return
	}
	state := u.states[id]
	state.count = 0
	if !state.issuedAt.IsZero() && !state.eligibleSince.After(state.issuedAt) {
		state.eligibleSince = time.Time{}
	}
	state.resetAt = time.Now()
	state.generation++
	u.states[id] = state
}
func (u *AutoContinueUseCase) RecordIssued(id supervisor.SessionID, hash [32]byte, at time.Time) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.states == nil {
		u.states = make(map[supervisor.SessionID]autoContinueState)
	}
	s := u.states[id]
	if at.Before(s.resetAt) {
		return
	}
	s.hash = hash
	s.hasHash = true
	s.issuedAt = at
	u.states[id] = s
}
func (u *AutoContinueUseCase) entered(id supervisor.SessionID, since time.Time) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.states == nil {
		u.states = make(map[supervisor.SessionID]autoContinueState)
	}
	s := u.states[id]
	if since.After(s.eligibleSince) {
		s.eligibleSince = since
	}
	u.states[id] = s
}
func (u *AutoContinueUseCase) increment(id supervisor.SessionID) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.states == nil {
		u.states = make(map[supervisor.SessionID]autoContinueState)
	}
	s := u.states[id]
	s.count++
	u.states[id] = s
}
func (u *AutoContinueUseCase) incrementIfGeneration(id supervisor.SessionID, generation uint64) {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := u.states[id]
	if s.generation != generation {
		return
	}
	if u.states == nil {
		u.states = make(map[supervisor.SessionID]autoContinueState)
	}
	s.count++
	u.states[id] = s
}
func (u *AutoContinueUseCase) begin(request autoContinueRequest) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	id := request.in.SessionID
	if u.inflight == nil {
		u.inflight = make(map[supervisor.SessionID]bool)
	}
	if u.inflight[id] {
		if u.pending == nil {
			u.pending = make(map[supervisor.SessionID]autoContinueRequest)
		}
		u.pending[id] = request
		return false
	}
	u.inflight[id] = true
	return true
}
func (u *AutoContinueUseCase) next(id supervisor.SessionID) (autoContinueRequest, bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	request, ok := u.pending[id]
	delete(u.pending, id)
	if ok {
		return request, true
	}
	delete(u.inflight, id)
	return autoContinueRequest{}, false
}

func (u *AutoContinueUseCase) run(request autoContinueRequest) error {
	for {
		var err error
		if request.before != nil {
			err = request.before(request.ctx, request.in.SessionID)
		}
		if err == nil {
			err = u.evaluate(request.ctx, request.in)
		}
		next, ok := u.next(request.in.SessionID)
		if !ok {
			return err
		}
		request = next
	}
}

// EvaluateAsync leaves the observer free to process the screen confirmation
// while the send gate waits for it.
func (u *AutoContinueUseCase) EvaluateAsync(ctx context.Context, in AutoContinueInput, before func(context.Context, supervisor.SessionID) error) {
	if in.EnteredFromWorking && u.Sessions != nil {
		if s, err := u.Sessions.Load(in.SessionID); err == nil && s.State == supervisor.SessionAwaitingRecommendation {
			u.entered(in.SessionID, s.StateSince)
		}
	}
	request := autoContinueRequest{ctx: ctx, in: in, before: before}
	if !u.begin(request) {
		return
	}
	go func() {
		if err := u.run(request); err != nil {
			slog.Warn("auto continuation evaluation failed", "error", err)
		}
	}()
}
func (u *AutoContinueUseCase) Evaluate(ctx context.Context, in AutoContinueInput) error {
	if in.EnteredFromWorking && u.Sessions != nil {
		if s, err := u.Sessions.Load(in.SessionID); err == nil && s.State == supervisor.SessionAwaitingRecommendation {
			u.entered(in.SessionID, s.StateSince)
		}
	}
	request := autoContinueRequest{ctx: ctx, in: in}
	if !u.begin(request) {
		return nil
	}
	return u.run(request)
}
func (u *AutoContinueUseCase) evaluate(ctx context.Context, in AutoContinueInput) error {
	if u.Sessions == nil {
		return nil
	}
	s, err := u.Sessions.Load(in.SessionID)
	if err != nil {
		return err
	}
	if s.State != supervisor.SessionAwaitingRecommendation {
		return nil
	}
	if !in.StaySince.IsZero() && !in.StaySince.Equal(s.StateSince) {
		return nil
	}
	state := u.state(in.SessionID)
	if u.IsStopped != nil && u.IsStopped(in.SessionID) {
		return nil
	}
	if s.StopScope != supervisor.StopScopeNone && s.StopScope != "" {
		return nil
	}
	if in.ContextPercent == nil || *in.ContextPercent > rotationContextPercent {
		return nil
	}
	if s.GitPhase != supervisor.GitPhaseNone && s.GitPhase != "" || s.AutoContinueDisabledReason != "" || s.Rotation != nil {
		return nil
	}
	if in.RequireSettle && !in.Settled {
		return nil
	}
	// A candidate must have entered from working after the previous send began.
	if state.eligibleSince.IsZero() || !state.eligibleSince.After(state.issuedAt) {
		return nil
	}
	body, ok := u.Extractor.Extract(in.Rows)
	if !ok {
		return u.block(ctx, in.SessionID, s.StateSince, store.ReasonUndetermined, supervisor.StopRecommendationUndetermined, "SUP_RECOMMENDATION_UNDETERMINED")
	}
	hash := sha256.Sum256([]byte(textpolicy.NormalizeRecommendationForHash(body)))
	if state.hasHash && hash == state.hash {
		return u.block(ctx, in.SessionID, s.StateSince, store.ReasonRepeated, supervisor.StopRecommendationRepeated, "SUP_RECOMMENDATION_REPEATED")
	}
	categories, unknown := textpolicy.Classify(body)
	if unknown {
		return u.block(ctx, in.SessionID, s.StateSince, store.ReasonUndetermined, supervisor.StopRecommendationUndetermined, "SUP_RECOMMENDATION_UNDETERMINED")
	}
	if len(categories) > 0 {
		return u.block(ctx, in.SessionID, s.StateSince, store.EventReason(categories[0]), supervisor.StopRecommendationForbidden, "SUP_RECOMMENDATION_FORBIDDEN")
	}
	if state.count >= autoContinueMaxConsecutive {
		return u.block(ctx, in.SessionID, s.StateSince, store.ReasonConsecutiveLimit, supervisor.StopAutoContinueLimit, "SUP_AUTO_CONTINUE_LIMIT_REACHED")
	}
	if u.Send == nil {
		return nil
	}
	if u.state(in.SessionID).generation != state.generation {
		return nil
	}
	result, err := u.Send.Execute(context.WithValue(ctx, recommendationHashContextKey{}, hash), SendInput{SessionID: in.SessionID, Kind: supervisor.SendKindRecommendationContinue})
	if err != nil {
		return err
	}
	if result.Confirmed {
		u.incrementIfGeneration(in.SessionID, state.generation)
	}
	return nil
}
func (u *AutoContinueUseCase) block(ctx context.Context, id supervisor.SessionID, since time.Time, reason store.EventReason, stop supervisor.SupervisorStopReason, code string) error {
	blocked := false
	if err := UpdateSession(u.Sessions, id, func(s *supervisor.ManagedSession) error {
		if s.State == supervisor.SessionAwaitingRecommendation && s.StateSince.Equal(since) {
			s.PreviousState = s.State
			if err := s.Apply(supervisor.EventStop, supervisor.TransitionContext{StopReason: stop}, time.Now()); err != nil {
				return err
			}
			blocked = true
		}
		return nil
	}); err != nil {
		return err
	}
	if !blocked {
		return nil
	}
	if u.RecordStructured != nil {
		if err := u.RecordStructured(store.Event{Kind: supervisor.EventKindRecommendationBlocked, SessionID: id.String(), Reason: reason, Code: code}); err != nil {
			return err
		}
	}
	if u.Notify != nil {
		u.Notify(supervisor.EventKindRecommendationBlocked, id.String())
	}
	return nil
}
