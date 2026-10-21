package usecase

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
)

func waitCommitState(t *testing.T, sessions SessionRecordStore, id supervisor.SessionID, want supervisor.SupervisorSessionState) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		current, err := sessions.Load(id)
		if err != nil {
			t.Fatal(err)
		}
		if current.State == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("session did not reach %s", want)
		case <-time.After(time.Millisecond):
		}
	}
}

func armAndSettleIdle(t *testing.T, sessions SessionRecordStore, id supervisor.SessionID, detector *changingDetector) time.Time {
	t.Helper()
	waitCommitState(t, sessions, id, supervisor.SessionWorking)
	detector.set(detect.StateIdle)
	waitCommitState(t, sessions, id, supervisor.SessionIdle)
	current, err := sessions.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	current.StateSince = time.Now().Add(-2 * commitSettle)
	if err := sessions.Save(current); err != nil {
		t.Fatal(err)
	}
	return current.StateSince
}

func TestObserveCommitEvaluatesEachSettledIdleStay(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionIdle)
	detector := &changingDetector{}
	detector.set(detect.StateWorking)
	commits := make(chan struct{}, 3)
	autoSends := make(chan string, 1)
	auto := &AutoContinueUseCase{Send: observeAutoSender{events: autoSends}}
	u := ObserveSessionUseCase{
		Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: sessions,
		AutoContinue: auto, Interval: time.Millisecond,
		EvaluateCommit: func(context.Context, supervisor.SessionID) error {
			commits <- struct{}{}
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	pty := newObserveSendPTY()
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	defer func() {
		cancel()
		_ = pty.Close()
		if err := <-done; err != nil && err != context.Canceled {
			t.Error(err)
		}
	}()

	waitCommit := func() {
		t.Helper()
		select {
		case <-commits:
		case <-time.After(time.Second):
			t.Fatal("settled idle stay did not evaluate commit")
		}
	}
	armAndSettleIdle(t, sessions, s.SessionID, detector)
	waitCommit()
	// Give the observer multiple ticks to expose a duplicate evaluation.
	time.Sleep(20 * time.Millisecond)
	select {
	case <-commits:
		t.Fatal("commit evaluated twice in the same idle stay")
	default:
	}
	detector.set(detect.StateWorking)
	waitCommitState(t, sessions, s.SessionID, supervisor.SessionWorking)
	detector.set(detect.StateIdle)
	waitCommitState(t, sessions, s.SessionID, supervisor.SessionIdle)
	current, err := sessions.Load(s.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	current.StateSince = time.Now().Add(-2 * commitSettle)
	if err := sessions.Save(current); err != nil {
		t.Fatal(err)
	}
	waitCommit()
	time.Sleep(20 * time.Millisecond)
	select {
	case <-commits:
		t.Fatal("commit evaluated twice in the new idle stay")
	default:
	}
	select {
	case <-autoSends:
		t.Fatal("auto-continue sent input during idle")
	default:
	}
	auto.mu.Lock()
	started := auto.inflight != nil || auto.states != nil
	auto.mu.Unlock()
	if started {
		t.Fatal("auto-continue started during idle")
	}
}

func TestObserveIdleWithoutAutoContinueEvaluatesCommit(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionIdle)
	detector := &changingDetector{}
	detector.set(detect.StateWorking)
	var calls atomic.Int32
	called := make(chan struct{}, 1)
	u := ObserveSessionUseCase{
		Detect:   DetectSessionStateUseCase{Detector: detector},
		Sessions: sessions, Interval: time.Millisecond,
		EvaluateCommit: func(context.Context, supervisor.SessionID) error {
			calls.Add(1)
			called <- struct{}{}
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	pty := newObserveSendPTY()
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	defer func() { cancel(); _ = pty.Close(); <-done }()
	armAndSettleIdle(t, sessions, s.SessionID, detector)
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("commit evaluation requires auto-continue")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("commit evaluations=%d, want 1", got)
	}
}

func TestObserveCommitWaitsForIdleEvaluationBeforeRecommendationEvaluation(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionIdle)
	detector := &changingDetector{}
	detector.set(detect.StateWorking)
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseFirst:
		default:
			close(releaseFirst)
		}
	})
	secondStarted := make(chan struct{}, 1)
	var calls atomic.Int32
	u := ObserveSessionUseCase{
		Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: sessions, Interval: time.Millisecond,
		EvaluateCommit: func(ctx context.Context, _ supervisor.SessionID) error {
			if calls.Add(1) == 1 {
				close(firstStarted)
				select {
				case <-releaseFirst:
				case <-ctx.Done():
					return ctx.Err()
				}
			} else {
				secondStarted <- struct{}{}
			}
			return nil
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	pty := newObserveSendPTY()
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	defer func() { cancel(); _ = pty.Close(); <-done }()
	armAndSettleIdle(t, sessions, s.SessionID, detector)
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("idle commit evaluation did not start")
	}
	detector.set(detect.StateWorking)
	waitCommitState(t, sessions, s.SessionID, supervisor.SessionWorking)
	detector.set(detect.StateAwaitingRecommendation)
	waitCommitState(t, sessions, s.SessionID, supervisor.SessionAwaitingRecommendation)
	current, err := sessions.Load(s.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	current.StateSince = time.Now().Add(-2 * commitSettle)
	if err := sessions.Save(current); err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondStarted:
		t.Fatal("recommendation commit evaluation overlapped the idle evaluation")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("recommendation commit evaluation did not run after idle evaluation")
	}
}

func TestObserveIdleCommitVerifiedClosesGitPhase(t *testing.T) {
	commit, fixture, _, _, _ := commitTestFixture(t)
	s, sessions, _ := observeSendFixture(t, supervisor.SessionIdle)
	s.GitConfigFingerprint = fixture.value.GitConfigFingerprint
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	commit.Sessions = sessions
	detector := &changingDetector{}
	detector.set(detect.StateWorking)
	completed := make(chan time.Time, 1)
	commit.AfterGitPhase = func(id supervisor.SessionID, since time.Time) {
		if id != s.SessionID {
			t.Errorf("completion session=%s, want %s", id, s.SessionID)
		}
		completed <- since
	}
	u := ObserveSessionUseCase{
		Detect:   DetectSessionStateUseCase{Detector: detector},
		Sessions: sessions, CommitVerified: commit, Interval: time.Millisecond,
	}
	ctx, cancel := context.WithCancel(context.Background())
	pty := newObserveSendPTY()
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	defer func() { cancel(); _ = pty.Close(); <-done }()
	idleSince := armAndSettleIdle(t, sessions, s.SessionID, detector)
	select {
	case since := <-completed:
		if !since.Equal(idleSince) {
			t.Fatalf("completion stay=%v, want %v", since, idleSince)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("idle commit evaluation did not close git phase")
	}
	current, err := sessions.Load(s.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if current.GitPhase != supervisor.GitPhaseNone {
		t.Fatalf("git phase=%s, want none", current.GitPhase)
	}
}
