package usecase

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/textpolicy"
)

type changingDetector struct{ state atomic.Value }

func (d *changingDetector) set(state detect.DetectedState) { d.state.Store(state) }
func (d *changingDetector) Detect(context.Context, detect.DetectInput) (detect.DetectResult, error) {
	state := d.state.Load().(detect.DetectedState)
	return detect.DetectResult{State: state, ProcessingIndicator: state == detect.StateWorking, ContextPercent: ptrInt(20)}, nil
}

type observeStopRepository struct {
	sessions *store.SessionStore
	id       supervisor.SessionID
}

func (r observeStopRepository) List(context.Context) ([]supervisor.ManagedSession, error) {
	s, err := r.sessions.Load(r.id)
	return []supervisor.ManagedSession{s}, err
}
func (r observeStopRepository) Save(_ context.Context, s supervisor.ManagedSession) error {
	return r.sessions.Save(s)
}

func waitObservedState(t *testing.T, states *ObservedStateStore, id supervisor.SessionID, want detect.DetectedState) {
	t.Helper()
	deadline := time.After(time.Second)
	for {
		if state, ok := states.Get(id); ok && state == want {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("detector did not reach %s", want)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

type autoSender struct {
	u     *AutoContinueUseCase
	sends int
}

type observeAutoSender struct{ events chan string }

func (s observeAutoSender) Execute(context.Context, SendInput) (SendResult, error) {
	s.events <- "send"
	return SendResult{Confirmed: true}, nil
}

func (s *autoSender) Execute(ctx context.Context, in SendInput) (SendResult, error) {
	s.sends++
	s.u.RecordIssued(in.SessionID, ctx.Value(recommendationHashContextKey{}).([32]byte), time.Now())
	return SendResult{SendID: "send", Confirmed: true}, nil
}

type autoSessions struct{ session supervisor.ManagedSession }

func (s *autoSessions) Load(supervisor.SessionID) (supervisor.ManagedSession, error) {
	return s.session, nil
}
func (s *autoSessions) Save(v supervisor.ManagedSession) error { s.session = v; return nil }

func autoFixture() (*AutoContinueUseCase, *autoSessions, *autoSender, *[]store.Event) {
	id := supervisor.SessionID("auto-test-1")
	now := time.Now()
	s := &autoSessions{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionAwaitingRecommendation, StateSince: now, GitPhase: supervisor.GitPhaseNone}}
	events := &[]store.Event{}
	u := &AutoContinueUseCase{Sessions: s, Extractor: supervisor.RecommendationExtractor{PreviousInput: "❯ 前の入力", RecommendationStart: "推奨:", InputTop: "────"}, RecordStructured: func(e store.Event) error { *events = append(*events, e); return nil }}
	sender := &autoSender{u: u}
	u.Send = sender
	return u, s, sender, events
}

func TestObservedWideTextIsClassifiedBeforeAutoContinue(t *testing.T) {
	for _, forbidden := range []string{"本番にデプロイ", "force push"} {
		t.Run(forbidden, func(t *testing.T) {
			vt := supervisor.NewVirtualTerminal(80, 12)
			lines := []string{"❯ synthetic input", "⏺ " + forbidden, "✻ Cooked for 2s · done 10:00", "────────", "❯\u00a0", "────────"}
			for i, line := range lines {
				_, _ = vt.Write([]byte(fmt.Sprintf("\x1b[%d;1H%s", i+1, line)))
			}
			body, ok := (supervisor.RecommendationExtractor{ObservedScreen: true}).Extract(vt.TextRows())
			if !ok || !strings.Contains(body, forbidden) {
				t.Fatalf("body=%q ok=%v", body, ok)
			}
			categories, unknown := textpolicy.Classify(body)
			if unknown || len(categories) == 0 {
				t.Fatalf("categories=%v unknown=%v", categories, unknown)
			}
		})
	}
}
func autoInput(body string) AutoContinueInput {
	return AutoContinueInput{SessionID: supervisor.SessionID("auto-test-1"), Rows: []string{"❯ 前の入力", "推奨:", body, "────", "❯\u00a0"}, ContextPercent: ptrInt(20), EnteredFromWorking: true}
}
func ptrInt(v int) *int { return &v }

func TestAutoContinueZeroValueStateWrites(t *testing.T) {
	id := supervisor.SessionID("zero-value")
	t.Run("ClearCandidate", func(t *testing.T) {
		u := &AutoContinueUseCase{}
		u.ClearCandidate(id)
		if u.states != nil {
			t.Fatal("ClearCandidate created state for an absent candidate")
		}
	})
	t.Run("increment", func(t *testing.T) {
		u := &AutoContinueUseCase{}
		u.increment(id)
		if got := u.Count(id); got != 1 {
			t.Fatalf("count=%d; want 1", got)
		}
	})
	t.Run("incrementIfGeneration", func(t *testing.T) {
		u := &AutoContinueUseCase{}
		u.incrementIfGeneration(id, 0)
		if got := u.Count(id); got != 1 {
			t.Fatalf("count=%d; want 1", got)
		}
	})
}

func TestAutoContinue_SCN09_SCN10_SCN11_SCN12_SafeAndFailClosed(t *testing.T) {
	for _, tc := range []struct {
		body     string
		wantSend int
		reason   store.EventReason
	}{
		{"テストを追加する", 1, ""},
		{"force push する", 0, store.ReasonForcePush},
		{"PR を作成する", 0, store.ReasonPullRequest},
		{"本番に deploy する", 0, store.ReasonDeploy},
		{"ファイルを削除する", 0, store.ReasonDeletion},
		{"production を更新する", 0, store.ReasonProduction},
		{"有料プランへ変更する", 0, store.ReasonBilling},
	} {
		u, s, sender, events := autoFixture()
		if err := u.Evaluate(context.Background(), autoInput(tc.body)); err != nil {
			t.Fatal(err)
		}
		if sender.sends != tc.wantSend {
			t.Errorf("%q sends=%d", tc.body, sender.sends)
		}
		if tc.reason != "" && (s.session.StopReason != supervisor.StopRecommendationForbidden || len(*events) == 0 || (*events)[0].Reason != tc.reason) {
			t.Errorf("%q stop=%s events=%v", tc.body, s.session.StopReason, *events)
		}
	}
	u, s, sender, events := autoFixture()
	u.Extractor = supervisor.RecommendationExtractor{}
	if err := u.Evaluate(context.Background(), autoInput("テストを追加する")); err != nil {
		t.Fatal(err)
	}
	if sender.sends != 0 || s.session.StopReason != supervisor.StopRecommendationUndetermined || (*events)[0].Reason != store.ReasonUndetermined {
		t.Fatalf("fail closed: sends=%d state=%+v events=%v", sender.sends, s.session, *events)
	}
}
func TestAutoContinue_SCN18_SCN27_SCN37_Wait(t *testing.T) {
	for _, change := range []func(*AutoContinueInput, *autoSessions){
		func(in *AutoContinueInput, _ *autoSessions) { in.ContextPercent = ptrInt(rotationContextPercent + 1) },
		func(in *AutoContinueInput, _ *autoSessions) { in.ContextPercent = nil },
		func(_ *AutoContinueInput, s *autoSessions) { s.session.GitPhase = supervisor.GitPhaseEvaluating },
		func(_ *AutoContinueInput, s *autoSessions) {
			s.session.AutoContinueDisabledReason = supervisor.AutoContinueDisabledPermissionModeUnsafe
		},
		func(in *AutoContinueInput, _ *autoSessions) { in.Settled = false; in.RequireSettle = true },
	} {
		u, s, sender, _ := autoFixture()
		in := autoInput("テストを追加する")
		change(&in, s)
		if err := u.Evaluate(context.Background(), in); err != nil {
			t.Fatal(err)
		}
		if sender.sends != 0 || s.session.State != supervisor.SessionAwaitingRecommendation {
			t.Fatalf("wait failed: %+v", s.session)
		}
	}
}
func TestAutoContinue_SCN25_SCN26_SCN36_RepeatLimitAndReset(t *testing.T) {
	u, s, sender, events := autoFixture()
	if err := u.Evaluate(context.Background(), autoInput("次へ 進める")); err != nil {
		t.Fatal(err)
	}
	s.session.State = supervisor.SessionAwaitingRecommendation
	s.session.StateSince = time.Now().Add(time.Second)
	in := autoInput("次へ　 進める")
	if err := u.Evaluate(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if sender.sends != 1 || s.session.StopReason != supervisor.StopRecommendationRepeated || (*events)[0].Reason != store.ReasonRepeated {
		t.Fatalf("repeat sends=%d stop=%s", sender.sends, s.session.StopReason)
	}
	u.Reset(s.session.SessionID)
	if u.Count(s.session.SessionID) != 0 {
		t.Fatal("reset count")
	}
}

func TestAutoContinue_SCN26_ConsecutiveLimit(t *testing.T) {
	u, s, sender, events := autoFixture()
	for i := 0; i < autoContinueMaxConsecutive; i++ {
		s.session.StateSince = time.Now().Add(time.Duration(i+1) * time.Second)
		if err := u.Evaluate(context.Background(), autoInput(fmt.Sprintf("安全な手順 %d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if sender.sends != autoContinueMaxConsecutive || u.Count(s.session.SessionID) != autoContinueMaxConsecutive {
		t.Fatalf("sends=%d count=%d", sender.sends, u.Count(s.session.SessionID))
	}
	s.session.StateSince = time.Now().Add(time.Hour)
	if err := u.Evaluate(context.Background(), autoInput("さらに安全な手順")); err != nil {
		t.Fatal(err)
	}
	if sender.sends != autoContinueMaxConsecutive || s.session.StopReason != supervisor.StopAutoContinueLimit || (*events)[0].Reason != store.ReasonConsecutiveLimit {
		t.Fatalf("limit sends=%d stop=%s events=%v", sender.sends, s.session.StopReason, *events)
	}
	u.Reset(s.session.SessionID)
	if u.Count(s.session.SessionID) != 0 {
		t.Fatal("resume or rotation reset failed")
	}
}
func TestAutoContinue_SCN36_OldCandidateAfterStop(t *testing.T) {
	u, s, sender, _ := autoFixture()
	u.RecordIssued(s.session.SessionID, sha256.Sum256([]byte("earlier")), s.session.StateSince.Add(time.Second))
	in := autoInput("安全な手順")
	in.EnteredFromWorking = false
	if err := u.Evaluate(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if sender.sends != 0 {
		t.Fatal("old recommendation sent")
	}
	s.session.StateSince = time.Now().Add(time.Hour)
	in.EnteredFromWorking = true
	if err := u.Evaluate(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if sender.sends != 1 {
		t.Fatal("new recommendation not sent")
	}
}

func TestAutoContinue_SCN36_HashRecordedAtSendIssued(t *testing.T) {
	id := supervisor.SessionID("auto-issued-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionAwaitingRecommendation, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	shot := blankScreen()
	shot.State = detect.StateAwaitingRecommendation
	screen := &sendTestScreen{shot: shot}
	hash := sha256.Sum256([]byte("normalized recommendation"))
	recordedBeforeWrite := false
	sender := &SendInputUseCase{Sessions: s, Screens: screen, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
	sender.BindRecommendationIssued(func(gotID supervisor.SessionID, gotHash [32]byte, at time.Time) {
		if gotID != id || gotHash != hash || at.IsZero() {
			t.Errorf("issued hash mismatch")
		}
		recordedBeforeWrite = true
	})
	sender.PTY = &sendTestPTY{onWrite: func() {
		if !recordedBeforeWrite {
			t.Error("PTY write preceded hash record")
		}
		echoScreen(screen, supervisor.RecommendationContinueText)
	}}
	_, _ = sender.Execute(context.WithValue(context.Background(), recommendationHashContextKey{}, hash), SendInput{SessionID: id, Kind: supervisor.SendKindRecommendationContinue})
	if !recordedBeforeWrite {
		t.Fatal("hash was not recorded")
	}
}

func TestAutoContinue_SCN37_ContextUnknownSharedWithRotation(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionWorking)
	records := make(chan supervisor.SupervisorEventKind, 4)
	auto := &AutoContinueUseCase{}
	observed := &ObservedStateStore{}
	detector := &switchableRecoveryDetector{}
	detector.set(detect.StateWorking, true)
	u := ObserveSessionUseCase{
		Detect:   DetectSessionStateUseCase{Detector: detector},
		Sessions: sessions, Observed: observed, AutoContinue: auto,
		Rotation: &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }},
		RecordStructured: func(e store.Event) error {
			if e.Kind == supervisor.EventKindContextUnknown {
				records <- e.Kind
			}
			return nil
		},
		Interval: time.Millisecond,
	}
	pty := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	defer func() {
		cancel()
		_ = pty.Close()
		if err := <-done; err != nil && err != context.Canceled {
			t.Error(err)
		}
	}()
	waitObservedState(t, observed, s.SessionID, detect.StateWorking)
	select {
	case <-records:
	case <-time.After(time.Second):
		t.Fatal("context_unknown missing")
	}
	detector.set(detect.StateAwaitingRecommendation, false)
	waitObservedState(t, observed, s.SessionID, detect.StateAwaitingRecommendation)
	select {
	case <-records:
	case <-time.After(time.Second):
		t.Fatal("context_unknown missing after recommendation")
	}
	select {
	case <-records:
		t.Fatal("context_unknown duplicated")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestAutoContinue_SCN25_SCN26_ResetKeepsDuplicateHashAndDropsOldIssue(t *testing.T) {
	u, s, _, _ := autoFixture()
	hash := sha256.Sum256([]byte("normalized"))
	issuedAt := time.Now()
	u.RecordIssued(s.session.SessionID, hash, issuedAt)
	u.increment(s.session.SessionID)
	u.Reset(s.session.SessionID)
	state := u.state(s.session.SessionID)
	if state.count != 0 || !state.hasHash || state.hash != hash || !state.eligibleSince.IsZero() {
		t.Fatalf("reset lost duplicate guard or retained candidate: %+v", state)
	}
	other := sha256.Sum256([]byte("old issue"))
	u.RecordIssued(s.session.SessionID, other, issuedAt.Add(-time.Second))
	if u.state(s.session.SessionID).hash != hash {
		t.Fatal("late old send replaced hash")
	}
}

func TestAutoContinue_SCN26_RotationCompletionResetsCount(t *testing.T) {
	auto, sessions, _, _ := autoFixture()
	auto.RecordIssued(sessions.session.SessionID, sha256.Sum256([]byte("prior")), time.Now())
	auto.increment(sessions.session.SessionID)
	sessions.session.State = supervisor.SessionRotating
	sessions.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendParallelInstruction}
	sessions.session.LastSend = &supervisor.LastSendRecord{Kind: supervisor.SendKindParallelInstruction, Confirmed: true}
	rotation := &RotateSessionUseCase{
		Sessions: sessions, AutoContinue: auto,
		ChildAlive: func(supervisor.SessionID) bool { return true },
		Observed:   func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle },
		Now:        time.Now,
	}
	if err := rotation.sendParallel(context.Background(), sessions.session); err != nil {
		t.Fatal(err)
	}
	if auto.Count(sessions.session.SessionID) != 0 || sessions.session.Rotation != nil {
		t.Fatalf("rotation completion did not reset: %+v", sessions.session)
	}
}

type autoStopRepository struct{ sessions *autoSessions }

func (r autoStopRepository) List(context.Context) ([]supervisor.ManagedSession, error) {
	return []supervisor.ManagedSession{r.sessions.session}, nil
}
func (r autoStopRepository) Save(_ context.Context, s supervisor.ManagedSession) error {
	return r.sessions.Save(s)
}

func TestAutoContinue_SCN26_ResumeResetsCount(t *testing.T) {
	auto, sessions, _, _ := autoFixture()
	id := sessions.session.SessionID
	auto.RecordIssued(id, sha256.Sum256([]byte("prior")), time.Now())
	auto.increment(id)
	sessions.session.State = supervisor.SessionStopped
	sessions.session.StopReason = supervisor.StopSession
	sessions.session.StopScope = supervisor.StopScopeSession
	sessions.session.PreviousState = supervisor.SessionWorking
	stops := NewStopState(t.TempDir())
	if err := stops.EnableSession(id); err != nil {
		t.Fatal(err)
	}
	control := NewStopControlUseCase(stops, autoStopRepository{sessions}, nil, nil)
	control.SetAutoContinue(auto)
	control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{ChildAlive: true, ObservedState: supervisor.SessionWorking}, nil
	})
	out, err := control.Resume(context.Background(), &id, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Resumed) != 1 || auto.Count(id) != 0 {
		t.Fatalf("resume=%+v count=%d", out, auto.Count(id))
	}
}

func TestAutoContinue_SCN27_StopPrecedesClassification(t *testing.T) {
	u, sessions, sender, events := autoFixture()
	u.IsStopped = func(supervisor.SessionID) bool { return true }
	if err := u.Evaluate(context.Background(), autoInput("force push")); err != nil {
		t.Fatal(err)
	}
	if sender.sends != 0 || sessions.session.State != supervisor.SessionAwaitingRecommendation || len(*events) != 0 {
		t.Fatalf("STOP did not suppress evaluation: state=%s events=%v", sessions.session.State, *events)
	}
}

func TestAutoContinue_SCN37_EntryRecordedWhilePreviousEvaluationRuns(t *testing.T) {
	u, sessions, _, _ := autoFixture()
	id := sessions.session.SessionID
	entered := make(chan struct{})
	release := make(chan struct{})
	u.EvaluateAsync(context.Background(), autoInput("安全な手順"), func(context.Context, supervisor.SessionID) error {
		close(entered)
		<-release
		return nil
	})
	<-entered
	sessions.session.StateSince = sessions.session.StateSince.Add(time.Second)
	u.EvaluateAsync(context.Background(), autoInput("次の安全な手順"), nil)
	if got := u.state(id).eligibleSince; !got.Equal(sessions.session.StateSince) {
		t.Errorf("new entry lost while evaluation in flight: %v", got)
	}
	close(release)
}

func TestAutoContinue_SCN36_ResetRetainsNewCandidateAfterIssue(t *testing.T) {
	u, sessions, _, _ := autoFixture()
	id := sessions.session.SessionID
	issued := sessions.session.StateSince.Add(time.Second)
	u.RecordIssued(id, sha256.Sum256([]byte("prior")), issued)
	sessions.session.StateSince = issued.Add(time.Second)
	u.entered(id, sessions.session.StateSince)
	u.Reset(id)
	if got := u.state(id).eligibleSince; !got.Equal(sessions.session.StateSince) {
		t.Errorf("new candidate lost on reset: %v", got)
	}
}

func TestAutoContinue_SCN36_ResetRetainsCandidateWithoutIssue(t *testing.T) {
	u, sessions, sender, _ := autoFixture()
	id := sessions.session.SessionID
	u.entered(id, sessions.session.StateSince)
	u.Reset(id)
	in := autoInput("新しい安全な手順")
	in.EnteredFromWorking = false
	if err := u.Evaluate(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if sender.sends != 1 {
		t.Fatalf("new candidate after STOP was lost: sends=%d", sender.sends)
	}
}

func TestAutoContinue_SCN37_InFlightReevaluationIsRetained(t *testing.T) {
	u, sessions, _, _ := autoFixture()
	events := make(chan string, 1)
	u.Send = observeAutoSender{events: events}
	entered := make(chan struct{})
	release := make(chan struct{})
	old := autoInput("古い安全な手順")
	old.StaySince = sessions.session.StateSince
	u.EvaluateAsync(context.Background(), old, func(context.Context, supervisor.SessionID) error {
		close(entered)
		<-release
		return nil
	})
	<-entered
	sessions.session.StateSince = sessions.session.StateSince.Add(time.Second)
	next := autoInput("新しい安全な手順")
	next.StaySince = sessions.session.StateSince
	u.EvaluateAsync(context.Background(), next, nil)
	close(release)
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("reevaluation was dropped")
	}
}

func TestAutoContinue_SCN37_StaleBlockDoesNotStopOrRecord(t *testing.T) {
	u, sessions, _, events := autoFixture()
	staleSince := sessions.session.StateSince
	sessions.session.StateSince = staleSince.Add(time.Second)
	if err := u.block(context.Background(), sessions.session.SessionID, staleSince, store.ReasonUndetermined, supervisor.StopRecommendationUndetermined, "SUP_RECOMMENDATION_UNDETERMINED"); err != nil {
		t.Fatal(err)
	}
	if sessions.session.State != supervisor.SessionAwaitingRecommendation || len(*events) != 0 {
		t.Fatalf("stale block changed state or recorded event: %+v %v", sessions.session, *events)
	}
}

func TestObserve_SCN37_CommitEvaluatedOncePerStay(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionAwaitingRecommendation)
	s.StateSince = time.Now().Add(-2 * commitSettle)
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	called := make(chan struct{}, 8)
	u := ObserveSessionUseCase{
		Detect:   DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateAwaitingRecommendation, ProcessingIndicator: true}}},
		Sessions: sessions, AutoContinue: &AutoContinueUseCase{}, Interval: time.Millisecond,
		EvaluateCommit: func(context.Context, supervisor.SessionID) error { called <- struct{}{}; return nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	pty := newObserveSendPTY()
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("commit evaluation did not run")
	}
	time.Sleep(30 * time.Millisecond)
	if len(called) != 0 {
		t.Errorf("commit evaluation repeated %d times", len(called)+1)
	}
	cancel()
	_ = pty.Close()
	<-done
}

func TestObserve_SCN13_PermissionRecordErrorKeepsObserving(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionWorking)
	pty := newObserveSendPTY()
	called := make(chan struct{}, 1)
	var attempts atomic.Int32
	var logs bytes.Buffer
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(previousLogger)
	u := ObserveSessionUseCase{
		Detect:   DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateAwaitingPermission}}},
		Sessions: sessions, Interval: time.Millisecond,
		SendInput: &SendInputUseCase{Sessions: sessions, Screens: &ObservedScreenStore{}, PTY: pty},
		PermissionPending: &PermissionPendingUseCase{Record: func(supervisor.SessionID, supervisor.SupervisorEventKind) error {
			attempts.Add(1)
			select {
			case called <- struct{}{}:
			default:
			}
			return fmt.Errorf("event store unavailable")
		}},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("permission observation was not attempted")
	}
	select {
	case err := <-done:
		t.Fatalf("observer stopped on event error: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	current, err := sessions.Load(s.SessionID)
	if err != nil || current.State != supervisor.SessionAwaitingPermission {
		t.Fatalf("permission state=%v, err=%v", current.State, err)
	}
	cancel()
	_ = pty.Close()
	<-done
	if attempts.Load() < 2 {
		t.Fatalf("record attempts=%d; want retries", attempts.Load())
	}
	if pty.writes.Load() != 0 {
		t.Fatalf("permission dialog received %d PTY writes", pty.writes.Load())
	}
	if got := strings.Count(logs.String(), "permission pending observation failed"); got != 1 {
		t.Fatalf("warning count=%d; want one per stay", got)
	}
}

func TestObserve_SCN37_CommitCompletesBeforeAutoContinue(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionWorking)
	events := make(chan string, 4)
	auto := &AutoContinueUseCase{
		Extractor: supervisor.RecommendationExtractor{PreviousInput: "previous", RecommendationStart: "recommendation:", InputTop: "top"},
		Send:      observeAutoSender{events: events},
	}
	observed := &ObservedStateStore{}
	detector := &switchableRecoveryDetector{}
	detector.result.Store(detect.DetectResult{State: detect.StateWorking, ProcessingIndicator: true, ContextPercent: ptrInt(20)})
	u := ObserveSessionUseCase{
		Detect:   DetectSessionStateUseCase{Detector: detector},
		Sessions: sessions, Observed: observed, AutoContinue: auto, Interval: time.Millisecond,
		RotationStatuslineAt: func(supervisor.SessionID) *time.Time { at := time.Now(); return &at },
		EvaluateCommit:       func(context.Context, supervisor.SessionID) error { events <- "commit"; return nil },
	}
	vt := supervisor.NewVirtualTerminal(80, 24)
	_, _ = vt.Write([]byte("previous\r\nrecommendation:\r\nsafe change\r\ntop\r\n"))
	ctx, cancel := context.WithCancel(context.Background())
	pty := newObserveSendPTY()
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, vt) }()
	defer func() { cancel(); _ = pty.Close(); <-done }()
	waitObservedState(t, observed, s.SessionID, detect.StateWorking)
	detector.result.Store(detect.DetectResult{State: detect.StateAwaitingRecommendation, ContextPercent: ptrInt(20)})
	waitObservedState(t, observed, s.SessionID, detect.StateAwaitingRecommendation)
	deadline := time.After(time.Second)
	var current supervisor.ManagedSession
	for {
		var err error
		current, err = sessions.Load(s.SessionID)
		if err != nil {
			t.Fatal(err)
		}
		if current.State == supervisor.SessionAwaitingRecommendation {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("session did not reach recommendation: %s", current.State)
		case <-time.After(time.Millisecond):
		}
	}
	current.StateSince = time.Now().Add(-2 * commitSettle)
	if err := sessions.Save(current); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"commit", "send"} {
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("event %q before %q", got, want)
			}
		case <-time.After(time.Second):
			current, _ := sessions.Load(s.SessionID)
			t.Fatalf("missing %s; session=%+v auto=%+v rows=%q", want, current, auto.state(s.SessionID), vt.Screen())
		}
	}
}

func TestStopControl_SCN36_ResumeOnlySendsRecommendationEnteredDuringStop(t *testing.T) {
	for _, newDuringStop := range []bool{false, true} {
		auto, sessions, sender, _ := autoFixture()
		id := sessions.session.SessionID
		issued := time.Now().Add(-time.Second)
		auto.RecordIssued(id, sha256.Sum256([]byte("prior")), issued)
		auto.entered(id, sessions.session.StateSince)
		stops := NewStopState(t.TempDir())
		control := NewStopControlUseCase(stops, autoStopRepository{sessions}, nil, nil)
		control.SetAutoContinue(auto)
		control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
			return supervisor.TransitionContext{ChildAlive: true, ObservedState: supervisor.SessionAwaitingRecommendation}, nil
		})
		if _, err := control.Stop(context.Background(), &id); err != nil {
			t.Fatal(err)
		}
		if newDuringStop {
			auto.entered(id, time.Now())
		}
		if _, err := control.Resume(context.Background(), &id, true, false); err != nil {
			t.Fatal(err)
		}
		in := autoInput("安全な作業を続ける")
		in.EnteredFromWorking = false
		if err := auto.Evaluate(context.Background(), in); err != nil {
			t.Fatal(err)
		}
		want := 0
		if newDuringStop {
			want = 1
		}
		if sender.sends != want {
			t.Errorf("newDuringStop=%v sends=%d, want %d, session=%+v, auto=%+v", newDuringStop, sender.sends, want, sessions.session, auto.state(id))
		}
	}
}

func TestObserve_SCN36_StopWorkingRecommendationResume(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionAwaitingRecommendation)
	s.StateSince = time.Now().Add(-2 * time.Hour)
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	requests := make(chan string, 2)
	auto := &AutoContinueUseCase{Extractor: supervisor.RecommendationExtractor{PreviousInput: "previous", RecommendationStart: "recommendation:", InputTop: "top"}, Send: observeAutoSender{events: requests}}
	auto.entered(s.SessionID, s.StateSince)
	stops := NewStopState(t.TempDir())
	control := NewStopControlUseCase(stops, observeStopRepository{sessions, s.SessionID}, nil, nil)
	control.SetAutoContinue(auto)
	clock := time.Now().Add(-time.Hour)
	control.now = func() time.Time { return clock }
	control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{ChildAlive: true, ObservedState: supervisor.SessionAwaitingRecommendation}, nil
	})
	if _, err := control.Stop(context.Background(), &s.SessionID); err != nil {
		t.Fatal(err)
	}
	detector := &changingDetector{}
	detector.set(detect.StateAwaitingRecommendation)
	observed := &ObservedStateStore{}
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: sessions, Stop: stops, AutoContinue: auto, Observed: observed, Interval: time.Millisecond,
		RotationStatuslineAt: func(supervisor.SessionID) *time.Time { at := time.Now(); return &at },
	}
	vt := supervisor.NewVirtualTerminal(80, 24)
	_, _ = vt.Write([]byte("previous\r\nrecommendation:\r\nsafe change\r\ntop\r\n"))
	ctx, cancel := context.WithCancel(context.Background())
	pty := newObserveSendPTY()
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, vt) }()
	defer func() {
		cancel()
		_ = pty.Close()
		if err := <-done; err != nil && err != context.Canceled {
			t.Error(err)
		}
	}()
	waitObservedState(t, observed, s.SessionID, detect.StateIdle)
	clock = clock.Add(time.Second)
	if _, err := control.Resume(context.Background(), &s.SessionID, true, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requests:
		t.Fatal("R0 sent after resume")
	case <-time.After(30 * time.Millisecond):
	}
	if _, err := control.Stop(context.Background(), &s.SessionID); err != nil {
		t.Fatal(err)
	}
	detector.set(detect.StateWorking)
	waitObservedState(t, observed, s.SessionID, detect.StateWorking)
	detector.set(detect.StateAwaitingRecommendation)
	waitObservedState(t, observed, s.SessionID, detect.StateAwaitingRecommendation)
	clock = clock.Add(time.Second)
	if _, err := control.Resume(context.Background(), &s.SessionID, true, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-requests:
	case <-time.After(time.Second):
		t.Fatal("R2 observed during STOP was not sent")
	}
}

func TestObserve_SCN37_CommitVerifiedCompletionBeforeAutoContinue(t *testing.T) {
	commit, fixture, _, _, _ := commitTestFixture(t)
	s, sessions, _ := observeSendFixture(t, supervisor.SessionAwaitingRecommendation)
	s.StateSince = time.Now().Add(-2 * commitSettle)
	s.GitConfigFingerprint = fixture.value.GitConfigFingerprint
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	commit.Sessions = sessions
	events := make(chan string, 4)
	commit.AfterGitPhase = func(id supervisor.SessionID, since time.Time) {
		if id != s.SessionID || !since.Equal(s.StateSince) {
			t.Errorf("completion stay mismatch: %s %v", id, since)
		}
		events <- "phase"
	}
	auto := &AutoContinueUseCase{Extractor: supervisor.RecommendationExtractor{PreviousInput: "previous", RecommendationStart: "recommendation:", InputTop: "top"}, Send: observeAutoSender{events: events}}
	auto.entered(s.SessionID, s.StateSince)
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateAwaitingRecommendation, ProcessingIndicator: true, ContextPercent: ptrInt(20)}}},
		Sessions: sessions, CommitVerified: commit, AutoContinue: auto, Interval: time.Millisecond,
		RotationStatuslineAt: func(supervisor.SessionID) *time.Time { at := time.Now(); return &at },
	}
	vt := supervisor.NewVirtualTerminal(80, 24)
	_, _ = vt.Write([]byte("previous\r\nrecommendation:\r\nsafe change\r\ntop\r\n"))
	ctx, cancel := context.WithCancel(context.Background())
	pty := newObserveSendPTY()
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, vt) }()
	defer func() {
		cancel()
		_ = pty.Close()
		if err := <-done; err != nil && err != context.Canceled {
			t.Error(err)
		}
	}()
	for _, want := range []string{"phase", "send"} {
		select {
		case got := <-events:
			if got != want {
				t.Fatalf("got %s, want %s", got, want)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("missing %s", want)
		}
	}
}

func TestObserve_SCN37_CommitSkippedStillCompletesEvaluation(t *testing.T) {
	commit, fixture, _, _, _ := commitTestFixture(t)
	s, sessions, _ := observeSendFixture(t, supervisor.SessionAwaitingRecommendation)
	s.StateSince = time.Now().Add(-2 * commitSettle)
	s.GitConfigFingerprint = fixture.value.GitConfigFingerprint
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	commit.Sessions = sessions
	commit.OwnedPaths = nil
	events := make(chan string, 1)
	auto := &AutoContinueUseCase{Extractor: supervisor.RecommendationExtractor{PreviousInput: "previous", RecommendationStart: "recommendation:", InputTop: "top"}, Send: observeAutoSender{events: events}}
	auto.entered(s.SessionID, s.StateSince)
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateAwaitingRecommendation, ProcessingIndicator: true, ContextPercent: ptrInt(20)}}},
		Sessions: sessions, CommitVerified: commit, AutoContinue: auto, Interval: time.Millisecond,
		RotationStatuslineAt: func(supervisor.SessionID) *time.Time { at := time.Now(); return &at },
	}
	vt := supervisor.NewVirtualTerminal(80, 24)
	_, _ = vt.Write([]byte("previous\r\nrecommendation:\r\nsafe change\r\ntop\r\n"))
	ctx, cancel := context.WithCancel(context.Background())
	pty := newObserveSendPTY()
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, vt) }()
	defer func() {
		cancel()
		_ = pty.Close()
		if err := <-done; err != nil && err != context.Canceled {
			t.Error(err)
		}
	}()
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("skipped commit evaluation never completed")
	}
}

func TestObserve_SCN37_StaleCompletionDoesNotEvaluateNewStay(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionAwaitingRecommendation)
	s.StateSince = time.Now().Add(-2 * commitSettle)
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	events := make(chan string, 2)
	auto := &AutoContinueUseCase{Extractor: supervisor.RecommendationExtractor{PreviousInput: "previous", RecommendationStart: "recommendation:", InputTop: "top"}, Send: observeAutoSender{events: events}}
	auto.entered(s.SessionID, s.StateSince)
	started := make(chan int, 2)
	first := make(chan struct{})
	second := make(chan struct{})
	var firstOnce, secondOnce sync.Once
	releaseFirst := func() { firstOnce.Do(func() { close(first) }) }
	releaseSecond := func() { secondOnce.Do(func() { close(second) }) }
	var calls atomic.Int32
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateAwaitingRecommendation, ProcessingIndicator: true, ContextPercent: ptrInt(20)}}}, Sessions: sessions, AutoContinue: auto, Interval: time.Millisecond,
		RotationStatuslineAt: func(supervisor.SessionID) *time.Time { at := time.Now(); return &at },
		EvaluateCommit: func(context.Context, supervisor.SessionID) error {
			call := int(calls.Add(1))
			started <- call
			if call == 1 {
				<-first
			} else {
				<-second
			}
			return nil
		},
	}
	vt := supervisor.NewVirtualTerminal(80, 24)
	_, _ = vt.Write([]byte("previous\r\nrecommendation:\r\nsafe change\r\ntop\r\n"))
	ctx, cancel := context.WithCancel(context.Background())
	pty := newObserveSendPTY()
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, vt) }()
	defer func() {
		releaseFirst()
		releaseSecond()
		cancel()
		_ = pty.Close()
		if err := <-done; err != nil && err != context.Canceled {
			t.Error(err)
		}
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first commit did not start")
	}
	current, err := sessions.Load(s.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	current.StateSince = current.StateSince.Add(time.Second)
	if err := sessions.Save(current); err != nil {
		t.Fatal(err)
	}
	auto.entered(s.SessionID, current.StateSince)
	select {
	case <-started:
		t.Fatal("second commit started before the first completed")
	case <-time.After(30 * time.Millisecond):
	}
	releaseFirst()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second commit did not start after the first completed")
	}
	select {
	case <-events:
		t.Fatal("stale completion evaluated new stay")
	case <-time.After(30 * time.Millisecond):
	}
	releaseSecond()
	select {
	case <-events:
	case <-time.After(time.Second):
		t.Fatal("current completion did not evaluate")
	}
}
