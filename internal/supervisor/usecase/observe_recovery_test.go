package usecase

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type restoredScreenDetector struct {
	calls atomic.Int32
}

type switchableRecoveryDetector struct {
	result atomic.Value
	calls  atomic.Int32
}

func (d *switchableRecoveryDetector) set(state detect.DetectedState, processing bool) {
	d.result.Store(detect.DetectResult{State: state, ProcessingIndicator: processing})
}

func (d *switchableRecoveryDetector) Detect(context.Context, detect.DetectInput) (detect.DetectResult, error) {
	d.calls.Add(1)
	return d.result.Load().(detect.DetectResult), nil
}

func TestSCNsup0154UnarmedRestoredReplyDoesNotEvaluateCommit(t *testing.T) {
	s, sessions := recoveryObservationFixture(t)
	s.Generation = 2
	s.State = supervisor.SessionStarting
	sessions.session = s
	detector := &switchableRecoveryDetector{}
	detector.set(detect.StateAwaitingRecommendation, false)
	commits := make(chan struct{}, 2)
	u := ObserveSessionUseCase{
		Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: sessions, Interval: time.Millisecond,
		EvaluateCommit: func(context.Context, supervisor.SessionID) error { commits <- struct{}{}; return nil },
	}
	pty := &waitingPTY{closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	vt := restoredRecommendationScreen(t)
	go func() { done <- u.Run(ctx, s, pty, vt) }()
	defer func() {
		cancel()
		_ = pty.Close()
		if err := <-done; err != nil && err != context.Canceled {
			t.Error(err)
		}
	}()
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionIdle)
	current, err := sessions.Load(s.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	current.StateSince = time.Now().Add(-2 * commitSettle)
	if err := sessions.Save(current); err != nil {
		t.Fatal(err)
	}
	observations := detector.calls.Load()
	deadline := time.After(time.Second)
	for detector.calls.Load() < observations+3 {
		select {
		case <-deadline:
			t.Fatal("observer did not repeat restored detection")
		case <-time.After(time.Millisecond):
		}
	}
	select {
	case <-commits:
		t.Fatal("unarmed restored reply triggered commit evaluation")
	default:
	}
	detector.set(detect.StateWorking, true)
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionWorking)
	detector.set(detect.StateIdle, false)
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionIdle)
	current, err = sessions.Load(s.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	current.StateSince = time.Now().Add(-2 * commitSettle)
	if err := sessions.Save(current); err != nil {
		t.Fatal(err)
	}
	select {
	case <-commits:
	case <-time.After(time.Second):
		t.Fatal("armed idle did not evaluate commit")
	}
}

func waitRecoveryState(t *testing.T, sessions *regressionSessionStore, id supervisor.SessionID, want supervisor.SupervisorSessionState) {
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
			t.Fatalf("state=%s want=%s", current.State, want)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestSCNsup0155And57ArmingRequiresProcessingIndicatorPerRun(t *testing.T) {
	s, sessions := recoveryObservationFixture(t)
	s.Generation = 2
	s.State = supervisor.SessionStarting
	sessions.session = s
	detector := &switchableRecoveryDetector{}
	detector.set(detect.StateIdle, false)
	observed := &ObservedStateStore{}
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: sessions, Observed: observed, Interval: time.Millisecond}
	pty := &waitingPTY{closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	vt := restoredRecommendationScreen(t)
	go func() { done <- u.Run(ctx, s, pty, vt) }()
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionIdle)
	detector.set(detect.StateWorking, false) // Rule 7 can change state, but cannot arm.
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionWorking)
	detector.set(detect.StateAwaitingRecommendation, false)
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionIdle)
	if got, _ := observed.Get(s.SessionID); got != detect.StateIdle {
		t.Fatalf("rule 7 armed recommendation: %s", got)
	}
	detector.set(detect.StateWorking, true) // Rule 2 arms this generation.
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionWorking)
	detector.set(detect.StateAwaitingRecommendation, false)
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionAwaitingRecommendation)
	if got, _ := observed.Get(s.SessionID); got != detect.StateAwaitingRecommendation {
		t.Fatalf("armed recommendation observed as %s", got)
	}
	cancel()
	_ = pty.Close()
	if err := <-done; err != nil && err != context.Canceled {
		t.Fatal(err)
	}

	// Reusing the same use case must not carry armed into the next Run.
	current, _ := sessions.Load(s.SessionID)
	current.Generation++
	current.State = supervisor.SessionStarting
	current.StateSince = time.Now()
	current.ChildStartedAt = &current.StateSince
	if err := sessions.Save(current); err != nil {
		t.Fatal(err)
	}
	detector.set(detect.StateAwaitingRecommendation, false)
	pty = &waitingPTY{closed: make(chan struct{})}
	ctx, cancel = context.WithCancel(context.Background())
	done = make(chan error, 1)
	vt = restoredRecommendationScreen(t)
	go func() { done <- u.Run(ctx, current, pty, vt) }()
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionIdle)
	if got, _ := observed.Get(s.SessionID); got != detect.StateIdle {
		t.Fatalf("new generation inherited armed: %s", got)
	}
	cancel()
	_ = pty.Close()
	if err := <-done; err != nil && err != context.Canceled {
		t.Fatal(err)
	}
}

func TestSCNsup0155ProcessingIndicatorEnablesAutoContinue(t *testing.T) {
	s, sessions := recoveryObservationFixture(t)
	s.Generation = 2
	s.State = supervisor.SessionStarting
	sessions.session = s
	detector := &switchableRecoveryDetector{}
	detector.set(detect.StateIdle, false)
	requests := make(chan string, 1)
	auto := &AutoContinueUseCase{Extractor: supervisor.RecommendationExtractor{PreviousInput: "previous", RecommendationStart: "recommendation:", InputTop: "top"}, Send: observeAutoSender{events: requests}}
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: sessions, AutoContinue: auto, Interval: time.Millisecond, RotationStatuslineAt: func(supervisor.SessionID) *time.Time {
		at := time.Now()
		return &at
	}}
	vt := supervisor.NewVirtualTerminal(80, 24)
	if _, err := vt.Write([]byte("previous\r\nrecommendation:\r\nsafe change\r\ntop\r\n")); err != nil {
		t.Fatal(err)
	}
	pty := &waitingPTY{closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, vt) }()
	defer func() {
		cancel()
		_ = pty.Close()
		if err := <-done; err != nil && err != context.Canceled {
			t.Error(err)
		}
	}()
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionIdle)
	detector.result.Store(detect.DetectResult{State: detect.StateWorking, ProcessingIndicator: true, ContextPercent: ptrInt(20)})
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionWorking)
	detector.result.Store(detect.DetectResult{State: detect.StateAwaitingRecommendation, ContextPercent: ptrInt(20)})
	waitRecoveryState(t, sessions, s.SessionID, supervisor.SessionAwaitingRecommendation)
	current, err := sessions.Load(s.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	current.StateSince = time.Now().Add(-2 * commitSettle)
	if err := sessions.Save(current); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-requests:
		if got != "send" {
			t.Fatalf("unexpected auto continuation result: %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("armed recommendation was not evaluated")
	}
}

func (d *restoredScreenDetector) Detect(ctx context.Context, in detect.DetectInput) (detect.DetectResult, error) {
	d.calls.Add(1)
	in.LastScreenChangeAt = in.Now.Add(-detect.QuietPeriod)
	return detect.NewCompositeDetector(detect.DefaultPatternSet()).Detect(ctx, in)
}

func restoredRecommendationScreen(t *testing.T) *supervisor.VirtualTerminal {
	t.Helper()
	vt := supervisor.NewVirtualTerminal(120, 24)
	border := strings.Repeat("─", 24)
	rows := []string{"❯ previous request", "⏺ Here is the completed answer.", "✻ Cooked for 2s · done 10:00", border, "❯\u00a0", border}
	if _, err := vt.Write([]byte(strings.Join(rows, "\r\n"))); err != nil {
		t.Fatal(err)
	}
	return vt
}

func TestSCNsup0153To58RestoredRecommendationStartsIdle(t *testing.T) {
	for _, generation := range []int{1, 2, 3} {
		t.Run(string(rune('0'+generation)), func(t *testing.T) {
			s, sessions := recoveryObservationFixture(t)
			s.Generation = generation
			s.State = supervisor.SessionStarting
			sessions.session = s
			observed := &ObservedStateStore{}
			var eventsMu sync.Mutex
			var events []store.Event
			requests := make(chan SendInput, 2)
			remote := &InitialRemoteControl{Request: func(_ context.Context, in SendInput) error { requests <- in; return nil }}
			auto := &AutoContinueUseCase{}
			detector := &restoredScreenDetector{}
			pty := &waitingPTY{closed: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			defer pty.Close()
			u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: sessions, Observed: observed, InitialRemoteControl: remote, AutoContinue: auto, Interval: time.Millisecond, RecordStructured: func(e store.Event) error {
				eventsMu.Lock()
				defer eventsMu.Unlock()
				events = append(events, e)
				return nil
			}}
			done := make(chan error, 1)
			vt := restoredRecommendationScreen(t)
			go func() { done <- u.Run(ctx, s, pty, vt) }()
			select {
			case in := <-requests:
				if in.Kind != supervisor.SendKindRemoteControl {
					t.Fatalf("unexpected send kind: %s", in.Kind)
				}
			case <-time.After(time.Second):
				current, _ := sessions.Load(s.SessionID)
				t.Fatalf("restored screen did not reach idle: %s", current.State)
			}
			deadline := time.After(time.Second)
			for detector.calls.Load() < 3 {
				select {
				case <-deadline:
					t.Fatal("observer did not repeat detection")
				case <-time.After(time.Millisecond):
				}
			}
			current, _ := sessions.Load(s.SessionID)
			got, _ := observed.Get(s.SessionID)
			if current.State != supervisor.SessionIdle || got != detect.StateIdle {
				t.Fatalf("state=%s observed=%s", current.State, got)
			}
			select {
			case <-requests:
				t.Fatal("remote control requested twice")
			default:
			}
			eventsMu.Lock()
			for _, event := range events {
				if event.Kind == supervisor.EventKindIllegalTransition || event.Kind == supervisor.EventKindRecommendationBlocked || event.Kind == supervisor.EventKindSessionStopped {
					eventsMu.Unlock()
					t.Fatalf("unexpected event: %+v", event)
				}
			}
			eventsMu.Unlock()
			auto.mu.Lock()
			autoEvaluated := len(auto.states) != 0 || len(auto.inflight) != 0
			auto.mu.Unlock()
			if autoEvaluated {
				t.Fatal("unarmed recommendation was evaluated")
			}
			cancel()
			if err := <-done; err != nil && err != context.Canceled {
				t.Fatal(err)
			}
		})
	}
}

func recoveryObservationFixture(t *testing.T) (supervisor.ManagedSession, *regressionSessionStore) {
	t.Helper()
	id, err := supervisor.NewSessionID("recovery-1")
	if err != nil {
		t.Fatal(err)
	}
	started := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: started, ChildPID: 123, ChildStartedAt: &started, ClaudeSessionID: "00000000-0000-0000-0000-000000000001", RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	return s, &regressionSessionStore{session: s}
}

func TestSCNsup0516ObserveStartsRecoveryOnce(t *testing.T) {
	s, sessions := recoveryObservationFixture(t)
	spawned := make(chan struct{}, 2)
	waiting := make(chan struct{})
	release := make(chan struct{})
	recovery := &RecoverSessionUseCase{Store: sessions, Now: func() time.Time { return s.StateSince }, Wait: func(context.Context, time.Duration) error { close(waiting); <-release; return nil }, Respawn: func(context.Context, supervisor.SessionID, RespawnKind) error { spawned <- struct{}{}; return nil }}
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: sessions, Recover: recovery, Interval: time.Millisecond}
	done := make(chan error, 1)
	go func() {
		done <- u.Run(context.Background(), s, exitedPTY{code: 1}, supervisor.NewVirtualTerminal(80, 24))
	}()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("recovery wait was not started")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer blocked on recovery wait")
	}
	close(release)
	select {
	case <-spawned:
	case <-time.After(time.Second):
		t.Fatal("recovery was not started")
	}
	select {
	case <-spawned:
		t.Fatal("recovery started twice")
	default:
	}
}

type recoveryPTY struct {
	pid  int
	code int
	done chan struct{}
	once sync.Once
}

func (p *recoveryPTY) Pid() int                  { return p.pid }
func (p *recoveryPTY) Read([]byte) (int, error)  { <-p.done; return 0, io.EOF }
func (p *recoveryPTY) Wait() (int, error)        { <-p.done; return p.code, nil }
func (p *recoveryPTY) Close() error              { return nil }
func (p *recoveryPTY) Kill(syscall.Signal) error { p.once.Do(func() { close(p.done) }); return nil }

type recoveryStarter struct{ pty *recoveryPTY }

func (s recoveryStarter) Start(string, []string, []string, string, uint16, uint16) (ManagedPTY, error) {
	return s.pty, nil
}

type exitAwareDetector struct{}

func (exitAwareDetector) Detect(_ context.Context, input detect.DetectInput) (detect.DetectResult, error) {
	if input.ProcessExited {
		return detect.DetectResult{State: detect.StateExited}, nil
	}
	return detect.DetectResult{State: detect.StateIdle}, nil
}

func TestSCNsup0516RecoveryObservesRespawnedChild(t *testing.T) {
	respawn, sessions, _, id := respawnFixture(t)
	initial, _ := sessions.Load(id)
	initial.State = supervisor.SessionWorking
	initial.ChildPID = 123
	sessions.session = initial
	child := &recoveryPTY{pid: 456, code: 0, done: make(chan struct{})}
	respawn.Starter = recoveryStarter{pty: child}
	var running sync.WaitGroup
	recovery := &RecoverSessionUseCase{Store: sessions, Now: respawn.Now, Wait: func(context.Context, time.Duration) error { return nil }, Running: &running}
	observer := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: exitAwareDetector{}}, Sessions: sessions, Recover: recovery, Respawn: &respawn, Interval: time.Millisecond, Now: func() time.Time { return initial.StateSince.Add(supervisor.RestartStableDuration) }}
	ctx, cancel := context.WithCancel(context.Background())
	if err := observer.Run(ctx, initial, exitedPTY{code: 1}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	cancel() // The old observation ends; recovery must use the supervisor lifetime.
	recovery.WaitForCompletion()
	current, _ := sessions.Load(id)
	if current.Generation != 2 || (current.State != supervisor.SessionStarting && current.State != supervisor.SessionIdle) || !ownedChildren.Owns(id, child.pid) {
		t.Fatalf("respawn not observed: %+v", current)
	}
	stableDeadline := time.After(time.Second)
	for current.RestartCount != 0 {
		select {
		case <-stableDeadline:
			t.Fatalf("respawn stability not observed: %+v", current)
		case <-time.After(time.Millisecond):
		}
		current, _ = sessions.Load(id)
	}
	if len(current.RestartTimes) != 1 {
		t.Fatalf("restart history changed: %+v", current.RestartTimes)
	}
	child.once.Do(func() { close(child.done) })
	deadline := time.After(time.Second)
	for {
		current, _ = sessions.Load(id)
		if current.State == supervisor.SessionExited && current.ChildPID == 0 && !ownedChildren.Owns(id, child.pid) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("respawn exit not observed: %+v", current)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestFix3ResumeRecoveryObservesNewChild(t *testing.T) {
	respawn, sessions, _, id := respawnFixture(t)
	child := &recoveryPTY{pid: 458, done: make(chan struct{})}
	defer child.once.Do(func() { close(child.done) })
	respawn.Starter = recoveryStarter{pty: child}
	observer := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: exitAwareDetector{}}, Sessions: sessions, Respawn: &respawn, Interval: time.Millisecond}
	if err := observer.ResumeRecovery(context.Background(), sessions.session); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(time.Second)
	for {
		current, _ := sessions.Load(id)
		if current.State == supervisor.SessionIdle {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("resume child not observed: %+v", current)
		case <-time.After(time.Millisecond):
		}
	}
	child.once.Do(func() { close(child.done) })
	deadline = time.After(time.Second)
	for {
		current, _ := sessions.Load(id)
		if current.ChildPID == 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("resume exit not observed: %+v", current)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestRespawnedChildStartupTimeoutAndLaterExit(t *testing.T) {
	respawn, sessions, _, id := respawnFixture(t)
	initial, _ := sessions.Load(id)
	initial.State = supervisor.SessionWorking
	sessions.session = initial
	child := &recoveryPTY{pid: 457, code: 0, done: make(chan struct{})}
	defer child.once.Do(func() { close(child.done) })
	respawn.Starter = recoveryStarter{pty: child}
	var running sync.WaitGroup
	recovery := &RecoverSessionUseCase{Store: sessions, Now: respawn.Now, Wait: func(context.Context, time.Duration) error { return nil }, Running: &running}
	observer := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateUndetermined}}}, Sessions: sessions, Recover: recovery, Respawn: &respawn, Interval: time.Millisecond, StartupTimeout: time.Nanosecond, Now: func() time.Time { return initial.StateSince.Add(time.Second) }}
	if err := observer.Run(context.Background(), initial, exitedPTY{code: 1}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	recovery.WaitForCompletion()
	deadline := time.After(time.Second)
	for {
		current, _ := sessions.Load(id)
		if current.State == supervisor.SessionStopped && current.StopReason == supervisor.StopRecoveryStartTimeout && current.ChildPID == child.pid {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("respawn timeout not observed: %+v", current)
		case <-time.After(time.Millisecond):
		}
	}
	child.once.Do(func() { close(child.done) })
	deadline = time.After(time.Second)
	for {
		current, _ := sessions.Load(id)
		if current.ChildPID == 0 && !ownedChildren.Owns(id, child.pid) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("stopped child exit not observed: %+v", current)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestSCNsup0516ExitRemovesOwnedPID(t *testing.T) {
	s, sessions := recoveryObservationFixture(t)
	ownedChildren.Register(s.SessionID, s.ChildPID)
	defer ownedChildren.Remove(s.SessionID, s.ChildPID)
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: sessions}
	if err := u.Run(context.Background(), s, exitedPTY{code: 0}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	if ownedChildren.Owns(s.SessionID, s.ChildPID) {
		t.Fatal("exited child remains owned")
	}
}

func TestSCNsup0518And41ObserveRecordsLimit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
		times int
		want  supervisor.SupervisorStopReason
	}{
		{"consecutive", supervisor.RestartMaxCount, 0, supervisor.StopRestartLimitConsecutive},
		{"daily", 0, supervisor.RestartMaxPerDay, supervisor.StopRestartLimitDaily},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, sessions := recoveryObservationFixture(t)
			s.RestartCount = tc.count
			for i := 0; i < tc.times; i++ {
				s.RestartTimes = append(s.RestartTimes, s.StateSince)
			}
			sessions.session = s
			var events []store.Event
			var notified []supervisor.SupervisorEventKind
			u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: sessions, Now: func() time.Time { return s.StateSince }, RecordStructured: func(e store.Event) error { events = append(events, e); return nil }, Notify: func(kind supervisor.SupervisorEventKind, _ string) { notified = append(notified, kind) }}
			if err := u.Run(context.Background(), s, exitedPTY{code: 1}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
				t.Fatal(err)
			}
			got, _ := sessions.Load(s.SessionID)
			if got.State != supervisor.SessionStopped || got.StopReason != tc.want {
				t.Fatalf("session=%+v", got)
			}
			found := false
			for _, e := range events {
				if e.Kind == supervisor.EventKindRestartLimitExceeded && e.Reason == store.EventReason(tc.want) && e.Code == "SUP_RESTART_LIMIT_EXCEEDED" {
					found = true
				}
			}
			if !found {
				t.Fatalf("limit event absent: %+v", events)
			}
			if len(notified) == 0 || notified[len(notified)-1] != supervisor.EventKindRestartLimitExceeded {
				t.Fatalf("limit notification absent: %+v", notified)
			}
		})
	}
}

func TestSCNsup0524ObserveCodexExitNotification(t *testing.T) {
	s, sessions := recoveryObservationFixture(t)
	s.CLI = supervisor.CLICodex
	sessions.session = s
	var notified []supervisor.SupervisorEventKind
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: sessions, Notify: func(kind supervisor.SupervisorEventKind, _ string) { notified = append(notified, kind) }}
	if err := u.Run(context.Background(), s, exitedPTY{code: 1}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	got, _ := sessions.Load(s.SessionID)
	if got.State != supervisor.SessionExited || len(notified) == 0 || notified[len(notified)-1] != supervisor.EventKindSessionExited {
		t.Fatalf("session=%+v notifications=%v", got, notified)
	}
}

func TestSCNsup0519ObserveResetsOnlyConsecutiveCount(t *testing.T) {
	s, sessions := recoveryObservationFixture(t)
	s.Generation = 2
	s.RestartCount = 1
	s.RestartTimes = []time.Time{s.StateSince}
	sessions.session = s
	pty := &waitingPTY{closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	now := s.StateSince.Add(supervisor.RestartStableDuration)
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateWorking}}}, Sessions: sessions, Interval: time.Millisecond, Now: func() time.Time { return now }}
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	deadline := time.After(time.Second)
	for {
		got, _ := sessions.Load(s.SessionID)
		if got.RestartCount == 0 {
			if len(got.RestartTimes) != 1 || !got.RestartTimes[0].Equal(s.StateSince) {
				t.Fatalf("restart history changed: %+v", got.RestartTimes)
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("stable count not reset")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	_ = pty.Close()
	<-done
}
