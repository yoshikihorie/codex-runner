package usecase

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type heldRotationSender struct {
	started chan struct{}
	release chan struct{}
}

type rotationLogWriter struct{ entries chan string }

func (w rotationLogWriter) Write(p []byte) (int, error) {
	w.entries <- string(p)
	return len(p), nil
}

type failFirstRotationStore struct {
	SessionRecordStore
	fail   atomic.Bool
	failed chan struct{}
}

func (s *failFirstRotationStore) Save(value supervisor.ManagedSession) error {
	if value.Rotation != nil && value.State == supervisor.SessionRotating && s.fail.Swap(false) {
		s.failed <- struct{}{}
		return errors.New("temporary rotation save failure")
	}
	return s.SessionRecordStore.Save(value)
}

type rotationStateDetector struct {
	state   atomic.Int32
	percent int
}

func (d *rotationStateDetector) Detect(context.Context, detect.DetectInput) (detect.DetectResult, error) {
	state := detect.StateWorking
	if d.state.Load() == 1 {
		state = detect.StateAwaitingPermission
	}
	return detect.DetectResult{State: state, ContextPercent: &d.percent}, nil
}

type observedRotationRespawner struct {
	sessions SessionRecordStore
	now      time.Time
}

func (r observedRotationRespawner) Execute(_ context.Context, id supervisor.SessionID, kind RespawnKind) error {
	if kind != RespawnHandoffStage5 {
		return errors.New("unexpected respawn kind")
	}
	return UpdateSession(r.sessions, id, func(s *supervisor.ManagedSession) error {
		s.ChildPID = 456
		s.ChildStartedAt = &r.now
		s.Generation++
		return nil
	})
}

func (s *heldRotationSender) Execute(context.Context, SendInput) (SendResult, error) {
	select {
	case s.started <- struct{}{}:
	default:
	}
	<-s.release
	return SendResult{}, errors.New("held send released")
}

func TestObserveSession_SCN02_StartDoesNotBlockObserver(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionIdle)
	now := s.StateSince.Add(time.Second)
	sender := &heldRotationSender{started: make(chan struct{}, 1), release: make(chan struct{})}
	failed := make(chan struct{}, 1)
	rotation := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }, Sessions: sessions, Send: sender, IsStopped: func(supervisor.SessionID) bool { return false }, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle }, ChildAlive: func(supervisor.SessionID) bool { return true }, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return supervisor.RotationCheckpointSnapshot{}, nil
	}, Now: func() time.Time { return now }, RecordStructured: func(e store.Event) error {
		if e.Kind == supervisor.EventKindRotateFailed {
			failed <- struct{}{}
		}
		return nil
	}}
	percent := 61
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle, ContextPercent: &percent}}}, Sessions: sessions, Rotation: rotation, RotationStatuslineAt: func(supervisor.SessionID) *time.Time { return &now }, Interval: time.Millisecond, Now: func() time.Time { return now }}
	pty := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	select {
	case <-sender.started:
	case <-time.After(time.Second):
		t.Fatal("rotation did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("observer returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer blocked on rotation send")
	}
	close(sender.release)
	select {
	case <-failed:
	case <-time.After(time.Second):
		t.Fatal("rotation worker did not finish")
	}
	_ = pty.Close()
}

func TestObserveSession_StartFailureAllowsNextRotationDecision(t *testing.T) {
	logs := make(chan string, 4)
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(rotationLogWriter{logs}, nil)))
	defer slog.SetDefault(previousLogger)
	s, records, _ := observeSendFixture(t, supervisor.SessionIdle)
	wrapped := &failFirstRotationStore{SessionRecordStore: records, failed: make(chan struct{}, 1)}
	wrapped.fail.Store(true)
	now := s.StateSince.Add(time.Second)
	sender := &heldRotationSender{started: make(chan struct{}, 1), release: make(chan struct{})}
	finished := make(chan struct{}, 1)
	rotation := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }, Sessions: wrapped, Send: sender, IsStopped: func(supervisor.SessionID) bool { return false }, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle }, ChildAlive: func(supervisor.SessionID) bool { return true }, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return supervisor.RotationCheckpointSnapshot{}, nil
	}, Now: func() time.Time { return now }, RecordStructured: func(e store.Event) error {
		if e.Kind == supervisor.EventKindRotateFailed {
			finished <- struct{}{}
		}
		return nil
	}}
	percent := 61
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle, ContextPercent: &percent}}}, Sessions: wrapped, Rotation: rotation, RotationStatuslineAt: func(supervisor.SessionID) *time.Time { return &now }, Interval: time.Millisecond, Now: func() time.Time { return now }}
	pty := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	defer func() {
		cancel()
		close(sender.release)
		_ = pty.Close()
		<-done
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("worker did not finish")
		}
	}()
	select {
	case <-wrapped.failed:
	case <-time.After(time.Second):
		t.Fatal("first Start did not fail")
	}
	select {
	case entry := <-logs:
		if !strings.Contains(entry, "reason=execute_failed") || strings.Contains(entry, "temporary rotation save failure") || strings.Contains(entry, "path=") {
			t.Fatalf("unsafe or missing rotation log: %s", entry)
		}
	case <-time.After(time.Second):
		t.Fatal("rotation worker failure was not logged")
	}
	select {
	case <-sender.started:
	case <-time.After(time.Second):
		t.Fatal("rotation was not reconsidered after Start failure")
	}
}

func TestObserveSession_RejectsUnwiredRotationDependencies(t *testing.T) {
	s, records, _ := observeSendFixture(t, supervisor.SessionIdle)
	now := s.StateSince.Add(time.Second)
	percent := 61
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle, ContextPercent: &percent}}}, Sessions: records, Rotation: &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }}, RotationStatuslineAt: func(supervisor.SessionID) *time.Time { return &now }, Interval: time.Millisecond, Now: func() time.Time { return now }}
	pty := newObserveSendPTY()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	defer pty.Close()
	if err := u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing rotation dependencies were not returned: %v", err)
	}
}

func TestObserveSession_EndRemovesRotationWakeChannel(t *testing.T) {
	s, records, _ := observeSendFixture(t, supervisor.SessionExited)
	rotationWakeChannel(s.SessionID)
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: records, Interval: time.Millisecond}
	if err := u.Run(context.Background(), s, exitedPTY{code: 0}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	if _, found := rotationObservationWakes.Load(s.SessionID); found {
		t.Fatal("ended session retained wake channel")
	}
}

func TestObserveSession_SCN22_RotatingChildExitGoesToRotation(t *testing.T) {
	logs := make(chan string, 4)
	previousLogger := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(rotationLogWriter{logs}, nil)))
	defer slog.SetDefault(previousLogger)
	s, sessions, _ := observeSendFixture(t, supervisor.SessionRotating)
	now := s.StateSince
	s.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendEnd, StartedAt: now, StepStartedAt: &now}
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	failures := make(chan store.Event, 1)
	rotation := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }, Sessions: sessions, Send: &rotationSender{}, IsStopped: func(supervisor.SessionID) bool { return false }, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionExited }, ChildAlive: func(supervisor.SessionID) bool { return false }, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return supervisor.RotationCheckpointSnapshot{}, nil
	}, Now: func() time.Time { return now }, RecordStructured: func(e store.Event) error {
		if e.Kind == supervisor.EventKindRotateFailed {
			failures <- e
			return errors.New("sensitive payload")
		}
		return nil
	}}
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: sessions, Rotation: rotation, Interval: time.Millisecond, Now: func() time.Time { return now }}
	if err := u.Run(context.Background(), s, exitedPTY{code: 1}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-failures:
		if event.Reason != store.EventReason(supervisor.RotateFailChildAbsent) {
			t.Fatalf("failure=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("rotation did not receive child exit")
	}
	select {
	case entry := <-logs:
		if !strings.Contains(entry, "reason=run_failed") || strings.Contains(entry, "sensitive payload") || strings.Contains(entry, "path=") {
			t.Fatalf("unsafe or missing run log: %s", entry)
		}
	case <-time.After(time.Second):
		t.Fatal("rotation run failure was not logged")
	}
	current, err := sessions.Load(s.SessionID)
	if err != nil || current.State != supervisor.SessionStopped || current.StopReason != supervisor.StopRotateStepFailed {
		t.Fatalf("session=%+v err=%v", current, err)
	}
}

func TestObserveSession_SCN17_PendingRemoteControlSentOnIdle(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionIdle)
	now := time.Now().UTC()
	s.PendingRemoteControl = true
	s.PendingRemoteControlSince = &now
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	screen := &sendTestScreen{shot: blankScreen()}
	confirmed := make(chan struct{}, 1)
	writer := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
	send := &SendInputUseCase{Sessions: sessions, Screens: screen, PTY: writer, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Second, RecordStructured: func(e store.Event) error {
		if e.Kind == supervisor.EventKindSendConfirmed {
			confirmed <- struct{}{}
		}
		return nil
	}}
	writer.afterEnter = func() {
		current, err := sessions.Load(s.SessionID)
		if err != nil {
			t.Error(err)
			return
		}
		shot := blankScreen()
		shot.ChangedAt = time.Now().Add(time.Second)
		notifySendTestPair(send, SendConfirmation{SessionID: s.SessionID, SendID: current.SendGate.Pending.SendID, Screen: shot})
	}
	pending := &PendingRemoteControlUseCase{Sessions: sessions, Send: send}
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle}}}, Sessions: sessions, PendingRemoteControl: pending, Interval: time.Millisecond}
	pty := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	select {
	case <-confirmed:
	case <-time.After(time.Second):
		t.Fatal("pending /rc was not sent")
	}
	if err := pending.OnObserved(context.Background(), s.SessionID, detect.StateIdle); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = pty.Close()
	<-done
	current, err := sessions.Load(s.SessionID)
	if err != nil || current.PendingRemoteControl || writer.wrote != "/rc" {
		t.Fatalf("session=%+v wrote=%q err=%v", current, writer.wrote, err)
	}
}

func TestObserveSession_SCN17_ChildExitExpiresPendingRemoteControl(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionIdle)
	now := s.StateSince
	s.PendingRemoteControl = true
	s.PendingRemoteControlSince = &now
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: sessions, PendingRemoteControl: &PendingRemoteControlUseCase{Sessions: sessions}, Interval: time.Millisecond}
	if err := u.Run(context.Background(), s, exitedPTY{code: 1}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	current, err := sessions.Load(s.SessionID)
	if err != nil || current.PendingRemoteControl || current.PendingRemoteControlSince != nil {
		t.Fatalf("session=%+v err=%v", current, err)
	}
}

func TestObserveSession_SCN01_17_RotationCompletesThenSendsPendingRemoteControl(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionIdle)
	now := s.StateSince.Add(time.Second)
	screen := &sendTestScreen{shot: blankScreen()}
	confirmed := make(chan struct{}, 1)
	writer := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
	send := &SendInputUseCase{Sessions: sessions, Screens: screen, PTY: writer, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Second, RecordStructured: func(e store.Event) error {
		if e.Kind == supervisor.EventKindSendConfirmed && e.SendKind == supervisor.SendKindRemoteControl {
			confirmed <- struct{}{}
		}
		return nil
	}}
	writer.afterEnter = func() {
		current, err := sessions.Load(s.SessionID)
		if err != nil {
			t.Error(err)
			return
		}
		shot := blankScreen()
		shot.ChangedAt = time.Now().Add(time.Second)
		notifySendTestPair(send, SendConfirmation{SessionID: s.SessionID, SendID: current.SendGate.Pending.SendID, Screen: shot})
	}
	pending := &PendingRemoteControlUseCase{Sessions: sessions, Send: send}
	completed := make(chan struct{}, 1)
	inspections := 0
	rotation := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: pending, ResumeReady: send.ResumeReady, Sessions: sessions, Send: &rotationSender{}, IsStopped: func(supervisor.SessionID) bool { return false }, Respawn: observedRotationRespawner{sessions: sessions, now: now}, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle }, ChildAlive: func(id supervisor.SessionID) bool {
		current, err := sessions.Load(id)
		return err == nil && current.ChildPID > 0
	}, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		inspections++
		if inspections == 1 {
			return supervisor.RotationCheckpointSnapshot{}, nil
		}
		mtime := now.Add(time.Second)
		return supervisor.RotationCheckpointSnapshot{Exists: true, Mtime: &mtime, HeadHash: "new"}, nil
	}, Now: func() time.Time { return now }, RecordStructured: func(e store.Event) error {
		if e.Kind == supervisor.EventKindRotateCompleted {
			completed <- struct{}{}
		}
		return nil
	}}
	percent := 61
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle, ContextPercent: &percent}}}, Sessions: sessions, Rotation: rotation, RotationStatuslineAt: func(supervisor.SessionID) *time.Time { return &now }, PendingRemoteControl: pending, Interval: time.Millisecond, Now: func() time.Time { return now }}
	pty := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("rotation did not complete")
	}
	select {
	case <-confirmed:
	case <-time.After(time.Second):
		t.Fatal("pending /rc was not sent")
	}
	if err := pending.OnObserved(context.Background(), s.SessionID, detect.StateIdle); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = pty.Close()
	<-done
	current, err := sessions.Load(s.SessionID)
	if err != nil || current.Rotation != nil || current.State != supervisor.SessionIdle || current.PendingRemoteControl || writer.wrote != "/rc" {
		t.Fatalf("session=%+v wrote=%q err=%v", current, writer.wrote, err)
	}
}

func TestObserveSession_SCN04_05_WorkingWaitRecordsPendingAndTimeoutReason(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionWorking)
	var seconds atomic.Int64
	seconds.Store(s.StateSince.Add(time.Second).Unix())
	now := func() time.Time { return time.Unix(seconds.Load(), 0).UTC() }
	percent := 61
	events := make(chan store.Event, 4)
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateWorking, ContextPercent: &percent}}}, Sessions: sessions, Rotation: &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }}, RotationStatuslineAt: func(supervisor.SessionID) *time.Time { at := now(); return &at }, RecordStructured: func(e store.Event) error {
		if e.Kind == supervisor.EventKindRotatePending || e.Kind == supervisor.EventKindRotateIdleWaitTimeout {
			events <- e
		}
		return nil
	}, Interval: time.Millisecond, Now: now}
	pty := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	select {
	case event := <-events:
		if event.Kind != supervisor.EventKindRotatePending {
			t.Fatalf("first event=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("working observation did not record pending")
	}
	seconds.Store(now().Add(rotationIdleWait + time.Second).Unix())
	pty.reads <- []byte("progress")
	select {
	case event := <-events:
		if event.Kind != supervisor.EventKindRotateIdleWaitTimeout || event.Reason != store.ReasonIdleWaitTimeout {
			t.Fatalf("second event=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("working wait did not time out")
	}
	cancel()
	_ = pty.Close()
	<-done
	current, err := sessions.Load(s.SessionID)
	if err != nil || current.State != supervisor.SessionWorking || current.Rotation != nil {
		t.Fatalf("session=%+v err=%v", current, err)
	}
}

func TestObserveSession_AwaitingPermissionPreservesRotationPendingClock(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionWorking)
	var seconds atomic.Int64
	seconds.Store(s.StateSince.Add(time.Second).Unix())
	now := func() time.Time { return time.Unix(seconds.Load(), 0).UTC() }
	detector := &rotationStateDetector{percent: 61}
	events := make(chan store.Event, 4)
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: sessions, Rotation: &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }}, RotationStatuslineAt: func(supervisor.SessionID) *time.Time { at := now(); return &at }, RecordStructured: func(e store.Event) error {
		if e.Kind == supervisor.EventKindRotatePending || e.Kind == supervisor.EventKindRotateIdleWaitTimeout {
			events <- e
		}
		return nil
	}, Interval: time.Millisecond, Now: now}
	pty := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	defer func() { cancel(); _ = pty.Close(); <-done }()
	select {
	case event := <-events:
		if event.Kind != supervisor.EventKindRotatePending {
			t.Fatalf("first event=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("working did not create rotate_pending")
	}
	wake := rotationWakeChannel(s.SessionID)
	for {
		select {
		case <-wake:
		default:
			goto permission
		}
	}
permission:
	detector.state.Store(1)
	pty.reads <- []byte("permission")
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("permission observation did not finish")
	}
	detector.state.Store(0)
	seconds.Store(now().Add(rotationIdleWait + time.Second).Unix())
	pty.reads <- []byte("working")
	select {
	case event := <-events:
		if event.Kind != supervisor.EventKindRotateIdleWaitTimeout {
			t.Fatalf("event after permission=%+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("permission observation reset idle-wait clock")
	}
}

func TestObserveSession_SCN04_04_ContextUnknownOncePerStateResidence(t *testing.T) {
	testRotationDecisionEventPerResidence(t, supervisor.CLIClaude, supervisor.EventKindContextUnknown)
}

func TestObserveSession_SCN04_25_RotationNotApplicableOncePerStateResidence(t *testing.T) {
	testRotationDecisionEventPerResidence(t, supervisor.CLICodex, supervisor.EventKindRotationNotApplicable)
}

func testRotationDecisionEventPerResidence(t *testing.T, cli supervisor.SupervisorCli, kind supervisor.SupervisorEventKind) {
	t.Helper()
	s, sessions, events := observeSendFixture(t, supervisor.SessionIdle)
	s.CLI = cli
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	recorded := make(chan struct{}, 4)
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle}}}, Sessions: sessions, Rotation: &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }}, RecordStructured: func(e store.Event) error {
		if err := observeSendRecord(events)(e); err != nil {
			return err
		}
		if e.Kind == kind {
			recorded <- struct{}{}
		}
		return nil
	}, Interval: time.Millisecond}
	pty := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	defer func() {
		cancel()
		_ = pty.Close()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("observer returned %v", err)
		}
	}()
	select {
	case <-recorded:
	case <-time.After(time.Second):
		t.Fatalf("%s was not recorded", kind)
	}
	wake := rotationWakeChannel(s.SessionID)
	for {
		select {
		case <-wake:
		default:
			goto drained
		}
	}
drained:
	pty.reads <- []byte("same-state-observation")
	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("repeat observation did not complete")
	}
	select {
	case <-recorded:
		t.Fatalf("%s was repeated during one state residence", kind)
	default:
	}
	if err := UpdateSession(sessions, s.SessionID, func(current *supervisor.ManagedSession) error {
		current.StateSince = current.StateSince.Add(time.Second)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-recorded:
	case <-time.After(time.Second):
		t.Fatalf("%s was not recorded for the next state residence", kind)
	}
}
