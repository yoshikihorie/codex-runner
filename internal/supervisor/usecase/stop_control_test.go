package usecase

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
)

type resumeRotationStore struct {
	mu sync.Mutex
	rotationStore
	onSave func(supervisor.ManagedSession)
}

type resumeRotationRepository struct{ store *resumeRotationStore }

func (r resumeRotationRepository) List(context.Context) ([]supervisor.ManagedSession, error) {
	v, err := r.store.Load(r.store.session.SessionID)
	return []supervisor.ManagedSession{v}, err
}
func (r resumeRotationRepository) Save(_ context.Context, v supervisor.ManagedSession) error {
	return r.store.Save(v)
}

func (s *resumeRotationStore) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.rotationStore.Load(id)
	if v.Rotation != nil {
		copy := *v.Rotation
		v.Rotation = &copy
	}
	return v, err
}

func (s *resumeRotationStore) Save(v supervisor.ManagedSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.onSave != nil {
		s.onSave(v)
	}
	if v.Rotation != nil {
		copy := *v.Rotation
		v.Rotation = &copy
	}
	return s.rotationStore.Save(v)
}

type resumeSignalRespawner struct {
	store  *resumeRotationStore
	called chan RespawnKind
}

func (r resumeSignalRespawner) Execute(_ context.Context, id supervisor.SessionID, kind RespawnKind) error {
	r.called <- kind
	return UpdateSession(r.store, id, func(s *supervisor.ManagedSession) error {
		s.ChildPID = 42
		return nil
	})
}

func TestStopControl_ResumePreparesBeforeFirstSaveAndStartsWorker(t *testing.T) {
	now := time.Date(2026, 10, 12, 3, 0, 0, 0, time.UTC)
	old := now.Add(-2 * rotationStepTimeout)
	id := supervisor.SessionID("resume-atomic-worker-1")
	store := &resumeRotationStore{rotationStore: rotationStore{session: supervisor.ManagedSession{
		SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSession,
		PreviousState: supervisor.SessionRotating, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateRestart, StepStartedAt: &old},
	}}}
	first := true
	store.onSave = func(s supervisor.ManagedSession) {
		if !first {
			return
		}
		first = false
		if s.State != supervisor.SessionRotating || s.Rotation.StepStartedAt == nil || !s.Rotation.StepStartedAt.Equal(now) {
			t.Errorf("first save was not prepared: %+v", s)
		}
	}
	called := make(chan RespawnKind, 1)
	stops := NewStopState(t.TempDir())
	if err := stops.EnableSession(id); err != nil {
		t.Fatal(err)
	}
	rotation := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, Sessions: store, Stop: stops, Send: &rotationSender{}, Respawn: resumeSignalRespawner{store, called},
		Now: func() time.Time { return now }, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true },
		Observed:   func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking },
		ChildAlive: func(supervisor.SessionID) bool { return true },
		Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
			return supervisor.RotationCheckpointSnapshot{}, nil
		},
	}
	control := NewStopControlUseCase(stops, resumeRotationRepository{store}, nil, nil)
	control.now = func() time.Time { return now }
	control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{HasRotation: true}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	control.SetRotationUseCase(rotation, ctx)
	if _, err := control.Resume(context.Background(), &id, true, false); err != nil {
		t.Fatal(err)
	}
	select {
	case kind := <-called:
		if kind != RespawnHandoffStage5 {
			t.Fatalf("respawn kind = %s", kind)
		}
	case <-time.After(time.Second):
		t.Fatal(errors.New("resume did not start rotation worker"))
	}
}

func TestStopControl_ResumeEndWaitsForWakeAfterOldDeadline(t *testing.T) {
	now := time.Date(2026, 10, 12, 3, 0, 0, 0, time.UTC)
	issued := now.Add(-2 * supervisor.SupervisorSendConfirmTimeout)
	id := supervisor.SessionID("resume-end-wake-1")
	store := &resumeRotationStore{rotationStore: rotationStore{session: supervisor.ManagedSession{
		SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSession,
		PreviousState: supervisor.SessionRotating, ChildPID: 10, Rotation: &supervisor.RotationRecord{
			Step: supervisor.RotateSendEnd, StepStartedAt: &issued, EndIssuedAt: &issued,
			CheckpointSnapshot: &supervisor.RotationCheckpointSnapshot{Exists: false},
		},
	}}}
	firstSave := true
	store.onSave = func(s supervisor.ManagedSession) {
		if !firstSave {
			return
		}
		firstSave = false
		if s.State != supervisor.SessionRotating || s.Rotation == nil || !s.Rotation.ResumeEnd || s.Rotation.StepStartedAt == nil || !s.Rotation.StepStartedAt.Equal(now) {
			t.Errorf("resume-end preparation missing from first save: %+v", s.Rotation)
		}
	}
	stops := NewStopState(t.TempDir())
	if err := stops.EnableSession(id); err != nil {
		t.Fatal(err)
	}
	wake := make(chan struct{}, 1)
	waiting := make(chan struct{}, 1)
	var waits atomic.Int32
	var ready atomic.Bool
	sent := make(chan struct{}, 1)
	rotation := &RotateSessionUseCase{PendingRemoteControl: &PendingRemoteControlUseCase{}, Sessions: store, Stop: stops, Send: &rotationSender{onSend: func(SendInput) { sent <- struct{}{} }},
		Now: func() time.Time { return now }, ResumeReady: func(supervisor.SessionID, time.Time) bool { return ready.Load() },
		Observed:   func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle },
		ChildAlive: func(supervisor.SessionID) bool { return true },
		Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
			return supervisor.RotationCheckpointSnapshot{}, nil
		},
		Wake: func(supervisor.SessionID) <-chan struct{} {
			waits.Add(1)
			select {
			case waiting <- struct{}{}:
			default:
			}
			return wake
		},
	}
	control := NewStopControlUseCase(stops, resumeRotationRepository{store}, nil, nil)
	control.now = func() time.Time { return now }
	control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{ChildAlive: true, HasRotation: true}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	control.SetRotationUseCase(rotation, ctx)
	if _, err := control.Resume(context.Background(), &id, true, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("worker spun on expired deadline instead of waiting for wake")
	}
	for range 100 {
		runtime.Gosched()
	}
	if got := waits.Load(); got != 1 {
		t.Fatalf("wait called %d times without observation", got)
	}
	ready.Store(true)
	wake <- struct{}{}
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("resume did not resend after wake")
	}
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("worker did not wait after resend")
	}
	current, err := store.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != supervisor.SessionRotating || current.Rotation.EndIssuedAt == nil || !current.Rotation.EndIssuedAt.Equal(now) {
		t.Fatalf("old send deadline survived resend: %+v", current)
	}
}

func TestStopControl_ResumeStage3WaitUsesInjectedClock(t *testing.T) {
	now := time.Date(2026, 10, 12, 3, 0, 0, 0, time.UTC)
	id := supervisor.SessionID("resume-stage3-clock-1")
	store := &resumeRotationStore{rotationStore: rotationStore{session: supervisor.ManagedSession{
		SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSession,
		PreviousState: supervisor.SessionRotating, ChildPID: 10,
		Rotation: &supervisor.RotationRecord{Step: supervisor.RotateConfirmHandoff, StepStartedAt: &now},
	}}}
	stops := NewStopState(t.TempDir())
	if err := stops.EnableSession(id); err != nil {
		t.Fatal(err)
	}
	waiting := make(chan struct{}, 1)
	var waits atomic.Int32
	rotation := &RotateSessionUseCase{PendingRemoteControl: &PendingRemoteControlUseCase{}, Sessions: store, Stop: stops, Send: &rotationSender{},
		Now: func() time.Time { return now }, ResumeReady: func(supervisor.SessionID, time.Time) bool { return false },
		Observed:   func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking },
		ChildAlive: func(supervisor.SessionID) bool { return true },
		Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
			return supervisor.RotationCheckpointSnapshot{}, nil
		},
		Wake: func(supervisor.SessionID) <-chan struct{} {
			waits.Add(1)
			select {
			case waiting <- struct{}{}:
			default:
			}
			return make(chan struct{})
		},
	}
	control := NewStopControlUseCase(stops, resumeRotationRepository{store}, nil, nil)
	control.now = func() time.Time { return now }
	control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{ChildAlive: true, HasRotation: true}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	control.SetRotationUseCase(rotation, ctx)
	if _, err := control.Resume(context.Background(), &id, true, false); err != nil {
		t.Fatal(err)
	}
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("stage 3 worker did not wait")
	}
	for range 100 {
		runtime.Gosched()
	}
	if got := waits.Load(); got != 1 {
		t.Fatalf("stage 3 spun with injected clock: waits=%d", got)
	}
}

func TestStopControl_ResumeWithoutChildRunsRecordedStage(t *testing.T) {
	for _, tc := range []struct {
		step        supervisor.SupervisorRotateStep
		wantRespawn RespawnKind
	}{
		{supervisor.RotateSendEnd, RespawnHandoffResume},
		{supervisor.RotateConfirmHandoff, RespawnHandoffResume},
		{supervisor.RotateSendExit, RespawnHandoffStage5},
		{supervisor.RotateRestart, RespawnHandoffStage5},
		{supervisor.RotateSendStart, ""},
		{supervisor.RotateSendParallelInstruction, ""},
	} {
		t.Run(string(tc.step), func(t *testing.T) {
			now := time.Date(2026, 10, 12, 3, 0, 0, 0, time.UTC)
			id := supervisor.SessionID("resume-no-child-1")
			var lastSend *supervisor.LastSendRecord
			if tc.step == supervisor.RotateSendExit {
				lastSend = &supervisor.LastSendRecord{Kind: supervisor.SendKindHandoffExit, Confirmed: true}
			}
			store := &resumeRotationStore{rotationStore: rotationStore{session: supervisor.ManagedSession{
				SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopRotateStepFailed,
				PreviousState: supervisor.SessionRotating, ClaudeSessionID: "claude-id", RestartCount: 3, LastSend: lastSend,
				Rotation: &supervisor.RotationRecord{Step: tc.step, StepStartedAt: &now},
			}}}
			stops := NewStopState(t.TempDir())
			if err := stops.EnableSession(id); err != nil {
				t.Fatal(err)
			}
			called := make(chan RespawnKind, 1)
			advanced := make(chan struct{}, 1)
			firstSave := true
			store.onSave = func(s supervisor.ManagedSession) {
				if firstSave {
					firstSave = false
					if (tc.step == supervisor.RotateSendEnd || tc.step == supervisor.RotateConfirmHandoff) && (s.Rotation == nil || !s.Rotation.ResumeSpawn) {
						t.Errorf("resume-spawn preparation missing from first save: %+v", s.Rotation)
					}
				}
				if s.Rotation != nil && s.Rotation.Step == supervisor.RotateRestart && s.Rotation.RespawnWaitUntil != nil {
					select {
					case advanced <- struct{}{}:
					default:
					}
				}
			}
			rotation := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, Sessions: store, Stop: stops, Send: &rotationSender{}, Respawn: resumeSignalRespawner{store, called},
				Now: func() time.Time { return now }, ResumeReady: func(supervisor.SessionID, time.Time) bool { return false },
				Observed:   func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking },
				ChildAlive: func(supervisor.SessionID) bool { v, _ := store.Load(id); return v.ChildPID > 0 },
				Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
					return supervisor.RotationCheckpointSnapshot{}, nil
				},
			}
			control := NewStopControlUseCase(stops, resumeRotationRepository{store}, nil, nil)
			control.now = func() time.Time { return now }
			control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
				return supervisor.TransitionContext{HasRotation: true}, nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			control.SetRotationUseCase(rotation, ctx)
			if _, err := control.Resume(context.Background(), &id, true, false); err != nil {
				t.Fatal(err)
			}
			if tc.wantRespawn != "" {
				select {
				case got := <-called:
					if got != tc.wantRespawn {
						t.Fatalf("respawn = %s, want %s", got, tc.wantRespawn)
					}
				case <-time.After(time.Second):
					t.Fatal("resume did not start worker")
				}
			} else {
				select {
				case <-advanced:
				case <-time.After(time.Second):
					t.Fatal("resume did not advance absent child to restart")
				}
			}
		})
	}
}

func TestStopControl_ResumeKeepsRecordedMissingCheckpoint(t *testing.T) {
	now := time.Date(2026, 10, 12, 3, 0, 0, 0, time.UTC)
	id := supervisor.SessionID("resume-missing-checkpoint-1")
	store := &resumeRotationStore{rotationStore: rotationStore{session: supervisor.ManagedSession{
		SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSession,
		PreviousState: supervisor.SessionRotating, ChildPID: 10,
		Rotation: &supervisor.RotationRecord{Step: supervisor.RotateSendEnd, StepStartedAt: &now,
			CheckpointSnapshot: &supervisor.RotationCheckpointSnapshot{Exists: false}},
	}}}
	stops := NewStopState(t.TempDir())
	if err := stops.EnableSession(id); err != nil {
		t.Fatal(err)
	}
	var inspections atomic.Int32
	atSend := make(chan int32, 1)
	sender := &rotationSender{onSend: func(SendInput) { atSend <- inspections.Load() }}
	rotation := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, Sessions: store, Stop: stops, Send: sender,
		Now: func() time.Time { return now }, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true },
		Observed:   func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle },
		ChildAlive: func(supervisor.SessionID) bool { return true },
		Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
			inspections.Add(1)
			return supervisor.RotationCheckpointSnapshot{Exists: true}, nil
		},
	}
	control := NewStopControlUseCase(stops, resumeRotationRepository{store}, nil, nil)
	control.now = func() time.Time { return now }
	control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{ChildAlive: true, HasRotation: true}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	control.SetRotationUseCase(rotation, ctx)
	if _, err := control.Resume(context.Background(), &id, true, false); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-atSend:
		if got != 0 {
			t.Fatalf("checkpoint was recaptured %d times before send", got)
		}
	case <-time.After(time.Second):
		t.Fatal("resume did not send #end")
	}
}

type resumeNotificationCapture struct {
	kinds []supervisor.SupervisorEventKind
}

func (n *resumeNotificationCapture) Notify(_ context.Context, kind supervisor.SupervisorEventKind, _ string) error {
	n.kinds = append(n.kinds, kind)
	return nil
}

func TestSCNsup0526StoppedOrphanRejectsWithOriginalReason(t *testing.T) {
	started := time.Now().UTC()
	id := supervisor.SessionID("alpha-1")
	records := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSupervisorRestarted, ChildPID: 123, ChildStartedAt: &started, ClaudeSessionID: "claude-alpha"}}}
	u := NewStopControlUseCase(NewStopState(t.TempDir()), records, nil, nil)
	u.SetResumeTransitionContext(NewProcessResumeTransitionContext(func(int) bool { return true }, func(int) (time.Time, bool) { return started, true }))
	spawned := false
	u.SetResumeContinuations(nil, func(context.Context, supervisor.ManagedSession) error { spawned = true; return nil })
	out, err := u.Resume(context.Background(), &id, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.StillStopped) != 1 || out.StillStopped[0].Reason != "orphan_child_alive" || records.sessions[0].StopReason != supervisor.StopSupervisorRestarted || spawned {
		t.Fatalf("out=%+v session=%+v spawned=%v", out, records.sessions[0], spawned)
	}
}

func TestOrphanResumeRefusalNotifies(t *testing.T) {
	started := time.Now().UTC()
	id := supervisor.SessionID("alpha-orphan-notify-1")
	records := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSupervisorRestarted, ChildPID: 123, ChildStartedAt: &started, ClaudeSessionID: "claude-alpha"}}}
	notifications := &resumeNotificationCapture{}
	u := NewStopControlUseCase(NewStopState(t.TempDir()), records, nil, notifications)
	u.SetResumeTransitionContext(NewProcessResumeTransitionContext(func(int) bool { return true }, func(int) (time.Time, bool) { return started, true }))
	_, err := u.Resume(context.Background(), &id, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if !containsEvent(notifications.kinds, supervisor.EventKindOrphanChildAlive) {
		t.Fatalf("notifications=%v", notifications.kinds)
	}
}

func TestFix3OrphanNotificationReachesNotifyUseCase(t *testing.T) {
	started := time.Now().UTC()
	id := supervisor.SessionID("alpha-orphan-real-notify-1")
	records := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSupervisorRestarted, ChildPID: 123, ChildStartedAt: &started, ClaudeSessionID: "claude-alpha"}}}
	runner := &fakeRunner{}
	notifier := NewNotifyUseCase("/bin/echo", runner, nil, time.Now)
	u := NewStopControlUseCase(NewStopState(t.TempDir()), records, nil, notifier)
	u.SetResumeTransitionContext(NewProcessResumeTransitionContext(func(int) bool { return true }, func(int) (time.Time, bool) { return started, true }))
	_, err := u.Resume(context.Background(), &id, true, false)
	if err != nil {
		t.Fatal(err)
	}
	notifier.Close()
	if len(runner.calls) != 1 || len(runner.calls[0]) != 2 || runner.calls[0][0] != string(supervisor.EventKindOrphanChildAlive) {
		t.Fatalf("notification calls=%v", runner.calls)
	}
}

func TestOwnedChildResumesWhileStopped(t *testing.T) {
	started := time.Now().UTC()
	id := supervisor.SessionID("alpha-owned")
	ownedChildren.Register(id, 123)
	defer ownedChildren.Remove(id, 123)
	s := supervisor.ManagedSession{SessionID: id, State: supervisor.SessionStopped, ChildPID: 123, ChildStartedAt: &started, ClaudeSessionID: "claude-alpha"}
	c, err := NewProcessResumeTransitionContext(func(int) bool { return true }, func(int) (time.Time, bool) { return started, true })(context.Background(), s)
	if err != nil || !c.ChildAlive || c.OrphanChildAlive {
		t.Fatalf("context=%+v err=%v", c, err)
	}
}

func TestStoppedOwnedChildResumesWithoutRespawn(t *testing.T) {
	started := time.Now().UTC()
	id := supervisor.SessionID("alpha-owned-resume-1")
	ownedChildren.Register(id, 987)
	defer ownedChildren.Remove(id, 987)
	records := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSession, ChildPID: 987, ChildStartedAt: &started, ClaudeSessionID: "claude-alpha", PreviousState: supervisor.SessionIdle}}}
	u := NewStopControlUseCase(NewStopState(t.TempDir()), records, nil, nil)
	u.SetResumeTransitionContext(NewProcessResumeTransitionContext(func(int) bool { return true }, func(int) (time.Time, bool) { return started, true }))
	spawned := false
	u.SetResumeContinuations(nil, func(context.Context, supervisor.ManagedSession) error { spawned = true; return nil })
	out, err := u.Resume(context.Background(), &id, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Resumed) != 1 || records.sessions[0].State != supervisor.SessionIdle || spawned {
		t.Fatalf("out=%+v record=%+v spawned=%v", out, records.sessions[0], spawned)
	}
}

func TestObservedOwnedChildResumesAfterStop(t *testing.T) {
	s, sessions := recoveryObservationFixture(t)
	pty := &waitingPTY{closed: make(chan struct{})}
	defer pty.Close()
	ownedChildren.Register(s.SessionID, s.ChildPID)
	defer ownedChildren.Remove(s.SessionID, s.ChildPID)
	stops := NewStopState(t.TempDir())
	control := NewStopControlUseCase(stops, regressionStopRepository{sessions}, nil, nil)
	control.SetResumeTransitionContext(NewProcessResumeTransitionContext(func(int) bool { return true }, func(int) (time.Time, bool) { return s.StateSince, true }))
	spawned := false
	control.SetResumeContinuations(nil, func(context.Context, supervisor.ManagedSession) error { spawned = true; return nil })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observer := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle}}}, Sessions: sessions, Stop: stops, Interval: time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- observer.Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	if _, err := control.Stop(context.Background(), &s.SessionID); err != nil {
		t.Fatal(err)
	}
	current, _ := sessions.Load(s.SessionID)
	if current.State != supervisor.SessionStopped || !ownedChildren.Owns(s.SessionID, s.ChildPID) {
		t.Fatalf("STOP lost living child: %+v", current)
	}
	out, err := control.Resume(context.Background(), &s.SessionID, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Resumed) != 1 || spawned {
		t.Fatalf("resume=%+v spawned=%t", out, spawned)
	}
	deadline := time.After(time.Second)
	for {
		current, _ = sessions.Load(s.SessionID)
		if current.State == supervisor.SessionIdle {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("owned child did not resume observation: %+v", current)
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	_ = pty.Close()
	<-done
}

func TestStopControl_SCN38_RotationResumeResetsConsecutiveCount(t *testing.T) {
	id := supervisor.SessionID("alpha-rotation-resume-1")
	now := time.Now().UTC()
	records := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSession, RestartCount: 2, RestartTimes: []time.Time{now}, Rotation: &supervisor.RotationRecord{StartedAt: now}}}}
	u := NewStopControlUseCase(NewStopState(t.TempDir()), records, nil, nil)
	if _, err := u.Resume(context.Background(), &id, true, false); err != nil {
		t.Fatal(err)
	}
	if records.sessions[0].RestartCount != 0 || len(records.sessions[0].RestartTimes) != 1 {
		t.Fatalf("rotation resume counters: %+v", records.sessions[0])
	}
}

func TestStopControl_SCN38_ExitTimeoutKeepsStopped(t *testing.T) {
	id := supervisor.SessionID("alpha-exit-38")
	now := time.Now().UTC()
	records := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopRotateStepFailed, PreviousState: supervisor.SessionRotating, ChildPID: 123, ChildStartedAt: &now, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateSendExit, FailReason: supervisor.RotateFailExitTimeout, StartedAt: now}}}}
	u := NewStopControlUseCase(NewStopState(t.TempDir()), records, nil, nil)
	u.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{ChildAlive: true, HasRotation: true}, nil
	})
	called := false
	u.SetResumeContinuations(func(context.Context, supervisor.ManagedSession) error { called = true; return nil }, nil)
	out, err := u.Resume(context.Background(), &id, true, false)
	if err != nil || len(out.StillStopped) != 1 || out.StillStopped[0].Reason != "exit_process_alive" || records.sessions[0].State != supervisor.SessionStopped || called {
		t.Fatalf("out=%+v err=%v session=%+v called=%v", out, err, records.sessions[0], called)
	}
}

func TestStopControl_ExitConfirmationStoppedKeepsStopped(t *testing.T) {
	id := supervisor.SessionID("alpha-exit-stop-1")
	now := time.Now().UTC()
	records := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSession, PreviousState: supervisor.SessionRotating, ChildPID: 123, ChildStartedAt: &now, SendGate: supervisor.SendGateRecord{Pending: &supervisor.PendingSend{Kind: supervisor.SendKindHandoffExit, IssuedAt: now}}, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateSendExit, StartedAt: now}}}}
	u := NewStopControlUseCase(NewStopState(t.TempDir()), records, nil, nil)
	u.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{ChildAlive: true, HasRotation: true}, nil
	})
	called := false
	u.SetResumeContinuations(func(context.Context, supervisor.ManagedSession) error { called = true; return nil }, nil)
	out, err := u.Resume(context.Background(), &id, true, false)
	if err != nil || len(out.StillStopped) != 1 || out.StillStopped[0].Reason != "exit_process_alive" || records.sessions[0].State != supervisor.SessionStopped || called {
		t.Fatalf("out=%+v err=%v session=%+v called=%v", out, err, records.sessions[0], called)
	}
}

func TestStopControl_SCN39_MissingResumeIDStaysStoppedBeforeTransition(t *testing.T) {
	id := supervisor.SessionID("missing-resume-id-1")
	now := time.Now().UTC()
	records := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopRotateStepFailed, PreviousState: supervisor.SessionRotating, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateSendEnd, StartedAt: now}}}}
	u := NewStopControlUseCase(NewStopState(t.TempDir()), records, nil, nil)
	u.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{HasRotation: true}, nil
	})
	out, err := u.Resume(context.Background(), &id, true, false)
	if err != nil || len(out.StillStopped) != 1 || out.StillStopped[0].Reason != "claude_session_id_missing" || records.sessions[0].State != supervisor.SessionStopped {
		t.Fatalf("out=%+v err=%v session=%+v", out, err, records.sessions[0])
	}
}

func TestStopControl_SCN40_RotationResumeReturnsBeforeSending(t *testing.T) {
	id := supervisor.SessionID("resume-no-send-40")
	now := time.Now().UTC()
	records := &regressionSessionStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSession, PreviousState: supervisor.SessionRotating, ChildPID: 10, ChildStartedAt: &now, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateSendExit, StepStartedAt: &now}}}
	stops := NewStopState(t.TempDir())
	held := &heldRotationSender{started: make(chan struct{}, 1), release: make(chan struct{})}
	rotation := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, Sessions: records, Stop: stops, Send: held, ResumeReady: func(supervisor.SessionID, time.Time) bool { return false }, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle }, ChildAlive: func(supervisor.SessionID) bool { return true }, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return supervisor.RotationCheckpointSnapshot{}, nil
	}}
	control := NewStopControlUseCase(stops, regressionStopRepository{records}, nil, nil)
	control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{ChildAlive: true, HasRotation: true}, nil
	})
	control.SetResumeContinuations(rotation.Resume, nil)
	done := make(chan error, 1)
	go func() { _, err := control.Resume(context.Background(), &id, true, false); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(held.release)
		t.Fatal("resume blocked on handoff send")
	}
	select {
	case <-held.started:
		t.Fatal("resume sent /exit synchronously")
	default:
	}
	close(held.release)
}

func TestStopControl_SCN36_ResumeStage3AdvancesAfterFreshCheckpoint(t *testing.T) {
	id := supervisor.SessionID("resume-stage3-36")
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	mtime := now.Add(time.Second)
	records := &regressionSessionStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionRotating, ChildPID: 10, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateConfirmHandoff, StepStartedAt: &now, EndIssuedAt: &now, CheckpointSnapshot: &supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "old"}}}}
	stops := NewStopState(t.TempDir())
	updated := false
	sender := &rotationSender{}
	rotation := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, Sessions: records, Stop: stops, Send: sender, ResumeReady: func(supervisor.SessionID, time.Time) bool { return false }, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle }, ChildAlive: func(supervisor.SessionID) bool { return true }, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		if !updated {
			return supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "old"}, nil
		}
		return supervisor.RotationCheckpointSnapshot{Exists: true, Mtime: &mtime, HeadHash: "new"}, nil
	}, Now: func() time.Time { return now }}
	control := NewStopControlUseCase(stops, regressionStopRepository{records}, nil, nil)
	control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		return supervisor.TransitionContext{ChildAlive: true, HasRotation: true}, nil
	})
	control.SetResumeContinuations(rotation.Resume, nil)
	if _, err := control.Stop(context.Background(), &id); err != nil {
		t.Fatal(err)
	}
	if _, err := control.Resume(context.Background(), &id, true, false); err != nil {
		t.Fatal(err)
	}
	if err := rotation.Step(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	updated = true
	if err := rotation.Step(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	current, _ := records.Load(id)
	if current.State != supervisor.SessionRotating || current.Rotation.Step != supervisor.RotateSendExit || len(sender.calls) != 0 {
		t.Fatalf("resume did not advance to stage 4 safely: %+v calls=%+v", current, sender.calls)
	}
}

type stopRotationRespawner struct {
	store *regressionSessionStore
	kind  RespawnKind
}

func (r *stopRotationRespawner) Execute(_ context.Context, _ supervisor.SessionID, kind RespawnKind) error {
	r.kind = kind
	s, err := r.store.Load("")
	if err != nil {
		return err
	}
	s.ChildPID = 456
	return r.store.Save(s)
}

func TestStopControl_SCN39_ResumeRecordedStageAfterStopClears(t *testing.T) {
	for _, tc := range []struct {
		name string
		step supervisor.SupervisorRotateStep
		want RespawnKind
	}{
		{"stage2", supervisor.RotateSendEnd, RespawnHandoffResume},
		{"stage3", supervisor.RotateConfirmHandoff, RespawnHandoffResume},
		{"stage5", supervisor.RotateRestart, RespawnHandoffStage5},
		{"stage6", supervisor.RotateSendStart, ""},
		{"stage7", supervisor.RotateSendParallelInstruction, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := supervisor.SessionID("resume-rotation-1")
			now := time.Now().UTC()
			initial := supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude, State: supervisor.SessionStopped, StopReason: supervisor.StopRotateStepFailed, PreviousState: supervisor.SessionRotating, ClaudeSessionID: "claude-id", RestartCount: 2, RestartTimes: []time.Time{now}, Rotation: &supervisor.RotationRecord{Step: tc.step, StartedAt: now, StepStartedAt: &now}}
			records := &regressionSessionStore{session: initial}
			stops := NewStopState(t.TempDir())
			if err := stops.EnableSession(id); err != nil {
				t.Fatal(err)
			}
			respawn := &stopRotationRespawner{store: records}
			rotation := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }, Sessions: records, Stop: stops, Send: &rotationSender{}, Respawn: respawn, Now: func() time.Time { return now }, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking }, ChildAlive: func(supervisor.SessionID) bool {
				s, _ := records.Load(id)
				return s.ChildPID > 0
			}}
			rotation.ResumeReady = func(supervisor.SessionID, time.Time) bool { return false }
			rotation.Inspect = func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
				return supervisor.RotationCheckpointSnapshot{}, nil
			}
			control := NewStopControlUseCase(stops, regressionStopRepository{records}, nil, nil)
			control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
				return supervisor.TransitionContext{HasRotation: true}, nil
			})
			control.SetResumeContinuations(rotation.Resume, nil)
			out, err := control.Resume(context.Background(), &id, true, false)
			if err != nil || len(out.Resumed) != 1 {
				t.Fatalf("out=%+v err=%v", out, err)
			}
			if err := rotation.Step(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			current, err := records.Load(id)
			if err != nil || current.State != supervisor.SessionRotating || current.RestartCount != 0 || len(current.RestartTimes) != 1 || respawn.kind != tc.want {
				t.Fatalf("session=%+v respawn=%s err=%v", current, respawn.kind, err)
			}
			if tc.want == "" && (current.Rotation.Step != supervisor.RotateRestart || current.Rotation.RespawnWaitUntil == nil) {
				t.Fatalf("rotation did not wait for restart: %+v", current.Rotation)
			}
		})
	}
}

func TestResumeWithoutRecoveryStarterPreservesCounters(t *testing.T) {
	id := supervisor.SessionID("alpha-no-starter-1")
	now := time.Now().UTC()
	records := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSession, ClaudeSessionID: "claude-alpha", RestartCount: 2, RestartTimes: []time.Time{now}}}}
	u := NewStopControlUseCase(NewStopState(t.TempDir()), records, nil, nil)
	if _, err := u.Resume(context.Background(), &id, true, false); err != nil {
		t.Fatal(err)
	}
	if records.sessions[0].RestartCount != 2 || len(records.sessions[0].RestartTimes) != 1 {
		t.Fatalf("counters reset without recovery: %+v", records.sessions[0])
	}
}

func TestUnownedChildWithUnknownStartFailsClosed(t *testing.T) {
	started := time.Now().UTC()
	s := supervisor.ManagedSession{SessionID: "alpha-unknown", State: supervisor.SessionStopped, ChildPID: 456, ChildStartedAt: &started}
	c, err := NewProcessResumeTransitionContext(func(int) bool { return true }, func(int) (time.Time, bool) { return time.Time{}, false })(context.Background(), s)
	if err != nil || !c.OrphanChildAlive {
		t.Fatalf("context=%+v err=%v", c, err)
	}
}

func TestSCNsup0527ResumeRecoveryUsesIDAndResetsCounters(t *testing.T) {
	respawn, record, spy, id := respawnFixture(t)
	record.session.State = supervisor.SessionStopped
	record.session.StopReason = supervisor.StopSupervisorRestarted
	record.session.RestartCount = 2
	record.session.RestartTimes = []time.Time{respawn.Now()}
	events := &stopEventCapture{}
	u := NewStopControlUseCase(NewStopState(t.TempDir()), regressionStopRepository{record}, events, nil)
	u.SetResumeContinuations(nil, func(ctx context.Context, _ supervisor.ManagedSession) error {
		return respawn.Execute(ctx, id, RespawnResumeRecovery)
	})
	out, err := u.Resume(context.Background(), &id, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Resumed) != 1 || spy.calls != 1 || !slices.Contains(spy.argv, "--resume") || record.session.Generation != 2 || record.session.RestartCount != 0 || len(record.session.RestartTimes) != 0 || !containsEvent(events.kinds, supervisor.EventKindRestartCountersReset) {
		t.Fatalf("out=%+v session=%+v argv=%v events=%v", out, record.session, spy.argv, events.kinds)
	}
}

func TestSCNsup0527DeadOrphanResumesRecovery(t *testing.T) {
	started := time.Now().UTC()
	id := supervisor.SessionID("alpha-1")
	records := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSupervisorRestarted, ChildPID: 123, ChildStartedAt: &started, ClaudeSessionID: "claude-alpha", RestartCount: 2, RestartTimes: []time.Time{started}}}}
	u := NewStopControlUseCase(NewStopState(t.TempDir()), records, nil, nil)
	u.SetResumeTransitionContext(NewProcessResumeTransitionContext(func(int) bool { return false }, func(int) (time.Time, bool) { return started, true }))
	spawned := false
	u.SetResumeContinuations(nil, func(context.Context, supervisor.ManagedSession) error { spawned = true; return nil })
	out, err := u.Resume(context.Background(), &id, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Resumed) != 1 || !spawned || records.sessions[0].ChildPID != 0 || records.sessions[0].RestartCount != 0 || len(records.sessions[0].RestartTimes) != 0 {
		t.Fatalf("out=%+v session=%+v spawned=%v", out, records.sessions[0], spawned)
	}
}
