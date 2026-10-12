package usecase

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
)

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

func TestRotationResumePreservesRestartCounters(t *testing.T) {
	id := supervisor.SessionID("alpha-rotation-resume-1")
	now := time.Now().UTC()
	records := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: id, State: supervisor.SessionStopped, StopReason: supervisor.StopSession, RestartCount: 2, RestartTimes: []time.Time{now}, Rotation: &supervisor.RotationRecord{StartedAt: now}}}}
	u := NewStopControlUseCase(NewStopState(t.TempDir()), records, nil, nil)
	if _, err := u.Resume(context.Background(), &id, true, false); err != nil {
		t.Fatal(err)
	}
	if records.sessions[0].RestartCount != 2 || len(records.sessions[0].RestartTimes) != 1 {
		t.Fatalf("counters reset during rotation: %+v", records.sessions[0])
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
