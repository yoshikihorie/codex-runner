package usecase

import (
	"context"
	"io"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

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
