package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
)

func TestSCNsup0516RecoveryWaitRespawnsAndCounts(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	sessions.session.State = supervisor.SessionRecovering
	recovery := RecoverSessionUseCase{Store: sessions, Respawn: u.Execute, Now: u.Now,
		Wait: func(_ context.Context, delay time.Duration) error {
			if delay != supervisor.RestartBackoff(0) {
				t.Fatalf("delay=%v", delay)
			}
			return nil
		}}
	if err := recovery.Execute(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if sessions.session.State != supervisor.SessionStarting || sessions.session.Generation != 2 || sessions.session.RestartCount != 1 || spy.calls != 1 {
		t.Fatalf("session=%+v calls=%d", sessions.session, spy.calls)
	}
}

func TestSCNsup0522StopDuringWaitPreventsSpawn(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	sessions.session.State = supervisor.SessionRecovering
	stops := NewStopState(t.TempDir())
	recovery := RecoverSessionUseCase{Store: sessions, Stop: stops, Respawn: u.Execute, Now: u.Now,
		Wait: func(context.Context, time.Duration) error { stops.SetGlobal(true); return nil }}
	if err := recovery.Execute(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if sessions.session.State != supervisor.SessionStopped || spy.calls != 0 {
		t.Fatalf("session=%+v calls=%d", sessions.session, spy.calls)
	}
	// This makes the STOP assertion sensitive: without STOP the same fixture must spawn.
	stops.SetGlobal(false)
	sessions.session.State = supervisor.SessionRecovering
	sessions.session.StopReason = ""
	recovery.Wait = func(context.Context, time.Duration) error { return nil }
	if err := recovery.Execute(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if spy.calls != 1 {
		t.Fatalf("without STOP, spawn calls=%d", spy.calls)
	}
}

func TestSCNsup0522DefaultWaitObservesStopBeforeBackoffEnds(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	sessions.session.State = supervisor.SessionRecovering
	sessions.session.RestartCount = supervisor.RestartMaxCount - 1
	if delay := supervisor.RestartBackoff(sessions.session.RestartCount); delay <= timingBatchWatchdog {
		t.Fatalf("fixture backoff %s must outlast watchdog", delay)
	}
	stops := NewStopState(t.TempDir())
	recovery := RecoverSessionUseCase{Store: sessions, Stop: stops, Respawn: u.Execute, Now: u.Now}
	watchdog := timingBatchContext(t)
	ctx, cancel := context.WithCancel(watchdog)
	waiting := make(chan error, 1)
	observed := &timingBatchDoneContext{Context: ctx, onDone: func() {
		waiting <- timingBatchDoneCaller(".waitRecoveryDelay")
	}}
	done := make(chan error, 1)
	go func() { done <- recovery.Execute(observed, id) }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			timingBatchAwait(t, done)
		}
	}()
	// Check the direct Done caller before setting STOP from the test goroutine.
	if arrivalErr := timingBatchAwait(t, waiting); arrivalErr != nil {
		t.Fatal(arrivalErr)
	}
	stops.SetGlobal(true)
	err := timingBatchAwait(t, done)
	joined = true
	timingBatchHealthy(t, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Wait != nil || sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopGlobal || spy.calls != 0 {
		t.Fatalf("session=%+v calls=%d", sessions.session, spy.calls)
	}
}

func TestSCNsup0545MissingCurrentClaudeIDStops(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	sessions.session.State = supervisor.SessionRecovering
	sessions.session.ClaudeSessionID = ""
	sessions.session.PreviousClaudeSessionID = "00000000-0000-0000-0000-000000000001"
	var notified supervisor.SupervisorEventKind
	recovery := RecoverSessionUseCase{Store: sessions, Respawn: u.Execute, Now: u.Now, Wait: func(context.Context, time.Duration) error { return nil }, Notify: func(kind supervisor.SupervisorEventKind, _ string) { notified = kind }}
	if err := recovery.Execute(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopClaudeSessionIDMissing || spy.calls != 0 || notified != supervisor.EventKindSessionStopped {
		t.Fatalf("session=%+v calls=%d", sessions.session, spy.calls)
	}
}

func TestSCNsup0518RecoveryLimitsStopBeforeSpawn(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
		times []time.Time
		want  supervisor.SupervisorStopReason
	}{
		{"consecutive", supervisor.RestartMaxCount, nil, supervisor.StopRestartLimitConsecutive},
		{"daily", 0, make([]time.Time, supervisor.RestartMaxPerDay), supervisor.StopRestartLimitDaily},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, sessions, spy, id := respawnFixture(t)
			sessions.session.State = supervisor.SessionRecovering
			sessions.session.RestartCount = tc.count
			for i := range tc.times {
				tc.times[i] = u.Now()
			}
			sessions.session.RestartTimes = tc.times
			var notified supervisor.SupervisorEventKind
			recovery := RecoverSessionUseCase{Store: sessions, Respawn: u.Execute, Now: u.Now, Wait: func(context.Context, time.Duration) error { return nil }, Notify: func(kind supervisor.SupervisorEventKind, _ string) { notified = kind }}
			if err := recovery.Execute(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			if sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != tc.want || spy.calls != 0 || notified != supervisor.EventKindRestartLimitExceeded {
				t.Fatalf("session=%+v calls=%d", sessions.session, spy.calls)
			}
		})
	}
}

func TestRecoveryUsesSupervisorLifetime(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	sessions.session.State = supervisor.SessionRecovering
	observerCtx, cancel := context.WithCancel(context.Background())
	cancel()
	recovery := RecoverSessionUseCase{Store: sessions, Respawn: u.Execute, Now: u.Now, LifetimeContext: context.Background(), Wait: func(ctx context.Context, _ time.Duration) error { return ctx.Err() }}
	if err := recovery.Execute(observerCtx, id); err != nil {
		t.Fatal(err)
	}
	if spy.calls != 1 {
		t.Fatalf("spawn calls=%d", spy.calls)
	}
}

func TestRecoveryStartsRespawnObserver(t *testing.T) {
	u, sessions, _, id := respawnFixture(t)
	sessions.session.State = supervisor.SessionRecovering
	observed := make(chan struct{})
	u.Observe = func(_ context.Context, _ supervisor.ManagedSession, pty ManagedPTY) {
		_, _ = pty.Wait()
		close(observed)
	}
	recovery := RecoverSessionUseCase{Store: sessions, Respawn: u.Execute, Now: u.Now, Wait: func(context.Context, time.Duration) error { return nil }}
	if err := recovery.Execute(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	select {
	case <-observed:
	case <-time.After(time.Second):
		t.Fatal("recovery observer did not wait")
	}
}

func TestRecoveryCancellationRecordsStoppedState(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	sessions.session.State = supervisor.SessionRecovering
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	recovery := RecoverSessionUseCase{Store: sessions, Respawn: u.Execute, Now: u.Now, Wait: func(ctx context.Context, _ time.Duration) error { return ctx.Err() }}
	if err := recovery.Execute(ctx, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopSupervisorRestarted || spy.calls != 0 {
		t.Fatalf("session=%+v calls=%d", sessions.session, spy.calls)
	}
}

func TestSCNsup0520NormalExitDoesNotRecover(t *testing.T) {
	s, sessions := recoveryObservationFixture(t)
	called := false
	recovery := &RecoverSessionUseCase{Store: sessions, Respawn: func(context.Context, supervisor.SessionID, RespawnKind) error { called = true; return nil }}
	observer := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: sessions, Recover: recovery, Interval: time.Millisecond}
	if err := observer.Run(context.Background(), s, exitedPTY{code: 0}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	current, err := sessions.Load(s.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != supervisor.SessionExited || called {
		t.Fatalf("state=%s recovered=%v", current.State, called)
	}
}
