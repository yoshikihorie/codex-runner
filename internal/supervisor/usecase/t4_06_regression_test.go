package usecase

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/launch"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type testEvent = store.Event

type regressionSessionStore struct {
	mu      sync.Mutex
	session supervisor.ManagedSession
	saves   int
	saved   chan struct{}
}

func (s *regressionSessionStore) Load(supervisor.SessionID) (supervisor.ManagedSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneRegressionSession(s.session), nil
}

func (s *regressionSessionStore) Save(v supervisor.ManagedSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.session = cloneRegressionSession(v)
	s.saves++
	if s.saved != nil {
		select {
		case s.saved <- struct{}{}:
		default:
		}
	}
	return nil
}

func cloneRegressionPointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func cloneRegressionSession(v supervisor.ManagedSession) supervisor.ManagedSession {
	v.ChildStartedAt = cloneRegressionPointer(v.ChildStartedAt)
	v.SendGate.LastConfirmedAt = cloneRegressionPointer(v.SendGate.LastConfirmedAt)
	v.SendGate.Pending = cloneRegressionPointer(v.SendGate.Pending)
	v.LastSend = cloneRegressionPointer(v.LastSend)
	v.RestartTimes = append([]time.Time(nil), v.RestartTimes...)
	if v.Rotation != nil {
		rotation := *v.Rotation
		rotation.StepStartedAt = cloneRegressionPointer(rotation.StepStartedAt)
		rotation.EndIssuedAt = cloneRegressionPointer(rotation.EndIssuedAt)
		rotation.RespawnWaitUntil = cloneRegressionPointer(rotation.RespawnWaitUntil)
		if rotation.CheckpointSnapshot != nil {
			snapshot := *rotation.CheckpointSnapshot
			snapshot.Mtime = cloneRegressionPointer(snapshot.Mtime)
			rotation.CheckpointSnapshot = &snapshot
		}
		v.Rotation = &rotation
	}
	v.PendingRemoteControlSince = cloneRegressionPointer(v.PendingRemoteControlSince)
	return v
}

type constantDetector struct {
	result detect.DetectResult
	calls  int
}

func (d *constantDetector) Detect(context.Context, detect.DetectInput) (detect.DetectResult, error) {
	d.calls++
	return d.result, nil
}

type endPTY struct{ waited bool }

func (p *endPTY) Read([]byte) (int, error) { return 0, io.EOF }
func (p *endPTY) Wait() (int, error)       { p.waited = true; return 1, nil }
func (p *endPTY) Close() error             { return nil }

type waitingPTY struct{ closed chan struct{} }

func (p *waitingPTY) Read([]byte) (int, error) { <-p.closed; return 0, io.EOF }
func (p *waitingPTY) Wait() (int, error)       { <-p.closed; return 1, nil }
func (p *waitingPTY) Close() error {
	select {
	case <-p.closed:
	default:
		close(p.closed)
	}
	return nil
}

type exitedPTY struct{ code int }

func (p exitedPTY) Read([]byte) (int, error) { return 0, io.EOF }
func (p exitedPTY) Wait() (int, error)       { return p.code, nil }
func (exitedPTY) Close() error               { return nil }

func TestSCNsup01007ExitWhileStoppedClearsPIDAndRecordsEvents(t *testing.T) {
	id, _ := supervisor.NewSessionID("stopped-1")
	started := time.Now().UTC()
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StateSince: started, StopScope: supervisor.StopScopeGlobal, StopReason: supervisor.StopGlobal, ChildPID: 123, ChildStartedAt: &started, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	store := &regressionSessionStore{session: s}
	stops := NewStopState(t.TempDir())
	stops.SetSession(id, true)
	var recorded, notified []supervisor.SupervisorEventKind
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: store, Stop: stops, RecordStructured: func(event testEvent) error {
		k := event.Kind
		recorded = append(recorded, k)

		return nil
	}, Notify: func(k supervisor.SupervisorEventKind, _ string) { notified = append(notified, k) }}
	_ = u.Run(context.Background(), s, exitedPTY{code: 0}, supervisor.NewVirtualTerminal(80, 24))
	if store.session.State != supervisor.SessionStopped || store.session.ChildPID != 0 || store.session.ChildStartedAt != nil {
		t.Fatalf("session after exit while STOP = %#v", store.session)
	}
	if !containsEvent(recorded, supervisor.EventKindExitWhileStopped) || !containsEvent(notified, supervisor.EventKindExitWhileStopped) {
		t.Fatalf("exit events recorded=%v notified=%v", recorded, notified)
	}
}

func containsEvent(events []supervisor.SupervisorEventKind, want supervisor.SupervisorEventKind) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}

type waitBeforeEOFPTY struct {
	closed chan struct{}
	once   sync.Once
}

func (p *waitBeforeEOFPTY) Read([]byte) (int, error) { <-p.closed; return 0, io.EOF }
func (*waitBeforeEOFPTY) Wait() (int, error)         { return 1, nil }
func (p *waitBeforeEOFPTY) Close() error             { p.once.Do(func() { close(p.closed) }); return nil }

func TestSCNsup01044WaitDetectsExitWithoutPTYEOF(t *testing.T) {
	id, _ := supervisor.NewSessionID("wait-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "wait", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: time.Now(), StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	record := &regressionSessionStore{session: s}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	observer := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: record}
	if err := observer.Run(ctx, s, &waitBeforeEOFPTY{closed: make(chan struct{})}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	got, _ := record.Load(id)
	if got.State != supervisor.SessionStopped || got.ChildPID != 0 {
		t.Fatalf("Wait exit result = state %s pid %d", got.State, got.ChildPID)
	}
}

func TestSCNsup01044FakeCLIGrandchildKeepsPTYOpen(t *testing.T) {
	cwd := t.TempDir()
	script := filepath.Join(t.TempDir(), "fake-claude")
	if err := os.WriteFile(script, []byte("#!/bin/sh\n/bin/sleep 5 &\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	pty, err := proc.StartPTYSession("/bin/sh", []string{"/bin/sh", script}, cwd, proc.BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pty.Kill(syscall.SIGKILL); _ = pty.Close() }()
	id, _ := supervisor.NewSessionID("grandchild-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "grandchild", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: time.Now(), ChildPID: pty.Pid(), StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	record := &regressionSessionStore{session: s}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: record}).Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24))
	}()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("observer waited for PTY EOF after CLI child exited")
	}
	got, _ := record.Load(id)
	if got.State != supervisor.SessionExited {
		t.Fatalf("child exit state=%s", got.State)
	}
}

func TestSCNsup01001PTYIdleScreenTransitionsStartingToIdleAndRecordsEvent(t *testing.T) {
	cwd := t.TempDir()
	script := filepath.Join(t.TempDir(), "fake-claude")
	screen := "──────────────────────────────────────────────────────────────────────────────\n❯\u00a0\n──────────────────────────────────────────────────────────────────────────────\n"
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' '"+screen+"'\n/bin/sleep 5\n"), 0700); err != nil {
		t.Fatal(err)
	}
	pty, err := proc.StartPTYSession("/bin/sh", []string{"/bin/sh", script}, cwd, proc.BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := supervisor.NewSessionID("pty-idle-1")
	started := time.Now().UTC()
	session := supervisor.ManagedSession{SessionID: id, ProjectID: "pty-idle", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStarting, StateSince: started, ChildPID: pty.Pid(), ChildStartedAt: &started, StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	sessions := &regressionSessionStore{session: session}
	var stateChanged int
	detector := detect.NewCompositeDetector(detect.DefaultPatternSet())
	detector.Quiet.Period = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	observer := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: sessions, Interval: time.Millisecond, RecordStructured: func(event testEvent) error {
		kind := event.Kind
		if kind == supervisor.EventKindStateChanged {
			stateChanged++
		}

		return nil
	}}
	go func() { done <- observer.Run(ctx, session, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		current, loadErr := sessions.Load(id)
		if loadErr != nil {
			cancel()
			t.Fatal(loadErr)
		}
		if current.State == supervisor.SessionIdle {
			break
		}
		time.Sleep(time.Millisecond)
	}
	current, _ := sessions.Load(id)
	cancel()
	<-done
	if current.State != supervisor.SessionIdle || stateChanged == 0 {
		t.Fatalf("PTY observation state=%s state_changed=%d", current.State, stateChanged)
	}
}

func TestSCNsup01033StoppedObservationDoesNotTransition(t *testing.T) {
	id, _ := supervisor.NewSessionID("demo-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StateSince: time.Now(), StopReason: supervisor.StopGlobal, StopScope: supervisor.StopScopeGlobal, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	store := &regressionSessionStore{session: s}
	stop := NewStopState(t.TempDir())
	stop.SetGlobal(true)
	detector := &constantDetector{result: detect.DetectResult{State: detect.StateIdle}}
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: store, Stop: stop}
	if err := u.Run(context.Background(), s, &endPTY{}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	if detector.calls == 0 {
		t.Fatal("STOP must not suppress observation")
	}
	if s.State != supervisor.SessionStopped {
		t.Fatalf("state changed during STOP: %s", s.State)
	}
}

type regressionStopRepository struct{ record *regressionSessionStore }

func (r regressionStopRepository) List(context.Context) ([]supervisor.ManagedSession, error) {
	s, err := r.record.Load("")
	return []supervisor.ManagedSession{s}, err
}
func (r regressionStopRepository) Save(_ context.Context, s supervisor.ManagedSession) error {
	return r.record.Save(s)
}

type signalingDetector struct{ called chan struct{} }

func (d signalingDetector) Detect(context.Context, detect.DetectInput) (detect.DetectResult, error) {
	select {
	case d.called <- struct{}{}:
	default:
	}
	return detect.DetectResult{State: detect.StateWorking}, nil
}

func TestSCNsup01033StopWrittenDuringObservationRemainsStopped(t *testing.T) {
	id, _ := supervisor.NewSessionID("race-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "race", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: time.Now(), StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	record := &regressionSessionStore{session: s}
	stopState := NewStopState(t.TempDir())
	called := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	observer := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: signalingDetector{called}}, Sessions: record, Stop: stopState, Interval: time.Millisecond}
	go func() {
		done <- observer.Run(ctx, s, &waitingPTY{closed: make(chan struct{})}, supervisor.NewVirtualTerminal(80, 24))
	}()
	select {
	case <-called:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("observer did not start")
	}
	stopper := NewStopControlUseCase(stopState, regressionStopRepository{record}, nil, nil)
	if _, err := stopper.Stop(context.Background(), &id); err != nil {
		cancel()
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	state, _ := record.Load(id)
	if state.State != supervisor.SessionStopped {
		cancel()
		t.Fatalf("state after observer cycle = %s", state.State)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("observer return = %v", err)
	}
	state, _ = record.Load(id)
	if state.State != supervisor.SessionStopped {
		t.Fatalf("observer restored stale state %s", state.State)
	}
}

func TestSCNsup01034UnsafeModePersistsLatchOnce(t *testing.T) {
	id, _ := supervisor.NewSessionID("demo-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionIdle, StateSince: time.Now(), StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	store := &regressionSessionStore{session: s}
	calls := 0
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle, PermissionMode: detect.PermissionUnsafe}}}, Sessions: store, RecordStructured: func(event testEvent) error {
		kind := event.Kind
		if kind == supervisor.EventKindPermissionModeUnsafe {
			calls++
		}

		return nil
	}}
	_ = u.Run(context.Background(), s, &endPTY{}, supervisor.NewVirtualTerminal(80, 24))
	if store.session.AutoContinueDisabledReason != supervisor.AutoContinueDisabledPermissionModeUnsafe {
		t.Fatalf("unsafe latch not persisted: %q", store.session.AutoContinueDisabledReason)
	}
	if calls != 1 {
		t.Fatalf("unsafe event count = %d; want 1", calls)
	}
}

func TestSCNsup01044StartupTimeoutRecordsEvent(t *testing.T) {
	id, _ := supervisor.NewSessionID("demo-1")
	started := time.Now().Add(-time.Hour)
	for _, generation := range []int{1, 2, 3} {
		s := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: generation, State: supervisor.SessionStarting, StateSince: started, StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
		sessions := &regressionSessionStore{session: s}
		pty := &waitingPTY{closed: make(chan struct{})}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		done := make(chan error, 1)
		calls := 0
		go func() {
			done <- (ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateUndetermined}}}, Sessions: sessions, StartupTimeout: time.Nanosecond, Interval: time.Millisecond, RecordStructured: func(event testEvent) error {
				kind := event.Kind
				reason := string(event.Reason)
				if kind == supervisor.EventKindSessionFailed || kind == supervisor.EventKindSessionStopped && reason == string(supervisor.StopRecoveryStartTimeout) {
					calls++
				}

				return nil
			}}).Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24))
		}()
		if generation >= 2 {
			deadline := time.After(time.Second / 2)
			for {
				current, _ := sessions.Load(id)
				if current.State == supervisor.SessionStopped {
					break
				}
				select {
				case <-deadline:
					t.Fatal("timeout did not stop session")
				default:
					time.Sleep(time.Millisecond)
				}
			}
			_ = pty.Close()
		}
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		cancel()
		current, _ := sessions.Load(id)
		if generation == 1 && current.State != supervisor.SessionFailed || generation >= 2 && (current.State != supervisor.SessionStopped || current.StopReason != supervisor.StopRecoveryStartTimeout) || calls != 1 {
			t.Fatalf("generation %d timeout = %#v, events=%d", generation, current, calls)
		}
	}
}

func TestSCNsup01023CorruptRecordsAreListed(t *testing.T) {
	root := t.TempDir()
	store := store.NewSessionStore(root)
	id, _ := supervisor.NewSessionID("broken-1")
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sessions", id.String()+".json"), []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	var corruptNotifications int
	result, err := (ReconcileOnStartupUseCase{Store: ReconcileSessionStoreAdapter{Store: store}, RecordStructured: func(event testEvent) error {
		k := event.Kind
		if k == supervisor.EventKindStateFileCorrupt {
			corruptNotifications++
		}

		return nil
	}}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0].State != supervisor.SessionStopped || result[0].StopReason != supervisor.StopStateFileCorrupt || result[0].ProjectID != "" {
		t.Fatalf("corrupt reconcile result = %#v", result)
	}
	if corruptNotifications != 1 {
		t.Fatalf("new corrupt record notifications=%d, want 1", corruptNotifications)
	}
	_, err = (ReconcileOnStartupUseCase{Store: ReconcileSessionStoreAdapter{Store: store}, RecordStructured: func(event testEvent) error {
		k := event.Kind
		if k == supervisor.EventKindStateFileCorrupt {
			corruptNotifications++
		}

		return nil
	}}).Execute()
	if err != nil || corruptNotifications != 1 {
		t.Fatalf("previous corrupt record notifications=%d, err=%v", corruptNotifications, err)
	}
}

func TestSCNsup01023ReconcileDoesNotSignalOrphan(t *testing.T) {
	id, _ := supervisor.NewSessionID("orphan-1")
	child := exec.Command("/bin/sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _, _ = child.Process.Wait() })
	started := time.Now().UTC()
	session := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionIdle, StateSince: started, ChildPID: child.Process.Pid, ChildStartedAt: &started, StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	store := &reconcileMemoryStore{sessions: []supervisor.ManagedSession{session}}
	orphanNotified := false
	result, err := (ReconcileOnStartupUseCase{Store: store, IsAlive: func(pid int) bool { return syscall.Kill(pid, 0) == nil }, RecordStructured: func(event testEvent) error {
		k := event.Kind
		if k == supervisor.EventKindOrphanChildAlive {
			orphanNotified = true
		}

		return nil
	}, Notify: func(k supervisor.SupervisorEventKind, _ string) {
		if k == supervisor.EventKindOrphanChildAlive {
			orphanNotified = true
		}
	}, ProcessStartedAt: func(int) (time.Time, bool) { return started.Add(time.Second), true }}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("reconcile signaled the previous child: %v", err)
	}
	// FD-sup-05.md:183-186 separates the reconciliation stop reason from the orphan notification.
	if len(result) != 1 || result[0].State != supervisor.SessionStopped || result[0].StopReason != supervisor.StopSupervisorRestarted || !orphanNotified {
		t.Fatalf("orphan reconcile result=%#v notified=%t", result, orphanNotified)
	}
}

func TestSCNsup01023ReconcileRequiresMatchingProcessStartTime(t *testing.T) {
	id, _ := supervisor.NewSessionID("pid-reuse-1")
	started := time.Now().UTC()
	session := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StateSince: started, StopScope: supervisor.StopScopeGlobal, StopReason: supervisor.StopGlobal, ChildPID: 99, ChildStartedAt: &started, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	store := &reconcileMemoryStore{sessions: []supervisor.ManagedSession{session}}
	var orphanEvents int
	rows, err := (ReconcileOnStartupUseCase{Store: store, IsAlive: func(int) bool { return true }, ProcessStartedAt: func(int) (time.Time, bool) { return started.Add(time.Second + time.Nanosecond), true }, RecordStructured: func(event testEvent) error {
		k := event.Kind
		if k == supervisor.EventKindOrphanChildAlive {
			orphanEvents++
		}

		return nil
	}}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if orphanEvents != 0 || rows[0].ChildPID != 0 || rows[0].ChildStartedAt != nil {
		t.Fatalf("PID reused process was classified alive: events=%d session=%#v", orphanEvents, rows[0])
	}
}

func TestSCNsup01023ReconcileStartingWithoutPIDIsExited(t *testing.T) {
	id, _ := supervisor.NewSessionID("interrupted-1")
	session := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStarting, StateSince: time.Now(), StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	store := &reconcileMemoryStore{sessions: []supervisor.ManagedSession{session}}
	var recorded supervisor.SupervisorEventKind
	result, err := (ReconcileOnStartupUseCase{Store: store, RecordStructured: func(event testEvent) error {
		k := event.Kind
		recorded = k
		return nil
	}}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if result[0].State != supervisor.SessionExited || recorded != supervisor.EventKindSessionStartInterrupted {
		t.Fatalf("interrupted reconcile=%#v event=%s", result[0], recorded)
	}
}

func TestSCNsup01020DoesNotEnumerateManualCLIProcesses(t *testing.T) {
	fakeCLI := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(fakeCLI, []byte("#!/bin/sh\nexec /bin/sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	manual := exec.Command(fakeCLI)
	if err := manual.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manual.Process.Kill(); _, _ = manual.Process.Wait() })
	store := &reconcileMemoryStore{}
	checks := 0
	result, err := (ReconcileOnStartupUseCase{Store: store, IsAlive: func(int) bool { checks++; return true }}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 || checks != 0 {
		t.Fatalf("empty managed store caused process inspection: result=%v checks=%d", result, checks)
	}
	if err := manual.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("unmanaged fake CLI was terminated: %v", err)
	}
}

type reconcileMemoryStore struct{ sessions []supervisor.ManagedSession }

func (s *reconcileMemoryStore) List() ([]supervisor.ManagedSession, []supervisor.SessionID, []supervisor.SessionID, error) {
	return append([]supervisor.ManagedSession(nil), s.sessions...), nil, nil, nil
}
func (s *reconcileMemoryStore) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	for _, session := range s.sessions {
		if session.SessionID == id {
			return session, nil
		}
	}
	return supervisor.ManagedSession{}, os.ErrNotExist
}
func (s *reconcileMemoryStore) Save(v supervisor.ManagedSession) error {
	for i := range s.sessions {
		if s.sessions[i].SessionID == v.SessionID {
			s.sessions[i] = v
			return nil
		}
	}
	s.sessions = append(s.sessions, v)
	return nil
}

type startMemoryStore struct {
	session    supervisor.ManagedSession
	saved      bool
	saves      int
	failSaveAt int
}

func (s *startMemoryStore) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	if s.saved && s.session.SessionID == id {
		return s.session, nil
	}
	return supervisor.ManagedSession{}, os.ErrNotExist
}
func (s *startMemoryStore) Save(v supervisor.ManagedSession) error {
	s.saves++
	if s.failSaveAt > 0 && s.saves == s.failSaveAt {
		return errors.New("save failed")
	}
	s.session = v
	s.saved = true
	return nil
}

type cleanupManagedPTY struct {
	closed bool
	killed bool
	done   chan struct{}
}

func (*cleanupManagedPTY) Pid() int                    { return 43211 }
func (p *cleanupManagedPTY) Close() error              { p.closed = true; return nil }
func (p *cleanupManagedPTY) Kill(syscall.Signal) error { p.killed = true; close(p.done); return nil }
func (p *cleanupManagedPTY) Wait() (int, error)        { <-p.done; return -1, errors.New("killed") }

type cleanupStarter struct{ pty *cleanupManagedPTY }

func (s cleanupStarter) Start(string, []string, []string, string, uint16, uint16) (ManagedPTY, error) {
	return s.pty, nil
}

type failingStarter struct{}

func (failingStarter) Start(string, []string, []string, string, uint16, uint16) (ManagedPTY, error) {
	return nil, errors.New("spawn failed")
}

func TestSCNsup01037SpawnFailureDeletesRecordAndPersistsProjectEvent(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(`{"auth_mode":"chatgpt","OPENAI_API_KEY":null}`), 0600); err != nil {
		t.Fatal(err)
	}
	cwd := t.TempDir()
	binary := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	id, _ := supervisor.NewSessionID("spawn-fail-1")
	sessions := &startMemoryStore{}
	stateDir := t.TempDir()
	events := store.NewEventStore(t.TempDir())
	recorder := EventStoreRecorder{Events: events}
	u := StartSessionUseCase{Config: StartSessionConfig{StateDir: stateDir, CodexdPath: "/bin/true", MaxSessions: 1, Columns: 80, Rows: 24}, Projects: map[string]StartProject{"spawn-fail": {ID: "spawn-fail", CLI: supervisor.CLIClaude, CWD: cwd, BinaryPath: binary}}, Store: sessions, Starter: failingStarter{}, Billing: launch.BillingGuard{ManagedSettingsPath: filepath.Join(home, "managed.json")}, ParentEnv: func() []string { return nil }, RecordStartFailure: func(projectID string, reason store.SupervisorStartFailReason) {
		if err := recorder.RecordStartFailed(projectID, reason, time.Now()); err != nil {
			t.Errorf("persist start failure: %v", err)
		}
	}}
	if _, err := u.Execute("spawn-fail", id); err == nil {
		t.Fatal("spawn failure was accepted")
	}
	if sessions.saved {
		t.Fatal("failed start record was retained")
	}
	if _, err := os.Stat(filepath.Join(stateDir, "statusline", id.String()+".settings.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("settings file remains: %v", err)
	}
	all, _, err := events.ReadAll()
	if err != nil || len(all) != 1 || all[0].Kind != supervisor.EventKindSessionStartFailed || all[0].SessionID != "" || all[0].ProjectID != "spawn-fail" || all[0].Reason != "spawn_failed" {
		t.Fatalf("start failure events=%#v err=%v", all, err)
	}
}

func TestSCNsup01029SpawnPIDSaveFailureCleansUpChild(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(`{"auth_mode":"chatgpt","OPENAI_API_KEY":null}`), 0600); err != nil {
		t.Fatal(err)
	}
	cwd := t.TempDir()
	binary := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	id, _ := supervisor.NewSessionID("cleanup-1")
	sessions := &startMemoryStore{failSaveAt: 2}
	child := &cleanupManagedPTY{done: make(chan struct{})}
	u := StartSessionUseCase{Config: StartSessionConfig{StateDir: t.TempDir(), CodexdPath: "/bin/true", MaxSessions: 1, Columns: 80, Rows: 24}, Projects: map[string]StartProject{"cleanup": {ID: "cleanup", CLI: supervisor.CLIClaude, CWD: cwd, BinaryPath: binary}}, Store: sessions, Starter: cleanupStarter{pty: child}, Billing: launch.BillingGuard{ManagedSettingsPath: filepath.Join(home, "managed.json")}, ParentEnv: func() []string { return nil }, CleanupGrace: time.Nanosecond}
	if _, err := u.Execute("cleanup", id); err == nil {
		t.Fatal("PID persistence failure accepted")
	}
	if !child.closed || !child.killed || sessions.saved {
		t.Fatalf("child cleanup closed=%t killed=%t session_saved=%t", child.closed, child.killed, sessions.saved)
	}
}
func (s *startMemoryStore) Delete(supervisor.SessionID) error { s.saved = false; return nil }
func (s *startMemoryStore) List() ([]supervisor.ManagedSession, error) {
	if !s.saved {
		return nil, nil
	}
	return []supervisor.ManagedSession{s.session}, nil
}

type fakeManagedPTY struct{}

func (fakeManagedPTY) Pid() int                  { return 43210 }
func (fakeManagedPTY) Close() error              { return nil }
func (fakeManagedPTY) Kill(syscall.Signal) error { return nil }
func (fakeManagedPTY) Wait() (int, error)        { return 0, nil }

type startSpy struct {
	store       *startMemoryStore
	sawStarting bool
	argv        []string
	env         []string
}

func (s *startSpy) Start(_ string, argv, env []string, _ string, _, _ uint16) (ManagedPTY, error) {
	s.sawStarting = s.store.saved && s.store.session.State == supervisor.SessionStarting && s.store.session.ChildPID == 0
	s.argv = append([]string(nil), argv...)
	s.env = append([]string(nil), env...)
	return fakeManagedPTY{}, nil
}

func TestSCNsup01001SCNsup01029SCNsup01037StartPersistsBeforeSpawn(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(`{"auth_mode":"chatgpt","OPENAI_API_KEY":null}`), 0600); err != nil {
		t.Fatal(err)
	}
	cwd := t.TempDir()
	binary := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	stateDir := t.TempDir()
	id, _ := supervisor.NewSessionID("demo-1")
	sessions := &startMemoryStore{}
	spy := &startSpy{store: sessions}
	u := StartSessionUseCase{Config: StartSessionConfig{StateDir: stateDir, CodexdPath: "/bin/true", MaxSessions: 2, Columns: 80, Rows: 24}, Projects: map[string]StartProject{"demo": {ID: "demo", CLI: supervisor.CLIClaude, CWD: cwd, BinaryPath: binary}}, Store: sessions, Starter: spy, Billing: launch.BillingGuard{ManagedSettingsPath: filepath.Join(home, "managed.json")}, ParentEnv: func() []string { return nil }}
	pty, err := u.Execute("demo", id)
	if err != nil {
		t.Fatal(err)
	}
	if !spy.sawStarting {
		t.Fatal("session was not stored as starting with empty pid before spawn")
	}
	if pty.Pid() != 43210 || sessions.session.ChildPID != 43210 || sessions.session.ChildStartedAt == nil {
		t.Fatal("spawn pid and start time were not persisted")
	}
	if len(spy.argv) < 4 || spy.argv[1] != "--settings" || !containsArgument(spy.argv, "--permission-mode") {
		t.Fatalf("launch argv does not follow launch contract: %v", spy.argv)
	}
	settings, err := os.ReadFile(filepath.Join(stateDir, "statusline", id.String()+".settings.json"))
	if err != nil || !containsJSONToken(settings, "statusLine") || !containsJSONToken(settings, "deny") {
		t.Fatalf("generated settings missing statusLine/deny: %v", err)
	}
}

func containsArgument(args []string, target string) bool {
	for _, arg := range args {
		if arg == target {
			return true
		}
	}
	return false
}
func containsJSONToken(data []byte, target string) bool {
	return strings.Contains(string(data), `"`+target+`"`)
}

func TestSCNsup01006MissingBinaryLeavesNoSession(t *testing.T) {
	cwd := t.TempDir()
	id, _ := supervisor.NewSessionID("demo-1")
	sessions := &startMemoryStore{}
	u := StartSessionUseCase{Config: StartSessionConfig{StateDir: t.TempDir(), MaxSessions: 2, Columns: 80, Rows: 24}, Projects: map[string]StartProject{"demo": {ID: "demo", CLI: supervisor.CLIClaude, CWD: cwd, BinaryPath: filepath.Join(t.TempDir(), "missing")}}, Store: sessions}
	if _, err := u.Execute("demo", id); err == nil {
		t.Fatal("missing executable accepted")
	}
	if sessions.saved {
		t.Fatal("missing executable created a session")
	}
}

func TestSCNsup01028BillingConfigurationRejectsStart(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	cwd := t.TempDir()
	if err := os.MkdirAll(filepath.Join(cwd, ".claude"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(cwd, ".claude", "settings.json"), []byte(`{"apiKeyHelper":"custom-helper"}`), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	id, _ := supervisor.NewSessionID("demo-1")
	sessions := &startMemoryStore{}
	events := store.NewEventStore(t.TempDir())
	recorder := EventStoreRecorder{Events: events}
	u := StartSessionUseCase{Config: StartSessionConfig{StateDir: t.TempDir(), CodexdPath: "/bin/true", MaxSessions: 2, Columns: 80, Rows: 24}, Projects: map[string]StartProject{"demo": {ID: "demo", CLI: supervisor.CLIClaude, CWD: cwd, BinaryPath: binary}}, Store: sessions, Starter: &startSpy{store: sessions}, Billing: launch.BillingGuard{ManagedSettingsPath: filepath.Join(home, "managed.json")}, ParentEnv: func() []string { return nil }, RecordStartFailure: func(project string, reason store.SupervisorStartFailReason) {
		if err := recorder.RecordStartFailed(project, reason, time.Now()); err != nil {
			t.Error(err)
		}
	}}
	if _, err := u.Execute("demo", id); err == nil || !strings.Contains(err.Error(), "SUP_BILLING_GUARD_FAILED") {
		t.Fatalf("billing guard error=%v", err)
	}
	if sessions.saved {
		t.Fatal("billing guard failure created a session")
	}
	rows, _, err := events.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Reason != "project_settings" || rows[0].ProjectID != "demo" {
		t.Fatalf("billing failure event=%#v", rows)
	}
}

func TestSCNsup01003SCNsup01004SCNsup01005StartPreflightRejects(t *testing.T) {
	cwd := t.TempDir()
	id, _ := supervisor.NewSessionID("demo-2")
	otherID, _ := supervisor.NewSessionID("other-1")
	active := supervisor.ManagedSession{SessionID: otherID, ProjectID: "other", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: time.Now(), StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	for _, tc := range []struct {
		name, projectID, want string
		max                   int
		existing              []supervisor.ManagedSession
	}{
		{"SCN-sup-01-04", "missing", "SUP_PROJECT_NOT_FOUND", 4, nil},
		{"SCN-sup-01-03", "demo", "SUP_SESSION_LIMIT_REACHED", 1, []supervisor.ManagedSession{active}},
		{"SCN-sup-01-05", "demo", "SUP_SESSION_ALREADY_RUNNING", 4, []supervisor.ManagedSession{{SessionID: otherID, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: time.Now(), StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := &startMemoryStore{saved: len(tc.existing) > 0}
			if mem.saved {
				mem.session = tc.existing[0]
			}
			projects := map[string]StartProject{"demo": {ID: "demo", CLI: supervisor.CLIClaude, CWD: cwd, BinaryPath: "/bin/sh"}}
			u := StartSessionUseCase{Config: StartSessionConfig{MaxSessions: tc.max}, Projects: projects, Store: mem}
			_, err := u.Execute(tc.projectID, id)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v; want %s", err, tc.want)
			}
			if tc.name == "SCN-sup-01-03" && !errors.Is(err, ErrSessionLimitReached) {
				t.Fatalf("limit error does not match sentinel: %v", err)
			}
		})
	}
}

func TestSCNsup01017StartUsesInstanceLock(t *testing.T) {
	id, _ := supervisor.NewSessionID("demo-1")
	store := &startMemoryStore{}
	called := false
	u := StartSessionUseCase{Store: store, Lock: func() (func(), error) { called = true; return nil, os.ErrExist }}
	if _, err := u.Execute("demo", id); !errors.Is(err, os.ErrExist) {
		t.Fatalf("lock failure=%v", err)
	}
	if !called || store.saved {
		t.Fatal("instance lock was skipped or session was created")
	}
}

func TestSCNsup01012UndeterminedNotifiesOncePerInterval(t *testing.T) {
	id, _ := supervisor.NewSessionID("demo-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: time.Now(), StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	notices := 0
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateUndetermined}}}, Sessions: &regressionSessionStore{session: s}, Stop: NewStopState(t.TempDir()), Interval: time.Millisecond, UndeterminedNotifyAfter: 3 * time.Millisecond, Notify: func(k supervisor.SupervisorEventKind, _ string) {
		if k == supervisor.EventKindUndeterminedLong {
			notices++
		}
	}}
	_ = u.Run(ctx, s, &waitingPTY{closed: make(chan struct{})}, supervisor.NewVirtualTerminal(80, 24))
	if notices != 1 {
		t.Fatalf("undetermined notifications=%d; want one for the undetermined interval", notices)
	}
}

func TestSCNsup01012UndeterminedNotifiesAgainAfterNewInterval(t *testing.T) {
	id, _ := supervisor.NewSessionID("demo-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: time.Now(), StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()
	calls, notices := 0, 0
	detector := detectorFunc(func() detect.DetectResult {
		calls++
		if calls >= 10 && calls <= 14 {
			return detect.DetectResult{State: detect.StateWorking}
		}
		return detect.DetectResult{State: detect.StateUndetermined}
	})
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: &regressionSessionStore{session: s}, Interval: time.Millisecond, UndeterminedNotifyAfter: 4 * time.Millisecond, Notify: func(k supervisor.SupervisorEventKind, _ string) {
		if k == supervisor.EventKindUndeterminedLong {
			notices++
		}
	}}
	_ = u.Run(ctx, s, &waitingPTY{closed: make(chan struct{})}, supervisor.NewVirtualTerminal(80, 24))
	if notices != 2 {
		t.Fatalf("undetermined notifications=%d; want one notification in each of two intervals", notices)
	}
}

type detectorFunc func() detect.DetectResult

func (f detectorFunc) Detect(context.Context, detect.DetectInput) (detect.DetectResult, error) {
	return f(), nil
}

func TestSCNsup01033IllegalTransitionIsLoggedOncePerStatePair(t *testing.T) {
	id, _ := supervisor.NewSessionID("demo-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStarting, StateSince: time.Now(), StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	count := 0
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateAwaitingRecommendation}}}, Sessions: &regressionSessionStore{session: s}, Interval: time.Millisecond, RecordStructured: func(event testEvent) error {
		k := event.Kind
		if k == supervisor.EventKindIllegalTransition {
			count++
		}

		return nil
	}}
	_ = u.Run(ctx, s, &waitingPTY{closed: make(chan struct{})}, supervisor.NewVirtualTerminal(80, 24))
	if count != 1 {
		t.Fatalf("illegal transition event count=%d; want 1", count)
	}
}

func TestRealSessionStoreAdapterDoesNotDeadlock(t *testing.T) {
	raw := store.NewSessionStore(t.TempDir())
	adapter := SessionStoreAdapter{Store: raw}
	id, _ := supervisor.NewSessionID("adapter-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "adapter", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStarting, StateSince: time.Now().UTC(), StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	steps := []struct {
		name string
		run  func() error
	}{
		{"create", func() error { return CreateSession(adapter, s) }},
		{"update", func() error {
			return UpdateSession(adapter, id, func(v *supervisor.ManagedSession) error { v.ChildPID = 42; return nil })
		}},
		{"reconcile", func() error {
			_, err := (ReconcileOnStartupUseCase{Store: ReconcileSessionStoreAdapter{Store: raw}, IsAlive: func(int) bool { return false }}).Execute()
			return err
		}},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			done := make(chan error, 1)
			go func() { done <- step.run() }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatal("real SessionStore adapter deadlocked")
			}
		})
	}
}

func TestUpdateSessionSkipsUnchangedAndFailedWrites(t *testing.T) {
	id, _ := supervisor.NewSessionID("update-1")
	s := supervisor.ManagedSession{SessionID: id}
	record := &regressionSessionStore{session: s}
	if err := UpdateSession(record, id, func(*supervisor.ManagedSession) error { return nil }); err != nil {
		t.Fatal(err)
	}
	want := errors.New("update failed")
	if err := UpdateSession(record, id, func(v *supervisor.ManagedSession) error { v.ChildPID = 9; return want }); !errors.Is(err, want) {
		t.Fatalf("update error = %v", err)
	}
	if record.saves != 0 || record.session.ChildPID != 0 {
		t.Fatalf("unexpected save: %#v", record)
	}
}

type detachedPTY struct {
	done                  chan struct{}
	closeCalls, killCalls int
}

func (p *detachedPTY) Read([]byte) (int, error)  { <-p.done; return 0, io.EOF }
func (p *detachedPTY) Wait() (int, error)        { <-p.done; return 0, nil }
func (p *detachedPTY) Close() error              { p.closeCalls++; return nil }
func (p *detachedPTY) Kill(syscall.Signal) error { p.killCalls++; return nil }

func TestObservationCancellationLeavesChildAttached(t *testing.T) {
	id, _ := supervisor.NewSessionID("detach-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "detach", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: time.Now().UTC(), StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	record := &regressionSessionStore{session: s}
	pty := &detachedPTY{done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := (ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateWorking}}}, Sessions: record}).Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24))
	close(pty.done)
	if !errors.Is(err, context.Canceled) || pty.closeCalls != 0 || pty.killCalls != 0 {
		t.Fatalf("cancel result=%v close=%d kill=%d", err, pty.closeCalls, pty.killCalls)
	}
}

func TestObservedStateDeletedAfterChildExit(t *testing.T) {
	id, _ := supervisor.NewSessionID("observed-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "observed", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StateSince: time.Now().UTC(), StopReason: supervisor.StopGlobal, StopScope: supervisor.StopScopeGlobal, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	record := &regressionSessionStore{session: s}
	observed := &ObservedStateStore{}
	_ = (ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle}}}, Sessions: record, Observed: observed}).Run(context.Background(), s, exitedPTY{}, supervisor.NewVirtualTerminal(80, 24))
	state, ok := observed.Get(id)
	if ok {
		t.Fatalf("observed state=%s found=%t", state, ok)
	}
}

func TestSCNsup01001RealEventStoreRecordsTransition(t *testing.T) {
	id, _ := supervisor.NewSessionID("events-1")
	stateDir := t.TempDir()
	raw := store.NewSessionStore(stateDir)
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "events", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStarting, StateSince: time.Now().UTC(), StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	if err := raw.Save(s); err != nil {
		t.Fatal(err)
	}
	events := store.NewEventStore(stateDir)
	pty := &waitingPTY{closed: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- (ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle}}}, Sessions: SessionStoreAdapter{Store: raw}, Interval: time.Millisecond, RecordStructured: (EventStoreRecorder{Events: events}).RecordStructured}).Run(ctx, s, pty, supervisor.NewVirtualTerminal(80, 24))
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		rows, _, err := events.ReadAll()
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		for _, event := range rows {
			if event.Kind == supervisor.EventKindStateChanged {
				cancel()
				_ = pty.Close()
				<-done
				if event.FromState != supervisor.SessionStarting || event.ToState != supervisor.SessionIdle || event.Reason != "" {
					t.Fatalf("state event=%#v", event)
				}
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	_ = pty.Close()
	<-done
	t.Fatal("state_changed not recorded")
}

func TestSCNsup01007RealEventStoreRecordsSpawnCode(t *testing.T) {
	id, _ := supervisor.NewSessionID("timeout-1")
	stateDir := t.TempDir()
	raw := store.NewSessionStore(stateDir)
	started := time.Now().Add(-time.Hour).UTC()
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "timeout", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStarting, StateSince: started, StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	if err := raw.Save(s); err != nil {
		t.Fatal(err)
	}
	events := store.NewEventStore(stateDir)
	pty := &waitingPTY{closed: make(chan struct{})}
	if err := (ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateUndetermined}}}, Sessions: SessionStoreAdapter{Store: raw}, StartupTimeout: time.Nanosecond, Interval: time.Millisecond, RecordStructured: (EventStoreRecorder{Events: events}).RecordStructured}).Run(context.Background(), s, pty, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	_ = pty.Close()
	rows, _, err := events.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range rows {
		if event.Kind == supervisor.EventKindSessionFailed && event.Code == "SUP_SPAWN_FAILED" && event.Reason == "" {
			return
		}
	}
	t.Fatalf("session_failed with spawn code missing: %#v", rows)
}

func TestNewCorruptRecordNotifiedOnceDuringStartListing(t *testing.T) {
	root := t.TempDir()
	id, _ := supervisor.NewSessionID("corrupt-1")
	if err := os.MkdirAll(filepath.Join(root, "sessions"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "sessions", id.String()+".json"), []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	count := 0
	events := store.NewEventStore(root)
	u := StartSessionUseCase{Store: SessionStoreAdapter{Store: store.NewSessionStore(root)}, RecordStructured: func(event testEvent) error {
		if event.Kind == supervisor.EventKindStateFileCorrupt {
			count++
		}
		return (EventStoreRecorder{Events: events}).RecordStructured(event)
	}}
	for i := 0; i < 2; i++ {
		if _, err := u.existingSessions(); err != nil {
			t.Fatal(err)
		}
	}
	if count != 1 {
		t.Fatalf("corruption events=%d; want 1", count)
	}
	rows, _, err := events.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Kind != supervisor.EventKindStateFileCorrupt || rows[0].SessionID != id.String() {
		t.Fatalf("corruption rows=%#v", rows)
	}
}

func TestGuardStartReleasesStopStateMutexAndSerializesStop(t *testing.T) {
	state := NewStopState(t.TempDir())
	entered := make(chan struct{})
	release := make(chan struct{})
	startDone := make(chan error, 1)
	go func() {
		startDone <- state.GuardStart(func() error {
			close(entered)
			if state.IsGlobalStopped() {
				return errors.New("unexpected STOP")
			}
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("start blocked on state mutex")
	}
	stopDone := make(chan struct{})
	go func() { state.SetGlobal(true); close(stopDone) }()
	select {
	case <-stopDone:
		t.Fatal("STOP overtook in-flight start")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-startDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopDone:
	case <-time.After(time.Second):
		t.Fatal("STOP remained blocked")
	}
	if err := state.GuardStart(func() error { return nil }); !errors.Is(err, ErrStopActive) {
		t.Fatalf("start after STOP = %v", err)
	}
}

func TestReconcileUsesDarwinProcessStartTime(t *testing.T) {
	child := exec.Command("/bin/sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = child.Process.Kill(); _, _ = child.Process.Wait() })
	started, ok := proc.ProcessStartTime(child.Process.Pid)
	if !ok {
		t.Fatal("process start time unavailable")
	}
	id, _ := supervisor.NewSessionID("actual-orphan-1")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "actual", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: started, ChildPID: child.Process.Pid, ChildStartedAt: &started, StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	mem := &reconcileMemoryStore{sessions: []supervisor.ManagedSession{s}}
	rows, err := (ReconcileOnStartupUseCase{Store: mem}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// FD-sup-05.md:183-186 assigns supervisor_restarted while reporting a live orphan separately.
	if rows[0].StopReason != supervisor.StopSupervisorRestarted {
		t.Fatalf("orphan stop reason=%s", rows[0].StopReason)
	}
}

func TestSCNsup01006RealEventStoreStartFailure(t *testing.T) {
	root := t.TempDir()
	events := store.NewEventStore(root)
	recorder := EventStoreRecorder{Events: events}
	id, _ := supervisor.NewSessionID("demo-1")
	notified := ""
	u := StartSessionUseCase{Config: StartSessionConfig{MaxSessions: 1}, Projects: map[string]StartProject{"demo": {ID: "demo", CLI: supervisor.CLIClaude, CWD: t.TempDir(), BinaryPath: filepath.Join(root, "missing")}}, Store: &startMemoryStore{}, RecordStartFailure: func(project string, reason store.SupervisorStartFailReason) {
		if err := recorder.RecordStartFailed(project, reason, time.Now()); err != nil {
			t.Error(err)
		}
	}, Notify: func(_ supervisor.SupervisorEventKind, session string) { notified = session }}
	if _, err := u.Execute("demo", id); !errors.Is(err, ErrSpawnFailed) {
		t.Fatalf("start error=%v", err)
	}
	rows, _, err := events.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Kind != supervisor.EventKindSessionStartFailed || rows[0].SessionID != "" || rows[0].ProjectID != "demo" || rows[0].Reason != store.ReasonSpawnFailed || notified != "" {
		t.Fatalf("start event=%#v notified=%q", rows, notified)
	}
}

func TestExitWithoutClaudeIDClearsChildAndRefusesResume(t *testing.T) {
	id, _ := supervisor.NewSessionID("missing-id-1")
	started := time.Now().UTC()
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "missing-id", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionIdle, StateSince: started, ChildPID: 123, ChildStartedAt: &started, StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	record := &regressionSessionStore{session: s}
	if err := (ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: record}).Run(context.Background(), s, exitedPTY{code: 1}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
		t.Fatal(err)
	}
	got, _ := record.Load(id)
	if got.State != supervisor.SessionStopped || got.StopReason != supervisor.StopClaudeSessionIDMissing || got.ChildPID != 0 || got.ChildStartedAt != nil {
		t.Fatalf("exit state=%#v", got)
	}
	stops := NewStopState(t.TempDir())
	uc := NewStopControlUseCase(stops, regressionStopRepository{record}, nil, nil)
	out, err := uc.Resume(context.Background(), &id, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.StillStopped) != 1 || out.StillStopped[0].Reason != "claude_session_id_missing" {
		t.Fatalf("resume result=%#v", out)
	}
}

func TestReconcileRealStoreOrphanEventAndResumeGuard(t *testing.T) {
	root := t.TempDir()
	raw := store.NewSessionStore(root)
	id, _ := supervisor.NewSessionID("orphan-real-1")
	started, ok := proc.ProcessStartTime(os.Getpid())
	if !ok {
		t.Fatal("process start time unavailable")
	}
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "orphan-real", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: started, ChildPID: os.Getpid(), ChildStartedAt: &started, StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	if err := raw.Save(s); err != nil {
		t.Fatal(err)
	}
	events := store.NewEventStore(root)
	rows, err := (ReconcileOnStartupUseCase{Store: ReconcileSessionStoreAdapter{Store: raw}, RecordStructured: (EventStoreRecorder{Events: events}).RecordStructured}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	// FD-sup-05.md:183-186 keeps the reconciliation reason independent of orphan_child_alive.
	if len(rows) != 1 || rows[0].StopReason != supervisor.StopSupervisorRestarted {
		t.Fatalf("reconciled=%#v", rows)
	}
	eventRows, _, err := events.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range eventRows {
		if event.Kind == supervisor.EventKindSessionStopped && event.Reason == store.EventReason(supervisor.StopSupervisorRestarted) {
			found = true
		}
	}
	if !found {
		t.Fatalf("orphan stop event missing: %#v", eventRows)
	}
	// FD-sup-01.md:175-179 checks the live unowned PID, rather than inferring orphanhood from stop_reason.
	ctx, err := NewProcessResumeTransitionContext(nil, nil)(context.Background(), rows[0])
	if err != nil {
		t.Fatal(err)
	}
	ctx.ResumeScope = supervisor.StopScopeNone
	if err := rows[0].Apply(supervisor.EventResume, ctx, time.Now()); err == nil || !strings.Contains(err.Error(), "SUP_ORPHAN_CHILD_ALIVE") {
		t.Fatalf("orphan resume error=%v", err)
	}
}
