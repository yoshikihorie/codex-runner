package usecase

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

func TestObserveSuppliesCompletionSelectionToEnterGate(t *testing.T) {
	s, sessions, events := observeSendFixture(t, supervisor.SessionIdle)
	screens := &ObservedScreenStore{}
	send := &SendInputUseCase{Screens: screens}
	p := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateWorking}}}, Sessions: sessions, SendInput: send, RecordStructured: observeSendRecord(events), CompletionSelection: func(rows []string) (bool, string) {
		for _, row := range rows {
			if strings.Contains(row, "❯ /rc") {
				return true, "/rc"
			}
		}
		return false, ""
	}, Interval: time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, p, supervisor.NewVirtualTerminal(80, 24)) }()
	p.reads <- []byte("❯\u00a0/rc\r\n  ❯ /rc")
	deadline := time.After(time.Second)
	for {
		shot := screens.Snapshot(s.SessionID)
		if shot.CompletionMenu {
			if shot.SelectedCompletion != "/rc" || !screenReadyForEnter(shot, "/rc") {
				t.Fatalf("snapshot=%+v", shot)
			}
			shot.CompletionMenu = false
			shot.SelectedCompletion = ""
			if screenReadyForEnter(shot, "/rc") {
				t.Fatal("unknown completion must block enter")
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("completion selection not observed")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	_ = p.Close()
	if err := <-done; err != nil && err != context.Canceled {
		t.Fatal(err)
	}
	if !screens.Snapshot(s.SessionID).ObservedAt.IsZero() {
		t.Fatal("observer retained a stale screen after exit")
	}
}

func TestObservedScreenStoreOldOwnerCannotDeleteNewSnapshot(t *testing.T) {
	id := supervisor.SessionID("alpha-1")
	screens := &ObservedScreenStore{}
	oldOwner, newOwner := uint64(1), uint64(2)
	screens.SetSnapshotOwned(id, ScreenSnapshot{Rows: []string{"old"}, ObservedAt: time.Now()}, oldOwner)
	screens.SetSnapshotOwned(id, ScreenSnapshot{Rows: []string{"new"}, ObservedAt: time.Now()}, newOwner)
	screens.DeleteOwned(id, oldOwner)
	if shot := screens.Snapshot(id); len(shot.Rows) != 1 || shot.Rows[0] != "new" {
		t.Fatalf("old owner deleted new snapshot: %+v", shot)
	}
	screens.SetSnapshotOwned(id, ScreenSnapshot{Rows: []string{"late old"}, ObservedAt: time.Now()}, oldOwner)
	if shot := screens.Snapshot(id); shot.Rows[0] != "new" {
		t.Fatalf("old owner replaced new snapshot: %+v", shot)
	}
	screens.DeleteOwned(id, newOwner)
	if shot := screens.Snapshot(id); !shot.ObservedAt.IsZero() {
		t.Fatalf("new owner failed to clear snapshot: %+v", shot)
	}
}

func TestSendInputEndSessionKeepsNewOwnerSnapshot(t *testing.T) {
	id := supervisor.SessionID("alpha-1")
	screens := &ObservedScreenStore{}
	screens.SetSnapshotOwned(id, ScreenSnapshot{Rows: []string{"new"}, ObservedAt: time.Now()}, 2)
	u := &SendInputUseCase{Screens: screens}
	u.EndSession(id, 1)
	if shot := screens.Snapshot(id); len(shot.Rows) != 1 || shot.Rows[0] != "new" {
		t.Fatalf("old session deleted new screen: %+v", shot)
	}
}

func TestObserveOldLoopExitKeepsNewLoopScreen(t *testing.T) {
	session, sessions, events := observeSendFixture(t, supervisor.SessionIdle)
	screens := &ObservedScreenStore{}
	start := func(content string) (context.CancelFunc, *observeSendPTY, <-chan error) {
		u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle}}}, Sessions: sessions, SendInput: &SendInputUseCase{Screens: screens}, RecordStructured: observeSendRecord(events), Interval: time.Millisecond}
		ctx, cancel := context.WithCancel(context.Background())
		pty := newObserveSendPTY()
		done := make(chan error, 1)
		go func() { done <- u.Run(ctx, session, pty, supervisor.NewVirtualTerminal(80, 24)) }()
		pty.reads <- []byte(content)
		deadline := time.After(time.Second)
		for {
			shot := screens.Snapshot(session.SessionID)
			if len(shot.Rows) != 0 && strings.Contains(shot.Rows[0], content) {
				return cancel, pty, done
			}
			select {
			case <-deadline:
				cancel()
				t.Fatal("screen not observed")
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}
	cancelOld, oldPTY, oldDone := start("old screen")
	cancelNew, newPTY, newDone := start("new screen")
	defer func() { cancelNew(); _ = newPTY.Close(); <-newDone }()
	cancelOld()
	_ = oldPTY.Close()
	if err := <-oldDone; err != nil && err != context.Canceled {
		t.Fatal(err)
	}
	if shot := screens.Snapshot(session.SessionID); len(shot.Rows) == 0 || !strings.Contains(shot.Rows[0], "new screen") {
		t.Fatalf("old observer removed new screen: %+v", shot)
	}
}

type observeSendPTY struct {
	reads  chan []byte
	exits  chan int
	once   sync.Once
	writes atomic.Int32
}

func newObserveSendPTY() *observeSendPTY {
	return &observeSendPTY{reads: make(chan []byte, 2), exits: make(chan int, 1)}
}
func (p *observeSendPTY) Read(dst []byte) (int, error) {
	b, ok := <-p.reads
	if !ok {
		return 0, io.EOF
	}
	return copy(dst, b), nil
}
func (p *observeSendPTY) Wait() (int, error)             { return <-p.exits, nil }
func (p *observeSendPTY) Close() error                   { p.once.Do(func() { close(p.reads) }); return nil }
func (p *observeSendPTY) Write(data []byte) (int, error) { p.writes.Add(1); return len(data), nil }
func (p *observeSendPTY) WriteLine(context.Context, string, time.Duration, func() bool) error {
	p.writes.Add(1)
	return nil
}

func observeSendFixture(t *testing.T, state supervisor.SupervisorSessionState) (supervisor.ManagedSession, *store.SessionStore, *store.EventStore) {
	t.Helper()
	root := t.TempDir()
	id, err := supervisor.NewSessionID("observe-send-1")
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	s := supervisor.ManagedSession{SchemaVersion: 1, SessionID: id, ProjectID: "observe-send", CLI: supervisor.CLIClaude, Generation: 1, State: state, StateSince: now, StopScope: supervisor.StopScopeNone, ChildPID: 123, ChildStartedAt: &now, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 2}}
	sessions := store.NewSessionStore(root)
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	return s, sessions, store.NewEventStore(root)
}
func observeSendRecord(events *store.EventStore) func(store.Event) error {
	return func(e store.Event) error { _, err := events.Record(e); return err }
}

type channelOperationalLog struct{ entries chan store.SupervisorLogEntry }

func (l channelOperationalLog) Record(entry store.SupervisorLogEntry) error {
	l.entries <- entry
	return nil
}

func TestObserveInitialRemoteControlEventFailureWritesWarn(t *testing.T) {
	s, sessions, events := observeSendFixture(t, supervisor.SessionStarting)
	s.Generation = 2
	s.RemoteControlConfirmed = true
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	logs := make(chan store.SupervisorLogEntry, 1)
	requested := make(chan struct{}, 1)
	remote := &InitialRemoteControl{
		Request:          func(context.Context, SendInput) error { requested <- struct{}{}; return nil },
		RecordStructured: func(store.Event) error { return errors.New("event write failed") },
		OperationalLog:   channelOperationalLog{entries: logs},
	}
	p := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle}}}, Sessions: sessions, InitialRemoteControl: remote, RecordStructured: observeSendRecord(events), Interval: time.Millisecond}
	go func() { done <- u.Run(ctx, s, p, supervisor.NewVirtualTerminal(80, 24)) }()
	select {
	case entry := <-logs:
		if entry.Level != store.LogLevelWarn || entry.Kind != supervisor.EventKindRemoteControlSkipped || entry.SessionID != s.SessionID.String() || entry.Reason != store.LogReasonEventWriteFailed {
			t.Fatalf("unexpected warning: %+v", entry)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not log event failure")
	}
	select {
	case <-requested:
		t.Fatal("remote control requested after failed skip event")
	default:
	}
	cancel()
	_ = p.Close()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not stop")
	}
}

func TestSCNsup0135SCNsup0239ObserveRequestsInitialRemoteControl(t *testing.T) {
	s, sessions, events := observeSendFixture(t, supervisor.SessionStarting)
	requested := make(chan SendInput, 2)
	requestContexts := make(chan context.Context, 1)
	supervisorCtx, supervisorCancel := context.WithCancel(context.Background())
	defer supervisorCancel()
	release := make(chan struct{})
	defer close(release)
	screens := &ObservedScreenStore{}
	remote := &InitialRemoteControl{Request: func(requestCtx context.Context, in SendInput) error {
		if screens.Snapshot(in.SessionID).ObservedAt.IsZero() {
			t.Error("request preceded observed screen")
		}
		requestContexts <- requestCtx
		requested <- in
		<-release
		return nil
	}}
	p := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateIdle}}}, Sessions: sessions, SendInput: &SendInputUseCase{Screens: screens}, InitialRemoteControl: remote, SupervisorContext: supervisorCtx, RecordStructured: observeSendRecord(events), Interval: time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, p, supervisor.NewVirtualTerminal(80, 24)) }()
	select {
	case in := <-requested:
		if in.SessionID != s.SessionID || in.Kind != supervisor.SendKindRemoteControl {
			t.Fatalf("request=%+v", in)
		}
	case <-time.After(time.Second):
		t.Fatal("initial /rc not requested")
	}
	select {
	case <-requested:
		t.Fatal("duplicate /rc request")
	case <-time.After(20 * time.Millisecond):
	}
	cancel()
	_ = p.Close()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer waited for /rc request")
	}
	requestCtx := <-requestContexts
	if requestCtx.Err() != nil {
		t.Fatal("observer cancellation canceled /rc request")
	}
	supervisorCancel()
	if requestCtx.Err() != context.Canceled {
		t.Fatal("supervisor cancellation did not cancel /rc request")
	}
}

func TestSCNsup0224SCNsup0231ObserveNotifiesScreenWhileStopped(t *testing.T) {
	s, sessions, events := observeSendFixture(t, supervisor.SessionIdle)
	pending := supervisor.PendingSend{SendID: s.SessionID.String() + "-s1", Kind: supervisor.SendKindRemoteControl, IssuedAt: s.StateSince}
	s.SendGate.Pending = &pending
	s.SendGate.EverSent = true
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	send := &SendInputUseCase{}
	confirmations := send.confirmationChannel(s.SessionID)
	stops := NewStopState(t.TempDir())
	stops.SetSession(s.SessionID, true)
	p := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateWorking, Basis: []detect.DetectionBasis{detect.BasisOutputQuiet}}}}, Sessions: sessions, Stop: stops, SendInput: send, RecordStructured: observeSendRecord(events), Interval: time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, p, supervisor.NewVirtualTerminal(80, 24)) }()
	p.reads <- []byte("❯\u00a0\x1b[2mExample\x1b[0m")
	select {
	case c := <-confirmations:
		if c.SendID != pending.SendID || !c.Screen.ChangedAt.After(pending.IssuedAt) || c.Screen.Processing || len(c.Screen.Rows) == 0 || len(c.Screen.Cells) == 0 || len(c.Screen.Cells[0]) == 0 || !c.Screen.Cells[0][2].Faint {
			t.Fatalf("confirmation=%+v", c)
		}
	case <-time.After(time.Second):
		t.Fatal("screen update not forwarded")
	}
	cancel()
	_ = p.Close()
	if err := <-done; err != nil && err != context.Canceled {
		t.Fatal(err)
	}
}

func TestSCNsup0222SCNsup0235ObserveExitWithPending(t *testing.T) {
	for _, tc := range []struct {
		name      string
		kind      supervisor.SupervisorSendKind
		want      supervisor.SupervisorSessionState
		stop      supervisor.SupervisorStopReason
		confirmed bool
		crWritten bool
	}{
		{"handoff_exit_before_cr", supervisor.SendKindHandoffExit, supervisor.SessionStopped, supervisor.StopProcessExitWithPending, false, false},
		{"handoff_exit_after_cr", supervisor.SendKindHandoffExit, supervisor.SessionExited, "", true, true},
		{"other", supervisor.SendKindHandoffEnd, supervisor.SessionStopped, supervisor.StopProcessExitWithPending, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, sessions, events := observeSendFixture(t, supervisor.SessionIdle)
			pending := supervisor.PendingSend{SendID: s.SessionID.String() + "-s1", Kind: tc.kind, IssuedAt: s.StateSince}
			s.SendGate.Pending = &pending
			s.SendGate.EverSent = true
			s.ClaudeSessionID = "00000000-0000-0000-0000-000000000001"
			if err := sessions.Save(s); err != nil {
				t.Fatal(err)
			}
			send := &SendInputUseCase{}
			pty := &exitedPTY{code: 0}
			if tc.crWritten {
				send.markCR(s.SessionID, pending.SendID)
				if tc.kind == supervisor.SendKindHandoffExit {
					send.exitSends = map[supervisor.SessionID]*handoffExitSend{s.SessionID: {pending: pending, in: SendInput{SessionID: s.SessionID, Kind: pending.Kind}, owner: pty, marked: true, write: exitCRSucceeded}}
				}
			}
			confirmations := send.confirmationChannel(s.SessionID)
			u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: sessions, SendInput: send, RecordStructured: observeSendRecord(events)}
			if err := u.Run(context.Background(), s, pty, supervisor.NewVirtualTerminal(80, 24)); err != nil {
				t.Fatal(err)
			}
			select {
			case confirmation := <-confirmations:
				if !confirmation.ProcessExited || confirmation.SendID != pending.SendID {
					t.Fatalf("confirmation=%+v", confirmation)
				}
			default:
				t.Fatal("process exit was not forwarded")
			}
			got, err := sessions.Load(s.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			if got.State != tc.want || got.StopReason != tc.stop || got.SendGate.LastSendConfirmed != tc.confirmed || (got.SendGate.Pending == nil) != tc.confirmed {
				t.Fatalf("session=%+v", got)
			}
			all, _, err := events.ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			confirmedCount := 0
			for _, event := range all {
				if event.Kind == supervisor.EventKindSendConfirmed {
					confirmedCount++
				}
				if event.Kind == supervisor.EventKindSendUnconfirmed {
					t.Fatalf("unexpected unconfirmed event: %+v", event)
				}
			}
			if (confirmedCount == 1) != tc.confirmed {
				t.Fatalf("confirmed events=%d", confirmedCount)
			}
		})
	}
}

func TestObserveC1ObservationAdvancesOnlyOnOutput(t *testing.T) {
	s, sessions, events := observeSendFixture(t, supervisor.SessionIdle)
	screens := &ObservedScreenStore{}
	send := &SendInputUseCase{Screens: screens}
	p := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateWorking}}}, Sessions: sessions, SendInput: send, RecordStructured: observeSendRecord(events), Interval: time.Millisecond}
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, p, supervisor.NewVirtualTerminal(80, 24)) }()
	defer func() {
		cancel()
		_ = p.Close()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	}()
	wait := func(want uint64) ScreenSnapshot {
		t.Helper()
		deadline := time.After(time.Second)
		for {
			shot := screens.Snapshot(s.SessionID)
			if shot.Observation == want {
				return shot
			}
			select {
			case <-deadline:
				t.Fatalf("observation=%d want=%d", shot.Observation, want)
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}
	p.reads <- []byte("❯\u00a0")
	first := wait(1)
	time.Sleep(5 * time.Millisecond)
	if got := screens.Snapshot(s.SessionID).Observation; got != 1 {
		t.Fatalf("timer advanced observation: %d", got)
	}
	// Output that leaves all cells unchanged is still a distinct observation.
	p.reads <- []byte("\r")
	second := wait(2)
	if !second.ChangedAt.Equal(first.ChangedAt) {
		t.Fatalf("unchanged cells advanced ChangedAt: %v -> %v", first.ChangedAt, second.ChangedAt)
	}
	p.reads <- []byte("different")
	third := wait(3)
	if !third.ChangedAt.After(second.ChangedAt) {
		t.Fatal("changed output did not advance ChangedAt")
	}
}

// scn45PTY drives the real sender and observer with the same PTY identity.
// Hooks run outside all store locks; channels, not sleeps, order concurrent work.
type scn45PTY struct {
	*observeSendPTY
	screens     *ObservedScreenStore
	id          supervisor.SessionID
	afterEnter  func()
	beforeEnter func()
	writeErr    error
}

func (p *scn45PTY) WriteLine(_ context.Context, text string, _ time.Duration, before func() bool) error {
	p.screens.SetSnapshot(p.id, ScreenSnapshot{Rows: []string{"❯\u00a0" + text}, State: detect.StateIdle})
	if p.beforeEnter != nil {
		p.beforeEnter()
	}
	if !before() {
		return proc.ErrBeforeEnter
	}
	if p.afterEnter != nil {
		p.afterEnter()
	}
	return p.writeErr
}

type scn45Sender struct {
	send    *SendInputUseCase
	timeout time.Duration
	after   func(SendResult, error)
}

func (s scn45Sender) Execute(ctx context.Context, in SendInput) (SendResult, error) {
	in.ConfirmTimeout = s.timeout
	r, err := s.send.Execute(ctx, in)
	if s.after != nil {
		s.after(r, err)
	}
	return r, err
}

type scn45Fixture struct {
	t        *testing.T
	initial  supervisor.ManagedSession
	sessions *store.SessionStore
	events   *store.EventStore
	send     *SendInputUseCase
	pty      *scn45PTY
	rotate   *RotateSessionUseCase
	observer ObserveSessionUseCase
}

func newSCN45(t *testing.T) *scn45Fixture {
	t.Helper()
	initial, sessions, events := observeSendFixture(t, supervisor.SessionRotating)
	initial.ClaudeSessionID = "00000000-0000-0000-0000-000000000001"
	initial.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendExit, StepStartedAt: &initial.StateSince}
	if err := sessions.Save(initial); err != nil {
		t.Fatal(err)
	}
	screens := &ObservedScreenStore{}
	screens.SetSnapshot(initial.SessionID, blankScreen())
	pty := &scn45PTY{observeSendPTY: newObserveSendPTY(), screens: screens, id: initial.SessionID}
	send := &SendInputUseCase{Sessions: sessions, Screens: screens, PTY: pty, StopState: NewStopState(t.TempDir()), IsStopped: func(supervisor.SessionID) bool { return false }, RecordStructured: observeSendRecord(events)}
	f := &scn45Fixture{t: t, initial: initial, sessions: sessions, events: events, send: send, pty: pty}
	f.observer = ObserveSessionUseCase{Sessions: sessions, SendInput: send, Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, RecordStructured: observeSendRecord(events)}
	f.rotate = &RotateSessionUseCase{Sessions: sessions, Send: scn45Sender{send: send, timeout: 5 * time.Millisecond}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }, IsStopped: func(supervisor.SessionID) bool { return false }, ChildAlive: func(id supervisor.SessionID) bool { s, err := sessions.Load(id); return err == nil && s.ChildPID != 0 }, RecordStructured: observeSendRecord(events)}
	return f
}
func (f *scn45Fixture) current() supervisor.ManagedSession {
	f.t.Helper()
	s, err := f.sessions.Load(f.initial.SessionID)
	if err != nil {
		f.t.Fatal(err)
	}
	return s
}
func (f *scn45Fixture) exit() error {
	f.pty.exits <- 0
	return f.observer.Run(context.Background(), f.initial, f.pty, supervisor.NewVirtualTerminal(80, 24))
}
func (f *scn45Fixture) counts(confirmed, unconfirmed int) {
	f.t.Helper()
	all, _, err := f.events.ReadAll()
	if err != nil {
		f.t.Fatal(err)
	}
	c, u := 0, 0
	for _, e := range all {
		if e.Kind == supervisor.EventKindSendConfirmed {
			c++
		}
		if e.Kind == supervisor.EventKindSendUnconfirmed {
			u++
		}
	}
	if c != confirmed || u != unconfirmed {
		f.t.Fatalf("confirmed=%d unconfirmed=%d want=%d/%d events=%+v", c, u, confirmed, unconfirmed, all)
	}
}
func (f *scn45Fixture) stage5() {
	f.t.Helper()
	s := f.current()
	if s.Rotation.Step != supervisor.RotateRestart || s.SendGate.Pending != nil {
		f.t.Fatalf("did not reach stage5: %+v", s)
	}
}
func TestSCNsup0245ReturnWindow(t *testing.T) {
	f := newSCN45(t)
	f.pty.afterEnter = func() {
		if err := f.exit(); err != nil {
			t.Fatal(err)
		}
		s := f.current()
		if s.LastSend != nil && s.LastSend.Confirmed || s.SendGate.Pending == nil {
			t.Fatalf("confirmed before CR success: %+v", s)
		}
	}
	if _, err := f.rotate.sendExit(context.Background(), f.initial); err != nil {
		t.Fatal(err)
	}
	f.stage5()
	f.counts(1, 0)
}
func TestSCNsup0245ClockIndependent(t *testing.T) {
	for _, offset := range []time.Duration{0, -time.Hour} {
		t.Run(offset.String(), func(t *testing.T) {
			f := newSCN45(t)
			at := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
			f.send.Now = func() time.Time { return at }
			f.observer.Now = func() time.Time { return at.Add(offset) }
			f.pty.afterEnter = func() {
				if err := f.exit(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := f.rotate.sendExit(context.Background(), f.initial); err != nil {
				t.Fatal(err)
			}
			f.stage5()
			f.counts(1, 0)
		})
	}
}
func TestSCNsup0245CleanupBeforeCollect(t *testing.T) {
	f := newSCN45(t)
	f.pty.afterEnter = func() {
		if err := f.exit(); err != nil {
			t.Fatal(err)
		}
		f.send.EndSession(f.initial.SessionID)
		f.send.mu.Lock()
		r := f.send.exitSends[f.initial.SessionID]
		retained := r != nil && r.marked && r.write == exitWritingCR
		f.send.mu.Unlock()
		if !retained {
			t.Fatal("cleanup lost in-flight CR")
		}
	}
	if _, err := f.rotate.sendExit(context.Background(), f.initial); err != nil {
		t.Fatal(err)
	}
	f.stage5()
	f.counts(1, 0)
	if len(f.send.exitSends) != 0 {
		t.Fatal("record not collected")
	}
}
func TestSCNsup0245CRWriteFailure(t *testing.T) {
	f := newSCN45(t)
	f.pty.writeErr = errors.New("CR write failed")
	retained := false
	f.send.RecordStructured = func(e store.Event) error {
		if e.Kind == supervisor.EventKindSendUnconfirmed {
			f.send.mu.Lock()
			r := f.send.exitSends[f.initial.SessionID]
			retained = r != nil && r.marked && r.write == exitCRFailed
			f.send.mu.Unlock()
		}
		return observeSendRecord(f.events)(e)
	}
	f.pty.afterEnter = func() {
		if err := f.exit(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.rotate.sendExit(context.Background(), f.initial); !errors.Is(err, f.pty.writeErr) {
		t.Fatalf("write failure lost: %v", err)
	}
	s := f.current()
	if s.Rotation.Step != supervisor.RotateSendExit || s.Rotation.FailReason != supervisor.RotateFailChildAbsent || s.State != supervisor.SessionStopped || s.LastSend != nil {
		t.Fatalf("CR failure treated as success: %+v", s)
	}
	f.counts(0, 1)
	if !retained || len(f.send.exitSends) != 0 {
		t.Fatal("failed CR not retained until collection")
	}
}
func TestSCNsup0245UnconfirmedThenChildDeath(t *testing.T) {
	f := newSCN45(t)
	var sendID string
	f.rotate.Send = scn45Sender{send: f.send, timeout: time.Millisecond, after: func(result SendResult, err error) {
		if err == nil || result.Confirmed {
			t.Fatalf("result=%+v err=%v", result, err)
		}
		sendID = result.SendID
		if len(f.send.exitSends) != 0 {
			t.Fatal("result not collected before death")
		}
		f.counts(0, 1)
		if err := f.exit(); err != nil {
			t.Fatal(err)
		}
	}}
	_, err := f.rotate.sendExit(context.Background(), f.initial)
	if err == nil {
		t.Fatal("send timeout was not reported")
	}
	f.stage5()
	f.counts(0, 1)
	if s := f.current(); s.LastSend == nil || s.LastSend.SendID != sendID || s.LastSend.Confirmed {
		t.Fatalf("unconfirmed result changed: %+v", s.LastSend)
	}
}

func TestSCNsup0245ConfirmedBeforeDeadline(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelled), func(t *testing.T) {
			f := newSCN45(t)
			// Signal CR-success persistence without delivering an exit notification.
			crSaved := make(chan struct{})
			f.send.Sessions = &scn45SaveSignal{SessionRecordStore: f.sessions, saved: crSaved}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct {
				r SendResult
				e error
			}, 1)
			go func() {
				r, e := f.send.Execute(ctx, SendInput{SessionID: f.initial.SessionID, Kind: supervisor.SendKindHandoffExit, Origin: supervisor.SendOriginHandoff, ConfirmTimeout: 200 * time.Millisecond})
				done <- struct {
					r SendResult
					e error
				}{r, e}
			}()
			select {
			case <-crSaved:
			case <-time.After(time.Second):
				t.Fatal("CR success not persisted")
			}
			saved := make(chan struct{})
			release := make(chan struct{})
			f.observer.RecordStructured = func(e store.Event) error {
				if e.Kind == supervisor.EventKindSendConfirmed {
					close(saved)
					<-release
				}
				return observeSendRecord(f.events)(e)
			}
			observed := make(chan error, 1)
			go func() { observed <- f.exit() }()
			scn45Receive(t, saved)
			f.send.EndSession(f.initial.SessionID)
			f.send.mu.Lock()
			retained := f.send.exitSends[f.initial.SessionID] != nil
			f.send.mu.Unlock()
			if !retained {
				t.Fatal("cleanup removed saved result before collection")
			}
			if cancelled {
				cancel()
			}
			got := scn45Receive(t, done)
			close(release)
			if err := scn45Receive(t, observed); err != nil {
				t.Fatal(err)
			}
			if got.e != nil || !got.r.Confirmed {
				t.Fatalf("saved success lost: %+v err=%v", got.r, got.e)
			}
			f.counts(1, 0)
		})
	}
}

// This wrapper intentionally uses UpdateSession's shared-lock fallback.
type scn45SaveSignal struct {
	SessionRecordStore
	saved chan struct{}
	once  sync.Once
}

func (s *scn45SaveSignal) Save(v supervisor.ManagedSession) error {
	err := s.SessionRecordStore.Save(v)
	if err == nil && v.LastSend != nil && v.LastSend.Kind == supervisor.SendKindHandoffExit {
		s.once.Do(func() { close(s.saved) })
	}
	return err
}

type scn45CopyBarrier struct {
	SessionRecordStore
	armed  bool
	copied func()
}

func (s *scn45CopyBarrier) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	v, err := s.SessionRecordStore.Load(id)
	if err == nil && s.armed {
		s.armed = false
		s.copied()
	}
	return v, err
}
func TestSCNsup0245ExitBetweenFinalCopyAndMark(t *testing.T) {
	f := newSCN45(t)
	copied := false
	entered := false
	barrier := &scn45CopyBarrier{SessionRecordStore: f.sessions}
	barrier.copied = func() {
		copied = true
		if err := f.exit(); err != nil {
			t.Fatal(err)
		}
		f.send.mu.Lock()
		r := f.send.exitSends[f.initial.SessionID]
		beforeMark := r != nil && r.exited && !r.marked
		f.send.mu.Unlock()
		if !beforeMark {
			t.Fatal("exit not registered before mark")
		}
	}
	f.send.Sessions = barrier
	f.pty.beforeEnter = func() { barrier.armed = true }
	f.pty.afterEnter = func() { entered = true }
	result, err := f.send.Execute(context.Background(), SendInput{SessionID: f.initial.SessionID, Kind: supervisor.SendKindHandoffExit, Origin: supervisor.SendOriginHandoff})
	if !copied || entered || !errors.Is(err, proc.ErrBeforeEnter) || result.Confirmed || f.current().LastSend != nil {
		t.Fatalf("copied=%v entered=%v result=%+v err=%v", copied, entered, result, err)
	}
	f.counts(0, 0)
}

type scn45BlockedSave struct {
	SessionRecordStore
	entered chan struct{}
	release chan struct{}
	fail    error
	once    sync.Once
}

func (s *scn45BlockedSave) Save(v supervisor.ManagedSession) error {
	if v.LastSend != nil && v.LastSend.Confirmed {
		s.once.Do(func() { close(s.entered) })
		<-s.release
		if s.fail != nil {
			return s.fail
		}
	}
	return s.SessionRecordStore.Save(v)
}

type scn45UpdateProbe struct {
	SessionRecordStore
	ctx       context.Context
	attempted chan struct{}
	watch     atomic.Bool
	once      sync.Once
}

func (s *scn45UpdateProbe) UpdateSession(id supervisor.SessionID, update func(*supervisor.ManagedSession) error) error {
	if s.watch.Load() || s.ctx.Err() != nil {
		s.once.Do(func() { close(s.attempted) })
	}
	return UpdateSessionWith(sessionLocker, id, func() (supervisor.ManagedSession, error) { return s.Load(id) }, s.Save, update)
}
func TestSCNsup0245DeadlineDuringSave(t *testing.T) {
	for _, fail := range []bool{false, true} {
		for _, cancelled := range []bool{false, true} {
			t.Run(fmt.Sprintf("fail=%v/cancel=%v", fail, cancelled), func(t *testing.T) {
				f := newSCN45(t)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				save := &scn45BlockedSave{SessionRecordStore: f.sessions, entered: make(chan struct{}), release: make(chan struct{})}
				writeErr := errors.New("confirmation save failed")
				if fail {
					save.fail = writeErr
				}
				crSaved := make(chan struct{})
				signal := &scn45SaveSignal{SessionRecordStore: save, saved: crSaved}
				probe := &scn45UpdateProbe{SessionRecordStore: signal, ctx: ctx, attempted: make(chan struct{})}
				f.send.Sessions = probe
				f.observer.Sessions = signal
				done := make(chan struct {
					r SendResult
					e error
				}, 1)
				go func() {
					r, e := f.send.Execute(ctx, SendInput{SessionID: f.initial.SessionID, Kind: supervisor.SendKindHandoffExit, Origin: supervisor.SendOriginHandoff, ConfirmTimeout: 200 * time.Millisecond})
					done <- struct {
						r SendResult
						e error
					}{r, e}
				}()
				scn45Receive(t, crSaved)
				observed := make(chan error, 1)
				go func() { observed <- f.exit() }()
				scn45Receive(t, save.entered)
				if s := f.current(); s.LastSend == nil || s.LastSend.Confirmed {
					t.Fatal("reservation published before save")
				}
				probe.watch.Store(true)
				if cancelled {
					cancel()
				}
				scn45Receive(t, probe.attempted)
				select {
				case result := <-done:
					t.Fatalf("returned before save completion: %+v", result)
				default:
				}
				close(save.release)
				observeErr := scn45Receive(t, observed)
				result := scn45Receive(t, done)
				if fail {
					if !errors.Is(observeErr, writeErr) || result.r.Confirmed || result.e == nil || cancelled && !errors.Is(result.e, context.Canceled) {
						t.Fatalf("observer=%v sender=%+v/%v", observeErr, result.r, result.e)
					}
					f.counts(0, 1)
				} else {
					if observeErr != nil || result.e != nil || !result.r.Confirmed {
						t.Fatalf("observer=%v sender=%+v/%v", observeErr, result.r, result.e)
					}
					f.counts(1, 0)
				}
			})
		}
	}
}
func TestSCNsup0245EventFailureKeepsSavedSuccess(t *testing.T) {
	f := newSCN45(t)
	eventErr := errors.New("confirmed event failed")
	attempts := 0
	f.send.RecordStructured = func(e store.Event) error {
		if e.Kind == supervisor.EventKindSendConfirmed {
			attempts++
			return eventErr
		}
		return observeSendRecord(f.events)(e)
	}
	f.pty.afterEnter = func() {
		if err := f.exit(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.rotate.sendExit(context.Background(), f.initial); !errors.Is(err, eventErr) {
		t.Fatalf("event error lost: %v", err)
	}
	f.stage5()
	f.counts(0, 0)
	p := supervisor.PendingSend{SendID: f.current().LastSend.SendID, Kind: supervisor.SendKindHandoffExit}
	result, err := f.send.confirmSend(SendInput{SessionID: f.initial.SessionID, Kind: p.Kind, Origin: supervisor.SendOriginHandoff}, p)
	if err != nil || !result.Confirmed || attempts != 1 {
		t.Fatalf("result=%+v err=%v attempts=%d", result, err, attempts)
	}
	if err := f.send.unconfirmed(SendInput{SessionID: f.initial.SessionID, Kind: p.Kind}, p); err != nil {
		t.Fatal(err)
	}
	f.counts(0, 0)
}
func TestSCNsup0245DuplicateAndLateObservation(t *testing.T) {
	f := newSCN45(t)
	f.pty.afterEnter = func() {
		for i := 0; i < 2; i++ {
			if err := f.exit(); err != nil {
				t.Fatal(err)
			}
		}
	}
	result, err := f.send.Execute(context.Background(), SendInput{SessionID: f.initial.SessionID, Kind: supervisor.SendKindHandoffExit, Origin: supervisor.SendOriginHandoff})
	if err != nil || !result.Confirmed {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	for i := 0; i < 2; i++ {
		f.send.NotifyConfirmation(SendConfirmation{SessionID: f.initial.SessionID, SendID: result.SendID, ProcessExited: true})
		p := supervisor.PendingSend{SendID: result.SendID, Kind: supervisor.SendKindHandoffExit}
		_, err := f.send.updateExitSession(f.sessions, f.initial.SessionID, func(v *supervisor.ManagedSession, tx *exitCommit) error {
			f.send.observeExit(v, p, f.pty, tx)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(f.send.exitSends) != 0 || len(f.send.confirm) != 0 {
		t.Fatal("late observation recreated transient records")
	}
	f.counts(1, 0)
}

func (p *scn45PTY) Pid() int                  { return 123 }
func (p *scn45PTY) Kill(syscall.Signal) error { return nil }
func TestSCNsup0245EarlyExitPathsDoNotRegister(t *testing.T) {
	for _, path := range []string{"shutdown", "stopping", "old_pty", "old_send_id"} {
		t.Run(path, func(t *testing.T) {
			f := newSCN45(t)
			registry := &PTYRegistry{}
			registry.Register(f.initial.SessionID, f.pty)
			registry.RegisterManaged(f.initial.SessionID, f.pty)
			f.send.PTYs = registry
			f.pty.afterEnter = func() {
				switch path {
				case "shutdown":
					registry.BeginShutdown()
				case "stopping":
					registry.MarkStopping(f.initial.SessionID)
				case "old_pty":
					registry.RegisterManaged(f.initial.SessionID, &fakeManagedPTY{})
				case "old_send_id":
					if err := UpdateSession(f.sessions, f.initial.SessionID, func(v *supervisor.ManagedSession) error {
						p := *v.SendGate.Pending
						p.SendID = v.SessionID.String() + "-s1"
						v.SendGate.Pending = &p
						return nil
					}); err != nil {
						t.Fatal(err)
					}
				}
				f.pty.exits <- 0
				if err := f.observer.Run(context.Background(), f.initial, observedPTY{ManagedPTY: f.pty, Reader: f.pty}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
					t.Fatal(err)
				}
				f.send.mu.Lock()
				r := f.send.exitSends[f.initial.SessionID]
				registered := r != nil && r.exited
				f.send.mu.Unlock()
				if registered {
					t.Fatal("excluded exit was registered")
				}
			}
			result, err := f.send.Execute(context.Background(), SendInput{SessionID: f.initial.SessionID, Kind: supervisor.SendKindHandoffExit, Origin: supervisor.SendOriginHandoff, ConfirmTimeout: time.Millisecond})
			if err == nil || result.Confirmed {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			f.counts(0, 1)
		})
	}
}
func TestSCNsup0245OtherSessionProgressesDuringCR(t *testing.T) {
	f := newSCN45(t)
	other := f.initial
	other.SessionID = supervisor.SessionID("other-45")
	if err := f.sessions.Save(other); err != nil {
		t.Fatal(err)
	}
	f.pty.screens.SetSnapshot(other.SessionID, blankScreen())
	second := &scn45PTY{observeSendPTY: newObserveSendPTY(), screens: f.pty.screens, id: other.SessionID}
	registry := &PTYRegistry{}
	registry.Register(f.initial.SessionID, f.pty)
	registry.Register(other.SessionID, second)
	f.send.PTYs = registry
	entered, release := make(chan struct{}), make(chan struct{})
	f.pty.afterEnter = func() { close(entered); <-release }
	firstDone := make(chan struct {
		r SendResult
		e error
	}, 1)
	go func() {
		r, e := f.send.Execute(context.Background(), SendInput{SessionID: f.initial.SessionID, Kind: supervisor.SendKindHandoffExit, Origin: supervisor.SendOriginHandoff})
		firstDone <- struct {
			r SendResult
			e error
		}{r, e}
	}()
	scn45Receive(t, entered)
	duplicate, err := f.send.Execute(context.Background(), SendInput{SessionID: f.initial.SessionID, Kind: supervisor.SendKindHandoffExit, Origin: supervisor.SendOriginHandoff})
	if err != nil || duplicate.Blocked != supervisor.SendBlockedPending {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
	second.afterEnter = func() {
		second.exits <- 0
		if err := f.observer.Run(context.Background(), other, second, supervisor.NewVirtualTerminal(80, 24)); err != nil {
			t.Fatal(err)
		}
	}
	secondDone := make(chan struct {
		r SendResult
		e error
	}, 1)
	go func() {
		r, e := f.send.Execute(context.Background(), SendInput{SessionID: other.SessionID, Kind: supervisor.SendKindHandoffExit, Origin: supervisor.SendOriginHandoff})
		secondDone <- struct {
			r SendResult
			e error
		}{r, e}
	}()
	got := scn45Receive(t, secondDone)
	if got.e != nil || !got.r.Confirmed {
		t.Fatalf("second session blocked: %+v/%v", got.r, got.e)
	}
	if err := f.exit(); err != nil {
		t.Fatal(err)
	}
	close(release)
	first := scn45Receive(t, firstDone)
	if first.e != nil || !first.r.Confirmed {
		t.Fatalf("first=%+v/%v", first.r, first.e)
	}
	f.counts(2, 0)
}
func scn45Receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("SCN45 barrier was not reached")
		var zero T
		return zero
	}
}

// A failed durable success write must never be used for stage-4 compensation.
type scn45RejectProof struct {
	SessionRecordStore
	err error
}

func (s scn45RejectProof) Save(v supervisor.ManagedSession) error {
	if v.LastSend != nil && v.LastSend.Kind == supervisor.SendKindHandoffExit {
		return s.err
	}
	return s.SessionRecordStore.Save(v)
}
func TestSCNsup0245SuccessProofSaveFailure(t *testing.T) {
	f := newSCN45(t)
	saveErr := errors.New("CR proof save failed")
	f.send.Sessions = scn45RejectProof{SessionRecordStore: f.sessions, err: saveErr}
	f.pty.afterEnter = func() {
		if err := f.exit(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := f.rotate.sendExit(context.Background(), f.initial); !errors.Is(err, saveErr) {
		t.Fatalf("save error lost: %v", err)
	}
	s := f.current()
	if s.LastSend != nil || s.Rotation.FailReason != supervisor.RotateFailChildAbsent || s.Rotation.Step != supervisor.RotateSendExit {
		t.Fatalf("failed proof used: %+v", s)
	}
	f.counts(0, 0)
	if len(f.send.exitSends) != 0 {
		t.Fatal("save failure leaked active record")
	}
}
func TestSCNsup0245PastSuccessCannotRescueCurrentFailure(t *testing.T) {
	f := newSCN45(t)
	old := &supervisor.LastSendRecord{SendID: f.initial.SessionID.String() + "-s1", Kind: supervisor.SendKindHandoffExit, IssuedAt: f.initial.StateSince, Confirmed: true}
	if err := UpdateSession(f.sessions, f.initial.SessionID, func(v *supervisor.ManagedSession) error { v.LastSend = old; return nil }); err != nil {
		t.Fatal(err)
	}
	f.pty.writeErr = errors.New("current CR failed")
	f.pty.afterEnter = func() {
		if err := f.exit(); err != nil {
			t.Fatal(err)
		}
	}
	_, err := f.rotate.sendExit(context.Background(), f.initial)
	if !errors.Is(err, f.pty.writeErr) {
		t.Fatalf("failure not reported: %v", err)
	}
	if s := f.current(); s.Rotation.FailReason != supervisor.RotateFailChildAbsent || s.LastSend.SendID != old.SendID {
		t.Fatalf("old success used for current send: %+v", s)
	}
	f.counts(0, 1)
}

func TestSCNsup0245ClockAfterSuccessfulReturn(t *testing.T) {
	for _, offset := range []time.Duration{0, -time.Hour} {
		t.Run(offset.String(), func(t *testing.T) {
			f := newSCN45(t)
			at := time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)
			f.send.Now = func() time.Time { return at }
			f.observer.Now = func() time.Time { return at.Add(offset) }
			crSaved := make(chan struct{})
			f.send.Sessions = &scn45SaveSignal{SessionRecordStore: f.sessions, saved: crSaved}
			f.rotate.Send = scn45Sender{send: f.send, timeout: time.Second}
			done := make(chan error, 1)
			go func() { _, err := f.rotate.sendExit(context.Background(), f.initial); done <- err }()
			scn45Receive(t, crSaved)
			// Success has returned from WriteLine and the unconfirmed CR proof is durable.
			if s := f.current(); s.LastSend == nil || s.LastSend.Confirmed || s.SendGate.Pending == nil {
				t.Fatalf("bad pre-exit proof: %+v", s)
			}
			if err := f.exit(); err != nil {
				t.Fatal(err)
			}
			if err := scn45Receive(t, done); err != nil {
				t.Fatal(err)
			}
			f.stage5()
			f.counts(1, 0)
		})
	}
}

func TestSCNsup0245ObserverEventFailureStillCleansUp(t *testing.T) {
	f := newSCN45(t)
	eventErr := errors.New("observer confirmed event failed")
	crSaved := make(chan struct{})
	f.send.Sessions = &scn45SaveSignal{SessionRecordStore: f.sessions, saved: crSaved}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct {
		r SendResult
		e error
	}, 1)
	go func() {
		r, e := f.send.Execute(ctx, SendInput{SessionID: f.initial.SessionID, Kind: supervisor.SendKindHandoffExit, Origin: supervisor.SendOriginHandoff, ConfirmTimeout: time.Second})
		done <- struct {
			r SendResult
			e error
		}{r, e}
	}()
	scn45Receive(t, crSaved)
	attempts := 0
	f.observer.RecordStructured = func(e store.Event) error {
		if e.Kind == supervisor.EventKindSendConfirmed {
			attempts++
			return eventErr
		}
		return observeSendRecord(f.events)(e)
	}
	if err := f.exit(); !errors.Is(err, eventErr) {
		t.Fatalf("observer error lost: %v", err)
	}
	cancel()
	got := scn45Receive(t, done)
	if got.e != nil || !got.r.Confirmed || attempts != 1 {
		t.Fatalf("sender=%+v/%v attempts=%d", got.r, got.e, attempts)
	}
	f.send.mu.Lock()
	cleaned := len(f.send.confirm) == 0 && len(f.send.crWritten) == 0 && len(f.send.crAt) == 0 && len(f.send.locks) == 0 && len(f.send.exitSends) == 0
	f.send.mu.Unlock()
	if !cleaned {
		t.Fatal("event failure skipped exit cleanup")
	}
	if err := f.rotate.deadDuringSend(f.initial, supervisor.RotateSendExit, got.r.SendID); err != nil {
		t.Fatal(err)
	}
	f.stage5()
	f.counts(0, 0)
}
