package usecase

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	p.reads <- []byte("❯\u00a0/rc\r\n❯ /rc")
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
			if tc.crWritten {
				send.markCR(s.SessionID, pending.SendID)
			}
			confirmations := send.confirmationChannel(s.SessionID)
			u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: &constantDetector{result: detect.DetectResult{State: detect.StateExited}}}, Sessions: sessions, SendInput: send, RecordStructured: observeSendRecord(events)}
			if err := u.Run(context.Background(), s, exitedPTY{code: 0}, supervisor.NewVirtualTerminal(80, 24)); err != nil {
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
