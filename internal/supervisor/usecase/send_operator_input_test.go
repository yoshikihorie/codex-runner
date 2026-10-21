package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

func operatorFixture(t *testing.T) (*SendOperatorInputUseCase, *sendTestStore, *sendTestScreen, *sendTestPTY, *[]store.Event) {
	t.Helper()
	id := supervisor.SessionID("alpha-1")
	sessions := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	pty := &sendTestPTY{}
	events := new([]store.Event)
	send := &SendInputUseCase{Sessions: sessions, Screens: screen, PTY: pty, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond, RecordStructured: func(e store.Event) error { *events = append(*events, e); return nil }}
	u := &SendOperatorInputUseCase{Sessions: sessions, Send: send, RecordStructured: send.RecordStructured}
	return u, sessions, screen, pty, events
}

func operatorText(t *testing.T, value string) supervisor.OperatorText {
	t.Helper()
	text, err := supervisor.NewOperatorText(value)
	if err != nil {
		t.Fatal(err)
	}
	return text
}

func TestSendOperatorInput_SCNSup0567_SendsExactTextAndAuditsLength(t *testing.T) {
	u, _, screen, pty, events := operatorFixture(t)
	const value = "全角 ＡＢＣ"
	pty.onWrite = func() { echoScreen(screen, value) }
	pty.afterEnter = func() {
		screen.shot.ChangedAt = time.Now().Add(time.Second)
		screen.shot.Rows[1] = "❯\u00a0"
		screen.shot.Cells[1] = []supervisor.Cell{{Text: "❯"}, {Text: "\u00a0"}}
		u.Send.NotifyConfirmation(SendConfirmation{SessionID: "alpha-1", SendID: "alpha-1-s1", Screen: screen.shot})
	}
	result, err := u.Execute(context.Background(), "alpha-1", operatorText(t, value), false)
	if err != nil || !result.Confirmed || result.SendID != "alpha-1-s1" || pty.wrote != value || pty.calls != 1 || len(pty.writes) != 2 || string(pty.writes[1]) != "\r" {
		t.Fatalf("result=%+v err=%v writes=%v", result, err, pty.writes)
	}
	if len(*events) != 2 || (*events)[0].Kind != supervisor.EventKindSendIssued || (*events)[1].Kind != supervisor.EventKindSendConfirmed {
		t.Fatalf("events=%+v", *events)
	}
	for _, event := range *events {
		if event.SendKind != supervisor.SendKindOperatorInput || event.TextLength == nil || *event.TextLength != len([]rune(value)) {
			t.Fatalf("audit event=%+v", event)
		}
	}
}

func TestSendOperatorInput_SCNSup0571_72_73_74_BlockedConditions(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*SendOperatorInputUseCase, *sendTestStore, *sendTestScreen)
		want  supervisor.SupervisorSendBlockedReason
	}{
		{"working state", func(_ *SendOperatorInputUseCase, s *sendTestStore, _ *sendTestScreen) {
			s.session.State = supervisor.SessionWorking
		}, supervisor.SendBlockedState},
		{"starting state", func(_ *SendOperatorInputUseCase, s *sendTestStore, _ *sendTestScreen) {
			s.session.State = supervisor.SessionStarting
		}, supervisor.SendBlockedState},
		{"permission state", func(_ *SendOperatorInputUseCase, s *sendTestStore, _ *sendTestScreen) {
			s.session.State = supervisor.SessionAwaitingPermission
		}, supervisor.SendBlockedState},
		{"rotating state", func(_ *SendOperatorInputUseCase, s *sendTestStore, _ *sendTestScreen) {
			s.session.State = supervisor.SessionRotating
		}, supervisor.SendBlockedState},
		{"recovering state", func(_ *SendOperatorInputUseCase, s *sendTestStore, _ *sendTestScreen) {
			s.session.State = supervisor.SessionRecovering
		}, supervisor.SendBlockedState},
		{"stopped state", func(_ *SendOperatorInputUseCase, s *sendTestStore, _ *sendTestScreen) {
			s.session.State = supervisor.SessionStopped
		}, supervisor.SendBlockedState},
		{"exited state", func(_ *SendOperatorInputUseCase, s *sendTestStore, _ *sendTestScreen) {
			s.session.State = supervisor.SessionExited
		}, supervisor.SendBlockedState},
		{"failed state", func(_ *SendOperatorInputUseCase, s *sendTestStore, _ *sendTestScreen) {
			s.session.State = supervisor.SessionFailed
		}, supervisor.SendBlockedState},
		{"awaiting recommendation", func(_ *SendOperatorInputUseCase, s *sendTestStore, _ *sendTestScreen) {
			s.session.State = supervisor.SessionAwaitingRecommendation
		}, supervisor.SendBlockedState},
		{"observed working", func(_ *SendOperatorInputUseCase, _ *sendTestStore, s *sendTestScreen) {
			s.shot.State = detect.StateWorking
		}, supervisor.SendBlockedState},
		{"observed unknown", func(_ *SendOperatorInputUseCase, _ *sendTestStore, s *sendTestScreen) {
			s.shot.State = detect.StateUndetermined
		}, supervisor.SendBlockedState},
		{"draft", func(_ *SendOperatorInputUseCase, _ *sendTestStore, s *sendTestScreen) { echoScreen(s, "draft") }, supervisor.SendBlockedState},
		{"selection", func(_ *SendOperatorInputUseCase, _ *sendTestStore, s *sendTestScreen) {
			s.shot.Rows = append(s.shot.Rows, "  ❯ choice")
		}, supervisor.SendBlockedState},
		{"stop", func(u *SendOperatorInputUseCase, _ *sendTestStore, _ *sendTestScreen) {
			u.Send.StopState.SetGlobal(true)
		}, supervisor.SendBlockedStop},
		{"session stop", func(u *SendOperatorInputUseCase, _ *sendTestStore, _ *sendTestScreen) {
			u.Send.StopState.SetSession("alpha-1", true)
		}, supervisor.SendBlockedStop},
		{"pending", func(_ *SendOperatorInputUseCase, s *sendTestStore, _ *sendTestScreen) {
			s.session.SendGate.Pending = &supervisor.PendingSend{SendID: "alpha-1-s1"}
		}, supervisor.SendBlockedPending},
		{"unconfirmed", func(_ *SendOperatorInputUseCase, s *sendTestStore, _ *sendTestScreen) {
			s.session.SendGate.EverSent = true
		}, supervisor.SendBlockedUnconfirmed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, sessions, screen, pty, events := operatorFixture(t)
			tc.setup(u, sessions, screen)
			before := sessions.session
			result, err := u.Execute(context.Background(), "alpha-1", operatorText(t, "hello"), false)
			if err != nil || result.Blocked != tc.want || pty.calls != 0 || len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindSendBlocked || (*events)[0].TextLength == nil || *(*events)[0].TextLength != 5 || sessions.session.State != before.State || sessions.session.SendGate.NextSeq != before.SendGate.NextSeq {
				t.Fatalf("result=%+v err=%v calls=%d events=%+v", result, err, pty.calls, *events)
			}
		})
	}
}

func TestSendOperatorInput_SCNSup0575_ForbiddenText(t *testing.T) {
	for _, value := range []string{"force push して", "ＰＲを作成して", "rm -rf して"} {
		u, _, _, pty, events := operatorFixture(t)
		_, err := u.Execute(context.Background(), "alpha-1", operatorText(t, value), false)
		var refusal *OperatorInputError
		if !errors.As(err, &refusal) || refusal.Code != "SUP_OPERATOR_INPUT_FORBIDDEN" || refusal.Reason != supervisor.OperatorInputRefuseForbiddenText || pty.calls != 0 || len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindOperatorInputRefused || (*events)[0].TextLength != nil {
			t.Fatalf("value=%q err=%v events=%+v", value, err, *events)
		}
	}
}

func TestSendOperatorInput_SCNSup0576_RateLimitClockAndNoRefresh(t *testing.T) {
	u, _, _, _, events := operatorFixture(t)
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	u.Now = func() time.Time { return now }
	_, _ = u.Execute(context.Background(), "alpha-1", operatorText(t, "first"), false)
	now = now.Add(900 * time.Millisecond)
	result, err := u.Execute(context.Background(), "alpha-1", operatorText(t, "second"), false)
	if err != nil || result.Blocked != supervisor.SendBlockedRateLimited {
		t.Fatalf("rate result=%+v err=%v", result, err)
	}
	now = now.Add(100 * time.Millisecond)
	result, err = u.Execute(context.Background(), "alpha-1", operatorText(t, "third"), false)
	if result.Blocked == supervisor.SendBlockedRateLimited || len(*events) < 3 {
		t.Fatalf("rate origin moved: result=%+v err=%v events=%+v", result, err, *events)
	}
}

func TestSendOperatorInput_SCNSup0576_IndependentRestartAndRejectedCalls(t *testing.T) {
	u, sessions, _, _, _ := operatorFixture(t)
	now := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	u.Now = func() time.Time { return now }
	_, _ = u.Execute(context.Background(), "alpha-1", operatorText(t, "first"), false)
	if len(u.last) != 1 {
		t.Fatalf("rate origins=%d", len(u.last))
	}
	sessions.session = supervisor.ManagedSession{SessionID: "beta-1", State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	result, _ := u.Execute(context.Background(), "beta-1", operatorText(t, "second"), false)
	if result.Blocked == supervisor.SendBlockedRateLimited || len(u.last) != 2 {
		t.Fatalf("other session rate result=%+v origins=%d", result, len(u.last))
	}
	sessions.session = supervisor.ManagedSession{SessionID: "alpha-1", State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	restarted := &SendOperatorInputUseCase{Sessions: sessions, Send: u.Send, Now: u.Now}
	if _, err := restarted.Execute(context.Background(), "alpha-1", supervisor.OperatorText{}, true); err == nil {
		t.Fatal("malformed call accepted")
	}
	if _, err := restarted.Execute(context.Background(), "alpha-1", operatorText(t, "rejected"), true); err == nil {
		t.Fatal("supervised caller accepted")
	}
	if len(restarted.last) != 0 {
		t.Fatalf("rejected calls moved origin: %d", len(restarted.last))
	}
	result, _ = restarted.Execute(context.Background(), "alpha-1", operatorText(t, "after restart"), false)
	if result.Blocked == supervisor.SendBlockedRateLimited || len(restarted.last) != 1 {
		t.Fatalf("restart rate result=%+v origins=%d", result, len(restarted.last))
	}
}

func TestSendOperatorInput_SCNSup0577_80_RefusalAndMissingSession(t *testing.T) {
	u, sessions, _, pty, events := operatorFixture(t)
	_, err := u.Execute(context.Background(), "alpha-1", operatorText(t, "hello"), true)
	var refusal *OperatorInputError
	if !errors.As(err, &refusal) || refusal.Reason != supervisor.OperatorInputRefuseSupervisedCaller || pty.calls != 0 || len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindOperatorInputRefused {
		t.Fatalf("refusal=%v events=%+v", err, *events)
	}
	_, err = u.Execute(context.Background(), "nope-9", operatorText(t, "hello"), false)
	if !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("missing session=%v", err)
	}
	sessions.session.State = supervisor.SessionExited
	result, err := u.Execute(context.Background(), "alpha-1", operatorText(t, "hello"), false)
	if err != nil || result.Blocked != supervisor.SendBlockedState {
		t.Fatalf("exited result=%+v err=%v", result, err)
	}
}

func TestSendOperatorInput_SCNSup0574_ConcurrentCalls(t *testing.T) {
	u, original, screen, pty, events := operatorFixture(t)
	locked := &lockedOperatorStore{session: original.session}
	u.Sessions = locked
	u.Send.Sessions = locked
	var eventMu sync.Mutex
	record := func(e store.Event) error {
		eventMu.Lock()
		defer eventMu.Unlock()
		*events = append(*events, e)
		return nil
	}
	u.RecordStructured = record
	u.Send.RecordStructured = record
	pty.onWrite = func() { echoScreen(screen, "hello") }
	var group sync.WaitGroup
	results := make(chan SendResult, 2)
	for range 2 {
		group.Add(1)
		go func() {
			defer group.Done()
			result, _ := u.Execute(context.Background(), "alpha-1", operatorText(t, "hello"), false)
			results <- result
		}()
	}
	group.Wait()
	close(results)
	blocked := 0
	for result := range results {
		if result.Blocked != "" {
			blocked++
		}
	}
	if blocked != 1 || pty.calls != 1 || len(*events) < 2 {
		t.Fatalf("blocked=%d calls=%d events=%+v", blocked, pty.calls, *events)
	}
}

func TestSendOperatorInput_SCNSup0572_PreWriteScreenChange(t *testing.T) {
	u, sessions, _, pty, events := operatorFixture(t)
	draft := blankScreen()
	draft.Rows[1] = "❯\u00a0draft"
	draft.Cells[1] = append(draft.Cells[1], supervisor.Cell{Text: "draft"})
	u.Send.Screens = &screenSequence{shots: []ScreenSnapshot{blankScreen(), blankScreen(), draft}}
	result, err := u.Execute(context.Background(), "alpha-1", operatorText(t, "hello"), false)
	if err != nil || result.Blocked != supervisor.SendBlockedState || pty.calls != 0 || sessions.session.State != supervisor.SessionIdle || sessions.session.SendGate.Pending != nil || len(*events) != 2 || (*events)[1].Kind != supervisor.EventKindSendBlocked {
		t.Fatalf("result=%+v err=%v calls=%d events=%+v", result, err, pty.calls, *events)
	}
}

func TestSendOperatorInput_PreWriteRequiresObservedIdle(t *testing.T) {
	for _, state := range []detect.DetectedState{detect.StateAwaitingRecommendation, detect.StateWorking} {
		t.Run(string(state), func(t *testing.T) {
			u, _, _, pty, _ := operatorFixture(t)
			changed := blankScreen()
			changed.State = state
			u.Send.Screens = &screenSequence{shots: []ScreenSnapshot{blankScreen(), blankScreen(), changed}}
			result, err := u.Execute(context.Background(), "alpha-1", operatorText(t, "hello"), false)
			if err != nil || result.Blocked != supervisor.SendBlockedState || pty.calls != 0 {
				t.Fatalf("state=%s result=%+v err=%v calls=%d", state, result, err, pty.calls)
			}
		})
	}
}

func TestSendOperatorInput_BeforeCRAllowsOnlyIdleOrWorking(t *testing.T) {
	for _, state := range []detect.DetectedState{detect.StateIdle, detect.StateWorking, detect.StateAwaitingRecommendation, detect.StateAwaitingPermission, detect.StateExited, detect.StateUndetermined} {
		t.Run(string(state), func(t *testing.T) {
			u, _, screen, pty, _ := operatorFixture(t)
			pty.onWrite = func() { echoScreen(screen, "hello"); screen.shot.State = state }
			_, _ = u.Execute(context.Background(), "alpha-1", operatorText(t, "hello"), false)
			wantCR := state == detect.StateIdle || state == detect.StateWorking
			if got := len(pty.writes) == 2; got != wantCR {
				t.Fatalf("state=%s CR=%v want=%v", state, got, wantCR)
			}
		})
	}
}

func TestSendOperatorInput_EchoTrimsOnlyForComparison(t *testing.T) {
	for _, value := range []string{" hello", "hello "} {
		t.Run(value, func(t *testing.T) {
			u, _, screen, pty, _ := operatorFixture(t)
			pty.onWrite = func() { echoScreen(screen, value) }
			_, _ = u.Execute(context.Background(), "alpha-1", operatorText(t, value), false)
			if pty.wrote != value || len(pty.writes) != 2 {
				t.Fatalf("wrote=%q writes=%d", pty.wrote, len(pty.writes))
			}
		})
	}
	// PT-29: wrapped long input is reserved for the T4-15 real-PTY check.
}

type operatorBeforeCRStore struct {
	sendTestStore
	failNextLoad bool
}

func (s *operatorBeforeCRStore) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	if s.failNextLoad {
		s.failNextLoad = false
		return supervisor.ManagedSession{}, errors.New("load failed")
	}
	return s.sendTestStore.Load(id)
}

func TestSendOperatorInput_BeforeCRRechecksPersistedAndObservedState(t *testing.T) {
	for _, tc := range []struct {
		name     string
		state    supervisor.SupervisorSessionState
		observed detect.DetectedState
		loadFail bool
		wantCR   bool
	}{
		{"idle", supervisor.SessionIdle, detect.StateIdle, false, true},
		{"working", supervisor.SessionWorking, detect.StateWorking, false, true},
		{"permission", supervisor.SessionAwaitingPermission, detect.StateIdle, false, false},
		{"recommendation", supervisor.SessionAwaitingRecommendation, detect.StateIdle, false, false},
		{"exited", supervisor.SessionExited, detect.StateIdle, false, false},
		{"rotating", supervisor.SessionRotating, detect.StateIdle, false, false},
		{"observed permission", supervisor.SessionIdle, detect.StateAwaitingPermission, false, false},
		{"observed recommendation", supervisor.SessionIdle, detect.StateAwaitingRecommendation, false, false},
		{"observed exited", supervisor.SessionIdle, detect.StateExited, false, false},
		{"load failure", supervisor.SessionIdle, detect.StateIdle, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, original, screen, pty, events := operatorFixture(t)
			sessions := &operatorBeforeCRStore{sendTestStore: *original}
			u.Sessions, u.Send.Sessions = sessions, sessions
			observed := &ObservedStateStore{}
			observed.Set("alpha-1", detect.StateIdle)
			u.Send.Observed = observed
			pty.onWrite = func() {
				echoScreen(screen, "hello")
				sessions.session.State = tc.state
				observed.Set("alpha-1", tc.observed)
				sessions.failNextLoad = tc.loadFail
			}
			_, err := u.Execute(context.Background(), "alpha-1", operatorText(t, "hello"), false)
			if got := len(pty.writes) == 2; got != tc.wantCR {
				t.Fatalf("CR=%v want=%v err=%v", got, tc.wantCR, err)
			}
			if !tc.wantCR && (len(*events) < 2 || (*events)[len(*events)-1].Kind != supervisor.EventKindSendAborted || sessions.session.SendGate.Pending != nil) {
				t.Fatalf("err=%v events=%+v gate=%+v", err, *events, sessions.session.SendGate)
			}
		})
	}
}

func TestSendOperatorInput_BeforeCRComparesLeadingSpaceExactly(t *testing.T) {
	for _, tc := range []struct {
		name, body, echo string
		wantCR           bool
	}{
		{"leading dropped", " hello", "hello", false},
		{"leading preserved", " hello", " hello", true},
		{"right padding", "hello ", "hello", true},
		{"other body", "hello", "hello world", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, _, screen, pty, events := operatorFixture(t)
			pty.onWrite = func() { echoScreen(screen, tc.echo) }
			_, _ = u.Execute(context.Background(), "alpha-1", operatorText(t, tc.body), false)
			if got := len(pty.writes) == 2; got != tc.wantCR || pty.wrote != tc.body {
				t.Fatalf("CR=%v want=%v wrote=%q", got, tc.wantCR, pty.wrote)
			}
			if !tc.wantCR && (*events)[len(*events)-1].Kind != supervisor.EventKindSendAborted {
				t.Fatalf("events=%+v", *events)
			}
		})
	}
}

func TestSendOperatorInput_UnconfirmedOperationalLog(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "cancel"}[canceled], func(t *testing.T) {
			u, _, screen, pty, _ := operatorFixture(t)
			root := t.TempDir()
			log, err := store.NewSupervisorLog(root, 0, 0)
			if err != nil {
				t.Fatal(err)
			}
			u.Send.OperationalLog = log
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			const body = "private body marker"
			pty.onWrite = func() { echoScreen(screen, body) }
			if canceled {
				pty.afterEnter = cancel
			}
			_, err = u.Execute(ctx, "alpha-1", operatorText(t, body), false)
			if err == nil {
				t.Fatal("unconfirmed send succeeded")
			}
			data, err := os.ReadFile(filepath.Join(root, "codexd-supervisor.log"))
			if err != nil {
				t.Fatal(err)
			}
			var entry store.SupervisorLogEntry
			if err := json.Unmarshal(data, &entry); err != nil {
				t.Fatalf("operational log is not one JSON entry: %v", err)
			}
			if entry.Kind != supervisor.EventKindSendUnconfirmed || entry.Level != store.LogLevelWarn || entry.SessionID != "alpha-1" || entry.SendID != "alpha-1-s1" || strings.Contains(string(data), body) {
				t.Fatalf("unexpected operational log: %s", data)
			}
		})
	}
}

func TestSendOperatorInput_SCNSup0572_73_CompletionAndUnreadableStop(t *testing.T) {
	for _, setup := range []struct {
		name string
		set  func(*testing.T, *SendOperatorInputUseCase, *sendTestScreen)
		want supervisor.SupervisorSendBlockedReason
	}{
		{"completion", func(_ *testing.T, _ *SendOperatorInputUseCase, s *sendTestScreen) { s.shot.CompletionMenu = true }, supervisor.SendBlockedState},
		{"unreadable stop", func(t *testing.T, u *SendOperatorInputUseCase, _ *sendTestScreen) {
			root := t.TempDir()
			blocker := filepath.Join(root, "blocker")
			if err := os.WriteFile(blocker, nil, 0600); err != nil {
				t.Fatal(err)
			}
			u.Send.StopState = NewStopState(blocker)
		}, supervisor.SendBlockedStop},
	} {
		t.Run(setup.name, func(t *testing.T) {
			u, _, screen, pty, _ := operatorFixture(t)
			setup.set(t, u, screen)
			result, err := u.Execute(context.Background(), "alpha-1", operatorText(t, "hello"), false)
			if err != nil || result.Blocked != setup.want || pty.calls != 0 {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, pty.calls)
			}
		})
	}
}

type lockedOperatorStore struct {
	mu      sync.Mutex
	session supervisor.ManagedSession
}

func (s *lockedOperatorStore) Load(supervisor.SessionID) (supervisor.ManagedSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.session, nil
}

func (s *lockedOperatorStore) Save(value supervisor.ManagedSession) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.session = value
	return nil
}

func TestSendOperatorInput_SCNSup0579_AbortAndUnconfirmed(t *testing.T) {
	for _, abort := range []bool{true, false} {
		u, sessions, screen, pty, events := operatorFixture(t)
		pty.onWrite = func() {
			if abort {
				echoScreen(screen, "other")
			} else {
				echoScreen(screen, "hello")
			}
		}
		_, err := u.Execute(context.Background(), "alpha-1", operatorText(t, "hello"), false)
		want := supervisor.EventKindSendUnconfirmed
		if abort {
			want = supervisor.EventKindSendAborted
		}
		if err == nil || sessions.session.State != supervisor.SessionStopped || pty.calls != 1 || len(*events) < 2 || (*events)[len(*events)-1].Kind != want {
			t.Fatalf("abort=%v err=%v state=%s events=%+v", abort, err, sessions.session.State, *events)
		}
		if strings.Contains(err.Error(), "hello") {
			t.Fatal("body in error")
		}
	}
}

func TestSendOperatorInput_SCNSup0573_StopBeforeCR(t *testing.T) {
	u, sessions, screen, pty, events := operatorFixture(t)
	pty.onWrite = func() {
		echoScreen(screen, "hello")
		u.Send.StopState.SetGlobal(true)
	}
	_, err := u.Execute(context.Background(), "alpha-1", operatorText(t, "hello"), false)
	var sendErr *OperatorInputError
	if !errors.As(err, &sendErr) || sendErr.Code != "SUP_SEND_ABORTED" || sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopSendAborted || len(pty.writes) != 1 || len(*events) != 2 || (*events)[1].Kind != supervisor.EventKindSendAborted {
		t.Fatalf("err=%v state=%s writes=%v events=%+v", err, sessions.session.State, pty.writes, *events)
	}
}
