package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type sendTestStore struct{ session supervisor.ManagedSession }

func TestSendInput_SCN23_ParallelInstructionFromProjectOrDefault(t *testing.T) {
	id := supervisor.SessionID("parallel-23")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, ProjectID: "project-a"}}
	u := &SendInputUseCase{Sessions: s, ProjectInstructions: map[string]string{"project-a": "project instruction"}}
	if got := u.parallelInstructionFor(s.session); got != "project instruction" {
		t.Fatalf("project instruction=%q", got)
	}
	s.session.ProjectID = "project-b"
	if got := u.parallelInstructionFor(s.session); got != defaultParallelInstruction {
		t.Fatalf("default instruction=%q", got)
	}
}

func TestSendInput_SCN0418_HandoffFailuresLeaveRotationState(t *testing.T) {
	for _, abort := range []bool{false, true} {
		t.Run(fmt.Sprint(abort), func(t *testing.T) {
			id, _ := supervisor.NewSessionID("alpha-1")
			s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionRotating, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
			screen := &sendTestScreen{shot: blankScreen()}
			pty := &sendTestPTY{onWrite: func() { echoScreen(screen, "#end") }}
			var writer PTYWriter = pty
			if abort {
				writer = failingBeforeCRPTY{errors.New("before CR")}
			}
			u := SendInputUseCase{Sessions: s, Screens: screen, PTY: writer, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
			_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindHandoffEnd, Origin: supervisor.SendOriginHandoff})
			if err == nil || s.session.State != supervisor.SessionRotating {
				t.Fatalf("err=%v state=%s", err, s.session.State)
			}
			if abort && s.session.SendGate.Pending != nil {
				t.Fatal("pre-CR abort left pending")
			}
			if !abort && s.session.SendGate.Pending == nil {
				t.Fatal("unconfirmed send lost pending")
			}
		})
	}
}

func TestSendInput_SCN21_EndIssuedAtPersistedBeforePTYWrite(t *testing.T) {
	id, _ := supervisor.NewSessionID("issued-end-21")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionRotating, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateSendEnd}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	checked := false
	pty := &sendTestPTY{onWrite: func() {
		if s.session.SendGate.Pending != nil && s.session.Rotation.EndIssuedAt != nil && s.session.Rotation.EndIssuedAt.Equal(s.session.SendGate.Pending.IssuedAt) {
			checked = true
		}
		echoScreen(screen, "#end")
	}}
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: pty, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
	_, _ = u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindHandoffEnd, Origin: supervisor.SendOriginHandoff})
	if !checked {
		t.Fatalf("issued time not durable before PTY write: %+v", s.session.Rotation)
	}
}

func TestSendInput_SCN40_ResumeReadyNeedsFreshQuietIdle(t *testing.T) {
	id, _ := supervisor.NewSessionID("resume-ready-40")
	resumeAt := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	shot := blankScreen()
	shot.ObservedAt = resumeAt.Add(time.Second)
	shot.ChangedAt = resumeAt.Add(time.Second)
	screen := &sendTestScreen{shot: shot}
	u := &SendInputUseCase{Screens: screen, Now: func() time.Time { return shot.ChangedAt.Add(detect.QuietPeriod) }}
	if !u.ResumeReady(id, resumeAt) {
		t.Fatal("fresh quiet idle rejected")
	}
	screen.shot.ChangedAt = resumeAt
	if u.ResumeReady(id, resumeAt) {
		t.Fatal("pre-resume screen accepted")
	}
	screen.shot = shot
	screen.shot.State = detect.StateWorking
	if u.ResumeReady(id, resumeAt) {
		t.Fatal("working screen accepted")
	}
}

func TestSendInput_SCN0418_ExitUsesCallerTimeout(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionRotating, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	pty := &sendTestPTY{onWrite: func() { echoScreen(screen, "/exit") }}
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: pty, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Hour}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := u.Execute(ctx, SendInput{SessionID: id, Kind: supervisor.SendKindHandoffExit, Origin: supervisor.SendOriginHandoff, ConfirmTimeout: time.Millisecond})
	if err == nil || errors.Is(err, context.DeadlineExceeded) || s.session.State != supervisor.SessionRotating {
		t.Fatalf("err=%v state=%s", err, s.session.State)
	}
}

type sendStateFlipStore struct {
	sendTestStore
	loads int
}
type sendConfirmationInspectStore struct {
	sendTestStore
	u            *SendInputUseCase
	readyAtIssue bool
}

func (s *sendConfirmationInspectStore) Save(v supervisor.ManagedSession) error {
	if v.SendGate.Pending != nil {
		s.readyAtIssue = s.u.confirm[v.SessionID] != nil
	}
	return s.sendTestStore.Save(v)
}

func TestFix3ConfirmationChannelReadyBeforePendingIssue(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendConfirmationInspectStore{sendTestStore: sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}}
	screen := &sendTestScreen{shot: blankScreen()}
	pty := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: pty, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
	s.u = u
	_, _ = u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if !s.readyAtIssue {
		t.Fatal("pending was visible before confirmation channel")
	}
}

func (s *sendStateFlipStore) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	s.loads++
	if s.loads == 2 {
		s.session.State = supervisor.SessionWorking
	}
	return s.sendTestStore.Load(id)
}

func TestFix3IssueRechecksPersistedState(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendStateFlipStore{sendTestStore: sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}}
	pty := &sendTestPTY{}
	var events []store.Event
	u := SendInputUseCase{Sessions: s, Screens: &sendTestScreen{shot: blankScreen()}, PTY: pty, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), RecordStructured: func(e store.Event) error { events = append(events, e); return nil }}
	got, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err != nil || got.Blocked != supervisor.SendBlockedState || pty.calls != 0 || s.session.SendGate.Pending != nil || len(events) != 1 || events[0].Kind != supervisor.EventKindSendBlocked {
		t.Fatalf("result=%+v err=%v session=%+v events=%+v", got, err, s.session, events)
	}
}

func (s *sendTestStore) Load(supervisor.SessionID) (supervisor.ManagedSession, error) {
	return s.session, nil
}
func (s *sendTestStore) Save(v supervisor.ManagedSession) error { s.session = v; return nil }

type sendTestPTY struct {
	calls      int
	wrote      string
	writes     [][]byte
	delay      time.Duration
	before     func() bool
	onWrite    func()
	afterEnter func()
	writeError error
}

func (p *sendTestPTY) WriteLine(_ context.Context, text string, delay time.Duration, before func() bool) error {
	p.calls++
	p.wrote = text
	p.writes = append(p.writes, []byte(text))
	p.delay = delay
	p.before = before
	if p.onWrite != nil {
		p.onWrite()
	}
	if !before() {
		return proc.ErrBeforeEnter
	}
	p.writes = append(p.writes, []byte{'\r'})
	if p.afterEnter != nil {
		p.afterEnter()
	}
	return p.writeError
}

type sendTestScreen struct{ shot ScreenSnapshot }

func (s *sendTestScreen) Snapshot(supervisor.SessionID) ScreenSnapshot { return s.shot }

type screenWithHook struct {
	shot ScreenSnapshot
	once bool
	hook func()
}

type screenSequence struct {
	shots []ScreenSnapshot
	next  int
}

func (s *screenSequence) Snapshot(supervisor.SessionID) ScreenSnapshot {
	index := s.next
	if index >= len(s.shots) {
		index = len(s.shots) - 1
	}
	s.next++
	return s.shots[index]
}

func (s *screenWithHook) Snapshot(supervisor.SessionID) ScreenSnapshot {
	if !s.once {
		s.once = true
		s.hook()
	}
	return s.shot
}
func blankScreen() ScreenSnapshot {
	return ScreenSnapshot{State: detect.StateIdle, Rows: []string{"────", "❯\u00a0", "────"}, Cells: [][]supervisor.Cell{{}, {{Text: "❯"}, {Text: "\u00a0"}}, {}}, ObservedAt: time.Now().UTC()}
}

func TestFix3MissingStopSourcesBlock(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	for _, tc := range []struct {
		name   string
		memory func(supervisor.SessionID) bool
		files  *StopState
	}{
		{"memory missing", nil, NewStopState(t.TempDir())},
		{"files missing", func(supervisor.SessionID) bool { return false }, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pty := &sendTestPTY{}
			var events []store.Event
			u := SendInputUseCase{Sessions: &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle}}, Screens: &sendTestScreen{shot: blankScreen()}, PTY: pty, IsStopped: tc.memory, StopState: tc.files, RecordStructured: func(e store.Event) error { events = append(events, e); return nil }}
			result, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
			if err != nil || result.Blocked != supervisor.SendBlockedStop || pty.calls != 0 || len(events) != 1 || events[0].Kind != supervisor.EventKindSendBlocked {
				t.Fatalf("result=%+v err=%v calls=%d events=%+v", result, err, pty.calls, events)
			}
		})
	}
}

func TestFix3ScreenEchoMustBeInputRow(t *testing.T) {
	shot := blankScreen()
	shot.Rows = []string{"❯\u00a0/rc", "status", "❯\u00a0"}
	if screenReadyForEnter(shot, "/rc") {
		t.Fatal("historic row must not satisfy enter guard")
	}
}

func TestFix3EndedSessionIgnoresConfirmation(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	u := &SendInputUseCase{}
	u.confirmationChannel(id)
	u.EndSession(id)
	u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "late"})
	if len(u.confirm) != 0 {
		t.Fatalf("recreated confirmation channel: %+v", u.confirm)
	}
}

func TestFix3Stage5DoesNotRequestInitialRemoteControl(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	count := 0
	r := &InitialRemoteControl{Request: func(context.Context, SendInput) error { count++; return nil }}
	s := supervisor.ManagedSession{SessionID: id, Generation: 2, State: supervisor.SessionRotating}
	if err := r.OnStateChange(context.Background(), supervisor.SessionRotating, s); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("stage 5 sent /rc %d times", count)
	}
}

func TestFix3RemoteControlForgetsEndedSession(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	r := &InitialRemoteControl{Request: func(context.Context, SendInput) error { return nil }}
	s := supervisor.ManagedSession{SessionID: id, Generation: 1, State: supervisor.SessionIdle}
	if err := r.OnStateChange(context.Background(), supervisor.SessionStarting, s); err != nil {
		t.Fatal(err)
	}
	r.EndSession(id)
	if len(r.requested) != 0 || len(r.started) != 0 {
		t.Fatalf("session remained in maps: requested=%v started=%v", r.requested, r.started)
	}
}

func echoScreen(s *sendTestScreen, value string) {
	s.shot.Rows[1] = "❯\u00a0" + value
	s.shot.Cells[1] = []supervisor.Cell{{Text: "❯"}, {Text: "\u00a0"}}
	for _, r := range value {
		s.shot.Cells[1] = append(s.shot.Cells[1], supervisor.Cell{Text: string(r)})
	}
}

func TestSendEchoWorkingCanReachEnter(t *testing.T) {
	s := &sendTestScreen{shot: blankScreen()}
	echoScreen(s, "/rc")
	s.shot.State = detect.StateWorking
	if !screenReadyForEnter(s.shot, "/rc") {
		t.Fatal("typed command echo must allow enter")
	}
}

func TestSendEchoWithRealDetectorCanReachEnter(t *testing.T) {
	s := &sendTestScreen{shot: blankScreen()}
	echoScreen(s, "/rc")
	now := time.Now()
	result, err := detect.NewCompositeDetector(detect.DefaultPatternSet()).Detect(context.Background(), detect.DetectInput{CLI: supervisor.CLIClaude, ScreenRows: s.shot.Rows, ScreenCells: s.shot.Cells, LastScreenChangeAt: now, Now: now})
	if err != nil {
		t.Fatal(err)
	}
	s.shot.State = result.State
	if result.State != detect.StateWorking || !screenReadyForEnter(s.shot, "/rc") {
		t.Fatalf("detected=%s ready=%v", result.State, screenReadyForEnter(s.shot, "/rc"))
	}
}

func TestSendRealDetectorEchoDoesNotAbort(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{}
	p.onWrite = func() {
		echoScreen(screen, "/rc")
		now := time.Now()
		result, err := detect.NewCompositeDetector(detect.DefaultPatternSet()).Detect(context.Background(), detect.DetectInput{CLI: supervisor.CLIClaude, ScreenRows: screen.shot.Rows, ScreenCells: screen.shot.Cells, LastScreenChangeAt: now, Now: now})
		if err != nil {
			t.Fatal(err)
		}
		screen.shot.State = result.State
	}
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Second}
	p.afterEnter = func() {
		shot := blankScreen()
		shot.ChangedAt = time.Now().Add(time.Second)
		u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "alpha-1-s1", Screen: shot})
	}
	got, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err != nil || !got.Confirmed || s.session.StopReason == supervisor.StopSendAborted {
		t.Fatalf("result=%+v err=%v session=%+v", got, err, s.session)
	}
}

func TestSendProcessingBlocksEnter(t *testing.T) {
	s := &sendTestScreen{shot: blankScreen()}
	echoScreen(s, "/rc")
	s.shot.Processing = true
	if screenReadyForEnter(s.shot, "/rc") {
		t.Fatal("processing display must block enter")
	}
}

func TestSendProcessingBlocksInitialWrite(t *testing.T) {
	shot := blankScreen()
	shot.Processing = true
	if screenReadyForWrite(shot) {
		t.Fatal("processing screen accepted for text write")
	}
}

func TestSendMissingStopReaderFailsClosed(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	u := SendInputUseCase{Sessions: &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle}}, Screens: &sendTestScreen{shot: blankScreen()}, PTY: &sendTestPTY{}}
	got, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err != nil || got.Blocked != supervisor.SendBlockedStop {
		t.Fatalf("result=%+v err=%v", got, err)
	}
}

func TestSendStopMarkerReadErrorFailsClosed(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	root := t.TempDir()
	if err := os.Symlink("STOP", filepath.Join(root, "STOP")); err != nil {
		t.Fatal(err)
	}
	stop := NewStopState(root)
	u := SendInputUseCase{Sessions: &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle}}, Screens: &sendTestScreen{shot: blankScreen()}, PTY: &sendTestPTY{}, StopState: stop}
	got, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err != nil || got.Blocked != supervisor.SendBlockedStop {
		t.Fatalf("result=%+v err=%v", got, err)
	}
}

type failingBeforeCRPTY struct{ err error }

func (p failingBeforeCRPTY) WriteLine(context.Context, string, time.Duration, func() bool) error {
	return p.err
}

func TestSendPreCRErrorsRestoreGate(t *testing.T) {
	for _, writeErr := range []error{context.Canceled, os.ErrClosed, proc.ErrBeforeEnter} {
		t.Run(writeErr.Error(), func(t *testing.T) {
			id, _ := supervisor.NewSessionID("alpha-1")
			s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
			u := SendInputUseCase{Sessions: s, Screens: &sendTestScreen{shot: blankScreen()}, PTY: failingBeforeCRPTY{writeErr}, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir())}
			_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
			if !errors.Is(err, writeErr) || s.session.SendGate.Pending != nil || s.session.SendGate.EverSent || s.session.SendGate.LastSendConfirmed || s.session.SendGate.NextSeq != 2 || s.session.StopReason != supervisor.StopSendAborted {
				t.Fatalf("err=%v session=%+v", err, s.session)
			}
		})
	}
}

func TestSendStopAppearingAfterIssueWritesNoText(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	p := &sendTestPTY{}
	checks := 0
	u := SendInputUseCase{Sessions: s, Screens: &sendTestScreen{shot: blankScreen()}, PTY: p, IsStopped: func(supervisor.SessionID) bool { checks++; return checks >= 3 }, StopState: NewStopState(t.TempDir())}
	got, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err != nil || got.Blocked != supervisor.SendBlockedStop || p.calls != 0 || s.session.SendGate.Pending != nil || s.session.SendGate.EverSent || s.session.SendGate.NextSeq != 2 || s.session.State != supervisor.SessionIdle {
		t.Fatalf("err=%v calls=%d gate=%+v", err, p.calls, s.session.SendGate)
	}
}

func TestSendSecondScreenRechecksKind(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	first := blankScreen()
	second := blankScreen()
	second.State = detect.StateAwaitingRecommendation
	screens := &screenSequence{shots: []ScreenSnapshot{first, second}}
	p := &sendTestPTY{}
	u := SendInputUseCase{Sessions: s, Screens: screens, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir())}
	result, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindSessionStart})
	if err != nil || result.Blocked != supervisor.SendBlockedState || p.calls != 0 || s.session.SendGate.Pending != nil {
		t.Fatalf("result=%+v err=%v calls=%d", result, err, p.calls)
	}
}

func TestSendTimeoutPreservesExistingStopAndReportsRecordError(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
	p.afterEnter = func() { s.session.State = supervisor.SessionStopped; s.session.StopReason = supervisor.StopGlobal }
	recordErr := errors.New("record failed")
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond, RecordStructured: func(e store.Event) error {
		if e.Kind == supervisor.EventKindSendUnconfirmed {
			return recordErr
		}
		return nil
	}}
	_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if !errors.Is(err, recordErr) || s.session.StopReason != supervisor.StopGlobal || s.session.SendGate.Pending == nil || s.session.SendGate.LastSendConfirmed {
		t.Fatalf("err=%v session=%+v", err, s.session)
	}
}

func TestSendContextCanceledAfterCRStopsUnconfirmed(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	ctx, cancel := context.WithCancel(context.Background())
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }, afterEnter: cancel}
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir())}
	_, err := u.Execute(ctx, SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if !errors.Is(err, context.Canceled) || s.session.State != supervisor.SessionStopped || s.session.StopReason != supervisor.StopSendNotConfirmed || s.session.SendGate.Pending == nil {
		t.Fatalf("err=%v session=%+v", err, s.session)
	}
}

func TestSendConfirmationQueueResetAndSessionCleanup(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	u := &SendInputUseCase{}
	for n := 0; n < 16; n++ {
		u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "old"})
	}
	old := u.confirmationChannel(id)
	newChannel := u.resetConfirmation(id)
	if old == newChannel || len(newChannel) != 0 {
		t.Fatal("confirmation queue retained stale messages")
	}
	u.markCR(id, "new")
	_ = u.sessionLock(id)
	u.EndSession(id)
	if len(u.confirm) != 0 || len(u.locks) != 0 || len(u.crWritten) != 0 {
		t.Fatalf("maps not cleaned: %+v", u)
	}
}

func TestSendRotatingRequiresHandoffOrigin(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionRotating, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "#end") }}
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
	blocked, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindHandoffEnd, Origin: supervisor.SendOriginNormal})
	if err != nil || blocked.Blocked != supervisor.SendBlockedState || p.calls != 0 {
		t.Fatalf("blocked=%+v err=%v calls=%d", blocked, err, p.calls)
	}
	issued, _ := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindHandoffEnd, Origin: supervisor.SendOriginHandoff})
	if issued.SendID == "" || p.calls != 1 {
		t.Fatalf("issued=%+v calls=%d", issued, p.calls)
	}
}

func TestSendObservedAtCannotConfirmWithoutChangedAt(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
	p.afterEnter = func() {
		shot := blankScreen()
		shot.ObservedAt = time.Now().Add(time.Hour)
		shot.ChangedAt = time.Now().Add(-time.Hour)
		u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "alpha-1-s1", Screen: shot})
	}
	result, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err == nil || result.Confirmed || s.session.SendGate.Pending == nil {
		t.Fatalf("result=%+v err=%v gate=%+v", result, err, s.session.SendGate)
	}
}

func TestSCNsup0216ConsecutiveSendsHaveUniqueIDs(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Second}
	p.afterEnter = func() {
		shot := blankScreen()
		shot.ChangedAt = time.Now().Add(time.Second)
		u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: s.session.SendGate.Pending.SendID, Screen: shot})
		screen.shot = blankScreen()
	}
	for n, want := range []string{"alpha-1-s1", "alpha-1-s2", "alpha-1-s3"} {
		got, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
		if err != nil || !got.Confirmed || got.SendID != want || s.session.SendGate.NextSeq != uint64(n+2) {
			t.Fatalf("send %d: result=%+v err=%v gate=%+v", n, got, err, s.session.SendGate)
		}
	}
}

func TestSCNsup0230RestartedExecuteIssuesFourthID(t *testing.T) {
	root := t.TempDir()
	id, _ := supervisor.NewSessionID("send-seq-1")
	now := time.Now().UTC()
	s := supervisor.ManagedSession{SchemaVersion: 1, SessionID: id, ProjectID: "send-seq", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionIdle, StateSince: now, StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 4, EverSent: true, LastSendConfirmed: true, LastConfirmedAt: &now}}
	if err := store.NewSessionStore(root).Save(s); err != nil {
		t.Fatal(err)
	}
	restarted := store.NewSessionStore(root)
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{}
	p.onWrite = func() {
		got, err := restarted.Load(id)
		if err != nil || got.SendGate.NextSeq != 5 || got.SendGate.Pending == nil || got.SendGate.Pending.SendID != "send-seq-1-s4" {
			t.Fatalf("durable issue=%+v err=%v", got.SendGate, err)
		}
		echoScreen(screen, "/rc")
	}
	u := SendInputUseCase{Sessions: restarted, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
	got, _ := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if got.SendID != "send-seq-1-s4" || p.calls != 1 {
		t.Fatalf("result=%+v calls=%d", got, p.calls)
	}
}

func TestInitialRemoteControlAfterPermission(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	count := 0
	r := InitialRemoteControl{Request: func(context.Context, SendInput) error { count++; return nil }}
	s := supervisor.ManagedSession{SessionID: id, Generation: 1, State: supervisor.SessionAwaitingPermission}
	_ = r.OnStateChange(context.Background(), supervisor.SessionStarting, s)
	s.State = supervisor.SessionIdle
	_ = r.OnStateChange(context.Background(), supervisor.SessionAwaitingPermission, s)
	if count != 1 {
		t.Fatalf("requests=%d", count)
	}
}

func TestSCNsup0215SCNsup0216SendIssuedBeforePTY(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	p := &sendTestPTY{}
	screen := &sendTestScreen{shot: blankScreen()}
	var events []store.Event
	p.onWrite = func() {
		if s.session.SendGate.Pending == nil || len(events) != 1 || events[0].Kind != supervisor.EventKindSendIssued {
			t.Fatal("PTY write preceded durable issue")
		}
	}
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), RecordStructured: func(e store.Event) error { events = append(events, e); return nil }, ConfirmTimeout: time.Millisecond}
	_, _ = u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if p.calls != 1 || len(events) < 1 || events[0].SendID != "alpha-1-s1" || s.session.SendGate.NextSeq != 2 {
		t.Fatalf("calls=%d events=%v gate=%+v", p.calls, events, s.session.SendGate)
	}
	encoded, _ := json.Marshal(events[0])
	if strings.Contains(string(encoded), "/rc") || strings.Contains(string(encoded), "text") {
		t.Fatalf("send event contains input text: %s", encoded)
	}
	if _, ok := reflect.TypeOf(SendInput{}).FieldByName("Text"); ok {
		t.Fatal("SendInput exposes arbitrary text")
	}
}

func TestSCNsup0201FirstRecommendationUsesFixedTextAndEnterDelay(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionAwaitingRecommendation, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	shot := blankScreen()
	shot.State = detect.StateAwaitingRecommendation
	screen := &sendTestScreen{shot: shot}
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, supervisor.RecommendationContinueText) }}
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
	result, _ := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRecommendationContinue})
	if result.SendID != "alpha-1-s1" || p.calls != 1 || p.wrote != supervisor.RecommendationContinueText || p.delay != supervisor.SupervisorSendEnterDelay || len(p.writes) != 2 || string(p.writes[0]) != supervisor.RecommendationContinueText || len(p.writes[1]) != 1 || p.writes[1][0] != '\r' {
		t.Fatalf("result=%+v calls=%d text=%q delay=%s", result, p.calls, p.wrote, p.delay)
	}
}

func TestSCNsup0202SCNsup0206Blocked(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	for _, tc := range []struct {
		name   string
		mutate func(*supervisor.ManagedSession)
		stop   bool
		reason supervisor.SupervisorSendBlockedReason
	}{
		{"stop", func(*supervisor.ManagedSession) {}, true, supervisor.SendBlockedStop},
		{"state", func(s *supervisor.ManagedSession) { s.State = supervisor.SessionStarting }, false, supervisor.SendBlockedState},
		{"pending", func(s *supervisor.ManagedSession) { s.SendGate.Pending = &supervisor.PendingSend{SendID: "alpha-1-s1"} }, false, supervisor.SendBlockedPending},
		{"unconfirmed", func(s *supervisor.ManagedSession) { s.SendGate.EverSent = true }, false, supervisor.SendBlockedUnconfirmed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
			tc.mutate(&s.session)
			p := &sendTestPTY{}
			var got store.Event
			u := SendInputUseCase{Sessions: s, Screens: &sendTestScreen{shot: blankScreen()}, PTY: p, IsStopped: func(supervisor.SessionID) bool { return tc.stop }, StopState: NewStopState(t.TempDir()), RecordStructured: func(e store.Event) error { got = e; return nil }}
			_, _ = u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
			if p.calls != 0 || got.Reason != store.EventReason(tc.reason) {
				t.Fatalf("calls=%d event=%+v", p.calls, got)
			}
		})
	}
}

func TestFix3SCNsup02017EveryRejectedStateRecordsBlock(t *testing.T) {
	rows := []struct {
		state    supervisor.SupervisorSessionState
		observed detect.DetectedState
		kind     supervisor.SupervisorSendKind
	}{
		{supervisor.SessionStopped, detect.StateIdle, supervisor.SendKindHandoffEnd},
		{supervisor.SessionStopped, detect.StateIdle, supervisor.SendKindRemoteControl},
		{supervisor.SessionRecovering, detect.StateIdle, supervisor.SendKindRemoteControl},
		{supervisor.SessionStarting, detect.StateIdle, supervisor.SendKindRemoteControl},
		{supervisor.SessionFailed, detect.StateIdle, supervisor.SendKindSessionStart},
		{supervisor.SessionExited, detect.StateIdle, supervisor.SendKindSessionStart},
		{supervisor.SessionAwaitingPermission, detect.StateIdle, supervisor.SendKindRecommendationContinue},
		{supervisor.SessionWorking, detect.StateIdle, supervisor.SendKindRecommendationContinue},
		{supervisor.SessionRotating, detect.StateIdle, supervisor.SendKindRecommendationContinue},
		{supervisor.SessionIdle, detect.StateWorking, supervisor.SendKindRecommendationContinue},
		{supervisor.SessionIdle, detect.StateAwaitingPermission, supervisor.SendKindRecommendationContinue},
		{supervisor.SessionIdle, detect.DetectedState(supervisor.SessionStarting), supervisor.SendKindSessionStart},
		{supervisor.SessionIdle, detect.DetectedState(supervisor.SessionRecovering), supervisor.SendKindRemoteControl},
		{supervisor.SessionAwaitingRecommendation, detect.StateAwaitingRecommendation, supervisor.SendKindSessionStart},
		{supervisor.SessionAwaitingRecommendation, detect.StateAwaitingRecommendation, supervisor.SendKindParallelInstruction},
		{supervisor.SessionAwaitingRecommendation, detect.StateAwaitingRecommendation, supervisor.SendKindHandoffExit},
	}
	for i, row := range rows {
		t.Run(fmt.Sprintf("row_%d", i), func(t *testing.T) {
			id, _ := supervisor.NewSessionID("alpha-1")
			s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: row.state, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
			shot := blankScreen()
			shot.State = row.observed
			pty := &sendTestPTY{}
			var events []store.Event
			u := SendInputUseCase{Sessions: s, Screens: &sendTestScreen{shot: shot}, PTY: pty, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), RecordStructured: func(e store.Event) error { events = append(events, e); return nil }}
			got, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: row.kind})
			if err != nil || got.Blocked != supervisor.SendBlockedState || pty.calls != 0 || len(events) != 1 || events[0].Kind != supervisor.EventKindSendBlocked || events[0].Reason != store.EventReason(supervisor.SendBlockedState) {
				t.Fatalf("result=%+v err=%v calls=%d events=%+v", got, err, pty.calls, events)
			}
		})
	}
}

func TestSCNsup0203ConfirmedPreviousSendAllowsNewID(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 2, EverSent: true, LastSendConfirmed: true}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
	result, _ := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if result.SendID != "alpha-1-s2" || p.calls != 1 || s.session.SendGate.LastSendConfirmed || s.session.SendGate.NextSeq != 3 {
		t.Fatalf("result=%+v gate=%+v", result, s.session.SendGate)
	}
}

func TestSCNsup0204TimeoutLeavesPendingAndDoesNotResend(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
	var events []store.Event
	var notified supervisor.SupervisorEventKind
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond, RecordStructured: func(e store.Event) error { events = append(events, e); return nil }, Notify: func(kind supervisor.SupervisorEventKind, _ string) { notified = kind }}
	_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err == nil || p.calls != 1 || s.session.State != supervisor.SessionStopped || s.session.StopReason != supervisor.StopSendNotConfirmed || s.session.SendGate.Pending == nil || s.session.SendGate.LastSendConfirmed || len(events) != 2 || events[1].Kind != supervisor.EventKindSendUnconfirmed || notified != supervisor.EventKindSendUnconfirmed {
		t.Fatalf("err=%v calls=%d session=%+v events=%v", err, p.calls, s.session, events)
	}
}

func TestSCNsup0205ConcurrentRequestDoesNotWaitForSendLock(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	entered := make(chan struct{})
	release := make(chan struct{})
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc"); close(entered); <-release }}
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
	done := make(chan struct{})
	go func() {
		_, _ = u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
		close(done)
	}()
	<-entered
	second, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err != nil || second.Blocked != supervisor.SendBlockedPending || p.calls != 1 {
		t.Fatalf("second=%+v err=%v calls=%d", second, err, p.calls)
	}
	close(release)
	<-done
}

func TestSCNsup0229DiscardedSendRequiresFreshQuietScreen(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	now := time.Now().UTC()
	for _, tc := range []struct {
		name              string
		observed, changed time.Time
		wantSend          bool
	}{
		{"stale", now.Add(-time.Second), now.Add(-10 * time.Second), false},
		{"not quiet", now.Add(time.Second), now, false},
		{"fresh quiet", now.Add(time.Second), now.Add(-10 * time.Second), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, StateSince: now, SendGate: supervisor.SendGateRecord{NextSeq: 2, EverSent: true, UnconfirmedDiscarded: true}}}
			shot := blankScreen()
			shot.ObservedAt = tc.observed
			shot.ChangedAt = tc.changed
			screen := &sendTestScreen{shot: shot}
			p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
			u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), Now: func() time.Time { return now }, ConfirmTimeout: time.Millisecond}
			_, _ = u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
			if (p.calls == 1) != tc.wantSend {
				t.Fatalf("calls=%d gate=%+v", p.calls, s.session.SendGate)
			}
		})
	}
}

func TestSCNsup0207SessionStopAffectsOnlyItsSession(t *testing.T) {
	a, _ := supervisor.NewSessionID("alpha-1")
	b, _ := supervisor.NewSessionID("beta-1")
	stop := NewStopState(t.TempDir())
	stop.SetSession(a, true)
	for _, tc := range []struct {
		id      supervisor.SessionID
		blocked bool
	}{{a, true}, {b, false}} {
		s := &sendTestStore{session: supervisor.ManagedSession{SessionID: tc.id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
		screen := &sendTestScreen{shot: blankScreen()}
		p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
		u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: stop.IsStopped, StopState: stop, ConfirmTimeout: time.Millisecond}
		_, _ = u.Execute(context.Background(), SendInput{SessionID: tc.id, Kind: supervisor.SendKindRemoteControl})
		if (p.calls == 0) != tc.blocked {
			t.Fatalf("id=%s calls=%d", tc.id, p.calls)
		}
	}
}

func TestSCNsup0208StopFileAppearsBeforeGateEvaluation(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	stop := NewStopState(t.TempDir())
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &screenWithHook{shot: blankScreen(), hook: func() {
		if err := stop.WriteSession(id); err != nil {
			t.Fatal(err)
		}
	}}
	p := &sendTestPTY{}
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: stop.IsStopped}
	result, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err != nil || result.Blocked != supervisor.SendBlockedStop || p.calls != 0 {
		t.Fatalf("result=%+v err=%v calls=%d", result, err, p.calls)
	}
}

func TestSCNsup0135SCNsup0239InitialRemoteControlOncePerGeneration(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	var sent []SendInput
	r := InitialRemoteControl{Request: func(_ context.Context, in SendInput) error { sent = append(sent, in); return nil }}
	base := supervisor.ManagedSession{SessionID: id, Generation: 1, State: supervisor.SessionIdle}
	if err := r.OnStateChange(context.Background(), supervisor.SessionStarting, base); err != nil {
		t.Fatal(err)
	}
	if err := r.OnStateChange(context.Background(), supervisor.SessionWorking, base); err != nil {
		t.Fatal(err)
	}
	base.Generation = 2
	if err := r.OnStateChange(context.Background(), supervisor.SessionStarting, base); err != nil {
		t.Fatal(err)
	}
	base.Generation = 3
	if err := r.OnStateChange(context.Background(), supervisor.SessionRotating, base); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 2 || sent[0].Kind != supervisor.SendKindRemoteControl || sent[1].Kind != supervisor.SendKindRemoteControl {
		t.Fatalf("requests=%v", sent)
	}
}

func TestSCNsup0222SCNsup0235ProcessExitWithPendingStopsWithoutResend(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{}
	p.onWrite = func() { echoScreen(screen, "/rc") }
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Second}
	p.afterEnter = func() {
		u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "alpha-1-s1", ProcessExited: true})
	}
	_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err == nil || s.session.State != supervisor.SessionStopped || s.session.StopReason != supervisor.StopProcessExitWithPending || s.session.SendGate.Pending == nil || p.calls != 1 {
		t.Fatalf("err=%v session=%+v calls=%d", err, s.session, p.calls)
	}
}

func TestSCNsup0223SCNsup0224ConfirmationRequiresPostCRScreen(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{}
	p.onWrite = func() { echoScreen(screen, "/rc") }
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Second}
	p.afterEnter = func() {
		u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "alpha-1-s1", Screen: blankScreen()})
		go func() {
			time.Sleep(time.Millisecond)
			shot := blankScreen()
			shot.ChangedAt = time.Now().Add(time.Second)
			u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "alpha-1-s1", Screen: shot})
		}()
	}
	result, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err != nil || !result.Confirmed || s.session.SendGate.Pending != nil || !s.session.SendGate.LastSendConfirmed {
		t.Fatalf("result=%+v err=%v gate=%+v", result, err, s.session.SendGate)
	}
}

func TestSCNsup0223CRRecheckRejectsPermissionState(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{}
	p.onWrite = func() { echoScreen(screen, "/rc"); screen.shot.State = detect.StateAwaitingPermission }
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir())}
	_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err == nil || s.session.State != supervisor.SessionStopped || s.session.StopReason != supervisor.StopSendAborted || s.session.SendGate.Pending != nil || s.session.SendGate.NextSeq != 2 {
		t.Fatalf("err=%v session=%+v", err, s.session)
	}
}

func TestSCNsup0233UnknownCompletionMenuAborts(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc"); screen.shot.Rows = append(screen.shot.Rows, "  ❯ /other") }}
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir())}
	_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err == nil || s.session.StopReason != supervisor.StopSendAborted || s.session.SendGate.Pending != nil {
		t.Fatalf("err=%v session=%+v", err, s.session)
	}
}

func TestSCNsup0232ExactSelectedCompletionAllowsSlashCommand(t *testing.T) {
	for _, command := range []string{"/rc", "/exit"} {
		t.Run(command, func(t *testing.T) {
			shot := blankScreen()
			screen := &sendTestScreen{shot: shot}
			echoScreen(screen, command)
			screen.shot.Rows = append(screen.shot.Rows, "  ❯ "+command)
			screen.shot.CompletionMenu = true
			screen.shot.SelectedCompletion = command
			if !screenReadyForEnter(screen.shot, command) {
				t.Fatalf("exact selected completion %q was rejected", command)
			}
		})
	}
}

func TestSCNsup0232CompletionCandidateMatching(t *testing.T) {
	for _, tc := range []struct {
		name, candidate, command string
		want                     bool
	}{
		{"alias", "/remote-control (rc) Example", "/rc", true},
		{"name", "/exit Example", "/exit", true},
		{"other", "/resume (continue) Example", "/rc", false},
		{"partial name", "/rcfoo Example", "/rc", false},
		{"description", "/other Example /rc", "/rc", false},
		{"unclosed alias", "/remote-control (rc Example", "/rc", false},
		{"invalid name", "remote-control (rc) Example", "/rc", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := completionCandidateMatches(tc.candidate, tc.command); got != tc.want {
				t.Fatalf("candidate=%q command=%q got=%v want=%v", tc.candidate, tc.command, got, tc.want)
			}
		})
	}
}

func TestSCNsup0232CompletionMenuSelection(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rows     []string
		menu     bool
		selected string
	}{
		{"candidate", []string{"  ❯ /remote-control (rc) Example", "    /other Example", "────", "❯\u00a0/rc", "────"}, true, "/remote-control (rc) Example"},
		{"history only", []string{"❯ /rc", "────", "❯\u00a0/rc", "────"}, false, ""},
		{"no input frame", []string{"  ❯ /rc Example", "❯\u00a0/rc"}, false, ""},
		{"permission", []string{"  ❯ /rc Example", "Enter to confirm", "────", "❯\u00a0/rc", "────"}, false, ""},
		{"trust", []string{"  ❯ Yes, I trust this project", "────", "❯\u00a0/rc", "────"}, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			menu, selected := CompletionSelection(tc.rows)
			if menu != tc.menu || selected != tc.selected {
				t.Fatalf("menu=%v selected=%q", menu, selected)
			}
		})
	}
}

func TestSCNsup0232AliasSelectionAllowsEnter(t *testing.T) {
	screen := &sendTestScreen{shot: blankScreen()}
	echoScreen(screen, "/rc")
	screen.shot.Rows = append([]string{"  ❯ /remote-control (rc) Example"}, screen.shot.Rows...)
	screen.shot.CompletionMenu, screen.shot.SelectedCompletion = CompletionSelection(screen.shot.Rows)
	if !screenReadyForEnter(screen.shot, "/rc") {
		t.Fatal("matching alias was rejected")
	}
}

func TestSCNsup0232AliasSelectionSendsCR(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	sessions := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{onWrite: func() {
		echoScreen(screen, "/rc")
		screen.shot.Rows = append([]string{"  ❯ /remote-control (rc) Example"}, screen.shot.Rows...)
		screen.shot.CompletionMenu, screen.shot.SelectedCompletion = CompletionSelection(screen.shot.Rows)
	}}
	u := &SendInputUseCase{Sessions: sessions, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Second}
	p.afterEnter = func() {
		shot := blankScreen()
		shot.ChangedAt = time.Now().Add(time.Second)
		u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "alpha-1-s1", Screen: shot})
	}
	result, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err != nil || !result.Confirmed || len(p.writes) != 2 || string(p.writes[1]) != "\r" {
		t.Fatalf("result=%+v err=%v writes=%q", result, err, p.writes)
	}
}

func TestSCNsup0232HistoryEchoDoesNotBlockEnter(t *testing.T) {
	screen := &sendTestScreen{shot: blankScreen()}
	echoScreen(screen, "/rc")
	screen.shot.Rows = append([]string{"❯ /rc"}, screen.shot.Rows...)
	screen.shot.CompletionMenu, screen.shot.SelectedCompletion = CompletionSelection(screen.shot.Rows)
	if !screenReadyForEnter(screen.shot, "/rc") {
		t.Fatal("history echo was treated as a selection menu")
	}
}

func TestSelectionMenuIndentAndColumnZero(t *testing.T) {
	for _, tc := range []struct {
		name string
		row  string
		menu bool
	}{
		{"one space", " ❯ 1. Yes", true},
		{"two spaces", "  ❯ 1. Yes", true},
		{"three spaces", "   ❯ 1. Yes", true},
		{"history echo", "❯ 1. Yes", false},
		{"input row", "❯\u00a0/rc", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasSelectionMenu([]string{tc.row}); got != tc.menu {
				t.Fatalf("hasSelectionMenu(%q)=%v, want %v", tc.row, got, tc.menu)
			}
			screen := &sendTestScreen{shot: blankScreen()}
			echoScreen(screen, "/rc")
			screen.shot.Rows = append([]string{tc.row}, screen.shot.Rows...)
			if got := screenReadyForEnter(screen.shot, "/rc"); got == tc.menu {
				t.Fatalf("screenReadyForEnter(%q)=%v", tc.row, got)
			}
		})
	}
}

func TestSCNsup0233CompletionExceptionOnlyForSlashCommands(t *testing.T) {
	for _, command := range []string{"#end", "#start", "continue with the recommendation", "parallel task"} {
		screen := &sendTestScreen{shot: blankScreen()}
		echoScreen(screen, command)
		screen.shot.Rows = append([]string{"  ❯ /exit Example"}, screen.shot.Rows...)
		screen.shot.CompletionMenu, screen.shot.SelectedCompletion = CompletionSelection(screen.shot.Rows)
		if screenReadyForEnter(screen.shot, command) {
			t.Fatalf("non-slash command %q accepted a selection menu", command)
		}
	}
}

func TestSCNsup0233WrongOrUnreadableCompletionAborts(t *testing.T) {
	for _, candidate := range []string{"/other", ""} {
		shot := blankScreen()
		screen := &sendTestScreen{shot: shot}
		echoScreen(screen, "/rc")
		screen.shot.Rows = append(screen.shot.Rows, "  ❯ /other")
		screen.shot.CompletionMenu = true
		screen.shot.SelectedCompletion = candidate
		if screenReadyForEnter(screen.shot, "/rc") {
			t.Fatalf("accepted completion %q", candidate)
		}
	}
}

func TestSCNsup0223WorkingStateAloneDoesNotConfirm(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{}
	p.onWrite = func() { echoScreen(screen, "#end") }
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: 10 * time.Millisecond}
	p.afterEnter = func() {
		shot := screen.shot
		shot.State = detect.StateWorking
		shot.ChangedAt = time.Now().Add(time.Second)
		u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "alpha-1-s1", Screen: shot})
	}
	result, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindHandoffEnd})
	if err == nil || result.Confirmed || s.session.StopReason != supervisor.StopSendNotConfirmed {
		t.Fatalf("result=%+v err=%v gate=%+v", result, err, s.session.SendGate)
	}
}

func TestSCNsup0224HandoffExitRequiresProcessExit(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{}
	p.onWrite = func() { echoScreen(screen, "/exit") }
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: 10 * time.Millisecond}
	p.afterEnter = func() {
		shot := blankScreen()
		shot.ObservedAt = time.Now().Add(time.Second)
		u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "alpha-1-s1", Screen: shot})
	}
	result, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindHandoffExit})
	if err == nil || result.Confirmed || s.session.StopReason != supervisor.StopSendNotConfirmed {
		t.Fatalf("result=%+v err=%v gate=%+v", result, err, s.session.SendGate)
	}
}

func TestFix3HandoffExitRequiresExitAfterCR(t *testing.T) {
	for _, after := range []bool{false, true} {
		t.Run(fmt.Sprint(after), func(t *testing.T) {
			id, _ := supervisor.NewSessionID("alpha-1")
			s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
			screen := &sendTestScreen{shot: blankScreen()}
			pty := &sendTestPTY{onWrite: func() { echoScreen(screen, "/exit") }}
			u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: pty, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Millisecond}
			pty.afterEnter = func() {
				at := time.Now().Add(-time.Second)
				if after {
					at = time.Now().Add(time.Second)
				}
				u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "alpha-1-s1", ProcessExited: true, ExitedAt: at})
			}
			got, _ := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindHandoffExit})
			if got.Confirmed != after {
				t.Fatalf("confirmed=%v after=%v", got.Confirmed, after)
			}
		})
	}
}

func TestFix3PreCRAbortPreservesExistingStopReason(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	pty := &sendTestPTY{onWrite: func() {
		echoScreen(screen, "/rc")
		s.session.State = supervisor.SessionStopped
		s.session.StopReason = supervisor.StopGlobal
	}}
	var events []store.Event
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: pty, IsStopped: func(supervisor.SessionID) bool { return s.session.State == supervisor.SessionStopped }, StopState: NewStopState(t.TempDir()), RecordStructured: func(e store.Event) error { events = append(events, e); return nil }}
	_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if !errors.Is(err, proc.ErrBeforeEnter) || s.session.StopReason != supervisor.StopGlobal || s.session.SendGate.Pending != nil || pty.calls != 1 || len(events) != 2 || events[1].Kind != supervisor.EventKindSendAborted {
		t.Fatalf("err=%v session=%+v calls=%d events=%+v", err, s.session, pty.calls, events)
	}
}

func TestFix3TextWriteFailureIsAbortNotUnconfirmed(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	writeErr := errors.New("text write failed")
	var events []store.Event
	u := SendInputUseCase{Sessions: s, Screens: &sendTestScreen{shot: blankScreen()}, PTY: failingBeforeCRPTY{writeErr}, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), RecordStructured: func(e store.Event) error { events = append(events, e); return nil }}
	_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if !errors.Is(err, writeErr) || s.session.StopReason != supervisor.StopSendAborted || len(events) != 2 || events[1].Kind != supervisor.EventKindSendAborted {
		t.Fatalf("err=%v session=%+v events=%+v", err, s.session, events)
	}
}

func TestFix3WriteLineArgumentFailureBlocksBeforeText(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	var events []store.Event
	u := SendInputUseCase{Sessions: s, Screens: &sendTestScreen{shot: blankScreen()}, PTY: failingBeforeCRPTY{errors.New("line text must not contain CR or LF")}, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), RecordStructured: func(e store.Event) error { events = append(events, e); return nil }}
	_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err == nil || s.session.State != supervisor.SessionIdle || s.session.SendGate.Pending != nil || len(events) != 2 || events[1].Kind != supervisor.EventKindSendBlocked {
		t.Fatalf("err=%v session=%+v events=%+v", err, s.session, events)
	}
}

func TestSCNsup0231ProcessingPatternConfirmsSend(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "#end") }}
	u := &SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), ConfirmTimeout: time.Second}
	p.afterEnter = func() {
		shot := screen.shot
		shot.State = detect.StateWorking
		shot.Processing = true
		shot.ChangedAt = time.Now().Add(time.Second)
		u.NotifyConfirmation(SendConfirmation{SessionID: id, SendID: "alpha-1-s1", Screen: shot})
	}
	result, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindHandoffEnd})
	if err != nil || !result.Confirmed || !s.session.SendGate.LastSendConfirmed || s.session.SendGate.Pending != nil {
		t.Fatalf("result=%+v err=%v gate=%+v", result, err, s.session.SendGate)
	}
}

func TestSCNsup0234AbortRestoresPreviousConfirmationButNotSequence(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 2, EverSent: true, LastSendConfirmed: true}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }}
	var notified supervisor.SupervisorEventKind
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return p.calls > 0 }, StopState: NewStopState(t.TempDir()), Notify: func(kind supervisor.SupervisorEventKind, _ string) { notified = kind }}
	_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err == nil || s.session.SendGate.Pending != nil || !s.session.SendGate.LastSendConfirmed || s.session.SendGate.NextSeq != 3 || s.session.StopReason != supervisor.StopSendAborted || notified != supervisor.EventKindSendAborted {
		t.Fatalf("err=%v session=%+v", err, s.session)
	}
}

func TestSCNsup0222AmbiguousPTYErrorKeepsPending(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	screen := &sendTestScreen{shot: blankScreen()}
	p := &sendTestPTY{onWrite: func() { echoScreen(screen, "/rc") }, writeError: errors.New("write failed")}
	u := SendInputUseCase{Sessions: s, Screens: screen, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir())}
	_, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: supervisor.SendKindRemoteControl})
	if err == nil || s.session.SendGate.Pending == nil || s.session.StopReason != supervisor.StopSendNotConfirmed {
		t.Fatalf("err=%v gate=%+v reason=%s", err, s.session.SendGate, s.session.StopReason)
	}
}

func TestSCNsup0219SendInputRejectsArbitraryKindAndHasNoTextField(t *testing.T) {
	typ := reflect.TypeOf(SendInput{})
	if typ.NumField() != 4 || typ.Field(0).Type != reflect.TypeOf(supervisor.SessionID("")) || typ.Field(1).Type != reflect.TypeOf(supervisor.SupervisorSendKind("")) || typ.Field(2).Type != reflect.TypeOf(supervisor.SendOriginNormal) || typ.Field(3).Type != reflect.TypeOf(time.Duration(0)) {
		t.Fatalf("unexpected input type: %v", typ)
	}
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	p := &sendTestPTY{}
	var logged supervisor.SupervisorSendBlockedReason
	u := SendInputUseCase{Sessions: s, Screens: &sendTestScreen{shot: blankScreen()}, PTY: p, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), LogBlocked: func(_ supervisor.SessionID, r supervisor.SupervisorSendBlockedReason) { logged = r }}
	result, err := u.Execute(context.Background(), SendInput{SessionID: id, Kind: "arbitrary"})
	if err != nil || result.Blocked != supervisor.SendBlockedNotFixedText || logged != supervisor.SendBlockedNotFixedText || p.calls != 0 {
		t.Fatalf("result=%+v err=%v logged=%s calls=%d", result, err, logged, p.calls)
	}
}

func TestSCNsup0219NotFixedTextWritesOperationalLog(t *testing.T) {
	id, _ := supervisor.NewSessionID("alpha-1")
	s := &sendTestStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionIdle, SendGate: supervisor.SendGateRecord{NextSeq: 1}}}
	root := t.TempDir()
	log, err := store.NewSupervisorLog(root, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	u := SendInputUseCase{Sessions: s, Screens: &sendTestScreen{shot: blankScreen()}, PTY: &sendTestPTY{}, IsStopped: func(supervisor.SessionID) bool { return false }, StopState: NewStopState(t.TempDir()), OperationalLog: log}
	_, err = u.Execute(context.Background(), SendInput{SessionID: id, Kind: "arbitrary"})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "codexd-supervisor.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "send_blocked") || !strings.Contains(string(data), id.String()) {
		t.Fatalf("missing send block log: %s", data)
	}
}

func TestSCNsup0219OnlySendInputUseCaseWritesPTY(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(file))))
	writes := regexp.MustCompile(`\b([A-Za-z_][A-Za-z_0-9.]*)\.Write\(`)
	nonPTYWriter := map[string]bool{"w.file": true, "tmp": true, "f": true, "out": true, "w.buffer": true, "b.Buffer": true, "vt": true, "h": true, "file": true}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if path == filepath.Join(filepath.Dir(file), "send_input.go") || path == filepath.Join(root, "internal", "proc", "ptysession.go") {
			return nil
		}
		if strings.Contains(string(data), ".WriteLine(") || strings.Contains(string(data), ".WriteEnter(") {
			t.Errorf("PTY write outside SendInputUseCase: %s", path)
		}
		for _, match := range writes.FindAllStringSubmatch(string(data), -1) {
			if !nonPTYWriter[match[1]] {
				t.Errorf("unreviewed Write caller %q: %s", match[1], path)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
