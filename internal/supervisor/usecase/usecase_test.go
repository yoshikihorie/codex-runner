package usecase

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

func TestStopStateFailClosedAndRecreatesRemovedFile(t *testing.T) {
	root := t.TempDir()
	state := NewStopState(root)
	if err := os.Mkdir(filepath.Join(root, "STOP"), 0700); err != nil {
		t.Fatal(err)
	}
	if !state.IsStopped("") {
		t.Fatal("unreadable STOP path must stop")
	}
	if err := os.Remove(filepath.Join(root, "STOP")); err != nil {
		t.Fatal(err)
	}
	state.SetGlobal(true)
	if err := state.CheckAndRepair(""); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "STOP")); err != nil {
		t.Fatal(err)
	}
}

func TestSCNsup0501_GlobalStopSetsMemoryAndFile(t *testing.T) {
	s := NewStopState(t.TempDir())
	s.SetGlobal(true)
	if err := s.WriteGlobal(); err != nil {
		t.Fatal(err)
	}
	if err := s.Poll(""); err != nil {
		t.Fatal(err)
	}
	if !s.IsStopped("") {
		t.Fatal("global stop missing")
	}
}
func TestSCNsup0502_ManualStopFileIsDetected(t *testing.T) {
	r := t.TempDir()
	s := NewStopState(r)
	if err := os.WriteFile(filepath.Join(r, "STOP"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if !s.IsStopped("") {
		t.Fatal("manual stop missing")
	}
}
func TestSCNsup0503_SessionStopIsScoped(t *testing.T) {
	s := NewStopState(t.TempDir())
	id := supervisor.SessionID("alpha-1")
	s.SetSession(id, true)
	if !s.IsStopped(id) || s.IsStopped("beta-1") {
		t.Fatal("session stop scope mismatch")
	}
}
func TestSCNsup0504_StopStateSupportsObservationAndStartGuard(t *testing.T) {
	s := NewStopState(t.TempDir())
	s.SetGlobal(true)
	if !s.IsStopped("alpha-1") {
		t.Fatal("stop should stay observable")
	}
	if ErrStopActive.Error() != "SUP_STOP_ACTIVE" {
		t.Fatalf("wrong start guard error: %v", ErrStopActive)
	}
}
func TestSCNsup0505_GlobalResumeOnlyResumesStopReason(t *testing.T) {
	TestStopControlResumePolicyAndGuard(t)
}
func TestSCNsup0506_SessionResumeBlockedByGlobalStop(t *testing.T) {
	s := NewStopState(t.TempDir())
	store := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: "alpha-1", State: supervisor.SessionStopped, StopScope: supervisor.StopScopeSession, StopReason: supervisor.StopSession, ClaudeSessionID: "claude-alpha"}}}
	if err := s.EnableGlobal(); err != nil {
		t.Fatal(err)
	}
	uc := NewStopControlUseCase(s, store, nil, nil)
	out, err := uc.Resume(context.Background(), ptrSession("ghost-9"), true, false)
	if err == nil || err.Error() != "SUP_SESSION_NOT_FOUND" {
		t.Fatalf("unknown session under global STOP: %v", err)
	}
	out, err = uc.Resume(context.Background(), ptrSession("alpha-1"), true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.StillStopped) != 1 || out.StillStopped[0].Reason != "still_stopped_by_global" {
		t.Fatalf("result = %+v", out)
	}
}
func TestSCNsup0507_ResumeDoesNotSend(t *testing.T) {
	pending := &supervisor.PendingSend{SendID: "send-1", Kind: supervisor.SendKindSessionStart}
	store := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: "alpha-1", State: supervisor.SessionStopped, StopScope: supervisor.StopScopeSession, StopReason: supervisor.StopSession, ClaudeSessionID: "claude-alpha", SendGate: supervisor.SendGateRecord{Pending: pending}}}}
	stops := NewStopState(t.TempDir())
	_ = stops.EnableSession("alpha-1")
	events := &stopEventCapture{}
	uc := NewStopControlUseCase(stops, store, events, nil)
	continued := false
	uc.SetResumeContinuations(func(context.Context, supervisor.ManagedSession) error { continued = true; return nil }, nil)
	if _, err := uc.Resume(context.Background(), ptrSession("alpha-1"), true, false); err != nil {
		t.Fatal(err)
	}
	if continued || store.sessions[0].SendGate.Pending != nil || !store.sessions[0].SendGate.UnconfirmedDiscarded {
		t.Fatalf("continued=%v gate=%+v", continued, store.sessions[0].SendGate)
	}
	for _, kind := range events.kinds {
		if kind == supervisor.EventKindSendPendingDiscarded {
			return
		}
	}
	t.Fatal("send_pending_discarded event missing")
}
func TestSCNsup0508_ResumeRechecksStopFile(t *testing.T) {
	r := t.TempDir()
	s := NewStopState(r)
	s.SetSession("alpha-1", true)
	if err := s.WriteSession("alpha-1"); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(r, "STOP.alpha-1")); err != nil {
		t.Fatal(err)
	}
	store := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: "alpha-1", State: supervisor.SessionStopped, StopScope: supervisor.StopScopeSession, StopReason: supervisor.StopSession, ClaudeSessionID: "claude-alpha"}}}
	uc := NewStopControlUseCase(s, store, nil, nil)
	if err := os.WriteFile(filepath.Join(r, "STOP.alpha-1"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	// Recreate after resume's clear step via the context provider, immediately before Apply.
	uc.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
		if err := os.WriteFile(filepath.Join(r, "STOP.alpha-1"), nil, 0600); err != nil {
			return supervisor.TransitionContext{}, err
		}
		return supervisor.TransitionContext{ClaudeSessionID: true}, nil
	})
	out, err := uc.Resume(context.Background(), ptrSession("alpha-1"), true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.StillStopped) != 1 || out.StillStopped[0].Reason != "stop_recreated" || store.sessions[0].State != supervisor.SessionStopped {
		t.Fatalf("out=%+v state=%s", out, store.sessions[0].State)
	}
}
func TestSCNsup0536_StopWorksWhenEventRecorderUnavailable(t *testing.T) {
	s := NewStopState(t.TempDir())
	s.SetGlobal(true)
	if !s.IsStopped("") {
		t.Fatal("event availability affected STOP")
	}
}
func TestSCNsup0539_StopIsIdempotent(t *testing.T) {
	store := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: "alpha-1", State: supervisor.SessionIdle}}}
	events, notifier := &stopEventCapture{}, &stopNotifyCapture{}
	uc := NewStopControlUseCase(NewStopState(t.TempDir()), store, events, notifier)
	for range 3 {
		if _, err := uc.Stop(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
	}
	count := 0
	for _, kind := range events.kinds {
		if kind == supervisor.EventKindSessionStopped {
			count++
		}
	}
	if count != 1 || len(notifier.kinds) != 1 {
		t.Fatalf("session_stopped events=%d notifications=%d", count, len(notifier.kinds))
	}
}
func TestSCNsup0540_UnknownSessionIsRejected(t *testing.T) {
	uc := NewStopControlUseCase(NewStopState(t.TempDir()), &memorySessions{}, nil, nil)
	if _, err := uc.Stop(context.Background(), ptrSession("missing-1")); err == nil {
		t.Fatal("unknown session accepted")
	}
}
func TestSCNsup0543_RemovedStopIsRecordedAndRecreated(t *testing.T) {
	r := t.TempDir()
	s := NewStopState(r)
	s.SetGlobal(true)
	if err := s.WriteGlobal(); err != nil {
		t.Fatal(err)
	}
	events, notifier := &stopEventCapture{}, &stopNotifyCapture{}
	_ = NewStopControlUseCase(s, &memorySessions{}, events, notifier)
	if err := os.Remove(filepath.Join(r, "STOP")); err != nil {
		t.Fatal(err)
	}
	if err := s.Poll(""); err != nil {
		t.Fatal(err)
	}
	if len(events.kinds) != 1 || events.kinds[0] != supervisor.EventKindStopFileRemoved || len(events.ids) != 1 || events.ids[0] != "" || len(notifier.kinds) != 1 || len(notifier.ids) != 1 || notifier.ids[0] != "" {
		t.Fatal("removal not recorded and notified")
	}
	if _, err := os.Stat(filepath.Join(r, "STOP")); err != nil {
		t.Fatal(err)
	}
}

type stopEventCapture struct {
	kinds []supervisor.SupervisorEventKind
	ids   []supervisor.SessionID
}

func (r *stopEventCapture) RecordEvent(id supervisor.SessionID, k supervisor.SupervisorEventKind, _ supervisor.SupervisorStopReason, _ supervisor.SupervisorExitReason, _ supervisor.SupervisorRotateFailReason, _ time.Time) {
	r.kinds = append(r.kinds, k)
	r.ids = append(r.ids, id)
}

type stopNotifyCapture struct {
	kinds []supervisor.SupervisorEventKind
	ids   []string
}

func (r *stopNotifyCapture) Notify(_ context.Context, k supervisor.SupervisorEventKind, id string) error {
	r.kinds = append(r.kinds, k)
	r.ids = append(r.ids, id)
	return nil
}
func TestSCNsup0544_ResumeRequiresTTYAndUnmanagedCaller(t *testing.T) {
	TestStopControlResumePolicyAndGuard(t)
}
func TestSCNsup0546_ResumeContinuationsAreCalled(t *testing.T) {
	store := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: "alpha-1", State: supervisor.SessionStopped, StopScope: supervisor.StopScopeSession, StopReason: supervisor.StopSession, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateWaitIdle}}}}
	stops := NewStopState(t.TempDir())
	stops.SetSession("alpha-1", true)
	_ = stops.WriteSession("alpha-1")
	uc := NewStopControlUseCase(stops, store, nil, nil)
	called := false
	uc.SetResumeContinuations(func(context.Context, supervisor.ManagedSession) error { called = true; return nil }, nil)
	_, err := uc.Resume(context.Background(), ptrSession("alpha-1"), true, false)
	if err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("resume continuation not called")
	}
}
func TestSCNsup0546_ResumeClearsPushRetrySuppression(t *testing.T) {
	store := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: "alpha-1", State: supervisor.SessionStopped, StopScope: supervisor.StopScopeSession, StopReason: supervisor.StopSession, ClaudeSessionID: "claude-alpha"}}}
	stops := NewStopState(t.TempDir())
	stops.SetSession("alpha-1", true)
	_ = stops.WriteSession("alpha-1")
	uc := NewStopControlUseCase(stops, store, nil, nil)
	cleared := false
	uc.SetResumeSuppression(suppressionCapture{clear: func() { cleared = true }})
	if _, err := uc.Resume(context.Background(), ptrSession("alpha-1"), true, false); err != nil {
		t.Fatal(err)
	}
	if !cleared {
		t.Fatal("suppression not cleared")
	}
}

type suppressionCapture struct{ clear func() }

func (s suppressionCapture) Clear(context.Context, supervisor.SessionID) error { s.clear(); return nil }
func TestSCNsup0550_HandoffContinuationPrecedesRecovery(t *testing.T) {
	store := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: "alpha-1", State: supervisor.SessionStopped, StopScope: supervisor.StopScopeSession, StopReason: supervisor.StopSession, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateWaitIdle}}}}
	stops := NewStopState(t.TempDir())
	stops.SetSession("alpha-1", true)
	_ = stops.WriteSession("alpha-1")
	uc := NewStopControlUseCase(stops, store, nil, nil)
	rotation, recovery := false, false
	uc.SetResumeContinuations(func(context.Context, supervisor.ManagedSession) error { rotation = true; return nil }, func(context.Context, supervisor.ManagedSession) error { recovery = true; return nil })
	if _, err := uc.Resume(context.Background(), ptrSession("alpha-1"), true, false); err != nil {
		t.Fatal(err)
	}
	if !rotation || recovery {
		t.Fatalf("continuations rotation=%v recovery=%v", rotation, recovery)
	}
}
func TestSCNsup0555_StoppedReasonIsPreservedByStop(t *testing.T) {
	store := &memorySessions{sessions: []supervisor.ManagedSession{{SessionID: "alpha-1", State: supervisor.SessionStopped, StopReason: supervisor.StopRespawnFailed}}}
	uc := NewStopControlUseCase(NewStopState(t.TempDir()), store, nil, nil)
	if _, err := uc.Stop(context.Background(), ptrSession("alpha-1")); err != nil {
		t.Fatal(err)
	}
	if store.sessions[0].StopReason != supervisor.StopRespawnFailed {
		t.Fatalf("reason overwritten: %s", store.sessions[0].StopReason)
	}
}
func TestSCNsup0556_NotifyCommandAllowsCurrentOwner(t *testing.T) {
	p := filepath.Join(t.TempDir(), "notify")
	if err := os.WriteFile(p, []byte("x"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := proc.ValidateExecutable(p); err != nil {
		t.Fatal(err)
	}
}

func TestNotifyRejectsStickyParent(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 01777); err != nil {
		t.Fatal(err)
	}
	command := filepath.Join(dir, "notify")
	if err := os.WriteFile(command, []byte("x"), 0700); err != nil {
		t.Fatal(err)
	}
	_, err := proc.ValidateExecutable(command)
	var validationErr *proc.ExecutableValidationError
	if !errors.As(err, &validationErr) || validationErr.Reason != "parent_writable_by_others" {
		t.Fatalf("error=%v, want parent_writable_by_others", err)
	}
}

func TestNotifyFailureRecordsExecutableValidationReason(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 01777); err != nil {
		t.Fatal(err)
	}
	command := filepath.Join(dir, "notify")
	if err := os.WriteFile(command, []byte("x"), 0700); err != nil {
		t.Fatal(err)
	}
	recorder := &fakeRecorder{}
	notify := NewNotifyUseCase(command, &fakeRunner{}, recorder, time.Now)
	if err := notify.Notify(context.Background(), supervisor.EventKindSessionStopped, "alpha-1"); err != nil {
		t.Fatal(err)
	}
	notify.Close()
	if len(recorder.events) != 1 || recorder.events[0].Kind != supervisor.EventKindNotifyFailed || recorder.events[0].Reason != "parent_writable_by_others" {
		t.Fatalf("notification failure record=%+v", recorder.events)
	}
}

func TestStopUnknownSessionHasNoSideEffects(t *testing.T) {
	root := t.TempDir()
	stops := NewStopState(root)
	events := &stopEventCapture{}
	uc := NewStopControlUseCase(stops, &memorySessions{}, events, nil)
	if _, err := uc.Stop(context.Background(), ptrSession("ghost-9")); err == nil {
		t.Fatal("unknown session accepted")
	}
	if stops.IsStopped("ghost-9") || len(events.kinds) != 0 {
		t.Fatalf("side effects: stopped=%v events=%v", stops.IsStopped("ghost-9"), events.kinds)
	}
	if _, err := os.Lstat(filepath.Join(root, "STOP.ghost-9")); !os.IsNotExist(err) {
		t.Fatalf("marker created: %v", err)
	}
}

func TestResumeUsesTransitionAndRejectsMissingClaudeID(t *testing.T) {
	session := supervisor.ManagedSession{SessionID: "alpha-1", State: supervisor.SessionStopped, StopScope: supervisor.StopScopeSession, StopReason: supervisor.StopSession, PreviousState: supervisor.SessionWorking}
	store := &memorySessions{sessions: []supervisor.ManagedSession{session}}
	stops := NewStopState(t.TempDir())
	_ = stops.EnableSession(session.SessionID)
	uc := NewStopControlUseCase(stops, store, nil, nil)
	called := false
	uc.SetResumeContinuations(nil, func(context.Context, supervisor.ManagedSession) error { called = true; return nil })
	out, err := uc.Resume(context.Background(), ptrSession(session.SessionID), true, false)
	if err != nil {
		t.Fatal(err)
	}
	if store.sessions[0].State != supervisor.SessionStopped || called || len(out.StillStopped) != 1 || out.StillStopped[0].Reason != "claude_session_id_missing" {
		t.Fatalf("out=%+v state=%s called=%v", out, store.sessions[0].State, called)
	}
}

func TestNotifyAfterCloseDoesNotPanic(t *testing.T) {
	recorder := &fakeRecorder{}
	n := NewNotifyUseCase("none", nil, recorder, time.Now)
	n.Close()
	if err := n.Notify(context.Background(), supervisor.EventKindSessionStopped, "alpha-1"); err != nil {
		t.Fatal(err)
	}
}

func TestNotifyCommandRejectsWritableParent(t *testing.T) {
	d := t.TempDir()
	if err := os.Chmod(d, 0777); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(d, "notify")
	if err := os.WriteFile(p, []byte("x"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := proc.ValidateExecutable(p); err == nil {
		t.Fatal("world-writable command directory accepted")
	}
}

func TestSCNsup0536_EventWriteFailureIsLogged(t *testing.T) {
	log := &captureOperationalLog{}
	recorder := EventStoreRecorder{Log: log}
	recorder.RecordEvent("", supervisor.EventKindStopSet, supervisor.StopGlobal, "", "", time.Now())
	if len(log.entries) != 1 || log.entries[0].Kind != supervisor.EventKindStopSet || log.entries[0].Reason != store.LogReasonEventWriteFailed {
		t.Fatalf("logs=%v", log.entries)
	}
}

type captureOperationalLog struct{ entries []store.SupervisorLogEntry }

func (l *captureOperationalLog) Record(entry store.SupervisorLogEntry) error {
	l.entries = append(l.entries, entry)
	return nil
}
func TestSCNsup0512_NotifyCommandIsRecheckedAfterSymlinkResolution(t *testing.T) {
	d := t.TempDir()
	target := filepath.Join(d, "unsafe")
	if err := os.WriteFile(target, []byte("x"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(target, 0777); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(d, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := proc.ValidateExecutable(link); err == nil {
		t.Fatal("unsafe resolved executable accepted")
	}
}
func TestSCNsup0509_NotificationTargetsAreSelected(t *testing.T) {
	if !notificationTarget(supervisor.EventKindSessionStopped) || !notificationTarget(supervisor.EventKindRotateIdleWaitTimeout) || notificationTarget("unknown") {
		t.Fatal("notification target selection mismatch")
	}
}

func TestStopStateRepairsPersistedScopesOnStartup(t *testing.T) {
	root := t.TempDir()
	state := NewStopState(root)
	if err := state.RepairFromSessions([]supervisor.ManagedSession{{SessionID: "alpha-1", StopScope: supervisor.StopScopeGlobal}, {SessionID: "beta-1", StopScope: supervisor.StopScopeSession}}); err != nil {
		t.Fatal(err)
	}
	for _, marker := range []string{"STOP", "STOP.beta-1"} {
		if _, err := os.Lstat(filepath.Join(root, marker)); err != nil {
			t.Fatalf("missing repaired marker %s: %v", marker, err)
		}
	}
	if !state.IsGlobalStopped() || !state.IsSessionStopped("beta-1") {
		t.Fatal("persisted stop scopes were not restored")
	}
}

func TestClearSessionRejectsPathLikeID(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "sentinel")
	if err := os.WriteFile(outside, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	state := NewStopState(filepath.Join(root, "state"))
	if err := state.ClearSession("../sentinel"); err == nil {
		t.Fatal("path-like session ID accepted")
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("outside file was touched: %v", err)
	}
}

func TestStopMarkerUsesLstatAndNoFollowWrite(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	marker := filepath.Join(root, "STOP.alpha-1")
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, marker); err != nil {
		t.Fatal(err)
	}
	state := NewStopState(root)
	if !state.IsSessionStopped("alpha-1") {
		t.Fatal("symlink marker was not treated as active STOP")
	}
	if err := state.WriteSession("alpha-1"); err == nil {
		t.Fatal("O_NOFOLLOW write accepted symlink marker")
	}
}
func TestSCNsup0510_NotificationUsesOnlyKindAndSessionID(t *testing.T) {
	r := &fakeRunner{}
	n := NewNotifyUseCase("/bin/echo", r, nil, time.Now)
	_ = n.Notify(context.Background(), supervisor.EventKindSessionStopped, "alpha-1")
	n.Close()
	if len(r.calls) != 1 || len(r.calls[0]) != 2 || r.calls[0][0] != string(supervisor.EventKindSessionStopped) || r.calls[0][1] != "alpha-1" || len(r.paths) != 1 || r.paths[0] != "/bin/echo" {
		t.Fatalf("unexpected args: %#v", r.calls)
	}
	for _, entry := range r.envs[0] {
		if strings.HasPrefix(entry, "HOME=") || strings.Contains(strings.ToUpper(entry), "PROXY=") {
			t.Fatalf("unsafe notification env: %v", r.envs[0])
		}
	}
}
func TestSCNsup0511_NotificationFailureDoesNotPropagate(t *testing.T) {
	r := &fakeRunner{err: errors.New("failure")}
	n := NewNotifyUseCase("/usr/bin/osascript", r, nil, time.Now)
	if err := n.Notify(context.Background(), supervisor.EventKindSessionStopped, "alpha-1"); err != nil {
		t.Fatal(err)
	}
	n.Close()
}
func TestSCNsup0513_NotificationDeduplicates(t *testing.T) {
	TestNotifyDeduplicatesAndValidatesArguments(t)
}
func TestSCNsup0514_NotificationQueueOverflowIsRecorded(t *testing.T) {
	TestNotifyQueueOverflowRecorded(t)
}
func TestSCNsup0515_NotificationDefaultAndNoneModes(t *testing.T) {
	runner := &fakeRunner{}
	n := NewNotifyUseCase("none", runner, nil, time.Now)
	_ = n.Notify(context.Background(), supervisor.EventKindSessionStopped, "alpha-1")
	n.Close()
	if len(runner.calls) != 0 {
		t.Fatalf("none mode started command: %#v", runner.calls)
	}
	osascriptRunner := &fakeRunner{}
	osascript := NewNotifyUseCase("/usr/bin/osascript", osascriptRunner, nil, time.Now)
	_ = osascript.Notify(context.Background(), supervisor.EventKindSessionStopped, "alpha-1")
	osascript.Close()
	if len(osascriptRunner.calls) != 1 || osascriptRunner.paths[0] != "/usr/bin/osascript" || len(osascriptRunner.calls[0]) != 2 || osascriptRunner.calls[0][0] != "-e" || !strings.Contains(osascriptRunner.calls[0][1], "session_stopped alpha-1") {
		t.Fatalf("unexpected osascript launch: paths=%v args=%v", osascriptRunner.paths, osascriptRunner.calls)
	}
	recorder := &fakeRecorder{}
	invalid := NewNotifyUseCase("none", nil, recorder, time.Now)
	if err := invalid.Notify(context.Background(), supervisor.EventKindSessionStopped, "bad id"); err == nil {
		t.Fatal("invalid notification id accepted")
	}
	invalid.Close()
	if len(recorder.kinds) != 1 || recorder.kinds[0] != supervisor.EventKindNotifyFailed {
		t.Fatalf("invalid ID failure not recorded: %v", recorder.kinds)
	}
}

func ptrSession(id supervisor.SessionID) *supervisor.SessionID { return &id }

type fakeRunner struct {
	calls [][]string
	paths []string
	envs  [][]string
	err   error
}

func (r *fakeRunner) Run(_ context.Context, path string, args []string, env []string) error {
	r.calls = append(r.calls, append([]string(nil), args...))
	r.paths = append(r.paths, path)
	r.envs = append(r.envs, append([]string(nil), env...))
	return r.err
}

type fakeRecorder struct {
	kinds  []supervisor.SupervisorEventKind
	events []EventRecord
}

func (r *fakeRecorder) RecordEvent(e EventRecord) {
	r.kinds = append(r.kinds, e.Kind)
	r.events = append(r.events, e)
}

func TestNotifyDeduplicatesAndValidatesArguments(t *testing.T) {
	runner := &fakeRunner{}
	recorder := &fakeRecorder{}
	clock := func() time.Time { return time.Unix(100, 0) }
	notify := NewNotifyUseCase("/bin/echo", runner, recorder, clock)
	if err := notify.Notify(context.Background(), supervisor.EventKindSessionStopped, "alpha-1"); err != nil {
		t.Fatal(err)
	}
	if err := notify.Notify(context.Background(), supervisor.EventKindSessionStopped, "alpha-1"); err != nil {
		t.Fatal(err)
	}
	notify.Close()
	if len(runner.calls) != 1 {
		t.Fatalf("calls = %d", len(runner.calls))
	}
	if err := notify.Notify(context.Background(), supervisor.EventKindSessionStopped, "bad id"); err == nil {
		t.Fatal("invalid session id accepted")
	}
}

func TestNotifyQueueOverflowRecorded(t *testing.T) {
	runner := &gateRunner{entered: make(chan struct{}), release: make(chan struct{}), started: make(chan []string, 3)}
	recorder := &fakeRecorder{}
	notify := NewNotifyUseCaseWithOptions("/bin/echo", runner, recorder, time.Now, time.Second, time.Minute, 1)
	_ = notify.Notify(context.Background(), supervisor.EventKindSessionStopped, "alpha-1")
	<-runner.entered
	_ = notify.Notify(context.Background(), supervisor.EventKindSessionFailed, "alpha-1")
	_ = notify.Notify(context.Background(), supervisor.EventKindSessionExited, "alpha-1")
	if len(recorder.kinds) == 0 || recorder.kinds[len(recorder.kinds)-1] != supervisor.EventKindNotifyDropped {
		t.Fatal("overflow was not recorded")
	}
	close(runner.release)
	notify.Close()
	if len(runner.started) != 2 {
		t.Fatalf("runner calls=%d, want first plus newest queued notification", len(runner.started))
	}
	first, newest := <-runner.started, <-runner.started
	if first[0] != string(supervisor.EventKindSessionStopped) || newest[0] != string(supervisor.EventKindSessionExited) {
		t.Fatalf("queued notifications were not oldest-first replacement: %v then %v", first, newest)
	}
}

type gateRunner struct {
	entered chan struct{}
	release chan struct{}
	started chan []string
}

func (r *gateRunner) Run(_ context.Context, _ string, args []string, _ []string) error {
	r.started <- append([]string(nil), args...)
	select {
	case r.entered <- struct{}{}:
	default:
	}
	<-r.release
	return nil
}

func TestStopControlResumePolicyAndGuard(t *testing.T) {
	store := &memorySessions{sessions: []supervisor.ManagedSession{
		{SessionID: "alpha-1", State: supervisor.SessionStopped, StopScope: supervisor.StopScopeGlobal, StopReason: supervisor.StopGlobal, ClaudeSessionID: "claude-alpha"},
		{SessionID: "beta-1", State: supervisor.SessionStopped, StopScope: supervisor.StopScopeGlobal, StopReason: supervisor.StopSendNotConfirmed, ClaudeSessionID: "claude-beta"},
	}}
	state := NewStopState(t.TempDir())
	state.SetGlobal(true)
	_ = state.WriteGlobal()
	uc := NewStopControlUseCase(state, store, nil, nil)
	if _, err := uc.Resume(context.Background(), nil, false, false); !errors.Is(err, ErrResumeForbidden) {
		t.Fatalf("guard err = %v", err)
	}
	result, err := uc.Resume(context.Background(), nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if store.sessions[0].State != supervisor.SessionStarting || store.sessions[1].State != supervisor.SessionStopped {
		t.Fatalf("states = %s,%s", store.sessions[0].State, store.sessions[1].State)
	}
	if len(result.StillStopped) != 1 || result.StillStopped[0].Reason != "reason_not_resumed_by_global" {
		t.Fatalf("result = %#v", result)
	}
}

type memorySessions struct{ sessions []supervisor.ManagedSession }

func (m *memorySessions) List(context.Context) ([]supervisor.ManagedSession, error) {
	return m.sessions, nil
}
func (m *memorySessions) Save(_ context.Context, s supervisor.ManagedSession) error {
	for i := range m.sessions {
		if m.sessions[i].SessionID == s.SessionID {
			m.sessions[i] = s
			return nil
		}
	}
	return nil
}
