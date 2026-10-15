package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/config"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/gitops"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
	supervisortransport "github.com/yoshikihorie/codex-runner/internal/supervisor/transport"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/usecase"
)

type wiringFingerprintGit struct {
	*gitops.GitRunner
	dir string
}

func (g wiringFingerprintGit) RevParseGitCommonDir(context.Context) ([]byte, error) {
	return []byte(g.dir), nil
}
func (g wiringFingerprintGit) RevParseGitDir(context.Context) ([]byte, error) {
	return []byte(g.dir), nil
}

type wiringPTY struct {
	pid    int
	done   chan struct{}
	once   sync.Once
	killed bool
}

func (p *wiringPTY) Pid() int                  { return p.pid }
func (p *wiringPTY) Read([]byte) (int, error)  { <-p.done; return 0, io.EOF }
func (p *wiringPTY) Wait() (int, error)        { <-p.done; return 0, nil }
func (p *wiringPTY) Close() error              { p.once.Do(func() { close(p.done) }); return nil }
func (p *wiringPTY) Kill(syscall.Signal) error { p.killed = true; return nil }

type wiringStarter struct {
	pty   *wiringPTY
	argv  []string
	calls int
}

func (s *wiringStarter) Start(_ string, argv, _ []string, _ string, _, _ uint16) (usecase.ManagedPTY, error) {
	s.calls++
	s.argv = slices.Clone(argv)
	return s.pty, nil
}

func wiringRuntime(t *testing.T) (*supervisorRuntime, string) {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "g42w-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(`{"auth_mode":"chatgpt"}`), 0600); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(home, "claude")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	projects := make([]config.SupervisorProject, 0, 2)
	for _, name := range []string{"alpha", "beta"} {
		cwd := filepath.Join(home, name)
		if err := os.Mkdir(cwd, 0700); err != nil {
			t.Fatal(err)
		}
		projects = append(projects, config.SupervisorProject{ID: name, CLI: "claude", CWD: cwd, VerifySHA256: strings.Repeat("a", 64)})
	}
	state := filepath.Join(home, "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	r, err := newSupervisorRuntime(context.Background(), config.SupervisorConfig{StateDir: state, NotifyCommand: "none", ClaudeBinaryPath: bin, MaxSessions: 2, Projects: projects})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.close)
	return r, home
}

func TestSuperviseWiring_G42ProjectResume(t *testing.T) {
	r, home := wiringRuntime(t)
	a, b := r.projectDeps("alpha"), r.projectDeps("beta")
	if a == nil || b == nil || a == b || a.CommitVerified == b.CommitVerified || a.Push == b.Push || a.ObserveSession == b.ObserveSession || r.projectDeps("alpha") != a || a.ObserveSession.AutoContinue != r.auto || b.ObserveSession.AutoContinue != r.auto || a.ObserveSession.Rotation != r.rotation || b.ObserveSession.Rotation != r.rotation {
		t.Fatal("project dependency or shared control wiring differs")
	}
	starters := map[string]*wiringStarter{}
	for i, name := range []string{"alpha", "beta"} {
		deps := r.projectDeps(name)
		gitDir := filepath.Join(home, name, ".git")
		if err := os.Mkdir(gitDir, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(gitDir, "config"), []byte("[core]\n\tbare = false\n"), 0600); err != nil {
			t.Fatal(err)
		}
		deps.CommitVerified.Git = wiringFingerprintGit{GitRunner: deps.CommitVerified.Git.(*gitops.GitRunner), dir: gitDir}
		starters[name] = &wiringStarter{pty: &wiringPTY{pid: 50001 + i, done: make(chan struct{})}}
		deps.ObserveSession.Respawn.Starter = starters[name]
		deps.ObserveSession.Respawn.ParentEnv = func() []string { return nil }
	}
	id := supervisor.SessionID("g42-beta-1")
	now := time.Now().UTC()
	session := supervisor.ManagedSession{SessionID: id, ProjectID: "beta", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StateSince: now, StopScope: supervisor.StopScopeSession, StopReason: supervisor.StopSupervisorRestarted, ClaudeSessionID: "00000000-0000-0000-0000-000000000001", SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartCount: 2, RestartTimes: []time.Time{now}, AutoContinueDisabledReason: supervisor.AutoContinueDisabledPermissionModeUnsafe, LastPushFailedHead: strings.Repeat("a", 40), LastFailureFingerprint: strings.Repeat("b", 64), LastVerifiedResult: "failed", LastVerifiedFingerprint: strings.Repeat("c", 64), GitConfigFingerprint: strings.Repeat("d", 64)}
	if err := r.sessions.Save(session); err != nil {
		t.Fatal(err)
	}
	if err := r.stops.EnableSession(id); err != nil {
		t.Fatal(err)
	}
	wantFingerprint, err := b.CommitVerified.CurrentGitConfigFingerprint(context.Background())
	if err != nil || wantFingerprint == "" || wantFingerprint == session.GitConfigFingerprint {
		t.Fatalf("fingerprint=%q err=%v", wantFingerprint, err)
	}
	out, err := r.control.Resume(context.Background(), &id, true, false)
	if err != nil || !slices.Equal(out.Resumed, []supervisor.SessionID{id}) {
		t.Fatalf("resume=%+v err=%v", out, err)
	}
	got, err := r.sessions.Load(id)
	if err != nil || got.AutoContinueDisabledReason != "" || got.LastPushFailedHead != "" || got.LastFailureFingerprint != "" || got.LastVerifiedResult != "" || got.GitConfigFingerprint != wantFingerprint || got.RestartCount != 0 || len(got.RestartTimes) != 0 {
		t.Fatalf("resume maintenance=%+v err=%v", got, err)
	}
	if starters["alpha"].calls != 0 || starters["beta"].calls != 1 || !slices.Contains(starters["beta"].argv, "--resume") {
		t.Fatalf("wrong project recovery: alpha=%d beta=%d argv=%v", starters["alpha"].calls, starters["beta"].calls, starters["beta"].argv)
	}
	events, _, err := r.events.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(events, func(e store.Event) bool {
		return e.SessionID == id.String() && e.Kind == supervisor.EventKindRestartCountersReset
	}) {
		t.Fatal("restart counter reset was not recorded")
	}
}

func TestSuperviseWiringStartSessionSequence(t *testing.T) {
	r, home := wiringRuntime(t)
	r.start.ParentEnv = func() []string { return nil }
	r.start.GitConfigFingerprint = nil
	r.stopSession.ChildAlive = func(int) bool { return false }
	socket := filepath.Join(home, "start.sock")
	server := &supervisortransport.Server{SocketPath: socket, Handler: r.newDispatch(), PeerCredentials: func(*net.UnixConn) (int, int, error) { return os.Geteuid(), os.Getpid(), nil }}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	defer func() { cancel(); <-done }()
	client := supervisortransport.Client{SocketPath: socket}
	request := supervisortransport.Request{SupervisorProtocolVersion: supervisortransport.SupervisorProtocolVersion, Verb: "start-session", Params: json.RawMessage(`{"project_id":"alpha"}`)}
	for n, want := range []string{"alpha-1", "alpha-2"} {
		pty := &wiringPTY{pid: 51001 + n, done: make(chan struct{})}
		r.start.Starter = &wiringStarter{pty: pty}
		var response supervisortransport.Response
		var err error
		for attempt := 0; attempt < 100; attempt++ {
			response, err = client.Call(context.Background(), request)
			if err == nil || !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ECONNREFUSED) {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
		if !response.OK || string(response.Result) != `{"session_id":"`+want+`"}` {
			t.Fatalf("start %d: %+v", n, response)
		}
		if n == 0 {
			out, err := r.stopSession.Execute(context.Background(), supervisor.SessionID(want))
			if err != nil || out.State != supervisor.SessionExited {
				t.Fatalf("stop=%+v err=%v", out, err)
			}
		}
	}
}

func TestSuperviseWiring_StopFreshAutoContinue(t *testing.T) {
	r, _ := wiringRuntime(t)
	id := supervisor.SessionID("g42-fresh-stop-1")
	now := time.Now().UTC()
	if err := r.sessions.Save(supervisor.ManagedSession{SessionID: id, ProjectID: "alpha", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: now}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Errorf("fresh runtime Stop panicked: %v", recovered)
		}
	}()
	if _, err := r.control.Stop(context.Background(), &id); err != nil {
		t.Fatal(err)
	}
}

func TestSuperviseWiring_G42RotationPrecedesRecovery(t *testing.T) {
	r, _ := wiringRuntime(t)
	id := supervisor.SessionID("g42-rotation-1")
	now := time.Now().UTC()
	session := supervisor.ManagedSession{SessionID: id, ProjectID: "alpha", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStopped, StateSince: now, StopScope: supervisor.StopScopeSession, StopReason: supervisor.StopRotateStepFailed, PreviousState: supervisor.SessionRotating, ClaudeSessionID: "00000000-0000-0000-0000-000000000002", SendGate: supervisor.SendGateRecord{NextSeq: 2}, RestartTimes: []time.Time{}, LastSend: &supervisor.LastSendRecord{Kind: supervisor.SendKindHandoffExit, SendID: id.String() + "-s1", Confirmed: true}, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateSendExit, StartedAt: now, StepStartedAt: &now}}
	if err := r.sessions.Save(session); err != nil {
		t.Fatal(err)
	}
	if err := r.stops.EnableSession(id); err != nil {
		t.Fatal(err)
	}
	out, err := r.control.Resume(context.Background(), &id, true, false)
	if err != nil || !slices.Equal(out.Resumed, []supervisor.SessionID{id}) {
		t.Fatalf("rotation resume=%+v err=%v", out, err)
	}
	got, err := r.sessions.Load(id)
	if err != nil || got.State != supervisor.SessionRotating || got.Rotation == nil || got.Rotation.Step != supervisor.RotateRestart {
		t.Fatalf("handoff continuation=%+v err=%v", got, err)
	}
}

func TestSuperviseWiring_ShutdownWaitsForObservation(t *testing.T) {
	r, _ := wiringRuntime(t)
	id := supervisor.SessionID("g42-close-1")
	pty := &wiringPTY{pid: 60001, done: make(chan struct{})}
	r.ptys.RegisterManaged(id, pty)
	defer r.ptys.RemoveManaged(id, pty)
	closed := make(chan struct{})
	go func() { r.close(); close(closed) }()
	select {
	case <-pty.done:
	case <-time.After(time.Second):
		t.Fatal("managed PTY was not closed")
	}
	select {
	case <-r.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("supervisor context was not canceled")
	}
	if !r.ptys.IsShuttingDown() || r.ctx.Err() == nil || pty.killed {
		t.Fatal("shutdown mark, context or signal order wrong")
	}
	select {
	case <-closed:
		t.Fatal("shutdown returned before observation cleanup")
	default:
	}
	r.ptys.RemoveManaged(id, pty)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown did not wait for observation cleanup")
	}
}

type wiringContextDetector struct{ percent int }

func (d wiringContextDetector) Detect(context.Context, detect.DetectInput) (detect.DetectResult, error) {
	return detect.DetectResult{State: detect.StateWorking, ContextPercent: &d.percent}, nil
}

func TestSuperviseWiring_StatusContextPercent(t *testing.T) {
	r, _ := wiringRuntime(t)
	deps := r.projectDeps("alpha")
	id := supervisor.SessionID("status-context-1")
	now := time.Now().UTC()
	session := supervisor.ManagedSession{SessionID: id, ProjectID: "alpha", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionWorking, StateSince: now, ChildStartedAt: &now}
	if err := r.sessions.Save(session); err != nil {
		t.Fatal(err)
	}
	dispatch := r.newDispatch()
	status := func() map[string]any {
		t.Helper()
		response := dispatch.Handle(context.Background(), supervisortransport.Request{SupervisorProtocolVersion: supervisortransport.SupervisorProtocolVersion, Verb: "status"}, 0)
		if !response.OK {
			t.Fatalf("status = %+v", response)
		}
		var body struct {
			Sessions []map[string]any `json:"sessions"`
		}
		if err := json.Unmarshal(response.Result, &body); err != nil {
			t.Fatal(err)
		}
		if len(body.Sessions) != 1 {
			t.Fatalf("sessions = %v", body.Sessions)
		}
		if _, ok := body.Sessions[0]["context_percent"]; !ok {
			t.Fatalf("context_percent key is absent: %v", body.Sessions[0])
		}
		return body.Sessions[0]
	}
	if view := status(); view["context_percent"] != nil {
		t.Fatalf("unknown context = %v", view)
	}
	if deps.ObserveSession.ContextPercent == nil {
		t.Fatal("observer lacks shared context store")
	}
	value := 55
	deps.ObserveSession.Detect.Detector = wiringContextDetector{percent: value}
	deps.ObserveSession.RotationStatuslineAt = func(supervisor.SessionID) *time.Time { at := now.Add(time.Second); return &at }
	deps.ObserveSession.Interval = time.Millisecond
	pty := &wiringPTY{pid: 50011, done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- deps.ObserveSession.Run(ctx, session, pty, supervisor.NewVirtualTerminal(80, 24)) }()
	deadline := time.After(time.Second)
	for {
		view := status()
		if view["context_percent"] == float64(value) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("observed context missing from status: %v", view)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	_ = pty.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("observer did not stop")
	}
	if view := status(); view["context_percent"] != nil {
		t.Fatalf("context remained after observer stopped: %v", view)
	}
}
