package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/config"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/gitops"
	supervisortransport "github.com/yoshikihorie/codex-runner/internal/supervisor/transport"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/usecase"
)

func TestSuperviseRuntimeScreenWiring(t *testing.T) {
	root := t.TempDir()
	r, err := newSupervisorRuntime(context.Background(), config.SupervisorConfig{StateDir: root, NotifyCommand: "none", MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.close)
	id := supervisor.SessionID("alpha-1")
	if err := r.sessions.Save(supervisor.ManagedSession{SessionID: id, ProjectID: "alpha", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStarting, StateSince: time.Now().UTC(), RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}); err != nil {
		t.Fatal(err)
	}
	r.screens.SetSnapshot(id, usecase.ScreenSnapshot{Rows: []string{"wired screen"}, ObservedAt: time.Now().UTC()})
	response := r.newDispatch().Handle(context.Background(), supervisortransport.Request{SupervisorProtocolVersion: supervisortransport.SupervisorProtocolVersion, Verb: "screen", Params: json.RawMessage(`{"session_id":"alpha-1"}`)}, 0)
	if !response.OK || !strings.Contains(string(response.Result), "wired screen") {
		t.Fatalf("response=%+v", response)
	}
}

type runtimeOperatorPTY struct {
	r     *supervisorRuntime
	id    supervisor.SessionID
	text  string
	calls int
}

func (p *runtimeOperatorPTY) WriteLine(_ context.Context, value string, _ time.Duration, before func() bool) error {
	p.calls++
	p.text = value
	shot := p.r.screens.Snapshot(p.id)
	shot.Rows[1] = "❯\u00a0" + value
	shot.Cells[1] = []supervisor.Cell{{Text: "❯"}, {Text: "\u00a0"}}
	for _, r := range value {
		shot.Cells[1] = append(shot.Cells[1], supervisor.Cell{Text: string(r)})
	}
	p.r.screens.SetSnapshot(p.id, shot)
	if !before() {
		return usecase.ErrStopActive
	}
	shot.Rows[1] = "❯\u00a0"
	shot.Cells[1] = []supervisor.Cell{{Text: "❯"}, {Text: "\u00a0"}}
	shot.ChangedAt = time.Now().Add(time.Second)
	p.r.screens.SetSnapshot(p.id, shot)
	p.r.send.NotifyConfirmation(usecase.SendConfirmation{SessionID: p.id, SendID: "alpha-1-s1", Screen: shot})
	return nil
}

func TestSuperviseRuntime_SCNSup0567_SendInputDispatchWiring(t *testing.T) {
	r, err := newSupervisorRuntime(context.Background(), config.SupervisorConfig{StateDir: t.TempDir(), NotifyCommand: "none", MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.close)
	id := supervisor.SessionID("alpha-1")
	if err := r.sessions.Save(supervisor.ManagedSession{SessionID: id, ProjectID: "alpha", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionIdle, StateSince: time.Now().UTC(), RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}); err != nil {
		t.Fatal(err)
	}
	r.screens.SetSnapshot(id, usecase.ScreenSnapshot{State: "idle", Rows: []string{"────", "❯\u00a0", "────"}, Cells: [][]supervisor.Cell{{}, {{Text: "❯"}, {Text: "\u00a0"}}, {}}, ObservedAt: time.Now()})
	r.observed.Set(id, detect.StateIdle)
	pty := &runtimeOperatorPTY{r: r, id: id}
	r.ptys.Register(id, pty)
	d := r.newDispatch()
	d.PeerResume = func(int, map[int]bool) (bool, bool, error) { return false, false, nil }
	const body = "日本語 ＡＢＣ"
	response := d.Handle(context.Background(), supervisortransport.Request{SupervisorProtocolVersion: supervisortransport.SupervisorProtocolVersion, Verb: "send-input", Params: json.RawMessage(`{"session_id":"alpha-1","text":"` + body + `"}`)}, 0)
	if !response.OK || pty.calls != 1 || pty.text != body || !strings.Contains(string(response.Result), `"confirmed":true`) {
		t.Fatalf("response=%+v calls=%d text=%q", response, pty.calls, pty.text)
	}
}

type shutdownPTY struct {
	close  func()
	killed bool
}

func (p *shutdownPTY) Pid() int                  { return 1 }
func (p *shutdownPTY) Close() error              { p.close(); return nil }
func (p *shutdownPTY) Kill(syscall.Signal) error { p.killed = true; return nil }
func (p *shutdownPTY) Wait() (int, error)        { return 0, nil }

func TestSuperviseShutdown_SCNSup0548(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sdown-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	r, err := newSupervisorRuntime(context.Background(), config.SupervisorConfig{StateDir: home, NotifyCommand: "none", MaxSessions: 1})
	if err != nil {
		t.Fatal(err)
	}
	id, _ := supervisor.NewSessionID("down-1")
	var pty *shutdownPTY
	pty = &shutdownPTY{close: func() {
		if !r.ptys.IsShuttingDown() || r.ctx.Err() != nil {
			t.Error("shutdown order wrong")
		}
		r.ptys.RemoveManaged(id, pty)
	}}
	r.ptys.RegisterManaged(id, pty)
	r.close()
	if pty.killed || r.ctx.Err() == nil {
		t.Fatal("signal sent or context not canceled")
	}
}

func TestSuperviseProjectDeps_SCNSup0548(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "swire-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	state := filepath.Join(home, "state")
	if err := os.MkdirAll(state, 0700); err != nil {
		t.Fatal(err)
	}
	cfg := config.SupervisorConfig{StateDir: state, MaxSessions: 2, NotifyCommand: "none", Projects: []config.SupervisorProject{{ID: "alpha", CWD: filepath.Join(home, "alpha"), CLI: "claude", VerifySHA256: strings.Repeat("a", 64)}, {ID: "beta", CWD: filepath.Join(home, "beta"), CLI: "claude", VerifySHA256: strings.Repeat("b", 64)}}}
	r, err := newSupervisorRuntime(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.close)
	a, b := r.projectDeps("alpha"), r.projectDeps("beta")
	if a == nil || b == nil {
		t.Fatal("project dependencies missing")
	}
	if a.ObserveSession.CompletionSelection == nil || b.ObserveSession.CompletionSelection == nil {
		t.Fatal("completion selection not wired")
	}
	menu, selected := a.ObserveSession.CompletionSelection([]string{"  ❯ /remote-control (rc) Example", "────", "❯\u00a0/rc", "────"})
	if !menu || selected != "/remote-control (rc) Example" {
		t.Fatalf("completion selection: menu=%v selected=%q", menu, selected)
	}
	if a == nil || b == nil || a == b || a.CommitVerified == b.CommitVerified || a.Push == b.Push || a.ObserveSession == b.ObserveSession || a.CommitVerified.Locker != b.CommitVerified.Locker || a.CommitVerified.CWD != cfg.Projects[0].CWD || b.CommitVerified.CWD != cfg.Projects[1].CWD {
		t.Fatal("project dependencies were shared incorrectly")
	}
	if a.CommitVerified.Git == b.CommitVerified.Git || a.CommitVerified.Git.(*gitops.GitRunner) != a.Push.Git.(*gitops.GitRunner) {
		t.Fatal("Git runner identity wrong")
	}
	if _, ok := a.CommitVerified.Git.(*gitops.GitRunner); !ok {
		t.Fatal("Git runner not configured")
	}
	if a.ObserveSession.SupervisorContext != r.ctx || b.ObserveSession.SupervisorContext != r.ctx || a.ObserveSession.AutoContinue != r.auto || b.ObserveSession.AutoContinue != r.auto {
		t.Fatal("lifetime or auto-continue wiring wrong")
	}
	if !r.auto.Extractor.ObservedScreen {
		t.Fatal("observed recommendation extractor not wired")
	}
	if r.rotation.PendingRemoteControl == nil || r.rotation.ResumeReady == nil || r.rotation.ChildAlive == nil || r.rotation.Wake == nil {
		t.Fatal("rotation not wired")
	}
}
