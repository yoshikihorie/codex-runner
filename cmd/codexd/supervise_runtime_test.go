package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/config"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/gitops"
)

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
