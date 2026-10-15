package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/config"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

func TestSupervisorRuntimeCommitVerifiedWiring(t *testing.T) {
	root := t.TempDir()
	repo := filepath.Join(root, "repo")
	state := filepath.Join(root, "state")
	if err := os.Mkdir(repo, 0700); err != nil {
		t.Fatal(err)
	}
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = repo
		cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	git("init", "-q")
	if err := os.WriteFile(filepath.Join(repo, "base.txt"), []byte("base"), 0600); err != nil {
		t.Fatal(err)
	}
	git("add", "--", "base.txt")
	git("-c", "user.name=setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "base")
	before := git("rev-parse", "HEAD")
	if err := os.Mkdir(filepath.Join(repo, "owned"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "owned", "new.txt"), []byte("new"), 0600); err != nil {
		t.Fatal(err)
	}
	verify := filepath.Join(root, "verify.sh")
	data := []byte("#!/bin/sh\nexit 0\n")
	if err := os.WriteFile(verify, data, 0700); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	cfg := config.SupervisorConfig{StateDir: state, NotifyCommand: "none", CommitAuthorName: "supervisor", CommitAuthorEmail: "supervisor@example.invalid", Projects: []config.SupervisorProject{{ID: "project", CWD: repo, CLI: "claude", OwnedPaths: []string{"owned"}, VerifyCommand: []string{verify}, VerifySHA256: fmt.Sprintf("%x", hash)}}}
	r, err := newSupervisorRuntime(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.close)
	commit := r.projectDeps("project").CommitVerified
	fingerprint, err := commit.CurrentGitConfigFingerprint(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	id, err := supervisor.NewSessionID("commit-wiring-1")
	if err != nil {
		t.Fatal(err)
	}
	session := supervisor.ManagedSession{SessionID: id, ProjectID: "project", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionIdle, StateSince: time.Now().Add(-time.Hour), StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}, GitConfigFingerprint: fingerprint}
	if err := r.sessions.Save(session); err != nil {
		t.Fatal(err)
	}
	if err := commit.Execute(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if after := git("rev-parse", "HEAD"); after == before {
		t.Fatal("commit was not created")
	}
	if got := git("show", "--format=", "--name-only", "HEAD"); got != "owned/new.txt" {
		t.Fatalf("committed paths = %q", got)
	}
	current, err := r.sessions.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if current.GitPhase != supervisor.GitPhaseNone || current.LastVerifiedResult != "passed" {
		t.Fatalf("session after commit: phase=%q result=%q", current.GitPhase, current.LastVerifiedResult)
	}
	events, err := os.ReadFile(filepath.Join(state, "events.jsonl"))
	if err != nil || !strings.Contains(string(events), `"kind":"commit_created"`) {
		t.Fatalf("commit event missing: %v: %s", err, events)
	}
}
