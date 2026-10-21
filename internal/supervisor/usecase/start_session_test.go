package usecase

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/gitops"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/launch"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

func TestStartSessionShutdownRejectsWithoutSpawnFailureRecord(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if err := os.Mkdir(filepath.Join(home, ".codex"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(`{"auth_mode":"chatgpt"}`), 0600); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(home, "claude")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	registry := &PTYRegistry{}
	registry.BeginShutdown()
	sessions := &startMemoryStore{}
	failures := 0
	u := StartSessionUseCase{Config: StartSessionConfig{StateDir: home, CodexdPath: binary, MaxSessions: 1, Columns: 80, Rows: 24}, Projects: map[string]StartProject{"alpha": {ID: "alpha", CLI: supervisor.CLIClaude, CWD: home, BinaryPath: binary}}, Store: sessions, PTYs: registry, ParentEnv: func() []string { return nil }, RecordStartFailure: func(string, store.SupervisorStartFailReason) { failures++ }}
	_, err = u.Execute("alpha", "alpha-1")
	if !errors.Is(err, ErrStopActive) || failures != 0 {
		t.Fatalf("error=%v failures=%d", err, failures)
	}
}

func TestStartSession_SCNsup0361_RecordsGitConfigFingerprint(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if err := os.Mkdir(filepath.Join(home, ".codex"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(`{"auth_mode":"chatgpt","OPENAI_API_KEY":null}`), 0600); err != nil {
		t.Fatal(err)
	}
	cwd := t.TempDir()
	commitTestGit(t, cwd, "init", "-q")
	binary := filepath.Join(t.TempDir(), "claude")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	id, _ := supervisor.NewSessionID("demo-61")
	sessions := &startMemoryStore{}
	commit := &CommitVerifiedChangesUseCase{Git: gitops.NewGitRunner(cwd, "supervisor", "supervisor@example.invalid", t.TempDir())}
	expected, err := commit.CurrentGitConfigFingerprint(context.Background())
	if err != nil || expected == "" {
		t.Fatalf("current fingerprint=%q err=%v", expected, err)
	}
	u := StartSessionUseCase{Config: StartSessionConfig{StateDir: t.TempDir(), CodexdPath: "/bin/true", MaxSessions: 1, Columns: 80, Rows: 24}, Projects: map[string]StartProject{"demo": {ID: "demo", CLI: supervisor.CLIClaude, CWD: cwd, BinaryPath: binary}}, Store: sessions, Starter: &startSpy{store: sessions}, Billing: launch.BillingGuard{ManagedSettingsPath: filepath.Join(home, "managed.json")}, ParentEnv: func() []string { return nil }, GitConfigFingerprint: func(projectID string) string {
		if projectID != "demo" {
			t.Fatal(projectID)
		}
		fingerprint, err := commit.CurrentGitConfigFingerprint(context.Background())
		if err != nil {
			return ""
		}
		return fingerprint
	}}
	if _, err := u.Execute("demo", id); err != nil {
		t.Fatal(err)
	}
	if sessions.session.GitConfigFingerprint != expected {
		t.Fatalf("fingerprint = %q", sessions.session.GitConfigFingerprint)
	}
}
