package usecase

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

func TestResumeMaintenance_SCNSup0546(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "smaint-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	sessions := store.NewSessionStore(home)
	id, _ := supervisor.NewSessionID("demo-46")
	s := supervisor.ManagedSession{SessionID: id, ProjectID: "demo", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionIdle, StateSince: time.Now().UTC(), AutoContinueDisabledReason: "permission_mode_unsafe", GitConfigFingerprint: strings.Repeat("a", 64), RestartCount: 3, RestartTimes: []time.Time{time.Now().UTC()}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	if err := sessions.Save(s); err != nil {
		t.Fatal(err)
	}
	maintenance := NewSessionResumeMaintenance(sessions, func(_ context.Context, project string) (string, error) {
		if project != "demo" {
			t.Fatal(project)
		}
		return strings.Repeat("b", 64), nil
	})
	if err := maintenance.DiscardPending(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.RefreshGitConfigFingerprint(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if err := maintenance.ResetRestartCounters(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	got, err := sessions.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.AutoContinueDisabledReason != "" || got.GitConfigFingerprint != strings.Repeat("b", 64) || got.RestartCount != 3 || len(got.RestartTimes) != 1 {
		t.Fatalf("record=%+v", got)
	}
	maintenance = NewSessionResumeMaintenance(sessions, func(context.Context, string) (string, error) { return "", errors.New("git unavailable") })
	if err := maintenance.RefreshGitConfigFingerprint(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	got, err = sessions.Load(id)
	if err != nil || got.GitConfigFingerprint != "" {
		t.Fatalf("fingerprint=%q err=%v", got.GitConfigFingerprint, err)
	}
}
