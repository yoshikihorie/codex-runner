package usecase

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

func TestSCNsup01038SharedPreflightWritesSettingsAndRemovesOldStatus(t *testing.T) {
	u, _, _, id := respawnFixture(t)
	statusDir := filepath.Join(u.Config.StateDir, "statusline")
	if err := os.MkdirAll(statusDir, 0700); err != nil {
		t.Fatal(err)
	}
	old := filepath.Join(statusDir, id.String()+".json")
	if err := os.WriteFile(old, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("old status: %v", err)
	}
	if _, err := os.Stat(filepath.Join(statusDir, id.String()+".settings.json")); err != nil {
		t.Fatalf("settings: %v", err)
	}
}

func TestSCNsup01039RespawnPreflightFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*RespawnSessionUseCase)
		reason store.SupervisorStartFailReason
	}{
		{"cwd", func(u *RespawnSessionUseCase) {
			p := u.Projects["alpha"]
			p.CWD = filepath.Join(u.Config.StateDir, "missing")
			u.Projects["alpha"] = p
		}, store.StartFailProjectCWDUnavailable},
		{"binary", func(u *RespawnSessionUseCase) {
			p := u.Projects["alpha"]
			p.BinaryPath = filepath.Join(u.Config.StateDir, "missing")
			u.Projects["alpha"] = p
		}, store.StartFailSpawnFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, sessions, spy, id := respawnFixture(t)
			tc.change(&u)
			var events []store.Event
			u.RecordStructured = func(e store.Event) error { events = append(events, e); return nil }
			if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
				t.Fatal(err)
			}
			if spy.calls != 0 || sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopRespawnFailed {
				t.Fatalf("record %+v, calls %d", sessions.session, spy.calls)
			}
			// FD-sup-05.md:135 requires state_changed when starting becomes stopped.
			if len(events) != 3 || events[0].Kind != supervisor.EventKindSessionStartFailed || events[0].ProjectID != "alpha" || events[0].Reason != store.EventReason(tc.reason) || events[1].Kind != supervisor.EventKindStateChanged || events[1].FromState != supervisor.SessionStarting || events[1].ToState != supervisor.SessionStopped || events[2].Kind != supervisor.EventKindSessionStopped || events[2].Reason != store.EventReason(supervisor.StopRespawnFailed) {
				t.Fatalf("events %+v", events)
			}
		})
	}
}

func TestFix3PreflightRecordsStateDirAndSizeFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*RespawnSessionUseCase)
		want   store.SupervisorStartFailReason
	}{
		{"state directory", func(u *RespawnSessionUseCase) { u.Config.StateDir = "relative" }, store.StartFailSettingsGenerationFailed},
		{"screen size", func(u *RespawnSessionUseCase) { u.Config.Columns = 0 }, store.StartFailSpawnFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, _, spy, id := respawnFixture(t)
			tc.change(&u)
			var got []store.SupervisorStartFailReason
			u.RecordStartFailure = func(_ string, r store.SupervisorStartFailReason) { got = append(got, r) }
			_ = u.Execute(context.Background(), id, RespawnRecovery)
			if spy.calls != 0 || len(got) != 1 || got[0] != tc.want {
				t.Fatalf("calls=%d reasons=%v", spy.calls, got)
			}
		})
	}
}

func TestSCNsup01039RespawnBillingPreflight(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".claude"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".claude", "settings.json"), []byte(`{"apiKeyHelper":"helper"}`), 0600); err != nil {
		t.Fatal(err)
	}
	var events []store.Event
	u.RecordStructured = func(e store.Event) error { events = append(events, e); return nil }
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	if spy.calls != 0 || sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopRespawnFailed {
		t.Fatalf("record %+v, calls %d", sessions.session, spy.calls)
	}
	// FD-sup-05.md:135 requires state_changed when starting becomes stopped.
	if len(events) != 3 || events[0].Kind != supervisor.EventKindSessionStartFailed || events[0].Reason != store.EventReason(store.StartFailUserSettings) || events[1].Kind != supervisor.EventKindStateChanged || events[1].FromState != supervisor.SessionStarting || events[1].ToState != supervisor.SessionStopped || events[2].Kind != supervisor.EventKindSessionStopped || events[2].Reason != store.EventReason(supervisor.StopRespawnFailed) {
		t.Fatalf("events %+v", events)
	}
}

func TestSCNsup0522RespawnStopBlocksSpawn(t *testing.T) {
	u, sessions, spy, id := respawnFixture(t)
	u.Stop = NewStopState(u.Config.StateDir)
	u.Stop.SetSession(id, true)
	if err := u.Execute(context.Background(), id, RespawnRecovery); err != nil {
		t.Fatal(err)
	}
	if spy.calls != 0 || sessions.session.State != supervisor.SessionStopped || sessions.session.StopReason != supervisor.StopSession {
		t.Fatalf("record %+v, calls %d", sessions.session, spy.calls)
	}
	if !slices.Equal(sessions.session.RestartTimes, []time.Time{}) {
		t.Fatal(sessions.session.RestartTimes)
	}
}
