package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

func TestStartFailureHasOnlyProjectAndReason(t *testing.T) {
	cases := []Event{
		{Kind: supervisor.EventKindSessionStartFailed, ProjectID: "demo", Reason: ReasonSpawnFailed, FromState: supervisor.SessionStarting},
		{Kind: supervisor.EventKindSessionStartFailed, ProjectID: "demo", Reason: ReasonSpawnFailed, ToState: supervisor.SessionFailed},
		{Kind: supervisor.EventKindSessionStartFailed, ProjectID: "demo", Reason: ReasonSpawnFailed, Code: "SUP_SPAWN_FAILED"},
		{Kind: supervisor.EventKindSessionStarted, SessionID: "demo-1", ProjectID: "demo"},
	}
	for _, event := range cases {
		if err := validateEvent(event); err == nil {
			t.Errorf("accepted forbidden fields: %#v", event)
		}
	}
}

func TestNewQuarantineIsReportedOnceForEveryReadPath(t *testing.T) {
	for _, path := range []string{"load", "list", "update"} {
		t.Run(path, func(t *testing.T) {
			root := t.TempDir()
			id, _ := supervisor.NewSessionID("corrupt-1")
			if err := os.MkdirAll(filepath.Join(root, "sessions"), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "sessions", id.String()+".json"), []byte("{"), 0600); err != nil {
				t.Fatal(err)
			}
			sessions := NewSessionStore(root)
			calls := 0
			sessions.SetQuarantineHandler(func(got supervisor.SessionID) {
				if got != id {
					t.Errorf("ID=%s", got)
				}
				calls++
			})
			switch path {
			case "load":
				_, _ = sessions.Load(id)
			case "list":
				_, _, _, _ = sessions.List()
			case "update":
				_ = sessions.UpdateSession(id, func(*supervisor.ManagedSession) error { return nil })
			}
			_, _, _, _ = sessions.List()
			if calls != 1 {
				t.Fatalf("quarantine callbacks=%d", calls)
			}
		})
	}
}
