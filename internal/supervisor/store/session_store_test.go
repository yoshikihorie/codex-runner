package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

func TestSessionStoreRoundTripAndOmitsScreenText(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	root := filepath.Join(os.Getenv("HOME"), "state")
	st := NewSessionStore(root)
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	s := supervisor.ManagedSession{SchemaVersion: 1, SessionID: mustID(t, "alpha-1"), ProjectID: "alpha", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStarting, StateSince: now, StopScope: supervisor.StopScopeNone, ClaudeSessionID: "00000000-0000-0000-0000-000000000001", PreviousClaudeSessionID: "00000000-0000-0000-0000-000000000002", ChildPID: 42, SendGate: supervisor.SendGateRecord{NextSeq: 9, EverSent: true, LastSendConfirmed: true, LastConfirmedAt: &now, UnconfirmedDiscarded: true, Pending: &supervisor.PendingSend{SendID: "alpha-1-s9", Kind: supervisor.SendKindRemoteControl, IssuedAt: now}}, LastSend: &supervisor.LastSendRecord{SendID: "alpha-1-s8", Kind: supervisor.SendKindRemoteControl, IssuedAt: now, Confirmed: true}, RestartCount: 2, RestartTimes: []time.Time{now}, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateSendRemoteControl, StartedAt: now, StepStartedAt: &now, EndIssuedAt: &now, CheckpointSnapshot: supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "0123456789012345678901234567890123456789"}, RespawnWaitUntil: &now, FailReason: "exit_timeout"}, PendingRemoteControl: true, PendingRemoteControlSince: &now, PreviousState: supervisor.SessionWorking, LastVerifiedFingerprint: strings.Repeat("a", 64), LastVerifiedResult: "passed", LastFailureFingerprint: strings.Repeat("b", 64), LastFailureReason: "verify_binary_changed", LastPushFailedHead: "0123456789012345678901234567890123456789", GitConfigFingerprint: strings.Repeat("c", 64)}
	if err := st.Save(s); err != nil {
		t.Fatal(err)
	}
	got, err := st.Load(s.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, s) {
		t.Fatalf("round trip mismatch:\n got %#v\nwant %#v", got, s)
	}
	path := filepath.Join(root, "sessions", "alpha-1.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) == "" || string(data) == "screen secret" || strings.Contains(string(data), "screen secret") || strings.Contains(string(data), "visible screen body") {
		t.Fatalf("invalid persisted data: %s", data)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("permissions=%o", info.Mode().Perm())
	}
}

func TestSessionStoreSeparatesCorruptRecord(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "sessions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "alpha-1.json")
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	st := NewSessionStore(root)
	_, err := st.Load(mustID(t, "alpha-1"))
	if err == nil || !errors.Is(err, ErrStateFileCorrupt) {
		t.Fatalf("error=%v", err)
	}
	matches, _ := filepath.Glob(path + ".corrupt*")
	if len(matches) != 1 {
		t.Fatalf("corrupt files=%v", matches)
	}
}

func mustID(t *testing.T, s string) supervisor.SessionID {
	t.Helper()
	id, err := supervisor.NewSessionID(s)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestSessionStoreAtomicFailureKeepsPreviousRecord(t *testing.T) {
	root := t.TempDir()
	st := NewSessionStore(root)
	s := supervisor.ManagedSession{SessionID: mustID(t, "atomic-1"), ProjectID: "atomic", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionStarting, StateSince: time.Now().UTC(), StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}}
	if err := st.Save(s); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(st.path(s.SessionID))
	if err != nil {
		t.Fatal(err)
	}
	st.writeAtomic = func(string, []byte, os.FileMode) error { return os.ErrPermission }
	s.State = supervisor.SessionIdle
	if err := st.Save(s); err == nil {
		t.Fatal("expected injected write failure")
	}
	after, err := os.ReadFile(st.path(s.SessionID))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("existing record changed after failed atomic write")
	}
}

func TestSessionStoreRejectsUnknownFieldAndWrongType(t *testing.T) {
	for _, body := range []string{`{"unknown":true}`, `{"schema_version":"wrong"}`} {
		root := t.TempDir()
		dir := filepath.Join(root, "sessions")
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, "alpha-1.json")
		if err := os.WriteFile(path, []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewSessionStore(root).Load(mustID(t, "alpha-1")); !errors.Is(err, ErrStateFileCorrupt) {
			t.Fatalf("body %s: error=%v", body, err)
		}
	}
}

func TestSessionStoreQuarantinesUnknownVersionAndDirectory(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		directory  bool
	}{{"unknown version", `{"schema_version":99}`, false}, {"directory record", "", true}} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "sessions")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "odd-1.json")
			if tc.directory {
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := NewSessionStore(root).Load(mustID(t, "odd-1")); !errors.Is(err, ErrStateFileCorrupt) {
				t.Fatalf("Load error=%v", err)
			}
		})
	}
}

func TestSessionStoreQuarantineNameAndIDListing(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "sessions")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "broken-7.json")
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	st := NewSessionStore(root)
	if _, err := st.Load(mustID(t, "broken-7")); !errors.Is(err, ErrStateFileCorrupt) {
		t.Fatalf("Load error=%v", err)
	}
	matches, err := filepath.Glob(path + ".corrupt-*")
	if err != nil || len(matches) != 1 {
		t.Fatalf("quarantine paths=%v err=%v", matches, err)
	}
	ids, err := st.CorruptSessionIDs()
	if err != nil || !reflect.DeepEqual(ids, []supervisor.SessionID{mustID(t, "broken-7")}) {
		t.Fatalf("corrupt IDs=%v err=%v", ids, err)
	}
}

func TestSessionStoreRejectsInvalidSessionOnSaveAndLoad(t *testing.T) {
	now := time.Now().UTC()
	valid := supervisor.ManagedSession{SchemaVersion: 1, SessionID: mustID(t, "invalid-1"), ProjectID: "invalid", CLI: supervisor.CLIClaude, State: supervisor.SessionIdle, Generation: 1, StateSince: now, StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	tests := []struct {
		name   string
		mutate func(*supervisor.ManagedSession)
	}{
		{"invalid session id", func(s *supervisor.ManagedSession) { s.SessionID = supervisor.SessionID("not a session") }},
		{"traversal session id", func(s *supervisor.ManagedSession) { s.SessionID = supervisor.SessionID("../x") }},
		{"empty session id", func(s *supervisor.ManagedSession) { s.SessionID = "" }},
		{"generation zero", func(s *supervisor.ManagedSession) { s.Generation = 0 }},
		{"unknown schema version", func(s *supervisor.ManagedSession) { s.SchemaVersion = 99 }},
		{"unknown stop reason", func(s *supervisor.ManagedSession) { s.State = supervisor.SessionStopped; s.StopReason = "unknown" }},
		{"unknown exit reason", func(s *supervisor.ManagedSession) { s.State = supervisor.SessionExited; s.ExitReason = "unknown" }},
		{"unknown previous state", func(s *supervisor.ManagedSession) { s.PreviousState = "unknown" }},
		{"unknown stop scope", func(s *supervisor.ManagedSession) { s.StopScope = "unknown" }},
		{"invalid project slug", func(s *supervisor.ManagedSession) { s.ProjectID = "../x" }},
		{"invalid UUID", func(s *supervisor.ManagedSession) { s.ClaudeSessionID = "bad" }},
		{"short fingerprint", func(s *supervisor.ManagedSession) { s.LastVerifiedFingerprint = strings.Repeat("a", 40) }},
		{"invalid send id", func(s *supervisor.ManagedSession) {
			s.LastSend = &supervisor.LastSendRecord{SendID: "junk", Kind: supervisor.SendKindRemoteControl, IssuedAt: now}
		}},
		{"invalid result", func(s *supervisor.ManagedSession) { s.LastVerifiedResult = "unknown" }},
		{"global scope while active", func(s *supervisor.ManagedSession) { s.StopScope = supervisor.StopScopeGlobal }},
		{"missing project", func(s *supervisor.ManagedSession) { s.ProjectID = "" }},
		{"push reason in commit field", func(s *supervisor.ManagedSession) { s.LastFailureReason = "foreign_commits" }},
		{"commit reason in rotation field", func(s *supervisor.ManagedSession) {
			s.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateWaitIdle, StartedAt: now, FailReason: "commit_rejected"}
		}},
		{"unknown send kind", func(s *supervisor.ManagedSession) {
			s.LastSend = &supervisor.LastSendRecord{Kind: "unknown", IssuedAt: now}
		}},
		{"unknown rotation step", func(s *supervisor.ManagedSession) {
			s.Rotation = &supervisor.RotationRecord{Step: "unknown", StartedAt: now}
		}},
		{"unknown state", func(s *supervisor.ManagedSession) { s.State = "unknown" }},
	}
	seen := make(map[string]bool, len(tests))
	for _, tc := range tests {
		seen[tc.name] = true
		t.Run(tc.name, func(t *testing.T) {
			bad := valid
			tc.mutate(&bad)
			if err := NewSessionStore(t.TempDir()).Save(bad); !errors.Is(err, ErrStateFileCorrupt) {
				t.Fatalf("Save error=%v", err)
			}
			root := t.TempDir()
			dir := filepath.Join(root, "sessions")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			data, err := json.Marshal(bad)
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "invalid-1.json")
			if err := os.WriteFile(path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := NewSessionStore(root).Load(mustID(t, "invalid-1")); !errors.Is(err, ErrStateFileCorrupt) {
				t.Fatalf("Load error=%v", err)
			}
			matches, err := filepath.Glob(path + ".corrupt-*")
			if err != nil || len(matches) != 1 {
				t.Fatalf("quarantine paths=%v err=%v", matches, err)
			}
			ids, err := NewSessionStore(root).CorruptSessionIDs()
			if err != nil || !reflect.DeepEqual(ids, []supervisor.SessionID{mustID(t, "invalid-1")}) {
				t.Fatalf("corrupt IDs=%v err=%v", ids, err)
			}
		})
	}
	for _, name := range []string{"invalid session id", "traversal session id", "empty session id", "unknown stop reason", "unknown exit reason", "unknown previous state", "unknown send kind", "unknown rotation step", "unknown stop scope", "invalid UUID", "short fingerprint", "invalid send id", "invalid result", "global scope while active", "missing project", "push reason in commit field", "commit reason in rotation field", "invalid project slug", "generation zero", "unknown schema version"} {
		if !seen[name] {
			t.Errorf("required invalid value has no case: %s", name)
		}
	}
}

func TestSessionStoreQuarantinesUnreadableFile(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "sessions", "unreadable-1.json")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	valid := supervisor.ManagedSession{SchemaVersion: 1, SessionID: mustID(t, "unreadable-1"), CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionIdle, StateSince: time.Now().UTC(), StopScope: supervisor.StopScopeNone, SendGate: supervisor.SendGateRecord{NextSeq: 1}, RestartTimes: []time.Time{}}
	body, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0000); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0600)
	st := NewSessionStore(root)
	if _, err := st.Load(mustID(t, "unreadable-1")); !errors.Is(err, ErrStateFileCorrupt) {
		t.Fatalf("Load error=%v", err)
	}
	matches, err := filepath.Glob(path + ".corrupt-*")
	if err != nil || len(matches) != 1 {
		t.Fatalf("quarantine paths=%v err=%v", matches, err)
	}
	ids, err := st.CorruptSessionIDs()
	if err != nil || !reflect.DeepEqual(ids, []supervisor.SessionID{mustID(t, "unreadable-1")}) {
		t.Fatalf("corrupt IDs=%v err=%v", ids, err)
	}
}

func TestSessionStoreRejectsIDPathEscape(t *testing.T) {
	root := t.TempDir()
	for _, value := range []string{"../x", ""} {
		id := supervisor.SessionID(value)
		path, err := NewSessionStore(root).sessionPath(id)
		if err == nil || path != "" {
			t.Errorf("sessionPath(%q)=(%q,%v)", value, path, err)
		}
	}
}

func TestSessionStoreRejectsScreenTextInRecordFields(t *testing.T) {
	now := time.Now().UTC()
	s := supervisor.ManagedSession{SchemaVersion: 1, SessionID: mustID(t, "screen-1"), ProjectID: "screen", CLI: supervisor.CLIClaude, Generation: 1, State: supervisor.SessionIdle, StateSince: now, StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}, ClaudeSessionID: "visible screen body"}
	if err := NewSessionStore(t.TempDir()).Save(s); !errors.Is(err, ErrStateFileCorrupt) {
		t.Fatalf("Save accepted screen text: %v", err)
	}
}
