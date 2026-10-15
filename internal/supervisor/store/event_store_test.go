package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

func TestEventStore_SCN0159RemoteControlSkippedFields(t *testing.T) {
	st := NewEventStore(t.TempDir())
	valid := Event{Kind: supervisor.EventKindRemoteControlSkipped, SessionID: "alpha-1", Reason: EventReason(supervisor.RemoteControlSkipResumedAfterConfirmed)}
	if _, err := st.Record(valid); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(st.root, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(bytesFirstLine(data), &fields); err != nil {
		t.Fatal(err)
	}
	for key := range fields {
		if key != "seq" && key != "ts" && key != "kind" && key != "session_id" && key != "reason" {
			t.Fatalf("unexpected event field %q", key)
		}
	}
	for _, mutate := range []func(*Event){
		func(e *Event) { e.SessionID = "" },
		func(e *Event) { e.Reason = "" },
		func(e *Event) { e.Reason = "other" },
		func(e *Event) { e.SendID = "alpha-1-s1" },
		func(e *Event) { e.SendKind = supervisor.SendKindRemoteControl },
		func(e *Event) { e.Code = "SUP_SEND_NOT_CONFIRMED" },
		func(e *Event) { e.ProjectID = "alpha" },
	} {
		v := valid
		mutate(&v)
		if _, err := st.Record(v); err == nil {
			t.Fatalf("invalid event accepted: %+v", v)
		}
	}
}

func TestEventStore_SCNSup0578_OperatorFieldsAreRestricted(t *testing.T) {
	st := NewEventStore(t.TempDir())
	length := 5
	for _, kind := range []supervisor.SupervisorEventKind{supervisor.EventKindSendIssued, supervisor.EventKindSendConfirmed, supervisor.EventKindSendAborted, supervisor.EventKindSendUnconfirmed, supervisor.EventKindSendBlocked} {
		event := Event{Kind: kind, SessionID: "alpha-1", SendKind: supervisor.SendKindOperatorInput, TextLength: &length}
		if kind == supervisor.EventKindSendBlocked {
			event.Reason = ReasonState
		}
		if _, err := st.Record(event); err != nil {
			t.Fatalf("valid %s event rejected: %v", kind, err)
		}
	}
	for _, event := range []Event{
		{Kind: supervisor.EventKindSendIssued, SessionID: "alpha-1", SendKind: supervisor.SendKindOperatorInput},
		{Kind: supervisor.EventKindSendIssued, SessionID: "alpha-1", SendKind: supervisor.SendKindRemoteControl, TextLength: &length},
		{Kind: supervisor.EventKindSendBlocked, SessionID: "alpha-1", SendKind: supervisor.SendKindRemoteControl, Reason: ReasonRateLimited},
		{Kind: supervisor.EventKindScreenRead, SessionID: "alpha-1", RowCount: &length, TextLength: &length},
		{Kind: supervisor.EventKindOperatorInputRefused, SessionID: "alpha-1", Reason: ReasonForbiddenText, TextLength: &length},
		{Kind: supervisor.EventKindOperatorInputRefused, SessionID: "alpha-1", Reason: ReasonForbiddenText, SendKind: supervisor.SendKindOperatorInput},
		{Kind: supervisor.EventKindOperatorInputRefused, SessionID: "alpha-1", Reason: ReasonForbiddenText, Code: "SUP_OPERATOR_INPUT_FORBIDDEN"},
		{Kind: supervisor.EventKindOperatorInputRefused, SessionID: "alpha-1", Reason: "free text"},
	} {
		if _, err := st.Record(event); err == nil {
			t.Fatalf("invalid event accepted: %+v", event)
		}
	}
	for _, reason := range []EventReason{ReasonSupervisedCaller, ReasonForbiddenText} {
		if _, err := st.Record(Event{Kind: supervisor.EventKindOperatorInputRefused, SessionID: "alpha-1", Reason: reason}); err != nil {
			t.Fatalf("reason %s rejected: %v", reason, err)
		}
	}
}

func TestEventStoreRestoresSequenceAndOmitsBody(t *testing.T) {
	root := t.TempDir()
	store := NewEventStore(root)
	first, err := store.Record(Event{Kind: supervisor.EventKindSessionStopped, SessionID: "alpha-1", Reason: EventReason(supervisor.StopGlobal)})
	if err != nil {
		t.Fatal(err)
	}
	secondStore := NewEventStore(root)
	second, err := secondStore.Record(Event{Kind: supervisor.EventKindSessionResumed, SessionID: "alpha-1"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Seq != 1 || second.Seq != 2 {
		t.Fatalf("seqs = %d,%d", first.Seq, second.Seq)
	}
	data, err := os.ReadFile(filepath.Join(root, "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret") || strings.Contains(string(data), "body") {
		t.Fatalf("body persisted: %s", data)
	}
	var decoded Event
	if err := json.Unmarshal(bytesFirstLine(data), &decoded); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(filepath.Join(root, "events.jsonl"))
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode = %o", info.Mode().Perm())
	}
}

func TestEventStore_SCNsup0557_LunaReasonVocabulary(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	st := NewEventStore(t.TempDir())
	for _, reason := range []EventReason{"home_unavailable", "config_missing", "config_invalid", "not_allowed"} {
		if _, err := st.Record(Event{Kind: supervisor.EventKindLunaImplNotAllowed, Reason: reason}); err != nil {
			t.Fatalf("reason %q: %v", reason, err)
		}
	}
	if _, err := st.Record(Event{Kind: supervisor.EventKindLunaImplNotAllowed, Reason: "configuration contents"}); err == nil {
		t.Fatal("invalid reason accepted")
	}
}

func TestEventStoreRecordsSessionStartFailureWithoutSessionID(t *testing.T) {
	store := NewEventStore(t.TempDir())
	for _, reason := range []string{"user_settings", "project_settings", "project_local_settings", "claude_json", "managed", "codex_auth", "child_env"} {
		event, err := store.Record(Event{Kind: supervisor.EventKindSessionStartFailed, ProjectID: "demo-project", Reason: EventReason(reason)})
		if err != nil {
			t.Fatalf("reason %q: %v", reason, err)
		}
		if event.SessionID != "" || event.ProjectID != "demo-project" || string(event.Reason) != reason {
			t.Fatalf("start failure event = %#v", event)
		}
	}
	for _, invalid := range []Event{
		{Kind: supervisor.EventKindSessionStartFailed, ProjectID: "not_a_slug", Reason: "codex_auth"},
		{Kind: supervisor.EventKindSessionStartFailed, ProjectID: "demo-project", Reason: "made_up"},
		{Kind: supervisor.EventKindSessionStartFailed, SessionID: "demo-1", ProjectID: "demo-project", Reason: "codex_auth"},
	} {
		if _, err := store.Record(invalid); err == nil {
			t.Errorf("accepted invalid start failure: %#v", invalid)
		}
	}
}

func TestEventStoreRejectsUnknownKindAndRotates(t *testing.T) {
	root := t.TempDir()
	store := NewEventStoreWithOptions(root, 160, 2)
	if _, err := store.Record(Event{Kind: "unknown"}); err == nil {
		t.Fatal("unknown event accepted")
	}
	for i := 0; i < 8; i++ {
		if _, err := store.Record(Event{Kind: supervisor.EventKindStopSet, Reason: ReasonStopGlobal, At: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "events.jsonl.1")); err != nil {
		t.Fatal(err)
	}
}

func TestSCNsup0509_EventRejectsUnknownReasonCodeAndStep(t *testing.T) {
	store := NewEventStore(t.TempDir())
	for _, event := range []Event{
		{Kind: supervisor.EventKindSessionStopped, Reason: "made_up"},
		{Kind: supervisor.EventKindSessionFailed, Code: "MADE_UP"},
		{Kind: supervisor.EventKindRotateStepDone, Step: "made_up"},
	} {
		if _, err := store.Record(event); err == nil {
			t.Errorf("accepted invalid enum fields: %#v", event)
		}
	}
}

func TestEventStore_SCN04_05_IdleWaitTimeoutReason(t *testing.T) {
	store := NewEventStore(t.TempDir())
	if _, err := store.Record(Event{Kind: supervisor.EventKindRotateIdleWaitTimeout, Reason: ReasonIdleWaitTimeout}); err != nil {
		t.Fatalf("rejected idle wait timeout reason: %v", err)
	}
	for _, reason := range []EventReason{ReasonSendNotConfirmed, ReasonStopGlobal, "made_up"} {
		if _, err := store.Record(Event{Kind: supervisor.EventKindRotateIdleWaitTimeout, Reason: reason}); err == nil {
			t.Errorf("accepted invalid timeout reason %q", reason)
		}
	}
}

func TestSCNsup0537_EventSequenceRestoredAfterRotation(t *testing.T) {
	root := t.TempDir()
	first := NewEventStoreWithOptions(root, 100, 2)
	for i := 0; i < 8; i++ {
		if _, err := first.Record(Event{Kind: supervisor.EventKindStopSet}); err != nil {
			t.Fatal(err)
		}
	}
	previous, _, _ := first.ReadAll()
	second, err := NewEventStoreWithOptions(root, 100, 2).Record(Event{Kind: supervisor.EventKindStopCleared})
	if err != nil {
		t.Fatal(err)
	}
	if len(previous) == 0 || second.Seq <= previous[len(previous)-1].Seq {
		t.Fatalf("seq did not increase: prior=%#v next=%d", previous, second.Seq)
	}
}

func TestSCNsup0538_EventLogRotatesAndRetainsGenerations(t *testing.T) {
	root := t.TempDir()
	store := NewEventStoreWithOptions(root, 100, 2)
	for i := 0; i < 20; i++ {
		if _, err := store.Record(Event{Kind: supervisor.EventKindStopSet}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "events.jsonl.2")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "events.jsonl.3")); !os.IsNotExist(err) {
		t.Fatalf("unexpected generation 3: %v", err)
	}
}

func TestSCNsup0549_OperationalLogRotatesAndRetainsGenerations(t *testing.T) {
	root := t.TempDir()
	log, err := NewSupervisorLog(root, 20, 2)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if err := log.Record(SupervisorLogEntry{Level: LogLevelInfo, Kind: supervisor.EventKindSupervisorStarted}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "codexd-supervisor.log.1")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "codexd-supervisor.log.2")); err != nil {
		t.Fatal(err)
	}
}

func TestEventStoreReasonEnumsAndEventSpecificReasons(t *testing.T) {
	store := NewEventStore(t.TempDir())
	reasonEnums := map[supervisor.SupervisorEventKind][]string{
		supervisor.EventKindSendBlocked:           {"stop", "state", "pending", "unconfirmed", "not_fixed_text"},
		supervisor.EventKindRecommendationBlocked: {"force_push", "pull_request", "deploy", "deletion", "production", "billing", "git_history", "migration", "privilege", "undetermined", "repeated", "consecutive_limit"},
		supervisor.EventKindSessionStopped:        {"stop_global", "stop_session", "supervisor_restarted", "send_not_confirmed", "send_aborted", "process_exit_with_pending", "recommendation_forbidden", "recommendation_undetermined", "recommendation_repeated", "auto_continue_limit", "rotate_step_failed", "respawn_failed", "recovery_start_timeout", "claude_session_id_missing", "restart_limit_consecutive", "restart_limit_daily", "orphan_child_alive", "state_file_corrupt"},
		supervisor.EventKindSessionExited:         {"stop_session_requested"},
		supervisor.EventKindRotateFailed:          {"idle_wait_timeout", "send_not_confirmed", "mtime_not_updated", "head_generation_unchanged", "not_idle_after_handoff", "exit_timeout", "spawn_failed", "startup_timeout", "start_not_settled", "child_absent", "restart_limit"},
		supervisor.EventKindCommitFailed:          {"path_escape", "staged_out_of_scope", "out_of_scope_uncommitted", "rename_partial", "git_config_changed", "gitattributes_filter", "git_config_include", "tree_mismatch_before_commit", "commit_rejected", "git_timeout", "git_unavailable", "git_failed", "git_output_too_large"},
		supervisor.EventKindPushBlocked:           {"detached_head", "protected_branch", "not_in_allowlist", "multiple_urls", "url_mismatch", "receivepack_configured", "remote_helper_configured", "git_config_changed", "no_tracking_ref", "foreign_commits", "argv_not_allowed", "git_config_include", "remote_head_unavailable", "remote_head_not_local", "git_failed", "git_output_too_large"},
		supervisor.EventKindVerifyFailed:          {"verify_binary_changed", "verify_cwd_invalid", "verify_exec_failed"},
		supervisor.EventKindRestartLimitExceeded:  {"restart_limit_consecutive", "restart_limit_daily"},
	}
	if _, err := store.Record(Event{Kind: supervisor.EventKindVerifyFailed, Code: "SUP_VERIFY_FAILED"}); err != nil {
		t.Errorf("rejected verify failure without reason: %v", err)
	}
	for kind, reasons := range reasonEnums {
		for _, reason := range reasons {
			if _, err := store.Record(Event{Kind: kind, Reason: EventReason(reason)}); err != nil {
				t.Errorf("rejected canonical %s reason %q: %v", kind, reason, err)
			}
		}
	}
	for _, event := range []Event{
		{Kind: supervisor.EventKindSendBlocked, Reason: "out_of_scope_uncommitted"},
		{Kind: supervisor.EventKindCommitFailed, Reason: "stop"},
		{Kind: supervisor.EventKindSessionExited, Reason: "stop_global"},
	} {
		if _, err := store.Record(event); err == nil {
			t.Errorf("accepted mismatched reason for %s: %q", event.Kind, event.Reason)
		}
	}
}

func TestEventStore_RejectsRemoteControlAsRotationStep(t *testing.T) {
	s := NewEventStore(t.TempDir())
	if _, err := s.Record(Event{Kind: supervisor.EventKindRotateStepDone, Step: "send_remote_control"}); err == nil {
		t.Fatal("remote control was accepted as an eighth rotation step")
	}
}

func TestEventStoreRejectsUntrustedIdentifiersAndEnums(t *testing.T) {
	store := NewEventStore(t.TempDir())
	for _, event := range []Event{
		{Kind: supervisor.EventKindPushSucceeded, Branch: "PROMPT BODY: secret"},
		{Kind: supervisor.EventKindPushSucceeded, Remote: "--upload-pack=bad"},
		{Kind: supervisor.EventKindSendIssued, SendID: "anything goes"},
		{Kind: supervisor.EventKindStateChanged, FromState: "anything goes"},
		{Kind: supervisor.EventKindStateChanged, ToState: "anything goes"},
		{Kind: supervisor.EventKindSendIssued, SendKind: "anything goes"},
		{Kind: supervisor.EventKindStateChanged, Basis: []supervisor.DetectionBasis{"anything goes"}},
		{Kind: supervisor.EventKindCommitCreated, CommitSHA: strings.Repeat("A", 40)},
	} {
		if _, err := store.Record(event); err == nil {
			t.Errorf("accepted invalid event fields: %#v", event)
		}
	}
}

func TestEventStoreRepairsPartialTailAndSkipsBrokenRows(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "events.jsonl")
	if err := os.WriteFile(path, []byte("not-json"), 0600); err != nil {
		t.Fatal(err)
	}
	store := NewEventStore(root)
	if _, err := store.Record(Event{Kind: supervisor.EventKindStopSet}); err != nil {
		t.Fatal(err)
	}
	all, skipped, err := store.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if skipped != 1 || len(all) != 1 || all[0].Seq != 1 {
		t.Fatalf("events=%#v skipped=%d", all, skipped)
	}
}

func TestEventStoreRestoresSequenceFromLargeLastLine(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "events.jsonl")
	large := `{"seq":9,"kind":"stop_set","payload":"` + strings.Repeat("x", 2<<20) + `"}` + "\n"
	if err := os.WriteFile(path, []byte(large), 0600); err != nil {
		t.Fatal(err)
	}
	store := NewEventStore(root)
	event, err := store.Record(Event{Kind: supervisor.EventKindStopSet})
	if err != nil {
		t.Fatal(err)
	}
	if event.Seq != 10 {
		t.Fatalf("seq=%d, want 10", event.Seq)
	}
}

func TestEventStoreContinuesAndWarnsWhenSequenceRestoreFails(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "events.jsonl.1"), 0700); err != nil {
		t.Fatal(err)
	}
	log, err := NewSupervisorLog(t.TempDir(), 1000, 2)
	if err != nil {
		t.Fatal(err)
	}
	event, err := NewEventStoreWithLogger(root, log).Record(Event{Kind: supervisor.EventKindStopSet})
	if err != nil {
		t.Fatal(err)
	}
	if event.Seq != 1 {
		t.Fatalf("seq=%d, want 1 after recovery failure", event.Seq)
	}
	data, err := os.ReadFile(log.path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), string(LogReasonSequenceRestoreFailed)) {
		t.Fatalf("missing sequence restore warning: %s", data)
	}
}

func TestEventStoreRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(t.TempDir(), "target")
	if err := os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, "events.jsonl")); err != nil {
		t.Fatal(err)
	}
	if _, err := NewEventStore(root).Record(Event{Kind: supervisor.EventKindStopSet}); err == nil {
		t.Fatal("followed events symlink")
	}
}

func TestSupervisorLogAcceptsOnlyStructuredRecords(t *testing.T) {
	root := t.TempDir()
	log, err := NewSupervisorLog(root, 1000, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err = log.Record(SupervisorLogEntry{Level: LogLevelWarn, Kind: supervisor.EventKindNotifyFailed, SessionID: "alpha-1", Reason: LogReasonSequenceRestoreFailed}); err != nil {
		t.Fatal(err)
	}
	if err = log.Record(SupervisorLogEntry{Level: LogLevelWarn, Kind: supervisor.EventKindNotifyFailed, Reason: "PROMPT BODY: secret"}); err == nil {
		t.Fatal("accepted arbitrary reason")
	}
	if _, err = os.Stat(filepath.Join(root, "codexd-supervisor.log")); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(root, "codexd-supervisor.log"))
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("operational log mode = %v, err=%v", info, err)
	}
	symlinkDir := t.TempDir()
	target := filepath.Join(t.TempDir(), "target")
	if err = os.WriteFile(target, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(target, filepath.Join(symlinkDir, "codexd-supervisor.log")); err != nil {
		t.Fatal(err)
	}
	if _, err = NewSupervisorLog(symlinkDir, 1000, 2); err == nil {
		t.Fatal("followed operational log symlink")
	}
}

func bytesFirstLine(data []byte) []byte {
	if i := strings.IndexByte(string(data), '\n'); i >= 0 {
		return data[:i]
	}
	return data
}

func TestSensitivePathEventPatternValidatedWithoutPath(t *testing.T) {
	st := NewEventStore(t.TempDir())
	if _, err := st.Record(Event{Kind: supervisor.EventKindSensitivePathBlocked, SessionID: "alpha-1", Code: "SUP_SENSITIVE_PATH_BLOCKED", Pattern: ".env*"}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Record(Event{Kind: supervisor.EventKindSensitivePathBlocked, SessionID: "alpha-1", Code: "SUP_SENSITIVE_PATH_BLOCKED", Pattern: "owned/.env.local"}); err == nil {
		t.Fatal("path accepted as pattern")
	}
}

func TestEventStore_SCN53_PushRejectReasonVocabulary(t *testing.T) {
	reasons := []EventReason{
		ReasonDetachedHead, ReasonProtectedBranch, ReasonNotInAllowlist, ReasonMultipleURLs,
		ReasonURLMismatch, ReasonReceivepackConfigured, ReasonRemoteHelperConfigured, ReasonGitConfigChanged,
		ReasonNoTrackingRef, ReasonForeignCommits, ReasonArgvNotAllowed, ReasonGitConfigInclude,
		ReasonRemoteHeadUnavailable, ReasonRemoteHeadNotLocal, ReasonGitFailed, ReasonGitOutputTooLarge,
	}
	if len(reasons) != 16 {
		t.Fatalf("push reject reason count=%d", len(reasons))
	}
	for _, reason := range reasons {
		if err := validateEvent(Event{Kind: supervisor.EventKindPushBlocked, Reason: reason, Code: "SUP_PUSH_REJECTED_POLICY"}); err != nil {
			t.Errorf("reason %s: %v", reason, err)
		}
	}
	if err := validateEvent(Event{Kind: supervisor.EventKindPushBlocked, Reason: ReasonGitTimeout, Code: "SUP_PUSH_REJECTED_POLICY"}); err == nil {
		t.Fatal("accepted commit-only git_timeout as push reason")
	}
}
