package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// Event contains only fields permitted by FD-sup-05 §16.1; it has no body field.
type Event struct {
	Seq       uint64                            `json:"seq"`
	At        time.Time                         `json:"ts"`
	Kind      supervisor.SupervisorEventKind    `json:"kind"`
	SessionID string                            `json:"session_id,omitempty"`
	FromState supervisor.SupervisorSessionState `json:"from_state,omitempty"`
	ToState   supervisor.SupervisorSessionState `json:"to_state,omitempty"`
	SendID    string                            `json:"send_id,omitempty"`
	SendKind  supervisor.SupervisorSendKind     `json:"send_kind,omitempty"`
	Basis     []supervisor.DetectionBasis       `json:"basis,omitempty"`
	Code      string                            `json:"code,omitempty"`
	Step      string                            `json:"step,omitempty"`
	Reason    EventReason                       `json:"reason,omitempty"`
	Count     int                               `json:"count,omitempty"`
	CommitSHA string                            `json:"commit_sha,omitempty"`
	Remote    string                            `json:"remote,omitempty"`
	Branch    string                            `json:"branch,omitempty"`
}

// Rotation limits are copied from the canonical validation-rules specification.
const eventLogMaxBytes int64 = 10000000
const eventLogRetentionCount = 5

var (
	commitSHAPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
	sendIDPattern    = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)*-s[1-9][0-9]*$`)
)

// EventStore appends and reads the supervisor's event JSONL generations.
type EventStore struct {
	mu              sync.Mutex
	root            string
	maxBytes        int64
	retention       int
	seq             uint64
	initialized     bool
	rootInitialized bool
	now             func() time.Time
	log             *SupervisorLog
}

// NewEventStore creates an append-only event store rooted at stateDir.
func NewEventStore(stateDir string) *EventStore {
	return NewEventStoreWithOptions(stateDir, eventLogMaxBytes, eventLogRetentionCount)
}

// NewEventStoreWithLogger creates an event store that reports recoverable sequence errors to log.
func NewEventStoreWithLogger(stateDir string, log *SupervisorLog) *EventStore {
	store := NewEventStore(stateDir)
	store.log = log
	return store
}

// NewEventStoreWithOptions creates an event store with explicit rotation limits.
func NewEventStoreWithOptions(stateDir string, maxBytes int64, retention int) *EventStore {
	if maxBytes < 1 {
		maxBytes = eventLogMaxBytes
	}
	if retention < 1 {
		retention = eventLogRetentionCount
	}
	return &EventStore{root: stateDir, maxBytes: maxBytes, retention: retention, now: time.Now}
}

func (s *EventStore) path() string { return filepath.Join(s.root, "events.jsonl") }

// Record validates and durably appends one event.
func (s *EventStore) Record(event Event) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateEvent(event); err != nil {
		return Event{}, err
	}
	if err := s.initialize(); err != nil {
		return Event{}, err
	}
	if !s.initialized {
		if err := s.restoreSeq(); err != nil {
			s.warn("sequence_restore_failed")
		}
		s.initialized = true
	}
	info, err := regularFileInfo(s.path())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Event{}, err
	}
	if info != nil && info.Size() > s.maxBytes {
		if err = rotateLog(s.path(), s.retention); err != nil {
			return Event{}, err
		}
	}
	event.Seq = s.seq + 1
	if event.At.IsZero() {
		event.At = s.now().UTC()
	} else {
		event.At = event.At.UTC()
	}
	data, err := json.Marshal(event)
	if err != nil {
		return Event{}, err
	}
	data = append(data, '\n')
	info, err = regularFileInfo(s.path())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return Event{}, err
	}
	if info != nil && info.Size()+int64(len(data)) > s.maxBytes {
		if err = rotateLog(s.path(), s.retention); err != nil {
			return Event{}, err
		}
	}
	fd, err := syscall.Open(s.path(), syscall.O_CREAT|syscall.O_APPEND|syscall.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return Event{}, err
	}
	f := os.NewFile(uintptr(fd), s.path())
	if err = ensureRegular(f); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return Event{}, err
	}
	s.seq = event.Seq
	return event, nil
}

func validateEvent(event Event) error {
	if !supervisor.ValidSupervisorEventKind(event.Kind) {
		return fmt.Errorf("invalid supervisor event kind: %q", event.Kind)
	}
	if event.Reason != "" && !validReasonForEvent(event.Kind, event.Reason) {
		return fmt.Errorf("invalid reason %q for event %q", event.Reason, event.Kind)
	}
	if event.Code != "" && !validEventCode(event.Code) {
		return fmt.Errorf("invalid supervisor event code: %q", event.Code)
	}
	if event.Step != "" && !validEventStep(event.Step) {
		return fmt.Errorf("invalid supervisor event step: %q", event.Step)
	}
	if event.SessionID != "" {
		if _, err := supervisor.NewSessionID(event.SessionID); err != nil {
			return err
		}
	}
	if event.FromState != "" && !validState(event.FromState) || event.ToState != "" && !validState(event.ToState) {
		return errors.New("invalid supervisor session state")
	}
	if event.SendKind != "" && !validSendKind(event.SendKind) {
		return errors.New("invalid supervisor send kind")
	}
	if event.SendID != "" && !sendIDPattern.MatchString(event.SendID) {
		return errors.New("invalid supervisor send id")
	}
	for _, basis := range event.Basis {
		if !validBasis(basis) {
			return errors.New("invalid supervisor detection basis")
		}
	}
	if event.CommitSHA != "" && !commitSHAPattern.MatchString(event.CommitSHA) {
		return errors.New("invalid commit sha")
	}
	if event.Branch != "" && !validGitRef(event.Branch) {
		return errors.New("invalid git branch")
	}
	if event.Remote != "" && !validGitRef(event.Remote) {
		return errors.New("invalid git remote")
	}
	return nil
}

func (s *EventStore) initialize() error {
	if s.rootInitialized {
		return nil
	}
	if err := os.MkdirAll(s.root, 0700); err != nil {
		return err
	}
	if err := os.Chmod(s.root, 0700); err != nil {
		return err
	}
	file, err := openNoFollow(s.path(), os.O_RDWR)
	if err == nil {
		if err = file.Chmod(0600); err == nil {
			err = file.Close()
		} else {
			_ = file.Close()
		}
		if err != nil {
			return err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if !s.initialized {
		if err := repairPartialTail(s.path()); err != nil {
			return err
		}
	}
	s.rootInitialized = true
	return nil
}

func (s *EventStore) warn(reason string) {
	if s.log != nil {
		_ = s.log.Record(SupervisorLogEntry{Level: LogLevelWarn, Kind: supervisor.EventKindNotifyFailed, Reason: SupervisorLogReason(reason)})
	}
}

func validBasis(basis supervisor.DetectionBasis) bool {
	switch basis {
	case supervisor.DetectionScreenPattern, supervisor.DetectionOutputQuiet, supervisor.DetectionProcessExit, supervisor.DetectionStructuredInfo:
		return true
	default:
		return false
	}
}

func validGitRef(value string) bool {
	if value == "" || value == "@" || strings.HasPrefix(value, "-") || strings.HasPrefix(value, ".") || strings.HasSuffix(value, ".") || strings.HasSuffix(value, ".lock") || strings.Contains(value, "..") || strings.Contains(value, "@{") || strings.ContainsAny(value, " ~^:?*[\\") {
		return false
	}
	if strings.HasPrefix(value, "/") || strings.HasSuffix(value, "/") || strings.Contains(value, "//") {
		return false
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || component == "." || component == ".." || strings.HasPrefix(component, ".") || strings.HasSuffix(component, ".lock") {
			return false
		}
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}

// Reason types mirror the canonical reason enums in enums.md and FD-sup-02/03/05.
// EventReason is a canonical reason value used by supervisor event records.
type EventReason string

const (
	ReasonStop                       EventReason = "stop"
	ReasonState                      EventReason = "state"
	ReasonPending                    EventReason = "pending"
	ReasonUnconfirmed                EventReason = "unconfirmed"
	ReasonNotFixedText               EventReason = "not_fixed_text"
	ReasonForcePush                  EventReason = "force_push"
	ReasonPullRequest                EventReason = "pull_request"
	ReasonDeploy                     EventReason = "deploy"
	ReasonDeletion                   EventReason = "deletion"
	ReasonProduction                 EventReason = "production"
	ReasonBilling                    EventReason = "billing"
	ReasonGitHistory                 EventReason = "git_history"
	ReasonMigration                  EventReason = "migration"
	ReasonPrivilege                  EventReason = "privilege"
	ReasonUndetermined               EventReason = "undetermined"
	ReasonRepeated                   EventReason = "repeated"
	ReasonConsecutiveLimit           EventReason = "consecutive_limit"
	ReasonStopGlobal                 EventReason = "stop_global"
	ReasonStopSession                EventReason = "stop_session"
	ReasonSupervisorRestarted        EventReason = "supervisor_restarted"
	ReasonSendNotConfirmed           EventReason = "send_not_confirmed"
	ReasonSendAborted                EventReason = "send_aborted"
	ReasonProcessExitWithPending     EventReason = "process_exit_with_pending"
	ReasonRecommendationForbidden    EventReason = "recommendation_forbidden"
	ReasonRecommendationUndetermined EventReason = "recommendation_undetermined"
	ReasonRecommendationRepeated     EventReason = "recommendation_repeated"
	ReasonAutoContinueLimit          EventReason = "auto_continue_limit"
	ReasonRotateStepFailed           EventReason = "rotate_step_failed"
	ReasonRespawnFailed              EventReason = "respawn_failed"
	ReasonRecoveryStartTimeout       EventReason = "recovery_start_timeout"
	ReasonClaudeSessionIDMissing     EventReason = "claude_session_id_missing"
	ReasonRestartLimitConsecutive    EventReason = "restart_limit_consecutive"
	ReasonRestartLimitDaily          EventReason = "restart_limit_daily"
	ReasonOrphanChildAlive           EventReason = "orphan_child_alive"
	ReasonStateFileCorrupt           EventReason = "state_file_corrupt"
	ReasonExitProcessAlive           EventReason = "exit_process_alive"
	ReasonStopRecreated              EventReason = "stop_recreated"
	ReasonStillStoppedByGlobal       EventReason = "still_stopped_by_global"
	ReasonNotResumedByGlobal         EventReason = "reason_not_resumed_by_global"
	ReasonIdleWaitTimeout            EventReason = "idle_wait_timeout"
	ReasonMtimeNotUpdated            EventReason = "mtime_not_updated"
	ReasonHeadGenerationUnchanged    EventReason = "head_generation_unchanged"
	ReasonNotIdleAfterHandoff        EventReason = "not_idle_after_handoff"
	ReasonExitTimeout                EventReason = "exit_timeout"
	ReasonSpawnFailed                EventReason = "spawn_failed"
	ReasonStartupTimeout             EventReason = "startup_timeout"
	ReasonStartNotSettled            EventReason = "start_not_settled"
	ReasonChildAbsent                EventReason = "child_absent"
	ReasonRestartLimit               EventReason = "restart_limit"
	ReasonPathEscape                 EventReason = "path_escape"
	ReasonStagedOutOfScope           EventReason = "staged_out_of_scope"
	ReasonOutOfScopeUncommitted      EventReason = "out_of_scope_uncommitted"
	ReasonRenamePartial              EventReason = "rename_partial"
	ReasonGitConfigChanged           EventReason = "git_config_changed"
	ReasonGitattributesFilter        EventReason = "gitattributes_filter"
	ReasonGitConfigInclude           EventReason = "git_config_include"
	ReasonTreeMismatchBeforeCommit   EventReason = "tree_mismatch_before_commit"
	ReasonCommitRejected             EventReason = "commit_rejected"
	ReasonGitTimeout                 EventReason = "git_timeout"
	ReasonGitUnavailable             EventReason = "git_unavailable"
	ReasonDetachedHead               EventReason = "detached_head"
	ReasonProtectedBranch            EventReason = "protected_branch"
	ReasonNotInAllowlist             EventReason = "not_in_allowlist"
	ReasonMultipleURLs               EventReason = "multiple_urls"
	ReasonURLMismatch                EventReason = "url_mismatch"
	ReasonReceivepackConfigured      EventReason = "receivepack_configured"
	ReasonNoTrackingRef              EventReason = "no_tracking_ref"
	ReasonForeignCommits             EventReason = "foreign_commits"
	ReasonArgvNotAllowed             EventReason = "argv_not_allowed"
	ReasonRemoteHeadUnavailable      EventReason = "remote_head_unavailable"
	ReasonRemoteHeadNotLocal         EventReason = "remote_head_not_local"
	ReasonVerifyBinaryChanged        EventReason = "verify_binary_changed"
	ReasonStopSessionRequested       EventReason = "stop_session_requested"
)

func validReasonForEvent(kind supervisor.SupervisorEventKind, value EventReason) bool {
	contains := func(values ...EventReason) bool {
		for _, candidate := range values {
			if value == candidate {
				return true
			}
		}
		return false
	}
	switch kind {
	case supervisor.EventKindSendBlocked:
		return contains(ReasonStop, ReasonState, ReasonPending, ReasonUnconfirmed, ReasonNotFixedText)
	case supervisor.EventKindRecommendationBlocked:
		return contains(ReasonForcePush, ReasonPullRequest, ReasonDeploy, ReasonDeletion, ReasonProduction, ReasonBilling, ReasonGitHistory, ReasonMigration, ReasonPrivilege, ReasonUndetermined, ReasonRepeated, ReasonConsecutiveLimit)
	case supervisor.EventKindCommitFailed:
		return contains(ReasonPathEscape, ReasonStagedOutOfScope, ReasonOutOfScopeUncommitted, ReasonRenamePartial, ReasonGitConfigChanged, ReasonGitattributesFilter, ReasonGitConfigInclude, ReasonTreeMismatchBeforeCommit, ReasonCommitRejected, ReasonGitTimeout, ReasonGitUnavailable)
	case supervisor.EventKindPushBlocked:
		return contains(ReasonDetachedHead, ReasonProtectedBranch, ReasonNotInAllowlist, ReasonMultipleURLs, ReasonURLMismatch, ReasonReceivepackConfigured, ReasonGitConfigChanged, ReasonNoTrackingRef, ReasonForeignCommits, ReasonArgvNotAllowed, ReasonGitConfigInclude, ReasonRemoteHeadUnavailable, ReasonRemoteHeadNotLocal)
	case supervisor.EventKindVerifyFailed:
		return value == ReasonVerifyBinaryChanged
	case supervisor.EventKindSessionStopped:
		return contains(ReasonStopGlobal, ReasonStopSession, ReasonSupervisorRestarted, ReasonSendNotConfirmed, ReasonSendAborted, ReasonProcessExitWithPending, ReasonRecommendationForbidden, ReasonRecommendationUndetermined, ReasonRecommendationRepeated, ReasonAutoContinueLimit, ReasonRotateStepFailed, ReasonRespawnFailed, ReasonRecoveryStartTimeout, ReasonClaudeSessionIDMissing, ReasonRestartLimitConsecutive, ReasonRestartLimitDaily, ReasonOrphanChildAlive, ReasonStateFileCorrupt)
	case supervisor.EventKindStopSet:
		return contains(ReasonStopGlobal, ReasonStopSession)
	case supervisor.EventKindRestartLimitExceeded:
		return contains(ReasonRestartLimitConsecutive, ReasonRestartLimitDaily)
	case supervisor.EventKindSessionExited:
		return value == ReasonStopSessionRequested
	case supervisor.EventKindRotateFailed:
		return contains(ReasonIdleWaitTimeout, ReasonSendNotConfirmed, ReasonMtimeNotUpdated, ReasonHeadGenerationUnchanged, ReasonNotIdleAfterHandoff, ReasonExitTimeout, ReasonSpawnFailed, ReasonStartupTimeout, ReasonStartNotSettled, ReasonChildAbsent, ReasonRestartLimit)
	default:
		return false
	}
}

func validEventCode(v string) bool {
	switch v {
	case "SUP_CONFIG_INVALID", "SUP_INSTANCE_LOCK_HELD", "SUP_PROJECT_NOT_FOUND", "SUP_SESSION_ALREADY_RUNNING", "SUP_SESSION_LIMIT_REACHED", "SUP_SESSION_NOT_FOUND", "SUP_SPAWN_FAILED", "SUP_ORPHAN_CHILD_ALIVE", "SUP_STATE_FILE_CORRUPT", "SUP_ILLEGAL_TRANSITION", "SUP_SEND_NOT_CONFIRMED", "SUP_SEND_BLOCKED", "SUP_RECOMMENDATION_FORBIDDEN", "SUP_RECOMMENDATION_UNDETERMINED", "SUP_PERMISSION_PENDING", "SUP_PERMISSION_MODE_UNSAFE", "SUP_CONFIG_PATH_CONFLICT", "SUP_BILLING_GUARD_FAILED", "SUP_SEND_ABORTED", "SUP_RECOMMENDATION_REPEATED", "SUP_AUTO_CONTINUE_LIMIT_REACHED", "SUP_RESUME_FORBIDDEN", "SUP_VERIFY_FAILED", "SUP_SENSITIVE_PATH_BLOCKED", "SUP_COMMIT_FAILED", "SUP_PUSH_REJECTED_POLICY", "SUP_PUSH_FAILED", "SUP_ROTATE_STEP_FAILED", "SUP_STOP_ACTIVE", "SUP_RESTART_LIMIT_EXCEEDED", "SUP_NOTIFY_FAILED", "SUP_CLIENT_UNAVAILABLE", "SUP_VERB_UNKNOWN", "SUP_REQUEST_MALFORMED", "SUP_PEER_REJECTED":
		return true
	}
	return false
}

func validEventStep(v string) bool {
	switch v {
	case "wait_idle", "send_end", "confirm_handoff", "send_exit", "restart", "send_start", "send_parallel_instruction", "send_remote_control":
		return true
	}
	return false
}

func (s *EventStore) restoreSeq() error {
	for _, path := range []string{fmt.Sprintf("%s.1", s.path()), s.path()} {
		f, err := openNoFollow(path, os.O_RDONLY)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		seq, readErr := lastSequence(f)
		if readErr == nil && seq > s.seq {
			s.seq = seq
		}
		if err = f.Close(); err != nil {
			return err
		}
		if readErr != nil {
			return readErr
		}
	}
	return nil
}

func lastSequence(file *os.File) (uint64, error) {
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	position := info.Size()
	line := make([]byte, 0, 4096)
	chunk := make([]byte, 32*1024)
	for position > 0 {
		start := position - int64(len(chunk))
		if start < 0 {
			start = 0
		}
		count := int(position - start)
		if _, err = file.ReadAt(chunk[:count], start); err != nil && !errors.Is(err, io.EOF) {
			return 0, err
		}
		for index := count - 1; index >= 0; index-- {
			if chunk[index] == '\n' && len(line) > 0 {
				var event Event
				if json.Unmarshal(reverseBytes(line), &event) == nil {
					return event.Seq, nil
				}
				line = line[:0]
			} else if chunk[index] != '\n' {
				line = append(line, chunk[index])
			}
		}
		position = start
	}
	if len(line) > 0 {
		var event Event
		if json.Unmarshal(reverseBytes(line), &event) == nil {
			return event.Seq, nil
		}
	}
	return 0, nil
}

func reverseBytes(data []byte) []byte {
	for left, right := 0, len(data)-1; left < right; left, right = left+1, right-1 {
		data[left], data[right] = data[right], data[left]
	}
	return data
}

// ReadAll returns events ordered by sequence and the number of malformed rows skipped.
func (s *EventStore) ReadAll() ([]Event, int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.initialize(); err != nil {
		return nil, 0, err
	}
	paths := make([]string, 0, s.retention+1)
	for i := s.retention; i >= 1; i-- {
		paths = append(paths, fmt.Sprintf("%s.%d", s.path(), i))
	}
	paths = append(paths, s.path())
	all, skipped := []Event{}, 0
	for _, path := range paths {
		f, err := openNoFollow(path, os.O_RDONLY)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, skipped, err
		}
		r := bufio.NewReader(f)
		for {
			line, readErr := r.ReadBytes('\n')
			if len(line) > 0 {
				var event Event
				if json.Unmarshal(bytesTrimNewline(line), &event) != nil {
					skipped++
				} else {
					all = append(all, event)
				}
			}
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				_ = f.Close()
				return nil, skipped, readErr
			}
		}
		if err = f.Close(); err != nil {
			return nil, skipped, err
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Seq < all[j].Seq })
	return all, skipped, nil
}

func bytesTrimNewline(line []byte) []byte { return bytes.TrimSuffix(line, []byte{'\n'}) }
