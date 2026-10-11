package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	atom "github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var ErrStateFileCorrupt = errors.New("SUP_STATE_FILE_CORRUPT")

type SessionStore struct {
	root        string
	writeAtomic func(string, []byte, os.FileMode) error
}

// NewSessionStore creates a store rooted at stateDir.
func NewSessionStore(stateDir string) *SessionStore {
	return &SessionStore{root: stateDir, writeAtomic: atom.WriteAtomic}
}
func (s *SessionStore) path(id supervisor.SessionID) string {
	path, _ := s.sessionPath(id)
	return path
}

func (s *SessionStore) sessionPath(id supervisor.SessionID) (string, error) {
	valid, err := supervisor.NewSessionID(id.String())
	if err != nil {
		return "", fmt.Errorf("%w: invalid session id", ErrStateFileCorrupt)
	}
	root, err := filepath.Abs(s.root)
	if err != nil {
		return "", err
	}
	base := filepath.Join(root, "sessions")
	path := filepath.Join(base, valid.String()+".json")
	rel, err := filepath.Rel(base, path)
	if err != nil || strings.HasPrefix(rel, "..") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("%w: session path escapes store", ErrStateFileCorrupt)
	}
	return path, nil
}

// Save validates and atomically persists a session record.
func (s *SessionStore) Save(v supervisor.ManagedSession) error {
	path, err := s.sessionPath(v.SessionID)
	if err != nil {
		return err
	}
	if v.SchemaVersion == 0 {
		v.SchemaVersion = 1
	}
	if v.SendGate.NextSeq == 0 {
		v.SendGate.NextSeq = 1
	}
	if v.RestartTimes == nil {
		v.RestartTimes = []time.Time{}
	}
	if v.StopScope == "" {
		v.StopScope = supervisor.StopScopeNone
	}
	if err = validateSession(v, v.SessionID); err != nil {
		return fmt.Errorf("%w: %v", ErrStateFileCorrupt, err)
	}
	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err = os.Chmod(dir, 0700); err != nil {
		return err
	}
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return s.writeAtomic(path, append(data, '\n'), 0600)
}

// Load reads a session record, quarantining corrupt records before returning an error.
func (s *SessionStore) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	path, err := s.sessionPath(id)
	if err != nil {
		return supervisor.ManagedSession{}, err
	}
	data, readErr := os.ReadFile(path)
	if readErr != nil {
		if errors.Is(readErr, os.ErrNotExist) {
			return supervisor.ManagedSession{}, readErr
		}
		return supervisor.ManagedSession{}, s.quarantine(path, readErr)
	}
	var v supervisor.ManagedSession
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	err = dec.Decode(&v)
	if err == nil {
		var extra any
		if e := dec.Decode(&extra); e != io.EOF {
			if e == nil {
				err = fmt.Errorf("trailing data")
			} else {
				err = e
			}
		}
	}
	if err == nil {
		err = validateSession(v, id)
	}
	if err != nil {
		return supervisor.ManagedSession{}, s.quarantine(path, err)
	}
	return v, nil
}
func (s *SessionStore) quarantine(path string, cause error) error {
	name := path + ".corrupt-" + time.Now().UTC().Format("20060102T150405.000000000")
	if e := os.Rename(path, name); e != nil {
		return fmt.Errorf("%w: %v (quarantine: %v)", ErrStateFileCorrupt, cause, e)
	}
	return fmt.Errorf("%w: %v", ErrStateFileCorrupt, cause)
}

// CorruptSessionIDs lists valid session IDs whose records have been quarantined.
func (s *SessionStore) CorruptSessionIDs() ([]supervisor.SessionID, error) {
	files, err := filepath.Glob(filepath.Join(s.root, "sessions", "*.json.corrupt-*"))
	if err != nil {
		return nil, err
	}
	out := make([]supervisor.SessionID, 0, len(files))
	for _, f := range files {
		name := filepath.Base(f)
		name = strings.TrimSuffix(name, filepath.Ext(strings.SplitN(name, ".corrupt-", 2)[0]))
		name = strings.SplitN(name, ".json.corrupt-", 2)[0]
		id, e := supervisor.NewSessionID(name)
		if e == nil {
			out = append(out, id)
		}
	}
	return out, nil
}

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
var shaPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var fingerprintPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func validateSession(v supervisor.ManagedSession, id supervisor.SessionID) error {
	if _, e := supervisor.NewSessionID(id.String()); e != nil {
		return e
	}
	if v.SchemaVersion != 1 || v.SessionID != id || !validState(v.State) || !validCLI(v.CLI) || v.Generation < 1 || v.StateSince.IsZero() || v.SendGate.NextSeq == 0 || v.RestartTimes == nil || v.ChildPID < 0 {
		return fmt.Errorf("invalid session fields")
	}
	if !validStopScope(v.StopScope) || !validStopReason(v.StopReason) || !validExitReason(v.ExitReason) || v.PreviousState != "" && !validState(v.PreviousState) {
		return fmt.Errorf("invalid enum")
	}
	if v.State != supervisor.SessionStopped && v.StopReason != "" {
		return fmt.Errorf("stop data outside stopped state")
	}
	if v.State != supervisor.SessionStopped && v.StopScope != supervisor.StopScopeNone {
		return fmt.Errorf("stop scope outside stopped state")
	}
	if v.State != supervisor.SessionExited && v.ExitReason != "" {
		return fmt.Errorf("exit reason state mismatch")
	}
	if v.ProjectID == "" || !slugPattern.MatchString(v.ProjectID) {
		return fmt.Errorf("invalid project id")
	}
	if v.ClaudeSessionID != "" && !uuidPattern.MatchString(v.ClaudeSessionID) || v.PreviousClaudeSessionID != "" && !uuidPattern.MatchString(v.PreviousClaudeSessionID) {
		return fmt.Errorf("invalid UUID")
	}
	for _, fp := range []string{v.LastVerifiedFingerprint, v.LastFailureFingerprint, v.GitConfigFingerprint} {
		if fp != "" && !fingerprintPattern.MatchString(fp) {
			return fmt.Errorf("invalid fingerprint")
		}
	}
	for _, fp := range []string{v.LastPushFailedHead, func() string {
		if v.Rotation != nil {
			return v.Rotation.CheckpointSnapshot.HeadHash
		}
		return ""
	}()} {
		if fp != "" && !shaPattern.MatchString(fp) {
			return fmt.Errorf("invalid fingerprint")
		}
	}
	if v.SendGate.Pending != nil && !validSendKind(v.SendGate.Pending.Kind) || v.LastSend != nil && !validSendKind(v.LastSend.Kind) {
		return fmt.Errorf("invalid send kind")
	}
	if v.SendGate.Pending != nil && !validSendID(v.SessionID, v.SendGate.Pending.SendID) || v.LastSend != nil && !validSendID(v.SessionID, v.LastSend.SendID) {
		return fmt.Errorf("invalid send id")
	}
	if v.LastVerifiedResult != "" && v.LastVerifiedResult != "passed" && v.LastVerifiedResult != "failed" {
		return fmt.Errorf("invalid verified result")
	}
	if v.Rotation != nil && (!validRotationStep(v.Rotation.Step) || !validRotationFailure(string(v.Rotation.FailReason)) || v.Rotation.CheckpointSnapshot.HeadHash != "" && !shaPattern.MatchString(v.Rotation.CheckpointSnapshot.HeadHash)) {
		return fmt.Errorf("invalid rotation")
	}
	if v.LastFailureReason != "" && !validFailure(string(v.LastFailureReason)) {
		return fmt.Errorf("invalid failure reason")
	}
	return nil
}

var slugPattern = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)

func validState(x supervisor.SupervisorSessionState) bool {
	switch x {
	case supervisor.SessionStarting, supervisor.SessionIdle, supervisor.SessionWorking, supervisor.SessionAwaitingPermission, supervisor.SessionAwaitingRecommendation, supervisor.SessionRotating, supervisor.SessionRecovering, supervisor.SessionStopped, supervisor.SessionExited, supervisor.SessionFailed:
		return true
	}
	return false
}
func validCLI(x supervisor.SupervisorCli) bool {
	return x == supervisor.CLIClaude || x == supervisor.CLICodex
}
func validStopScope(x supervisor.SupervisorStopScope) bool {
	return x == supervisor.StopScopeNone || x == supervisor.StopScopeSession || x == supervisor.StopScopeGlobal
}
func validStopReason(x supervisor.SupervisorStopReason) bool {
	switch x {
	case "", supervisor.StopGlobal, supervisor.StopSession, supervisor.StopSupervisorRestarted, supervisor.StopSendNotConfirmed, supervisor.StopSendAborted, supervisor.StopProcessExitWithPending, supervisor.StopRecommendationForbidden, supervisor.StopRecommendationUndetermined, supervisor.StopRecommendationRepeated, supervisor.StopAutoContinueLimit, supervisor.StopRotateStepFailed, supervisor.StopRespawnFailed, supervisor.StopRecoveryStartTimeout, supervisor.StopClaudeSessionIDMissing, supervisor.StopRestartLimitConsecutive, supervisor.StopRestartLimitDaily, supervisor.StopOrphanChildAlive, supervisor.StopStateFileCorrupt:
		return true
	}
	return false
}
func validExitReason(x supervisor.SupervisorExitReason) bool {
	return x == "" || x == supervisor.ExitStopSessionRequested
}
func validSendKind(x supervisor.SupervisorSendKind) bool {
	switch x {
	case supervisor.SendKindRecommendationContinue, supervisor.SendKindRemoteControl, supervisor.SendKindHandoffEnd, supervisor.SendKindSessionStart, supervisor.SendKindParallelInstruction, supervisor.SendKindHandoffExit:
		return true
	}
	return false
}
func validRotationStep(x supervisor.SupervisorRotateStep) bool {
	switch x {
	case "", supervisor.RotateWaitIdle, supervisor.RotateSendEnd, supervisor.RotateConfirmHandoff, supervisor.RotateSendExit, supervisor.RotateRestart, supervisor.RotateSendStart, supervisor.RotateSendParallelInstruction, supervisor.RotateSendRemoteControl:
		return true
	}
	return false
}
func validFailure(x string) bool {
	switch x {
	case "", "path_escape", "staged_out_of_scope", "out_of_scope_uncommitted", "rename_partial", "git_config_changed", "gitattributes_filter", "git_config_include", "tree_mismatch_before_commit", "commit_rejected", "git_timeout", "git_unavailable", "verify_binary_changed":
		return true
	}
	return false
}

func validRotationFailure(x string) bool {
	switch x {
	case "", "idle_wait_timeout", "send_not_confirmed", "mtime_not_updated", "head_generation_unchanged", "not_idle_after_handoff", "exit_timeout", "spawn_failed", "startup_timeout", "start_not_settled", "child_absent", "restart_limit":
		return true
	}
	return false
}

func validSendID(id supervisor.SessionID, sendID string) bool {
	return regexp.MustCompile(`^` + regexp.QuoteMeta(id.String()) + `-s[1-9][0-9]*$`).MatchString(sendID)
}
