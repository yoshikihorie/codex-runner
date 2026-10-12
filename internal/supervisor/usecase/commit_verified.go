package usecase

import (
	"context"
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/gitops"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

// SUPERVISOR_COMMIT_SETTLE_SECONDS in validation-rules.md.
const (
	commitSettle = 30 * time.Second
	// Error codes are copied from specifications/10-shared/published-language/error-codes.md.
	commitFailedCode         = "SUP_COMMIT_FAILED"
	verifyFailedCode         = "SUP_VERIFY_FAILED"
	sensitivePathBlockedCode = "SUP_SENSITIVE_PATH_BLOCKED"
)

type commitGit interface {
	RevParseToplevel(context.Context) ([]byte, error)
	RevParseGitCommonDir(context.Context) ([]byte, error)
	RevParseGitDir(context.Context) ([]byte, error)
	RevParseHead(context.Context) ([]byte, error)
	Status(context.Context) ([]byte, error)
	CheckAttrFilter(context.Context, []string) ([]byte, error)
	Add(context.Context, []string) error
	LsFilesStage(context.Context, []string) ([]byte, error)
	ResetPaths(context.Context, []string) error
	DiffCached(context.Context) ([]byte, error)
	WriteTree(context.Context) ([]byte, error)
	Commit(context.Context, string) error
}

type commitVerifier interface {
	Run(context.Context, []string, string, string) (int, error)
}

type commitProjectStore interface{ AppendCommit(string, string) error }

var commitSessionLocks sync.Map // session ID -> *sync.Mutex
var errOwnedPathEscape = errors.New("owned path escapes repository")

// CommitLocksHeld marks the session and repository locks held during AfterCommit.
// AfterCommit implementations must not acquire the same RepoLocker again.
type CommitLocksHeld struct{ session supervisor.SessionID }

// CommitVerifiedChangesUseCase evaluates one settled session.
type CommitVerifiedChangesUseCase struct {
	Git                 commitGit
	Verify              commitVerifier
	Sessions            SessionRecordStore
	Projects            commitProjectStore
	Stop                *StopState
	Locker              *gitops.RepoLocker
	CWD                 string
	OwnedPaths          []string
	VerifyCommand       []string
	VerifyCommandSHA256 string
	Record              func(store.Event) error
	Notify              func(supervisor.SupervisorEventKind, supervisor.SessionID)
	// AfterCommit runs with the session and repository locks already held.
	// It must not acquire the same RepoLocker again.
	AfterCommit func(context.Context, supervisor.ManagedSession, CommitLocksHeld) error
	// AfterGitPhase requests auto-continue re-evaluation after git_phase becomes none.
	AfterGitPhase     func(supervisor.SessionID, time.Time)
	Now               func() time.Time
	gitPhaseOnce      sync.Once
	gitPhaseMu        sync.Mutex
	gitPhaseListeners map[supervisor.SessionID]map[uint64]func(time.Time)
	gitPhaseNext      uint64
	suppressMu        sync.Mutex
	suppressed        map[supervisor.SessionID]string
}

// RegisterGitPhase installs one stable dispatcher and registers a session observer.
func (u *CommitVerifiedChangesUseCase) RegisterGitPhase(id supervisor.SessionID, notify func(time.Time)) func() {
	u.gitPhaseOnce.Do(func() {
		u.gitPhaseMu.Lock()
		defer u.gitPhaseMu.Unlock()
		previous := u.AfterGitPhase
		u.AfterGitPhase = func(session supervisor.SessionID, since time.Time) {
			if previous != nil {
				previous(session, since)
			}
			u.gitPhaseMu.Lock()
			listeners := make([]func(time.Time), 0, len(u.gitPhaseListeners[session]))
			for _, listener := range u.gitPhaseListeners[session] {
				listeners = append(listeners, listener)
			}
			u.gitPhaseMu.Unlock()
			for _, listener := range listeners {
				listener(since)
			}
		}
	})
	u.gitPhaseMu.Lock()
	defer u.gitPhaseMu.Unlock()
	if u.gitPhaseListeners == nil {
		u.gitPhaseListeners = make(map[supervisor.SessionID]map[uint64]func(time.Time))
	}
	if u.gitPhaseListeners[id] == nil {
		u.gitPhaseListeners[id] = make(map[uint64]func(time.Time))
	}
	u.gitPhaseNext++
	key := u.gitPhaseNext
	u.gitPhaseListeners[id][key] = notify
	return func() {
		u.gitPhaseMu.Lock()
		defer u.gitPhaseMu.Unlock()
		delete(u.gitPhaseListeners[id], key)
		if len(u.gitPhaseListeners[id]) == 0 {
			delete(u.gitPhaseListeners, id)
		}
	}
}

type commitSnapshot struct {
	entries        []gitops.StatusEntry
	owned          []gitops.StatusEntry
	paths          []string
	blobs          map[string]commitBlob
	fingerprint    string
	classification gitops.StatusClassification
}
type commitBlob struct{ mode, oid string }

func gitBlobID(data []byte) string {
	h := sha1.New()
	_, _ = fmt.Fprintf(h, "blob %d\x00", len(data))
	_, _ = h.Write(data)
	return fmt.Sprintf("%x", h.Sum(nil))
}

func commitFileBlob(root, path string) (commitBlob, error) {
	full := filepath.Join(root, filepath.FromSlash(path))
	info, err := os.Lstat(full)
	if errors.Is(err, os.ErrNotExist) {
		return commitBlob{}, nil
	}
	if err != nil {
		return commitBlob{}, err
	}
	var data []byte
	var mode string
	if info.Mode()&os.ModeSymlink != 0 {
		link, err := os.Readlink(full)
		if err != nil {
			return commitBlob{}, err
		}
		data, mode = []byte(link), "120000"
	} else if info.Mode().IsRegular() {
		data, err = os.ReadFile(full)
		if err != nil {
			return commitBlob{}, err
		}
		mode = "100644"
		if info.Mode()&0111 != 0 {
			mode = "100755"
		}
	} else {
		return commitBlob{}, fmt.Errorf("unsupported file type")
	}
	return commitBlob{mode: mode, oid: gitBlobID(data)}, nil
}

func (u *CommitVerifiedChangesUseCase) stopped(id supervisor.SessionID) bool {
	return u.Stop == nil || u.Stop.IsStopped(id)
}
func (u *CommitVerifiedChangesUseCase) record(e store.Event) error {
	if u.Record == nil {
		return nil
	}
	return u.Record(e)
}
func (u *CommitVerifiedChangesUseCase) event(s supervisor.ManagedSession, kind supervisor.SupervisorEventKind, code string, reason store.EventReason, count int, sha string, notify bool, pattern ...string) error {
	matched := ""
	if len(pattern) != 0 {
		matched = pattern[0]
	}
	err := u.record(store.Event{Kind: kind, SessionID: s.SessionID.String(), Code: code, Reason: reason, Count: count, CommitSHA: sha, Pattern: matched})
	if notify && u.Notify != nil {
		u.Notify(kind, s.SessionID)
	}
	return err
}
func (u *CommitVerifiedChangesUseCase) fail(s supervisor.ManagedSession, fingerprint string, kind supervisor.SupervisorEventKind, code string, reason store.EventReason, count int, notify bool, pattern ...string) error {
	if fingerprint != "" && s.LastFailureFingerprint == fingerprint && s.LastFailureReason == supervisor.SupervisorFailureReason(reason) {
		return u.suppressFailure(s, fingerprint)
	}
	if fingerprint != "" && u.Sessions != nil {
		if err := UpdateSession(u.Sessions, s.SessionID, func(current *supervisor.ManagedSession) error {
			current.LastFailureFingerprint = fingerprint
			current.LastFailureReason = supervisor.SupervisorFailureReason(reason)
			return nil
		}); err != nil {
			return err
		}
	}
	return u.event(s, kind, code, reason, count, "", notify, pattern...)
}
func (u *CommitVerifiedChangesUseCase) suppressFailure(s supervisor.ManagedSession, fingerprint string) error {
	u.suppressMu.Lock()
	if u.suppressed == nil {
		u.suppressed = make(map[supervisor.SessionID]string)
	}
	if u.suppressed[s.SessionID] == fingerprint {
		u.suppressMu.Unlock()
		return nil
	}
	u.suppressed[s.SessionID] = fingerprint
	u.suppressMu.Unlock()
	return u.event(s, supervisor.EventKindCommitSuppressedSameFingerprint, "", "", 0, "", false)
}
func (u *CommitVerifiedChangesUseCase) gitFail(s supervisor.ManagedSession, fp string, err error) error {
	reason := store.ReasonGitFailed
	switch {
	case errors.Is(err, gitops.ErrGitTimeout):
		reason = store.ReasonGitTimeout
	case strings.Contains(err.Error(), "git_unavailable"):
		reason = store.ReasonGitUnavailable
	case strings.Contains(err.Error(), "git_output_too_large"):
		reason = store.ReasonGitOutputTooLarge
	}
	return u.fail(s, fp, supervisor.EventKindCommitFailed, commitFailedCode, reason, 0, true)
}
func (u *CommitVerifiedChangesUseCase) configFingerprint(ctx context.Context) (string, bool, error) {
	common, err := u.Git.RevParseGitCommonDir(ctx)
	if err != nil {
		return "", false, err
	}
	gitDir, err := u.Git.RevParseGitDir(ctx)
	if err != nil {
		return "", false, err
	}
	entries, include, err := gitops.BuildGitConfigFingerprintEntries(strings.TrimSpace(string(common)), strings.TrimSpace(string(gitDir)))
	if err != nil {
		return "", true, err
	}
	return gitops.GitConfigFingerprintFromEntries(entries).SHA256, include, nil
}
func (u *CommitVerifiedChangesUseCase) failureFingerprint(ctx context.Context, root string, scope *gitops.PathScope) string {
	snapshot, err := u.snapshot(ctx, root, scope)
	if err != nil {
		return ""
	}
	return snapshot.fingerprint
}
func (u *CommitVerifiedChangesUseCase) snapshot(ctx context.Context, root string, scope *gitops.PathScope) (commitSnapshot, error) {
	var result commitSnapshot
	status, err := u.Git.Status(ctx)
	if err != nil {
		return result, err
	}
	entries, err := gitops.ParsePorcelainV1(status)
	if err != nil {
		return result, err
	}
	result.entries = entries
	result.classification = gitops.ClassifyStatus(entries, scope)
	head, err := u.Git.RevParseHead(ctx)
	if err != nil {
		return result, err
	}
	result.blobs = make(map[string]commitBlob)
	framed := make([]gitops.FingerprintEntry, 0, len(entries))
	pathSet := map[string]bool{}
	for _, e := range entries {
		for _, p := range []string{e.Path, e.OriginalPath} {
			if p != "" && scope.OwnsLexically(p) {
				if _, resolveErr := scope.Resolve(p); resolveErr != nil {
					return result, errOwnedPathEscape
				}
			}
		}
		inside := scope.Contains(e.Path) && (e.OriginalPath == "" || scope.Contains(e.OriginalPath))
		status := string([]byte{e.Index, e.Worktree})
		if inside {
			result.owned = append(result.owned, e)
			for _, p := range []string{e.Path, e.OriginalPath} {
				if p == "" || pathSet[p] {
					continue
				}
				pathSet[p] = true
				result.paths = append(result.paths, p)
				blob, err := commitFileBlob(root, p)
				if err != nil {
					return result, err
				}
				result.blobs[p] = blob
				value := blob.oid
				if value == "" {
					value = "deleted"
				}
				framed = append(framed, gitops.FingerprintEntry{Scope: "owned", Status: status, Path: p, Mode: blob.mode, ContentHash: value})
			}
		} else {
			framed = append(framed, gitops.FingerprintEntry{Scope: "outside", Status: status, Path: e.Path})
		}
	}
	sort.Strings(result.paths)
	result.fingerprint = gitops.TreeFingerprint(strings.TrimSpace(string(head)), framed)
	return result, nil
}

func filterConfigured(data []byte) bool {
	parts := strings.Split(string(data), "\x00")
	if len(parts) < 1 || parts[len(parts)-1] != "" || (len(parts)-1)%3 != 0 {
		return true
	}
	for i := 0; i+2 < len(parts)-1; i += 3 {
		if parts[i+1] != "filter" || parts[i+2] != "unspecified" && parts[i+2] != "unset" {
			return true
		}
	}
	return false
}
func stageMatches(data []byte, expected map[string]commitBlob) bool {
	got := make(map[string]commitBlob)
	parts := strings.Split(string(data), "\x00")
	if len(parts) < 1 || parts[len(parts)-1] != "" {
		return false
	}
	for _, row := range parts[:len(parts)-1] {
		meta, path, ok := strings.Cut(row, "\t")
		if !ok {
			return false
		}
		fields := strings.Fields(meta)
		if len(fields) != 3 || fields[2] != "0" || got[path].oid != "" {
			return false
		}
		got[path] = commitBlob{mode: fields[0], oid: fields[1]}
	}
	for path, want := range expected {
		if got[path] != want {
			return false
		}
		delete(got, path)
	}
	return len(got) == 0
}

func (u *CommitVerifiedChangesUseCase) Execute(ctx context.Context, id supervisor.SessionID) error {
	value, _ := commitSessionLocks.LoadOrStore(id, &sync.Mutex{})
	lock := value.(*sync.Mutex)
	if !lock.TryLock() {
		return nil
	}
	defer lock.Unlock()
	if u.Sessions == nil || u.Git == nil || u.Verify == nil || u.Projects == nil || u.Locker == nil {
		return fmt.Errorf("commit dependencies are required")
	}
	phases, ok := u.Sessions.(interface {
		SetGitPhase(supervisor.SessionID, supervisor.GitPhase)
		SetGitPhaseIfEligible(supervisor.SessionID, supervisor.GitPhase) (bool, error)
	})
	if !ok {
		return fmt.Errorf("git phase store is required")
	}
	s, err := u.Sessions.Load(id)
	if err != nil {
		return err
	}
	if s.LastFailureFingerprint == "" {
		u.suppressMu.Lock()
		delete(u.suppressed, id)
		u.suppressMu.Unlock()
	}
	now := time.Now
	if u.Now != nil {
		now = u.Now
	}
	if u.stopped(id) || s.State != supervisor.SessionIdle && s.State != supervisor.SessionAwaitingRecommendation || now().Sub(s.StateSince) < commitSettle || len(u.OwnedPaths) == 0 || len(u.VerifyCommand) == 0 {
		return nil
	}
	eligible, err := phases.SetGitPhaseIfEligible(id, supervisor.GitPhaseEvaluating)
	if err != nil {
		return err
	}
	if !eligible {
		return nil
	}
	defer func() {
		phases.SetGitPhase(id, supervisor.GitPhaseNone)
		u.gitPhaseMu.Lock()
		afterGitPhase := u.AfterGitPhase
		u.gitPhaseMu.Unlock()
		if afterGitPhase != nil {
			afterGitPhase(id, s.StateSince)
		}
	}()
	rootBytes, err := u.Git.RevParseToplevel(ctx)
	if err != nil {
		return u.gitFail(s, "", err)
	}
	root := strings.TrimSpace(string(rootBytes))
	scope, err := gitops.NewPathScope(root, u.CWD, u.OwnedPaths)
	if err != nil {
		return u.fail(s, "", supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonPathEscape, 0, true)
	}
	common, err := u.Git.RevParseGitCommonDir(ctx)
	if err != nil {
		return u.gitFail(s, "", err)
	}
	release, ok := u.Locker.TryLock(strings.TrimSpace(string(common)))
	if !ok {
		return u.event(s, supervisor.EventKindRepoBusy, "", "", 0, "", false)
	}
	defer release()
	if u.stopped(id) {
		return nil
	}
	configFP, include, err := u.configFingerprint(ctx)
	if include {
		return u.fail(s, u.failureFingerprint(ctx, root, scope), supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonGitConfigInclude, 0, true)
	}
	if err != nil {
		return u.gitFail(s, "", err)
	}
	if configFP != s.GitConfigFingerprint {
		return u.fail(s, u.failureFingerprint(ctx, root, scope), supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonGitConfigChanged, 0, true)
	}
	before, err := u.snapshot(ctx, root, scope)
	if errors.Is(err, errOwnedPathEscape) {
		return u.fail(s, "", supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonPathEscape, 0, true)
	}
	if err != nil {
		return u.gitFail(s, "", err)
	}
	if before.classification.RenamePartial {
		return u.fail(s, before.fingerprint, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonRenamePartial, 0, true)
	}
	if before.classification.StagedOutOfScope {
		return u.fail(s, before.fingerprint, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonStagedOutOfScope, 0, true)
	}
	if before.classification.OutOfScopeUncommitted {
		return u.fail(s, before.fingerprint, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonOutOfScopeUncommitted, len(before.entries)-len(before.owned), true)
	}
	if len(before.paths) > 0 {
		attr, err := u.Git.CheckAttrFilter(ctx, before.paths)
		if err != nil {
			return u.gitFail(s, before.fingerprint, err)
		}
		if filterConfigured(attr) {
			return u.fail(s, before.fingerprint, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonGitattributesFilter, 0, true)
		}
	}
	if len(before.owned) == 0 {
		if s.LastVerifiedFingerprint == before.fingerprint {
			return u.evaluatePush(ctx, s)
		}
		if err := UpdateSession(u.Sessions, id, func(current *supervisor.ManagedSession) error {
			current.LastVerifiedFingerprint = before.fingerprint
			current.LastVerifiedResult = "passed"
			return nil
		}); err != nil {
			return err
		}
		if err := u.event(s, supervisor.EventKindNoChanges, "", "", 0, "", false); err != nil {
			return err
		}
		return u.evaluatePush(ctx, s)
	}
	if s.LastFailureFingerprint == before.fingerprint {
		return u.suppressFailure(s, before.fingerprint)
	}
	if s.LastVerifiedFingerprint == before.fingerprint {
		return nil
	}
	matcher := gitops.NewSensitivePathMatcher()
	for _, path := range before.paths {
		if pattern := matcher.MatchPattern(path); pattern != "" {
			return u.fail(s, before.fingerprint, supervisor.EventKindSensitivePathBlocked, sensitivePathBlockedCode, "", 0, true, pattern)
		}
	}
	if u.stopped(id) {
		return nil
	}
	code, verifyErr := u.Verify.Run(ctx, u.VerifyCommand, u.VerifyCommandSHA256, u.CWD)
	if verifyErr != nil || code != 0 {
		reason := store.EventReason("")
		if verifyErr != nil {
			switch verifyErr.Error() {
			case "verify_binary_changed":
				reason = store.ReasonVerifyBinaryChanged
			case "verify_cwd_invalid":
				reason = store.ReasonVerifyCWDInvalid
			case "verify_exec_failed":
				reason = store.ReasonVerifyExecFailed
			}
		}
		if err := UpdateSession(u.Sessions, id, func(current *supervisor.ManagedSession) error {
			current.LastVerifiedFingerprint = before.fingerprint
			current.LastVerifiedResult = "failed"
			return nil
		}); err != nil {
			return err
		}
		return u.fail(s, before.fingerprint, supervisor.EventKindVerifyFailed, verifyFailedCode, reason, 0, true)
	}
	if u.stopped(id) {
		return nil
	}
	after, err := u.snapshot(ctx, root, scope)
	if errors.Is(err, errOwnedPathEscape) {
		return u.fail(s, before.fingerprint, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonPathEscape, 0, true)
	}
	if err != nil {
		return u.gitFail(s, before.fingerprint, err)
	}
	if after.fingerprint != before.fingerprint {
		return u.event(s, supervisor.EventKindTreeChangedDuringVerify, "", "", 0, "", false)
	}
	if u.stopped(id) {
		return nil
	}
	if err := u.Git.Add(ctx, after.paths); err != nil {
		return u.gitFail(s, before.fingerprint, err)
	}
	if u.stopped(id) {
		return nil
	}
	stage, err := u.Git.LsFilesStage(ctx, after.paths)
	if err != nil {
		if resetErr := u.Git.ResetPaths(ctx, after.paths); resetErr != nil {
			failureFP := before.fingerprint
			if current, snapshotErr := u.snapshot(ctx, root, scope); snapshotErr == nil {
				failureFP = current.fingerprint
			}
			rollbackFailure := fmt.Errorf("stage inspection failed: %v; rollback failed: %w", err, resetErr)
			if recordErr := u.gitFail(s, failureFP, rollbackFailure); recordErr != nil {
				return errors.Join(rollbackFailure, recordErr)
			}
			return rollbackFailure
		}
		return u.gitFail(s, before.fingerprint, err)
	}
	if !stageMatches(stage, after.blobs) {
		if resetErr := u.Git.ResetPaths(ctx, after.paths); resetErr != nil {
			failureFP := before.fingerprint
			if current, snapshotErr := u.snapshot(ctx, root, scope); snapshotErr == nil {
				failureFP = current.fingerprint
			}
			rollbackFailure := fmt.Errorf("stage mismatch rollback failed: %w", resetErr)
			if recordErr := u.gitFail(s, failureFP, rollbackFailure); recordErr != nil {
				return errors.Join(rollbackFailure, recordErr)
			}
			return rollbackFailure
		}
		return u.fail(s, before.fingerprint, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonTreeMismatchBeforeCommit, 0, true)
	}
	stagedStatus, err := u.Git.Status(ctx)
	if err != nil {
		return u.gitFail(s, before.fingerprint, err)
	}
	stagedEntries, err := gitops.ParsePorcelainV1(stagedStatus)
	if err != nil {
		return u.gitFail(s, before.fingerprint, err)
	}
	if gitops.ClassifyStatus(stagedEntries, scope).StagedOutOfScope {
		return u.fail(s, before.fingerprint, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonStagedOutOfScope, 0, true)
	}
	diff, err := u.Git.DiffCached(ctx)
	if err != nil {
		return u.gitFail(s, before.fingerprint, err)
	}
	for _, path := range strings.Split(strings.TrimSuffix(string(diff), "\x00"), "\x00") {
		if path != "" && !scope.Contains(path) {
			return u.fail(s, before.fingerprint, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonStagedOutOfScope, 0, true)
		}
	}
	if u.stopped(id) {
		return nil
	}
	tree, err := u.Git.WriteTree(ctx)
	if err != nil {
		return u.gitFail(s, before.fingerprint, err)
	}
	if u.stopped(id) {
		return nil
	}
	configNow, include, err := u.configFingerprint(ctx)
	if include {
		return u.fail(s, before.fingerprint, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonGitConfigInclude, 0, true)
	}
	if err != nil {
		return u.gitFail(s, before.fingerprint, err)
	}
	if configNow != s.GitConfigFingerprint {
		return u.fail(s, before.fingerprint, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonGitConfigChanged, 0, true)
	}
	if u.stopped(id) {
		return nil
	}
	treeNow, err := u.Git.WriteTree(ctx)
	if err != nil {
		return u.gitFail(s, before.fingerprint, err)
	}
	if string(treeNow) != string(tree) {
		return u.fail(s, before.fingerprint, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonTreeMismatchBeforeCommit, 0, true)
	}
	if u.stopped(id) {
		return nil
	}
	if err := u.Git.Commit(ctx, s.ProjectID); err != nil {
		failureFP := before.fingerprint
		if staged, snapshotErr := u.snapshot(ctx, root, scope); snapshotErr == nil {
			failureFP = staged.fingerprint
		}
		if errors.Is(err, gitops.ErrGitTimeout) || strings.Contains(err.Error(), "git_output_too_large") || strings.Contains(err.Error(), "git_unavailable") {
			return u.gitFail(s, failureFP, err)
		}
		return u.fail(s, failureFP, supervisor.EventKindCommitFailed, commitFailedCode, store.ReasonCommitRejected, 0, true)
	}
	shaBytes, err := u.Git.RevParseHead(ctx)
	if err != nil {
		return u.gitFail(s, before.fingerprint, err)
	}
	sha := strings.TrimSpace(string(shaBytes))
	if len(sha) != 40 {
		return fmt.Errorf("invalid commit sha")
	}
	if err := u.Projects.AppendCommit(s.ProjectID, sha); err != nil {
		return err
	}
	if err := UpdateSession(u.Sessions, id, func(current *supervisor.ManagedSession) error {
		current.LastVerifiedFingerprint = before.fingerprint
		current.LastVerifiedResult = "passed"
		current.LastFailureFingerprint = ""
		current.LastFailureReason = ""
		return nil
	}); err != nil {
		return err
	}
	if err := u.event(s, supervisor.EventKindCommitCreated, "", "", len(after.owned), sha, false); err != nil {
		return err
	}
	return u.evaluatePush(ctx, s)
}

func (u *CommitVerifiedChangesUseCase) evaluatePush(ctx context.Context, s supervisor.ManagedSession) error {
	if u.stopped(s.SessionID) || u.AfterCommit == nil {
		return nil
	}
	return u.AfterCommit(ctx, s, CommitLocksHeld{session: s.SessionID})
}
