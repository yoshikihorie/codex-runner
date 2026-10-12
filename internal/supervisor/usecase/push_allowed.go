package usecase

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/gitops"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

// pushGit exposes only the fixed git operations used by the push evaluation.
type pushGit interface {
	SymbolicRef(context.Context) ([]byte, error)
	RemoteURLs(context.Context, string) ([]byte, []byte, error)
	ConfigGetReceivepack(context.Context, string) ([]byte, error)
	ConfigGetUploadpack(context.Context, string) ([]byte, error)
	ConfigGetVcs(context.Context, string) ([]byte, error)
	RevParseGitCommonDir(context.Context) ([]byte, error)
	RevParseGitDir(context.Context) ([]byte, error)
	RevParseHead(context.Context) ([]byte, error)
	LsRemote(context.Context, string, string) ([]byte, error)
	CatFileExists(context.Context, string) error
	RevListSince(context.Context, string, string) ([]byte, error)
	Push(context.Context, string, string, string) error
}
type pushProjectStore interface {
	Load(string) (store.ProjectRecord, error)
}

// PushToAllowedRemoteUseCase runs under the locks handed off by AfterCommit.
type PushToAllowedRemoteUseCase struct {
	Git               pushGit
	Sessions          SessionRecordStore
	Projects          pushProjectStore
	Stop              *StopState
	Allowlist         []gitops.PushAllowlistEntry
	ProtectedBranches []string
	Record            func(store.Event) error
	Notify            func(supervisor.SupervisorEventKind, supervisor.SessionID)
	suppressedMu      sync.Mutex
	suppressed        map[supervisor.SessionID]string
}

func (u *PushToAllowedRemoteUseCase) clearSuppressed(id supervisor.SessionID) {
	u.suppressedMu.Lock()
	delete(u.suppressed, id)
	u.suppressedMu.Unlock()
}

const (
	pushRejectedCode = "SUP_PUSH_REJECTED_POLICY" // specifications/10-shared/published-language/error-codes.md
	pushFailedCode   = "SUP_PUSH_FAILED"
)

func (u *PushToAllowedRemoteUseCase) stopped(id supervisor.SessionID) bool {
	return u.Stop == nil || u.Stop.IsStopped(id)
}
func (u *PushToAllowedRemoteUseCase) event(s supervisor.ManagedSession, d gitops.PushDecision, kind supervisor.SupervisorEventKind, code string, reason store.EventReason, sha string, count ...int) error {
	if u.stopped(s.SessionID) {
		return nil
	}
	e := store.Event{Kind: kind, SessionID: s.SessionID.String(), Code: code, Reason: reason}
	if len(count) != 0 {
		e.Count = count[0]
	}
	if kind == supervisor.EventKindPushSucceeded || kind == supervisor.EventKindPushFailed {
		e.Remote = d.Remote
		e.Branch = d.Branch
		e.CommitSHA = sha
	}
	if u.Record != nil {
		if err := u.Record(e); err != nil {
			return err
		}
	}
	if code != "" && u.Notify != nil && !u.stopped(s.SessionID) {
		u.Notify(kind, s.SessionID)
	}
	return nil
}
func (u *PushToAllowedRemoteUseCase) blocked(s supervisor.ManagedSession, d gitops.PushDecision, count ...int) error {
	if u.stopped(s.SessionID) {
		return nil
	}
	return u.event(s, d, supervisor.EventKindPushBlocked, pushRejectedCode, store.EventReason(d.Reason), "", count...)
}
func (u *PushToAllowedRemoteUseCase) current(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	var current supervisor.ManagedSession
	err := UpdateSession(u.Sessions, id, func(s *supervisor.ManagedSession) error { current = *s; return nil })
	return current, err
}
func pushLines(out []byte) []string {
	if len(out) == 0 {
		return nil
	}
	raw := strings.Split(strings.TrimSuffix(string(out), "\n"), "\n")
	return raw
}
func pushConfigSet(out []byte, err error) (bool, error) {
	if errors.Is(err, gitops.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return len(out) > 0, nil
}

// Run accepts the lock token from CommitVerifiedChangesUseCase.AfterCommit.
func (u *PushToAllowedRemoteUseCase) Run(ctx context.Context, snapshot supervisor.ManagedSession, held CommitLocksHeld) error {
	id := snapshot.SessionID
	if held.session != id {
		return fmt.Errorf("push requires held commit locks")
	}
	if u.Sessions == nil || u.Git == nil || u.Projects == nil {
		return fmt.Errorf("push dependencies missing")
	}
	if u.stopped(id) || len(u.Allowlist) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s, err := u.current(id)
	if err != nil {
		return err
	}
	if u.stopped(id) {
		return nil
	}
	p := gitops.PushInput{Allowlist: u.Allowlist, ProtectedBranches: u.ProtectedBranches, PreviousFailedHead: s.LastPushFailedHead}
	policy := gitops.PushPolicy{}
	blocked := func(s supervisor.ManagedSession, d gitops.PushDecision, count ...int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return u.blocked(s, d, count...)
	}
	ref, err := u.Git.SymbolicRef(ctx)
	if errors.Is(err, gitops.ErrNotFound) {
		p.CurrentRef = ""
	} else if err != nil {
		p.LocalGitError = err
	} else {
		p.CurrentRef = strings.TrimSpace(string(ref))
	}
	if u.stopped(id) {
		return nil
	}
	// Checks 3-5 precede any remote URL lookup.
	d := policy.EvaluateBeforeURLs(p)
	if d.Reason != "" || d.Noop {
		return blocked(s, d)
	}
	remote := d.Remote
	fetch, push, err := u.Git.RemoteURLs(ctx, remote)
	if err != nil {
		p.LocalGitError = err
	} else {
		p.FetchURLs = pushLines(fetch)
		p.PushURLs = pushLines(push)
	}
	if u.stopped(id) {
		return nil
	}
	if p.LocalGitError == nil {
		var configured bool
		configured, err = pushConfigSet(u.Git.ConfigGetReceivepack(ctx, remote))
		if err != nil {
			p.LocalGitError = err
		} else {
			p.ReceivepackConfigured = configured
		}
	}
	if u.stopped(id) {
		return nil
	}
	if p.LocalGitError == nil {
		var configured bool
		configured, err = pushConfigSet(u.Git.ConfigGetUploadpack(ctx, remote))
		if err != nil {
			p.LocalGitError = err
		} else {
			p.RemoteHelperConfigured = configured
		}
	}
	if u.stopped(id) {
		return nil
	}
	if p.LocalGitError == nil {
		var configured bool
		configured, err = pushConfigSet(u.Git.ConfigGetVcs(ctx, remote))
		if err != nil {
			p.LocalGitError = err
		} else {
			p.RemoteHelperConfigured = p.RemoteHelperConfigured || configured
		}
	}
	if u.stopped(id) {
		return nil
	}
	var commonDir, gitDir, checkedFingerprint string
	if p.LocalGitError == nil {
		var common, rawGitDir []byte
		common, err = u.Git.RevParseGitCommonDir(ctx)
		if err == nil {
			rawGitDir, err = u.Git.RevParseGitDir(ctx)
		}
		if err != nil {
			p.LocalGitError = err
		} else {
			commonDir = strings.TrimSpace(string(common))
			gitDir = strings.TrimSpace(string(rawGitDir))
			var entries []gitops.FingerprintEntry
			entries, p.GitConfigInclude, err = gitops.BuildGitConfigFingerprintEntries(commonDir, gitDir)
			if err != nil {
				p.GitConfigInclude = true
			} else {
				checkedFingerprint = gitops.GitConfigFingerprintFromEntries(entries).SHA256
				p.GitConfigChanged = checkedFingerprint != s.GitConfigFingerprint
			}
		}
	}
	if u.stopped(id) {
		return nil
	}
	d = policy.EvaluateLocal(p)
	if d.Reason != "" {
		return blocked(s, d)
	}
	if d.Remote != remote {
		d.Reason = string(store.ReasonNotInAllowlist)
		return blocked(s, d)
	}
	recheckConfig := func() error {
		entries, include, err := gitops.BuildGitConfigFingerprintEntries(commonDir, gitDir)
		if err := ctx.Err(); err != nil {
			return err
		}
		if u.stopped(id) {
			return nil
		}
		switch {
		case err != nil:
			d.Reason = string(store.ReasonGitConfigInclude)
		case gitops.GitConfigFingerprintFromEntries(entries).SHA256 != checkedFingerprint:
			d.Reason = string(store.ReasonGitConfigChanged)
		case include:
			d.Reason = string(store.ReasonGitConfigInclude)
		}
		return nil
	}
	head, err := u.Git.RevParseHead(ctx)
	if err != nil {
		p.LocalGitError = err
		d = policy.EvaluateLocal(p)
		return blocked(s, d)
	}
	p.HeadSHA = strings.TrimSpace(string(head))
	if u.stopped(id) {
		return nil
	}
	ref, err = u.Git.SymbolicRef(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	if u.stopped(id) {
		return nil
	}
	if errors.Is(err, gitops.ErrNotFound) {
		d.Reason = string(store.ReasonDetachedHead)
		return blocked(s, d)
	}
	if err != nil {
		p.LocalGitError = err
		d = policy.EvaluateLocal(p)
		return blocked(s, d)
	}
	if strings.TrimSpace(string(ref)) != p.CurrentRef {
		d.Reason = string(store.ReasonNotInAllowlist)
		return blocked(s, d)
	}
	if err := recheckConfig(); err != nil {
		return err
	}
	if u.stopped(id) || d.Reason != "" {
		return blocked(s, d)
	}
	remoteOut, err := u.Git.LsRemote(ctx, d.Remote, "refs/heads/"+d.Branch)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if u.stopped(id) {
		return nil
	}
	if err != nil {
		p.RemoteHeadUnavailable = true
	} else {
		lines := pushLines(remoteOut)
		if len(lines) > 1 {
			p.RemoteHeadUnavailable = true
		} else if len(lines) == 1 {
			fields := strings.Fields(lines[0])
			if len(fields) != 2 || fields[1] != "refs/heads/"+d.Branch {
				p.RemoteHeadUnavailable = true
			} else {
				p.RemoteHeadSHA = fields[0]
			}
		}
	}
	if p.RemoteHeadSHA != "" && !p.RemoteHeadUnavailable {
		err = u.Git.CatFileExists(ctx, p.RemoteHeadSHA)
		if err := ctx.Err(); err != nil {
			return err
		}
		if u.stopped(id) {
			return nil
		}
		if errors.Is(err, gitops.ErrNotFound) {
			p.RemoteHeadLocal = false
		} else if err != nil {
			p.RemoteHeadLocalError = err
		} else {
			p.RemoteHeadLocal = true
		}
	}
	d = policy.EvaluateRemote(p, d)
	if d.Reason != "" {
		return blocked(s, d)
	}
	commits, err := u.Git.RevListSince(ctx, p.RemoteHeadSHA, p.HeadSHA)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if u.stopped(id) {
		return nil
	}
	if err != nil {
		p.CommitListError = err
	} else {
		p.CommitSHAs = pushLines(commits)
	}
	record, loadErr := u.Projects.Load(s.ProjectID)
	if loadErr == nil {
		p.SupervisorCommitSHAs = record.SupervisorCommitSHAs
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if u.stopped(id) {
		return nil
	}
	s, err = u.current(id)
	if err != nil {
		return err
	}
	p.PreviousFailedHead = s.LastPushFailedHead
	if err := ctx.Err(); err != nil {
		return err
	}
	if u.stopped(id) {
		return nil
	}
	d = policy.EvaluateCommits(p, d)
	if d.Reason != "" {
		if d.Reason == string(store.ReasonForeignCommits) {
			known := make(map[string]bool, len(p.SupervisorCommitSHAs))
			for _, sha := range p.SupervisorCommitSHAs {
				known[sha] = true
			}
			foreign := 0
			for _, sha := range p.CommitSHAs {
				if !known[sha] {
					foreign++
				}
			}
			return blocked(s, d, foreign)
		}
		return blocked(s, d)
	}
	if d.NoPushes {
		if err := ctx.Err(); err != nil {
			return err
		}
		return nil
	}
	if d.Suppressed {
		u.suppressedMu.Lock()
		if u.suppressed == nil {
			u.suppressed = make(map[supervisor.SessionID]string)
		}
		already := u.suppressed[id] == p.HeadSHA
		u.suppressed[id] = p.HeadSHA
		u.suppressedMu.Unlock()
		if !already {
			if err := ctx.Err(); err != nil {
				return err
			}
			return u.event(s, d, supervisor.EventKindPushSuppressedSameHead, "", "", "")
		}
		return nil
	}
	if u.stopped(id) {
		return nil
	}
	if err := recheckConfig(); err != nil {
		return err
	}
	if u.stopped(id) || d.Reason != "" {
		return blocked(s, d)
	}
	fetch, push, err = u.Git.RemoteURLs(ctx, d.Remote)
	if err := ctx.Err(); err != nil {
		return err
	}
	if u.stopped(id) {
		return nil
	}
	if err != nil {
		p.LocalGitError = err
		d = policy.EvaluateLocal(p)
		return blocked(s, d)
	}
	if !slices.Equal(pushLines(fetch), p.FetchURLs) || !slices.Equal(pushLines(push), p.PushURLs) {
		d.Reason = string(store.ReasonURLMismatch)
		return blocked(s, d)
	}
	if err := u.Git.Push(ctx, d.Remote, d.Branch, p.HeadSHA); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if u.stopped(id) {
			return nil
		}
		if gitops.PushErrorReason(err) == string(store.ReasonArgvNotAllowed) {
			d.Reason = string(store.ReasonArgvNotAllowed)
			return blocked(s, d)
		}
		saveErr := UpdateSession(u.Sessions, id, func(current *supervisor.ManagedSession) error { current.LastPushFailedHead = p.HeadSHA; return nil })
		eventErr := u.event(s, d, supervisor.EventKindPushFailed, pushFailedCode, "", p.HeadSHA)
		return errors.Join(saveErr, eventErr)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return u.event(s, d, supervisor.EventKindPushSucceeded, "", "", p.HeadSHA)
}
