package gitops

import (
	"errors"
	"path"
	"regexp"
	"strings"
)

type PushAllowlistEntry struct{ Remote, URL, Branch string }
type PushInput struct {
	LocalGitError, RemoteHeadLocalError, CommitListError error
	FetchURLs, PushURLs                                  []string
	ReceivepackConfigured                                bool
	RemoteHelperConfigured                               bool
	CurrentRef                                           string
	ProtectedBranches                                    []string
	Allowlist                                            []PushAllowlistEntry
	RemoteHeadSHA                                        string
	RemoteHeadLocal                                      bool
	RemoteHeadUnavailable                                bool
	CommitSHAs, SupervisorCommitSHAs                     []string
	HeadSHA, PreviousFailedHead                          string
	GitConfigChanged, GitConfigInclude                   bool
}
type PushDecision struct {
	Allowed, ReadyForRemote, ReadyForCommits bool
	Reason                                   string
	Remote, Branch                           string
	Noop, NoPushes, Suppressed               bool
}
type PushPolicy struct{}

const SupervisorGitHardeningArgs = "-c core.hooksPath=/dev/null -c core.fsmonitor=false -c core.sshCommand= -c push.recurseSubmodules=no -c push.pushOption= -c protocol.ext.allow=never -c commit.gpgsign=false -c push.gpgSign=false -c core.excludesFile=/dev/null -c core.attributesFile=/dev/null -c core.gitProxy= -c http.proxy= -c credential.helper=" // specifications/10-shared/validation-rules.md SUPERVISOR_GIT_HARDENING_ARGS

var hardeningArgs = strings.Fields(SupervisorGitHardeningArgs)
var shaPattern = regexp.MustCompile(`^[0-9a-f]{40}$`)
var remotePattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func validRefName(s string) bool {
	if s == "" || strings.HasPrefix(s, "-") || strings.HasPrefix(s, ".") || strings.HasSuffix(s, ".") || strings.HasSuffix(s, ".lock") || strings.Contains(s, "..") || strings.Contains(s, "//") || strings.ContainsAny(s, " ~^:?*[\\\x00\r\n") || strings.Contains(s, "@{") || s == "@" {
		return false
	}
	for _, part := range strings.Split(s, "/") {
		if part == "" || strings.HasPrefix(part, ".") || strings.HasSuffix(part, ".lock") {
			return false
		}
	}
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}
func validRemote(s string) bool {
	return remotePattern.MatchString(s) && s != "." && s != ".." && !strings.HasPrefix(s, "-")
}

// ResolveAllowlistEntry selects the unique branch entry, including URL, before evaluating push safety.
func ResolveAllowlistEntry(entries []PushAllowlistEntry, branch string, fetchURLs, pushURLs []string) (PushAllowlistEntry, bool) {
	var candidate, found PushAllowlistEntry
	count, matches := 0, 0
	for _, e := range entries {
		if e.Branch != branch {
			continue
		}
		count++
		candidate = e
		if len(fetchURLs) == 1 && len(pushURLs) == 1 && fetchURLs[0] == e.URL && pushURLs[0] == e.URL {
			found = e
			matches++
		}
	}
	if matches == 1 {
		return found, true
	}
	if matches > 1 {
		return PushAllowlistEntry{}, false
	}
	if count == 1 {
		return candidate, true
	}
	if count > 1 {
		return candidate, true
	}
	return PushAllowlistEntry{}, false
}

// Evaluate keeps the original all-input entry point for existing callers.
func (policy PushPolicy) Evaluate(p PushInput) PushDecision {
	d := policy.EvaluateLocal(p)
	if !d.ReadyForRemote {
		return d
	}
	d = policy.EvaluateRemote(p, d)
	if !d.ReadyForCommits {
		return d
	}
	return policy.EvaluateCommits(p, d)
}

// EvaluateBeforeURLs covers checks 3-5 before a remote URL is read.
func (PushPolicy) EvaluateBeforeURLs(p PushInput) PushDecision {
	d := PushDecision{}
	deny := func(reason string) PushDecision { d.Reason = reason; return d }
	if len(p.Allowlist) == 0 {
		d.Noop = true
		return d
	}
	if p.LocalGitError != nil {
		return deny(localGitFailureReason(p.LocalGitError))
	}
	if p.CurrentRef == "" || !strings.HasPrefix(p.CurrentRef, "refs/heads/") {
		return deny("detached_head")
	}
	branch := strings.TrimPrefix(p.CurrentRef, "refs/heads/")
	d.Branch = branch
	if !validRefName(branch) || strings.HasPrefix(branch, "refs/") || branch == "HEAD" {
		return deny("argv_not_allowed")
	}
	protected := append([]string{"main", "master", "develop"}, p.ProtectedBranches...)
	for _, b := range protected {
		if "refs/heads/"+b == p.CurrentRef {
			return deny("protected_branch")
		}
	}
	var remote string
	for _, entry := range p.Allowlist {
		if entry.Branch != branch {
			continue
		}
		if remote != "" && remote != entry.Remote {
			return deny("not_in_allowlist")
		}
		remote = entry.Remote
	}
	if remote == "" {
		return deny("not_in_allowlist")
	}
	if !validRemote(remote) {
		return deny("argv_not_allowed")
	}
	d.Remote = remote
	return d
}

// EvaluateLocal covers checks 3-8. Only ReadyForRemote permits ls-remote.
func (policy PushPolicy) EvaluateLocal(p PushInput) PushDecision {
	d := policy.EvaluateBeforeURLs(p)
	if d.Noop || d.Reason != "" {
		return d
	}
	deny := func(reason string) PushDecision { d.Reason = reason; return d }
	match, ok := ResolveAllowlistEntry(p.Allowlist, d.Branch, p.FetchURLs, p.PushURLs)
	if !ok || match.Remote != d.Remote {
		return deny("not_in_allowlist")
	}
	if len(p.FetchURLs) != 1 || len(p.PushURLs) != 1 {
		return deny("multiple_urls")
	}
	if p.FetchURLs[0] != match.URL || p.PushURLs[0] != match.URL {
		return deny("url_mismatch")
	}
	if p.ReceivepackConfigured {
		return deny("receivepack_configured")
	}
	if p.RemoteHelperConfigured {
		return deny("remote_helper_configured")
	}
	if p.GitConfigInclude {
		return deny("git_config_include")
	}
	if p.GitConfigChanged {
		return deny("git_config_changed")
	}
	d.ReadyForRemote = true
	return d
}

// EvaluateRemote covers check 9 after the caller obtains the actual remote head.
func (PushPolicy) EvaluateRemote(p PushInput, local PushDecision) PushDecision {
	if !local.ReadyForRemote || local.Reason != "" {
		return local
	}
	local.ReadyForRemote = false
	deny := func(reason string) PushDecision { local.Reason = reason; return local }
	if p.RemoteHeadUnavailable {
		return deny("remote_head_unavailable")
	}
	if p.RemoteHeadSHA == "" {
		return deny("no_tracking_ref")
	}
	if !shaPattern.MatchString(p.RemoteHeadSHA) {
		return deny("remote_head_unavailable")
	}
	if p.RemoteHeadLocalError != nil {
		return deny(localGitFailureReason(p.RemoteHeadLocalError))
	}
	if !p.RemoteHeadLocal {
		return deny("remote_head_not_local")
	}
	local.ReadyForCommits = true
	return local
}

// EvaluateCommits covers checks 10-11. Allowed is set only after both pass.
func (PushPolicy) EvaluateCommits(p PushInput, remote PushDecision) PushDecision {
	if !remote.ReadyForCommits || remote.Reason != "" {
		return remote
	}
	remote.ReadyForCommits = false
	deny := func(reason string) PushDecision { remote.Reason = reason; return remote }
	if p.CommitListError != nil {
		return deny(localGitFailureReason(p.CommitListError))
	}
	if !shaPattern.MatchString(p.HeadSHA) {
		return deny("foreign_commits")
	}
	set := map[string]bool{}
	for _, sha := range p.SupervisorCommitSHAs {
		if !shaPattern.MatchString(sha) {
			return deny("foreign_commits")
		}
		set[sha] = true
	}
	for _, sha := range p.CommitSHAs {
		if !shaPattern.MatchString(sha) || !set[sha] {
			return deny("foreign_commits")
		}
	}
	if len(p.CommitSHAs) == 0 {
		remote.NoPushes = true
		return remote
	}
	if !set[p.HeadSHA] || len(p.SupervisorCommitSHAs) == 0 || p.SupervisorCommitSHAs[len(p.SupervisorCommitSHAs)-1] != p.HeadSHA {
		return deny("foreign_commits")
	}
	if p.HeadSHA == p.PreviousFailedHead {
		remote.Suppressed = true
		return remote
	}
	remote.Allowed = true
	return remote
}

func localGitFailureReason(err error) string {
	if errors.Is(err, ErrGitOutputTooLarge) {
		return "git_output_too_large"
	}
	return "git_failed"
}

func ValidatePushArgv(argv []string, remote, branch, inspectedSHA string) error {
	if !shaPattern.MatchString(inspectedSHA) {
		return ErrPushArgvNotAllowed
	}
	if len(argv) < 2 || !path.IsAbs(argv[0]) || path.Clean(argv[0]) != argv[0] || strings.HasPrefix(branch, "refs/") || branch == "HEAD" {
		return ErrPushArgvNotAllowed
	}
	if !validRemote(remote) || !validRefName(branch) {
		return ErrPushArgvNotAllowed
	}
	want := append([]string{argv[0]}, BuildPushArgv(remote, branch, inspectedSHA)...)
	if !equal(argv, want) {
		return ErrPushArgvNotAllowed
	}
	return nil
}
func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
func BuildPushArgv(remote, branch, inspectedSHA string) []string {
	args := append([]string{}, hardeningArgs...)
	args = append(args, "-c", "remote."+remote+".proxy=", "-c", "push.followTags=false", "push", "--no-verify", "--recurse-submodules=no", "--", remote, inspectedSHA+":refs/heads/"+branch)
	return args
}
