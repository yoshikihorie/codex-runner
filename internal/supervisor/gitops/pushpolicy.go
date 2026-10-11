package gitops

import (
	"fmt"
	"path"
	"regexp"
	"strings"
)

type PushAllowlistEntry struct{ Remote, URL, Branch string }
type PushInput struct {
	FetchURLs, PushURLs                []string
	ReceivepackConfigured              bool
	CurrentRef                         string
	ProtectedBranches                  []string
	Allowlist                          []PushAllowlistEntry
	RemoteHeadSHA                      string
	RemoteHeadLocal                    bool
	RemoteHeadUnavailable              bool
	CommitSHAs, SupervisorCommitSHAs   []string
	HeadSHA, PreviousFailedHead        string
	GitConfigChanged, GitConfigInclude bool
}
type PushDecision struct {
	Allowed                    bool
	Reason                     string
	Remote, Branch             string
	Noop, NoPushes, Suppressed bool
}
type PushPolicy struct{}

const SupervisorGitHardeningArgs = "-c core.hooksPath=/dev/null -c core.fsmonitor=false -c core.sshCommand= -c push.recurseSubmodules=no -c push.pushOption= -c protocol.ext.allow=never -c commit.gpgsign=false"

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

func (PushPolicy) Evaluate(p PushInput) PushDecision {
	d := PushDecision{}
	deny := func(s string) PushDecision { d.Reason = s; return d }
	if len(p.Allowlist) == 0 {
		d.Noop = true
		return d
	}
	if p.CurrentRef == "" || !strings.HasPrefix(p.CurrentRef, "refs/heads/") {
		return deny("detached_head")
	}
	branch := strings.TrimPrefix(p.CurrentRef, "refs/heads/")
	d.Branch = branch
	if !validRefName(branch) {
		return deny("argv_not_allowed")
	}
	if strings.HasPrefix(branch, "refs/") || branch == "HEAD" {
		return deny("argv_not_allowed")
	}
	protected := append([]string{"main", "master", "develop"}, p.ProtectedBranches...)
	for _, b := range protected {
		if "refs/heads/"+b == p.CurrentRef {
			return deny("protected_branch")
		}
	}
	match, ok := ResolveAllowlistEntry(p.Allowlist, branch, p.FetchURLs, p.PushURLs)
	if !ok {
		return deny("not_in_allowlist")
	}
	if !validRemote(match.Remote) {
		return deny("argv_not_allowed")
	}
	d.Remote = match.Remote
	if len(p.FetchURLs) != 1 || len(p.PushURLs) != 1 {
		return deny("multiple_urls")
	}
	if p.FetchURLs[0] != match.URL || p.PushURLs[0] != match.URL {
		return deny("url_mismatch")
	}
	if p.ReceivepackConfigured {
		return deny("receivepack_configured")
	}
	if p.GitConfigInclude {
		return deny("git_config_include")
	}
	if p.GitConfigChanged {
		return deny("git_config_changed")
	}
	if p.RemoteHeadUnavailable {
		return deny("remote_head_unavailable")
	}
	if p.RemoteHeadSHA == "" {
		return deny("no_tracking_ref")
	}
	if !p.RemoteHeadLocal {
		return deny("remote_head_not_local")
	}
	if !shaPattern.MatchString(p.HeadSHA) {
		return deny("foreign_commits")
	}
	if !shaPattern.MatchString(p.RemoteHeadSHA) {
		return deny("remote_head_unavailable")
	}
	set := map[string]bool{}
	for _, s := range p.SupervisorCommitSHAs {
		if !shaPattern.MatchString(s) {
			return deny("foreign_commits")
		}
		set[s] = true
	}
	for _, s := range p.CommitSHAs {
		if !shaPattern.MatchString(s) {
			return deny("foreign_commits")
		}
		if !set[s] {
			return deny("foreign_commits")
		}
	}
	if len(p.CommitSHAs) == 0 {
		d.NoPushes = true
		return d
	}
	if !set[p.HeadSHA] || len(p.SupervisorCommitSHAs) == 0 || p.SupervisorCommitSHAs[len(p.SupervisorCommitSHAs)-1] != p.HeadSHA {
		return deny("foreign_commits")
	}
	if p.HeadSHA == p.PreviousFailedHead && p.HeadSHA != "" {
		d.Suppressed = true
		return d
	}
	d.Allowed = true
	return d
}

func ValidatePushArgv(argv []string, remote, branch string) error {
	if len(argv) < 2 || !path.IsAbs(argv[0]) || path.Clean(argv[0]) != argv[0] || strings.HasPrefix(branch, "refs/") || branch == "HEAD" {
		return fmt.Errorf("argv_not_allowed")
	}
	if !validRemote(remote) || !validRefName(branch) {
		return fmt.Errorf("argv_not_allowed")
	}
	want := append([]string{argv[0]}, BuildPushArgv(remote, branch)...)
	if !equal(argv, want) {
		return fmt.Errorf("argv_not_allowed")
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
func BuildPushArgv(remote, branch string) []string {
	args := append([]string{}, hardeningArgs...)
	args = append(args, "-c", "push.followTags=false", "push", "--no-verify", "--recurse-submodules=no", "--", remote, "refs/heads/"+branch+":refs/heads/"+branch)
	return args
}
