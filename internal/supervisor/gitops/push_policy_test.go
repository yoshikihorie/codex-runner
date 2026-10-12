package gitops

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestValidatePushArgv_SCN18_ExactShape(t *testing.T) {
	valid := append([]string{"/usr/bin/git"}, BuildPushArgv("origin", "topic", stringOf('b', 40))...)
	if err := ValidatePushArgv(valid, "origin", "topic", stringOf('b', 40)); err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(valid); i++ {
		t.Run(fmt.Sprintf("index_%d", i), func(t *testing.T) {
			bad := append([]string(nil), valid...)
			if i < len(valid)-2 {
				bad[i] += "x"
			} else if i == len(valid)-2 {
				bad[i] = ".."
			} else {
				bad[i] = "refs/heads/topic:refs/heads/other"
			}
			if ValidatePushArgv(bad, "origin", "topic", stringOf('b', 40)) == nil {
				t.Errorf("accepted changed argv[%d]=%q", i, bad[i])
			}
		})
	}
	for _, b := range []string{".hidden", "a b", "a\nb", "a\x00b", "a~b", "a^b", "a@{b", "a.lock", "a//b"} {
		if ValidatePushArgv(append([]string{"/usr/bin/git"}, BuildPushArgv("origin", b, stringOf('b', 40))...), "origin", b, stringOf('b', 40)) == nil {
			t.Errorf("accepted branch %q", b)
		}
	}
	for _, r := range []string{".", "..", "-bad"} {
		if ValidatePushArgv(append([]string{"/usr/bin/git"}, BuildPushArgv(r, "topic", stringOf('b', 40))...), r, "topic", stringOf('b', 40)) == nil {
			t.Errorf("accepted remote %q", r)
		}
	}
	for _, r := range []string{"a/b", "o$rigin", "orígin", "o#r", "tab\tname"} {
		if ValidatePushArgv(append([]string{"/usr/bin/git"}, BuildPushArgv(r, "topic", stringOf('b', 40))...), r, "topic", stringOf('b', 40)) == nil {
			t.Errorf("accepted remote %q", r)
		}
	}
	for _, b := range []string{"a\tb", "a\x01b", "a\x7fb", "refs/tags/v1", "HEAD"} {
		if ValidatePushArgv(append([]string{"/usr/bin/git"}, BuildPushArgv("origin", b, stringOf('b', 40))...), "origin", b, stringOf('b', 40)) == nil {
			t.Errorf("accepted prohibited branch %q", b)
		}
	}
	if ValidatePushArgv(append([]string{"/usr/../usr/bin/git"}, BuildPushArgv("origin", "topic", stringOf('b', 40))...), "origin", "topic", stringOf('b', 40)) == nil {
		t.Fatal("accepted non-normalized git path")
	}
}

func TestSupervisorGitHardeningArgs_SCN60_CanonicalValue(t *testing.T) {
	const want = "-c core.hooksPath=/dev/null -c core.fsmonitor=false -c core.sshCommand= -c push.recurseSubmodules=no -c push.pushOption= -c protocol.ext.allow=never -c commit.gpgsign=false -c push.gpgSign=false -c core.excludesFile=/dev/null -c core.attributesFile=/dev/null -c core.gitProxy= -c http.proxy= -c credential.helper=" // specifications/10-shared/validation-rules.md SUPERVISOR_GIT_HARDENING_ARGS
	if SupervisorGitHardeningArgs != want {
		t.Fatalf("hardening args = %q, want %q", SupervisorGitHardeningArgs, want)
	}
}

func TestPushArgv_SCN13_SCN18_RemoteProxyFixedShape(t *testing.T) {
	sha := stringOf('b', 40)
	want := append(strings.Fields(SupervisorGitHardeningArgs), "-c", "remote.origin.proxy=", "-c", "push.followTags=false", "push", "--no-verify", "--recurse-submodules=no", "--", "origin", sha+":refs/heads/topic")
	got := BuildPushArgv("origin", "topic", sha)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("push argv = %q, want %q", got, want)
	}
	valid := append([]string{"/usr/bin/git"}, want...)
	proxyIndex := len(strings.Fields(SupervisorGitHardeningArgs)) + 2
	for name, change := range map[string]func([]string) []string{
		"missing": func(argv []string) []string { return append(argv[:proxyIndex-1], argv[proxyIndex+1:]...) },
		"nonempty": func(argv []string) []string {
			argv[proxyIndex] = "remote.origin.proxy=http://example.invalid:8080"
			return argv
		},
		"wrong remote": func(argv []string) []string { argv[proxyIndex] = "remote.other.proxy="; return argv },
	} {
		t.Run(name, func(t *testing.T) {
			bad := change(append([]string(nil), valid...))
			if err := ValidatePushArgv(bad, "origin", "topic", sha); !errors.Is(err, ErrPushArgvNotAllowed) {
				t.Fatalf("argv = %q: %v", bad, err)
			}
		})
	}
}

func TestGitRunnerPushRejectsInvalidInspectedSHAWithoutStartingGit(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "git-started")
	fake := filepath.Join(dir, "git-fake")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\ntouch '"+marker+"'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := &GitRunner{git: fake, dir: dir, stateDir: t.TempDir()}
	for _, tc := range []struct {
		name string
		sha  string
	}{
		{"empty", ""},
		{"short", stringOf('b', 39)},
		{"uppercase", stringOf('B', 40)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.sha == "" {
				argv := BuildPushArgv("origin", "topic", tc.sha)
				if got := argv[len(argv)-1]; got != ":refs/heads/topic" {
					t.Fatalf("empty SHA refspec = %q", got)
				}
			}
			err := r.Push(context.Background(), "origin", "topic", tc.sha)
			if !errors.Is(err, ErrPushArgvNotAllowed) || PushErrorReason(err) != "argv_not_allowed" {
				t.Fatalf("invalid SHA classified as %q: %v", PushErrorReason(err), err)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("git started for invalid SHA: %v", err)
			}
		})
	}
}

func TestPushArgv_SCN18_SHARefspec(t *testing.T) {
	sha := stringOf('b', 40)
	argv := append([]string{"/usr/bin/git"}, BuildPushArgv("origin", "topic", sha)...)
	if got := argv[len(argv)-1]; got != sha+":refs/heads/topic" {
		t.Fatalf("refspec=%q", got)
	}
	if err := ValidatePushArgv(argv, "origin", "topic", sha); err != nil {
		t.Fatal(err)
	}
	for name, refspec := range map[string]string{
		"branch source":     "refs/heads/topic:refs/heads/topic",
		"HEAD source":       "HEAD:refs/heads/topic",
		"short sha":         "bbbbbbb:refs/heads/topic",
		"uppercase sha":     strings.ToUpper(sha) + ":refs/heads/topic",
		"caret":             sha + "^:refs/heads/topic",
		"tilde":             sha + "~1:refs/heads/topic",
		"at":                sha + "@{0}:refs/heads/topic",
		"glob":              "*:refs/heads/topic",
		"different sha":     stringOf('c', 40) + ":refs/heads/topic",
		"other destination": sha + ":refs/heads/other",
		"plus":              "+" + sha + ":refs/heads/topic",
	} {
		t.Run(name, func(t *testing.T) {
			bad := append([]string(nil), argv...)
			bad[len(bad)-1] = refspec
			if err := ValidatePushArgv(bad, "origin", "topic", sha); !errors.Is(err, ErrPushArgvNotAllowed) {
				t.Fatalf("refspec=%q err=%v", refspec, err)
			}
		})
	}
}

func TestPushPolicy_SCN13_ReasonsAndArgv(t *testing.T) {
	base := PushInput{FetchURLs: []string{"url"}, PushURLs: []string{"url"}, CurrentRef: "refs/heads/topic", Allowlist: []PushAllowlistEntry{{Remote: "origin", URL: "url", Branch: "topic"}}, RemoteHeadSHA: stringOf('a', 40), RemoteHeadLocal: true, CommitSHAs: []string{stringOf('b', 40)}, SupervisorCommitSHAs: []string{stringOf('b', 40)}, HeadSHA: stringOf('b', 40)}
	cases := []struct {
		name   string
		change func(*PushInput)
		want   string
	}{
		{"detached_head", func(p *PushInput) { p.CurrentRef = "" }, "detached_head"}, {"protected_branch", func(p *PushInput) { p.CurrentRef = "refs/heads/main" }, "protected_branch"}, {"not_in_allowlist", func(p *PushInput) { p.Allowlist = []PushAllowlistEntry{{Remote: "origin", URL: "url", Branch: "else"}} }, "not_in_allowlist"},
		{"multiple_urls", func(p *PushInput) { p.FetchURLs = append(p.FetchURLs, "url") }, "multiple_urls"}, {"url_mismatch", func(p *PushInput) { p.PushURLs = []string{"else"} }, "url_mismatch"}, {"receivepack_configured", func(p *PushInput) { p.ReceivepackConfigured = true }, "receivepack_configured"}, {"remote_helper_configured", func(p *PushInput) { p.RemoteHelperConfigured = true }, "remote_helper_configured"},
		{"git_config_changed", func(p *PushInput) { p.GitConfigChanged = true }, "git_config_changed"}, {"git_config_include", func(p *PushInput) { p.GitConfigInclude = true }, "git_config_include"}, {"no_tracking_ref", func(p *PushInput) { p.RemoteHeadSHA = "" }, "no_tracking_ref"},
		{"remote_head_unavailable", func(p *PushInput) { p.RemoteHeadUnavailable = true }, "remote_head_unavailable"}, {"remote_head_not_local", func(p *PushInput) { p.RemoteHeadLocal = false }, "remote_head_not_local"}, {"foreign_commits", func(p *PushInput) { p.CommitSHAs = []string{stringOf('c', 40)} }, "foreign_commits"},
		{"argv_not_allowed", func(p *PushInput) {
			p.Allowlist = []PushAllowlistEntry{{Remote: "-bad", URL: "url", Branch: "topic"}}
		}, "argv_not_allowed"}, {"push_suppressed_same_head", func(p *PushInput) { p.PreviousFailedHead = p.HeadSHA }, ""},
	}
	seen := map[string]bool{}
	for _, tc := range cases {
		p := base
		tc.change(&p)
		d := PushPolicy{}.Evaluate(p)
		if tc.name == "push_suppressed_same_head" {
			if !d.Suppressed || d.Reason != "" {
				t.Errorf("suppressed decision=%+v", d)
			}
			continue
		}
		seen[d.Reason] = true
		if d.Reason != tc.want {
			t.Errorf("%s got %q", tc.name, d.Reason)
		}
	}
	for _, tc := range cases {
		if tc.name == "push_suppressed_same_head" {
			continue
		}
		if !seen[tc.want] {
			t.Errorf("reason absent: %s", tc.want)
		}
	}
	if d := (PushPolicy{}).Evaluate(PushInput{}); !d.Noop || d.Reason != "" {
		t.Errorf("empty allowlist decision=%+v", d)
	}
	for _, argv := range [][]string{{"--force"}, {"-f"}, {"--force-with-lease"}, {"+refs/heads/x:refs/heads/x"}, {"--mirror"}, {"--delete"}, {":refs/heads/x"}, {"--tags"}, {"origin", "refs/heads/a:refs/heads/a", "refs/heads/b:refs/heads/b"}, {"-bad", "refs/heads/x:refs/heads/x"}, {}} {
		if ValidatePushArgv(append([]string{"/usr/bin/git"}, argv...), "origin", "x", stringOf('b', 40)) == nil {
			t.Errorf("accepted argv %q", argv)
		}
	}
	if err := ValidatePushArgv(append([]string{"/usr/bin/git"}, BuildPushArgv("origin", "x", stringOf('b', 40))...), "origin", "x", stringOf('b', 40)); err != nil {
		t.Fatal(err)
	}
	if got := BuildPushArgv("origin", "x", stringOf('b', 40)); !reflect.DeepEqual(got[len(got)-3:len(got)-2], []string{"--"}) {
		t.Fatal("missing separator")
	}
}

func TestPushPolicy_SCN28_CanonicalReasons(t *testing.T) {
	// Canonical reason vocabulary from FD-sup-03 §6.
	want := []string{"detached_head", "protected_branch", "not_in_allowlist", "multiple_urls", "url_mismatch", "receivepack_configured", "remote_helper_configured", "git_config_changed", "git_config_include", "remote_head_unavailable", "no_tracking_ref", "remote_head_not_local", "foreign_commits", "argv_not_allowed"}
	seen := map[string]bool{}
	base := PushInput{FetchURLs: []string{"url"}, PushURLs: []string{"url"}, CurrentRef: "refs/heads/topic", Allowlist: []PushAllowlistEntry{{Remote: "origin", URL: "url", Branch: "topic"}}, RemoteHeadSHA: stringOf('a', 40), RemoteHeadLocal: true, CommitSHAs: []string{stringOf('b', 40)}, SupervisorCommitSHAs: []string{stringOf('b', 40)}, HeadSHA: stringOf('b', 40)}
	cases := []struct {
		change func(*PushInput)
		reason string
	}{
		{func(p *PushInput) { p.CurrentRef = "" }, "detached_head"}, {func(p *PushInput) { p.CurrentRef = "refs/heads/main" }, "protected_branch"}, {func(p *PushInput) { p.Allowlist = []PushAllowlistEntry{{Branch: "other"}} }, "not_in_allowlist"},
		{func(p *PushInput) { p.FetchURLs = []string{"url", "url"} }, "multiple_urls"}, {func(p *PushInput) { p.PushURLs = []string{"else"} }, "url_mismatch"}, {func(p *PushInput) { p.ReceivepackConfigured = true }, "receivepack_configured"}, {func(p *PushInput) { p.RemoteHelperConfigured = true }, "remote_helper_configured"},
		{func(p *PushInput) { p.GitConfigChanged = true }, "git_config_changed"}, {func(p *PushInput) { p.GitConfigInclude = true }, "git_config_include"}, {func(p *PushInput) { p.RemoteHeadUnavailable = true }, "remote_head_unavailable"},
		{func(p *PushInput) { p.RemoteHeadSHA = "" }, "no_tracking_ref"}, {func(p *PushInput) { p.RemoteHeadLocal = false }, "remote_head_not_local"}, {func(p *PushInput) { p.CommitSHAs = []string{"bad"} }, "foreign_commits"},
		{func(p *PushInput) { p.Allowlist = []PushAllowlistEntry{{Remote: "-bad", URL: "url", Branch: "topic"}} }, "argv_not_allowed"},
	}
	for _, tc := range cases {
		p := base
		tc.change(&p)
		got := (PushPolicy{}).Evaluate(p).Reason
		if got != tc.reason {
			t.Errorf("reason=%q want=%q", got, tc.reason)
		}
		seen[got] = true
	}
	for _, reason := range want {
		if !seen[reason] {
			t.Errorf("canonical reason not exercised: %s", reason)
		}
	}
}

func TestPushPolicy_SCN13_SetAndHeadSemantics(t *testing.T) {
	base := PushInput{FetchURLs: []string{"url"}, PushURLs: []string{"url"}, CurrentRef: "refs/heads/topic", Allowlist: []PushAllowlistEntry{{Remote: "origin", URL: "url", Branch: "topic"}}, RemoteHeadSHA: stringOf('a', 40), RemoteHeadLocal: true, CommitSHAs: []string{stringOf('b', 40), stringOf('c', 40)}, SupervisorCommitSHAs: []string{stringOf('a', 40), stringOf('b', 40), stringOf('c', 40)}, HeadSHA: stringOf('c', 40)}
	if d := (PushPolicy{}).Evaluate(base); !d.Allowed {
		t.Fatalf("base: %+v", d)
	}
	reversed := base
	reversed.CommitSHAs = []string{stringOf('c', 40), stringOf('b', 40)}
	if d := (PushPolicy{}).Evaluate(reversed); !d.Allowed {
		t.Fatalf("reversed: %+v", d)
	}
	bad := base
	bad.HeadSHA = ""
	if (PushPolicy{}).Evaluate(bad).Allowed {
		t.Fatal("accepted empty head")
	}
	bad = base
	bad.CommitSHAs = []string{""}
	if (PushPolicy{}).Evaluate(bad).Allowed {
		t.Fatal("accepted empty commit")
	}
	bad = base
	bad.SupervisorCommitSHAs = []string{stringOf('b', 40), stringOf('c', 40)}
	if !(PushPolicy{}).Evaluate(bad).Allowed {
		t.Fatal("expected supervisor tail=head")
	}
	bad = base
	bad.SupervisorCommitSHAs = []string{stringOf('c', 40), stringOf('b', 40)}
	if (PushPolicy{}).Evaluate(bad).Allowed {
		t.Fatal("accepted head not latest supervisor commit")
	}
	bad = base
	bad.CommitSHAs = nil
	if d := (PushPolicy{}).Evaluate(bad); !d.NoPushes || d.Allowed {
		t.Fatalf("empty push set: %+v", d)
	}
	bad = base
	bad.Allowlist = []PushAllowlistEntry{{Remote: "origin", URL: "url", Branch: "topic"}, {Remote: "backup", URL: "url", Branch: "topic"}}
	if (PushPolicy{}).Evaluate(bad).Allowed {
		t.Fatal("accepted ambiguous remote")
	}
	bad = base
	bad.Allowlist = []PushAllowlistEntry{{Remote: "origin", URL: "url1", Branch: "topic"}, {Remote: "origin", URL: "url", Branch: "topic"}}
	if d := (PushPolicy{}).Evaluate(bad); !d.Allowed {
		t.Fatalf("URL matching entry was not selected: %+v", d)
	}
	bad = base
	bad.RemoteHeadSHA = "BAD"
	if d := (PushPolicy{}).Evaluate(bad); d.Reason != "remote_head_unavailable" {
		t.Fatalf("malformed remote head: %+v", d)
	}
	bad = base
	bad.HeadSHA = ""
	if d := (PushPolicy{}).Evaluate(bad); d.Reason != "foreign_commits" {
		t.Fatalf("empty head reason: %+v", d)
	}
	bad = base
	bad.CommitSHAs = []string{""}
	if d := (PushPolicy{}).Evaluate(bad); d.Reason != "foreign_commits" {
		t.Fatalf("empty commit reason: %+v", d)
	}
	bad = base
	bad.RemoteHeadSHA = strings.Repeat("A", 40)
	if d := (PushPolicy{}).Evaluate(bad); d.Reason != "remote_head_unavailable" {
		t.Fatalf("uppercase remote sha: %+v", d)
	}
	bad = base
	bad.CurrentRef = "refs/heads/"
	if (PushPolicy{}).Evaluate(bad).Allowed {
		t.Fatal("accepted empty branch")
	}
	for _, branch := range []string{"master", "develop", "release"} {
		p := base
		p.CurrentRef = "refs/heads/" + branch
		if branch == "release" {
			p.ProtectedBranches = []string{"release"}
		}
		if (PushPolicy{}).Evaluate(p).Reason != "protected_branch" {
			t.Errorf("branch %q was not protected", branch)
		}
	}
	p := base
	p.PushURLs = []string{"url", "url"}
	if (PushPolicy{}).Evaluate(p).Reason != "multiple_urls" {
		t.Fatal("multiple push URLs not rejected")
	}
}

func stringOf(b byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return string(out)
}

func TestPushPolicy_SCN53_LocalRejectionStopsEvaluation(t *testing.T) {
	base := PushInput{CurrentRef: "refs/heads/topic", Allowlist: []PushAllowlistEntry{{Remote: "origin", URL: "url", Branch: "topic"}}, FetchURLs: []string{"url"}, PushURLs: []string{"url"}}
	for _, tc := range []struct {
		name   string
		change func(*PushInput)
		want   string
	}{
		{"uploadpack", func(p *PushInput) { p.RemoteHelperConfigured = true }, "remote_helper_configured"},
		{"config", func(p *PushInput) { p.GitConfigChanged = true }, "git_config_changed"},
		{"local git", func(p *PushInput) { p.LocalGitError = errors.New("git failed") }, "git_failed"},
		{"large output", func(p *PushInput) { p.LocalGitError = ErrGitOutputTooLarge }, "git_output_too_large"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := base
			tc.change(&p)
			local := (PushPolicy{}).EvaluateLocal(p)
			if local.Reason != tc.want || local.ReadyForRemote {
				t.Fatalf("local decision=%+v", local)
			}
			if next := (PushPolicy{}).EvaluateRemote(p, local); next != local {
				t.Fatalf("remote stage proceeded: %+v", next)
			}
		})
	}
}
func TestPushPolicy_BeforeURLsChecksBranchWithoutURLSentinel(t *testing.T) {
	p := PushInput{CurrentRef: "refs/heads/topic", Allowlist: []PushAllowlistEntry{{Remote: "origin", URL: "url", Branch: "topic"}}}
	d := (PushPolicy{}).EvaluateBeforeURLs(p)
	if d.Reason != "" || d.Remote != "origin" || d.Branch != "topic" {
		t.Fatalf("pre-URL decision=%+v", d)
	}
	if local := (PushPolicy{}).EvaluateLocal(p); local.Reason != "multiple_urls" {
		t.Fatalf("full local decision=%+v", local)
	}
}

func TestPushPolicy_SCN13_StagesRequireRemoteThenCommits(t *testing.T) {
	p := PushInput{CurrentRef: "refs/heads/topic", Allowlist: []PushAllowlistEntry{{Remote: "origin", URL: "url", Branch: "topic"}}, FetchURLs: []string{"url"}, PushURLs: []string{"url"}, RemoteHeadSHA: stringOf('a', 40), RemoteHeadLocal: true, CommitSHAs: []string{stringOf('b', 40)}, SupervisorCommitSHAs: []string{stringOf('b', 40)}, HeadSHA: stringOf('b', 40)}
	policy := PushPolicy{}
	local := policy.EvaluateLocal(p)
	if !local.ReadyForRemote || local.Allowed {
		t.Fatalf("local=%+v", local)
	}
	remote := policy.EvaluateRemote(p, local)
	if !remote.ReadyForCommits || remote.Allowed {
		t.Fatalf("remote=%+v", remote)
	}
	if final := policy.EvaluateCommits(p, remote); !final.Allowed {
		t.Fatalf("final=%+v", final)
	}
	p.CommitListError = ErrGitOutputTooLarge
	if final := policy.EvaluateCommits(p, remote); final.Reason != "git_output_too_large" || final.Allowed {
		t.Fatalf("commit error=%+v", final)
	}
}

func TestValidatePushArgv_SCN52_RefsAndHEAD(t *testing.T) {
	for _, branch := range []string{"refs/heads/topic", "refs/tags/v1", "HEAD"} {
		argv := append([]string{"/usr/bin/git"}, BuildPushArgv("origin", branch, stringOf('b', 40))...)
		if err := ValidatePushArgv(argv, "origin", branch, stringOf('b', 40)); !errors.Is(err, ErrPushArgvNotAllowed) {
			t.Fatalf("branch=%q err=%v", branch, err)
		}
	}
}

func TestGitRunner_SCN13_PushErrorClassification(t *testing.T) {
	if PushErrorReason(ErrPushArgvNotAllowed) != "argv_not_allowed" {
		t.Fatal("argv classification")
	}
	for _, err := range []error{ErrGitTimeout, errors.New("git_failed"), errors.New("git_unavailable")} {
		if PushErrorReason(err) != "push_failed" {
			t.Fatalf("push error %v classified as %q", err, PushErrorReason(err))
		}
	}
}

func TestGitRunner_SCN13_UnavailableGitIsPushFailure(t *testing.T) {
	r := &GitRunner{}
	err := r.Push(context.Background(), "origin", "topic", stringOf('b', 40))
	var runErr *PushRunError
	if !errors.As(err, &runErr) || PushErrorReason(err) != "push_failed" {
		t.Fatalf("missing git classified as %q: %v", PushErrorReason(err), err)
	}
}

func TestGitRunner_SCN13_IsolatedPushEnvironment(t *testing.T) {
	dir := t.TempDir()
	home := t.TempDir()
	state := t.TempDir()
	socketDir, err := os.MkdirTemp("/tmp", "sk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
	socket := filepath.Join(socketDir, "agent.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	log := filepath.Join(dir, "env.log")
	fake := filepath.Join(dir, "git-fake")
	script := "#!/bin/sh\nprintf '%s\\n' \"$HOME\" \"$SSH_AUTH_SOCK\" \"$GIT_SSH_COMMAND\" > '" + log + "'\n"
	if err := os.WriteFile(fake, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	r := &GitRunner{git: fake, dir: dir, stateDir: state, passSSHAuthSock: true}
	r.SetParentEnv([]string{"PATH=/usr/bin:/bin", "HOME=" + home, "SSH_AUTH_SOCK=" + socket})
	if err := r.Push(context.Background(), "origin", "topic", stringOf('b', 40)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	want := home + "\n" + socket + "\nssh -F /dev/null -o BatchMode=yes -o StrictHostKeyChecking=yes\n"
	if string(data) != want {
		t.Fatalf("push env=%q want=%q", data, want)
	}
}

func TestGitRunner_SCN33_PushTimeoutInjection(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "git-slow")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nsleep 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := &GitRunner{git: fake, dir: dir, stateDir: t.TempDir()}
	r.SetParentEnv([]string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()})
	r.SetPushTimeout(20 * time.Millisecond)
	err := r.Push(context.Background(), "origin", "topic", stringOf('b', 40))
	if !errors.Is(err, ErrGitTimeout) || PushErrorReason(err) != "push_failed" {
		t.Fatalf("timeout classified as %q: %v", PushErrorReason(err), err)
	}
}
func TestGitRunner_SCN33_LsRemoteUsesPushTimeout(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "git-slow")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nsleep 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := &GitRunner{git: fake, dir: dir, stateDir: t.TempDir()}
	r.SetParentEnv([]string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()})
	r.SetPushTimeout(20 * time.Millisecond)
	_, err := r.LsRemote(context.Background(), "origin", "refs/heads/topic")
	if !errors.Is(err, ErrGitTimeout) {
		t.Fatalf("ls-remote timeout: %v", err)
	}
}
func TestGitRunner_CallerCancellationIsNotPushRunError(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "git-fake")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := &GitRunner{git: fake, dir: dir, stateDir: t.TempDir()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := r.Push(ctx, "origin", "topic", stringOf('b', 40))
	var runErr *PushRunError
	if !errors.Is(err, context.Canceled) || errors.As(err, &runErr) {
		t.Fatalf("caller cancel classified as push error: %v", err)
	}
}

func TestValidatePushArgv_SCN18_ForbiddenVariants(t *testing.T) {
	valid := append([]string{"/usr/bin/git"}, BuildPushArgv("origin", "topic", stringOf('b', 40))...)
	for _, forbidden := range []string{"--force", "-f", "--force-with-lease=main", "--mirror", "--all", "--prune", "--delete", "-d", "--tags", "--follow-tags"} {
		t.Run(forbidden, func(t *testing.T) {
			argv := append([]string(nil), valid...)
			argv = append(argv[:len(argv)-3], append([]string{forbidden}, argv[len(argv)-3:]...)...)
			if err := ValidatePushArgv(argv, "origin", "topic", stringOf('b', 40)); !errors.Is(err, ErrPushArgvNotAllowed) {
				t.Fatalf("accepted %q: %v", forbidden, err)
			}
		})
	}
	for _, refspec := range []string{"+refs/heads/topic:refs/heads/topic", ":refs/heads/topic", "refs/tags/v1:refs/tags/v1", "refs/heads/topic:refs/heads/other", "refs/heads/*:refs/heads/*"} {
		t.Run(refspec, func(t *testing.T) {
			argv := append([]string(nil), valid...)
			argv[len(argv)-1] = refspec
			if err := ValidatePushArgv(argv, "origin", "topic", stringOf('b', 40)); !errors.Is(err, ErrPushArgvNotAllowed) {
				t.Fatalf("accepted %q: %v", refspec, err)
			}
		})
	}
}

func pushPolicyGit(t *testing.T, dir, home string, args ...string) string {
	t.Helper()
	bin, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "TMPDIR=" + home, "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=" + os.DevNull, "GIT_TERMINAL_PROMPT=0"}
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(out))
}
func TestGitRunner_SCN18_MirrorConfigDoesNotForceBare(t *testing.T) {
	home := t.TempDir()
	local := t.TempDir()
	bare := filepath.Join(t.TempDir(), "remote.git")
	pushPolicyGit(t, local, home, "init", "-q", "--object-format=sha1", "-b", "topic")
	pushPolicyGit(t, local, home, "init", "-q", "--object-format=sha1", "--bare", bare)
	if err := os.WriteFile(filepath.Join(local, "file"), []byte("base"), 0600); err != nil {
		t.Fatal(err)
	}
	pushPolicyGit(t, local, home, "add", "file")
	pushPolicyGit(t, local, home, "-c", "user.name=Setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "base")
	pushPolicyGit(t, local, home, "remote", "add", "origin", bare)
	pushPolicyGit(t, local, home, "push", "origin", "refs/heads/topic:refs/heads/topic")
	other := t.TempDir()
	pushPolicyGit(t, other, home, "clone", "-q", bare, ".")
	pushPolicyGit(t, other, home, "checkout", "-q", "-b", "topic", "origin/topic")
	if err := os.WriteFile(filepath.Join(other, "file"), []byte("remote"), 0600); err != nil {
		t.Fatal(err)
	}
	pushPolicyGit(t, other, home, "add", "file")
	pushPolicyGit(t, other, home, "-c", "user.name=Setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "remote")
	pushPolicyGit(t, other, home, "push", "origin", "refs/heads/topic:refs/heads/topic")
	remoteHead := pushPolicyGit(t, other, home, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(local, "file"), []byte("local"), 0600); err != nil {
		t.Fatal(err)
	}
	pushPolicyGit(t, local, home, "add", "file")
	pushPolicyGit(t, local, home, "-c", "user.name=Setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "local")
	localHead := pushPolicyGit(t, local, home, "rev-parse", "HEAD")
	pushPolicyGit(t, local, home, "config", "remote.origin.mirror", "true")
	r := NewGitRunner(local, "", "", t.TempDir())
	r.SetParentEnv([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "TMPDIR=" + home, "GIT_CONFIG_GLOBAL=" + os.DevNull})
	err := r.Push(context.Background(), "origin", "topic", localHead)
	if err == nil {
		t.Fatal("mirror configuration allowed divergent push")
	}
	if got := pushPolicyGit(t, local, home, "--git-dir", bare, "rev-parse", "refs/heads/topic"); got != remoteHead {
		t.Fatalf("remote changed: %s want %s", got, remoteHead)
	}
}
func TestGitRunner_SCN14_URLRewritesAreVisible(t *testing.T) {
	home := t.TempDir()
	repo := t.TempDir()
	bare := filepath.Join(t.TempDir(), "remote.git")
	pushPolicyGit(t, repo, home, "init", "-q", "--object-format=sha1")
	pushPolicyGit(t, repo, home, "init", "-q", "--object-format=sha1", "--bare", bare)
	pushPolicyGit(t, repo, home, "config", "url."+bare+".insteadOf", "alias:")
	pushPolicyGit(t, repo, home, "config", "url."+bare+".pushInsteadOf", "send:")
	pushPolicyGit(t, repo, home, "remote", "add", "origin", "alias:")
	r := NewGitRunner(repo, "", "", t.TempDir())
	r.SetParentEnv([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "TMPDIR=" + home, "GIT_CONFIG_GLOBAL=" + os.DevNull})
	fetch, push, err := r.RemoteURLs(context.Background(), "origin")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(fetch)) != bare || strings.TrimSpace(string(push)) != bare {
		t.Fatalf("fetch=%q push=%q want=%q", fetch, push, bare)
	}
	pushPolicyGit(t, repo, home, "remote", "add", "send", "send:")
	fetch, push, err = r.RemoteURLs(context.Background(), "send")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(fetch)) != "send:" || strings.TrimSpace(string(push)) != bare {
		t.Fatalf("pushInsteadOf fetch=%q push=%q want=%q", fetch, push, bare)
	}
}
