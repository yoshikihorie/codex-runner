package gitops

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPathScopeResolveAndContains(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "src"), 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := NewPathScope(root, filepath.Join(root, "src"), []string{"."})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Paths(); !reflect.DeepEqual(got, []string{"src"}) {
		t.Fatalf("paths=%v", got)
	}
	if !s.Contains("src/a.go") || s.Contains("other/a.go") {
		t.Fatal("wrong containment")
	}
	for _, p := range []string{"../escape", "/tmp/absolute"} {
		if _, err := NewPathScope(root, root, []string{p}); err == nil {
			t.Fatalf("accepted %q", p)
		}
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := NewPathScope(root, root, []string{"link"}); err == nil {
		t.Fatal("accepted external symlink")
	}
}

func TestPathScopeSubdirectoryRelativeResolution(t *testing.T) {
	root := t.TempDir()
	cwd := filepath.Join(root, "module", "app")
	if err := os.MkdirAll(cwd, 0o755); err != nil {
		t.Fatal(err)
	}
	s, err := NewPathScope(root, cwd, []string{"lib"})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.Paths(); !reflect.DeepEqual(got, []string{"module/app/lib"}) {
		t.Fatalf("paths=%v", got)
	}
}

func TestPathScopeRejectsEscapeAndGitMetadata(t *testing.T) {
	root := t.TempDir()
	s, err := NewPathScope(root, root, []string{"."})
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"../x", "/etc/passwd", ".git", ".git/hooks/pre-commit", ".GIT/config"} {
		if s.Contains(p) {
			t.Errorf("Contains(%q)=true", p)
		}
	}
	for _, p := range []string{".git", ".git/hooks", "dir/.GIT"} {
		if _, err := NewPathScope(root, root, []string{p}); err == nil {
			t.Errorf("owned path %q accepted", p)
		}
	}
	alias := filepath.Join(root, "gl")
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, ".git"), alias); err == nil {
		if _, err := NewPathScope(root, root, []string{"gl"}); err == nil {
			t.Error("accepted symlink alias to .git")
		}
		if s.Contains("gl/hooks/pre-commit") {
			t.Error("Contains accepted symlink alias to .git")
		}
		if _, err := s.Resolve("gl/hooks/pre-commit"); err == nil {
			t.Error("resolved symlink alias to .git")
		}
	}
	if s.Contains("") {
		t.Error("empty path contained")
	}
	if _, err := s.Resolve(""); err == nil {
		t.Error("resolved empty path")
	}
	if _, err := s.Resolve("does-not-exist"); err != nil {
		t.Errorf("resolve non-existent in-scope path: %v", err)
	}
}

func TestValidatePushArgvExactShape(t *testing.T) {
	valid := append([]string{"/usr/bin/git"}, BuildPushArgv("origin", "topic")...)
	if err := ValidatePushArgv(valid, "origin", "topic"); err != nil {
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
			if ValidatePushArgv(bad, "origin", "topic") == nil {
				t.Errorf("accepted changed argv[%d]=%q", i, bad[i])
			}
		})
	}
	for _, b := range []string{".hidden", "a b", "a\nb", "a\x00b", "a~b", "a^b", "a@{b", "a.lock", "a//b"} {
		if ValidatePushArgv(append([]string{"/usr/bin/git"}, BuildPushArgv("origin", b)...), "origin", b) == nil {
			t.Errorf("accepted branch %q", b)
		}
	}
	for _, r := range []string{".", "..", "-bad"} {
		if ValidatePushArgv(append([]string{"/usr/bin/git"}, BuildPushArgv(r, "topic")...), r, "topic") == nil {
			t.Errorf("accepted remote %q", r)
		}
	}
	for _, r := range []string{"a/b", "o$rigin", "orígin", "o#r", "tab\tname"} {
		if ValidatePushArgv(append([]string{"/usr/bin/git"}, BuildPushArgv(r, "topic")...), r, "topic") == nil {
			t.Errorf("accepted remote %q", r)
		}
	}
	for _, b := range []string{"a\tb", "a\x01b", "a\x7fb", "refs/tags/v1", "HEAD"} {
		if ValidatePushArgv(append([]string{"/usr/bin/git"}, BuildPushArgv("origin", b)...), "origin", b) == nil {
			t.Errorf("accepted prohibited branch %q", b)
		}
	}
	if ValidatePushArgv(append([]string{"/usr/../usr/bin/git"}, BuildPushArgv("origin", "topic")...), "origin", "topic") == nil {
		t.Fatal("accepted non-normalized git path")
	}
}

func TestFingerprintsOrderIndependentAndConfigInclude(t *testing.T) {
	if TreeFingerprint("tree", []FingerprintEntry{{Path: "b"}, {Path: "a"}}) != TreeFingerprint("tree", []FingerprintEntry{{Path: "a"}, {Path: "b"}}) {
		t.Fatal("tree fingerprint depends on order")
	}
	if TreeFingerprint("tree", []FingerprintEntry{{Path: "a", ContentHash: "one"}}) == TreeFingerprint("tree", []FingerprintEntry{{Path: "a", ContentHash: "two"}}) {
		t.Fatal("tree fingerprint ignored content hash")
	}
	a := GitConfigFingerprintFromEntries([]FingerprintEntry{{Path: "hook", ContentHash: "hash"}}, []byte("[include]\n path = other\n"))
	if !a.HasInclude || a.SHA256 == "" {
		t.Fatalf("bad config fingerprint: %+v", a)
	}
	if GitConfigFingerprintFromEntries(nil, []byte("[core]\n path = unrelated\n")).HasInclude {
		t.Fatal("ordinary path key treated as include")
	}
	if !GitConfigFingerprintFromEntries(nil, []byte("[includeIf \"gitdir:repo\"]\n path = other\n")).HasInclude {
		t.Fatal("includeIf.path not detected")
	}
	for _, config := range [][]byte{
		[]byte("\xef\xbb\xbf[include] path = x\n"), []byte("[include]path=x\n"),
		[]byte("[core][include] path = x\n"), []byte("[core] [include] path = x\n"), []byte("[core]\t[includeIf \"gitdir:/\"]path=x\n"), []byte("[core][x][include]\n path=x\n"),
		[]byte("[include] ; c\npath=x\n"), []byte("[include] # c\npath=x\n"),
		[]byte("[includeIf\t\"gitdir:/\"]\npath=x\n"),
		append([]byte(strings.Repeat("x", 70*1024)+"\n"), []byte("[include]\npath=x\n")...),
	} {
		if !configHasInclude(config) {
			t.Errorf("include missed in %q", config[:min(len(config), 50)])
		}
	}
}

func TestBuildGitConfigFingerprintEntriesIncludesWorktreeAndToleratesHookLinks(t *testing.T) {
	common, worktree := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(common, "config"), []byte("[core]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worktree, "config.worktree"), []byte("[include]\npath=x\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	hooks := filepath.Join(common, "hooks")
	if err := os.Mkdir(hooks, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(hooks, "directory-hook"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("missing-target", filepath.Join(hooks, "broken-hook")); err != nil {
		t.Fatal(err)
	}
	entries, include, err := BuildGitConfigFingerprintEntries(common, worktree)
	if err != nil || !include {
		t.Fatalf("entries=%v include=%v err=%v", entries, include, err)
	}
	if len(entries) != 4 {
		t.Fatalf("entries=%v", entries)
	}
	if got := GitConfigFingerprintFromEntries(entries, []byte("[core]\n")); got.SHA256 == "" {
		t.Fatal("empty fingerprint")
	}
}

func TestBuildGitConfigFingerprintEntriesRejectsBrokenConfigLink(t *testing.T) {
	common, worktree := t.TempDir(), t.TempDir()
	if err := os.Symlink("missing-target", filepath.Join(common, "config")); err != nil {
		t.Fatal(err)
	}
	if _, include, err := BuildGitConfigFingerprintEntries(common, worktree); !include && err == nil {
		t.Fatal("existing config link with unreadable target was accepted")
	}
}

func TestSensitiveMatcher(t *testing.T) {
	m := NewSensitivePathMatcher()
	for _, p := range []string{".env.local", "tls.pem", "id_rsa.pub", "credentials-prod", "cert.p12", ".ENV", "Server.PEM", "id_RSA", "id_ed25519", "id_ecdsa", "id_dsa"} {
		if !m.Matches(p) {
			t.Errorf("did not match %q", p)
		}
	}
	if m.Matches("src/main.go") {
		t.Fatal("matched ordinary file")
	}
}

func TestParseActualGitPorcelainV1(t *testing.T) {
	git := "/usr/bin/git"
	repo := t.TempDir()
	run := func(args ...string) []byte {
		t.Helper()
		cmd := exec.Command(git, append([]string{"-C", repo}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("git %v: %v", args, err)
		}
		return out
	}
	write := func(name, contents string) {
		t.Helper()
		path := filepath.Join(repo, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	run("init", "--quiet")
	for name, contents := range map[string]string{
		"owned/ordinary.txt": "original", "owned/staged.txt": "original", "owned/rename-old.txt": "rename",
		"owned/partial-old.txt": "partial", "owned/delete.txt": "delete", "owned/space name.txt": "space",
		"owned/line\nbreak.txt": "line", "outside/staged.txt": "original", "outside/rename-old.txt": "partial",
	} {
		write(name, contents)
		run("add", "--", name)
	}
	run("-c", "user.name=test", "-c", "user.email=test@example.invalid", "commit", "--quiet", "-m", "fixture")

	write("owned/ordinary.txt", "changed")
	write("owned/staged.txt", "changed")
	run("add", "--", "owned/staged.txt")
	write("owned/new untracked.txt", "new")
	if err := os.Rename(filepath.Join(repo, "owned/rename-old.txt"), filepath.Join(repo, "owned/rename-new.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(repo, "owned/partial-old.txt"), filepath.Join(repo, "outside/partial-new.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(repo, "owned/delete.txt")); err != nil {
		t.Fatal(err)
	}
	write("owned/space name.txt", "space changed")
	write("owned/line\nbreak.txt", "line changed")
	write("outside/staged.txt", "changed")
	run("add", "-A", "--", "owned/rename-old.txt", "owned/rename-new.txt", "owned/partial-old.txt", "outside/partial-new.txt", "owned/delete.txt", "owned/space name.txt", "owned/line\nbreak.txt", "outside/staged.txt")

	status := run("status", "--porcelain=v1", "-z", "--untracked-files=all")
	entries, err := ParsePorcelainV1(status)
	if err != nil {
		t.Fatal(err)
	}
	byPath := make(map[string]StatusEntry, len(entries))
	for _, entry := range entries {
		byPath[entry.Path] = entry
	}
	cases := []struct {
		name, path, original, kind string
		index, worktree            byte
	}{
		{"ordinary modification", "owned/ordinary.txt", "", "ordinary", ' ', 'M'},
		{"staged modification", "owned/staged.txt", "", "ordinary", 'M', ' '},
		{"untracked file", "owned/new untracked.txt", "", "untracked", '?', '?'},
		{"rename inside scope", "owned/rename-new.txt", "owned/rename-old.txt", "rename", 'R', ' '},
		{"partial rename", "outside/partial-new.txt", "owned/partial-old.txt", "rename", 'R', ' '},
		{"deletion", "owned/delete.txt", "", "ordinary", 'D', ' '},
		{"space in path", "owned/space name.txt", "", "ordinary", 'M', ' '},
		{"newline in path", "owned/line\nbreak.txt", "", "ordinary", 'M', ' '},
		{"out of scope staged", "outside/staged.txt", "", "ordinary", 'M', ' '},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := byPath[tc.path]
			if !ok || got.OriginalPath != tc.original || got.Kind != tc.kind || got.Index != tc.index || got.Worktree != tc.worktree {
				t.Fatalf("entry=%+v found=%v", got, ok)
			}
		})
	}
	scope, err := NewPathScope(repo, repo, []string{"owned"})
	if err != nil {
		t.Fatal(err)
	}
	classification := ClassifyStatus(entries, scope)
	if !classification.RenamePartial || !classification.StagedOutOfScope || classification.OutOfScopeUncommitted {
		t.Fatalf("classification=%+v", classification)
	}
}

func TestPorcelainAcceptsIgnoredAndCopyStatuses(t *testing.T) {
	entries, err := ParsePorcelainV1([]byte("!! ignored\x00C  copied\x00source\x00?? new\x00"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 || entries[0].Kind != "ignored" || entries[1].Kind != "copy" || entries[1].OriginalPath != "source" || entries[2].Kind != "untracked" {
		t.Fatalf("entries=%+v", entries)
	}
	scope := &PathScope{paths: []string{"owned"}}
	for _, e := range []StatusEntry{{Kind: "rename", Index: 'R', Worktree: ' ', Path: "away/new", OriginalPath: "away/old"}, {Kind: "rename", Index: 'R', Worktree: ' ', Path: "away/new", OriginalPath: "else/old"}} {
		if got := ClassifyStatus([]StatusEntry{e}, scope); got.RenamePartial {
			t.Fatalf("both out-of-scope paths incorrectly partial: %+v", got)
		}
	}
}

func TestPushPolicyReasonsAndArgv(t *testing.T) {
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
		if ValidatePushArgv(append([]string{"/usr/bin/git"}, argv...), "origin", "x") == nil {
			t.Errorf("accepted argv %q", argv)
		}
	}
	if err := ValidatePushArgv(append([]string{"/usr/bin/git"}, BuildPushArgv("origin", "x")...), "origin", "x"); err != nil {
		t.Fatal(err)
	}
	if got := BuildPushArgv("origin", "x"); !reflect.DeepEqual(got[len(got)-3:len(got)-2], []string{"--"}) {
		t.Fatal("missing separator")
	}
}

func TestPushPolicyCoversCanonicalReasons(t *testing.T) {
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

func TestPushPolicySetAndHeadSemantics(t *testing.T) {
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

func TestPorcelainRejectsInvalidXYAndEmptyPath(t *testing.T) {
	for _, data := range [][]byte{[]byte("xM path\x00"), []byte(" M \x00"), []byte(" M path\x00\x00"), []byte("?M mixed\x00"), []byte("!M mixed\x00"), []byte("   path\x00"), []byte("?? no-final-nul"), []byte(" R target\x00")} {
		if _, err := ParsePorcelainV1(data); err == nil {
			t.Errorf("accepted malformed %q", data)
		}
	}
}

func stringOf(b byte, n int) string {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return string(out)
}

func TestRepoLockerTryLock(t *testing.T) {
	l := NewRepoLocker()
	root := t.TempDir()
	unlock, ok := l.TryLock(root)
	if !ok {
		t.Fatal("first lock failed")
	}
	if _, ok := l.TryLock(root); ok {
		t.Fatal("duplicate lock acquired")
	}
	unlock()
	if _, ok := l.TryLock(root); !ok {
		t.Fatal("lock not released")
	}
	alias := root + "-alias"
	if err := os.Symlink(root, alias); err != nil {
		t.Fatal(err)
	}
	if _, ok := l.TryLock(alias); ok {
		t.Fatal("symlink alias acquired distinct lock")
	}
	if _, ok := l.TryLock(filepath.Join(root, "missing")); ok {
		t.Fatal("nonexistent repository path locked")
	}
}

func TestTreeFingerprintDistinguishesMode(t *testing.T) {
	a := []FingerprintEntry{{Scope: "owned", Path: "owned/file", ContentHash: "same", Mode: "100644"}}
	b := []FingerprintEntry{{Scope: "owned", Path: "owned/file", ContentHash: "same", Mode: "120000"}}
	if TreeFingerprint("head", a) == TreeFingerprint("head", b) {
		t.Fatal("mode change was invisible")
	}
}

func TestMalformedConfigFailsClosed(t *testing.T) {
	for _, config := range [][]byte{[]byte("[core]\n bare = false\n= invalid\n"), []byte("[core\n"), []byte("[core]\n key = value\n[core \"unterminated]\n"), []byte("[core]\n key = \"unterminated\n"), []byte("[core]\n key = bad\x00value\n"), []byte("[core]\n @invalid = value\n")} {
		if !GitConfigFingerprintFromEntries(nil, config).HasInclude {
			t.Fatalf("malformed config accepted: %q", config)
		}
	}
}
