package gitops

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestGitRunner_NoForbiddenOperationMethods(t *testing.T) {
	typeOfRunner := reflect.TypeOf((*GitRunner)(nil))
	for _, name := range []string{"Merge", "Pull", "Rebase", "Amend", "ForcePush", "DeleteBranch", "DeleteTag", "ResetHard"} {
		if _, found := typeOfRunner.MethodByName(name); found {
			t.Errorf("forbidden GitRunner operation %s", name)
		}
	}
}

func newGitRunnerWithBinary(t *testing.T, git, dir, name, email string) *GitRunner {
	t.Helper()
	return &GitRunner{git: git, dir: dir, authorName: name, authorEmail: email, parentEnv: os.Environ(), stateDir: t.TempDir(), passSSHAuthSock: true}
}

func TestGitRunnerPathOperationsUseToplevelAndLiteralArguments(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	sub := filepath.Join(root, "sub")
	if err := os.Mkdir(sub, 0700); err != nil {
		t.Fatal(err)
	}
	name := "[literal].txt"
	if err := os.WriteFile(filepath.Join(sub, name), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	r := newGitRunnerWithBinary(t, "/usr/bin/git", sub, "author", "author@example.invalid")
	ctx := context.Background()
	if err := r.Add(ctx, []string{"sub/" + name}); err != nil {
		t.Fatalf("add from subdir: %v", err)
	}
	if out, err := r.CheckAttrFilter(ctx, []string{"sub/" + name}); err != nil || !strings.Contains(string(out), "unspecified") {
		t.Fatalf("attr: %q %v", out, err)
	}
	if out, err := r.DiffCached(ctx); err != nil || string(out) != "sub/"+name+"\x00" {
		t.Fatalf("cached paths: %q %v", out, err)
	}
	gitDir, err := r.RevParseGitDir(ctx)
	if err != nil || !filepath.IsAbs(strings.TrimSpace(string(gitDir))) {
		t.Fatalf("git dir: %q %v", gitDir, err)
	}
	commonDir, err := r.RevParseGitCommonDir(ctx)
	if err != nil || !filepath.IsAbs(strings.TrimSpace(string(commonDir))) {
		t.Fatalf("common dir: %q %v", commonDir, err)
	}
}

func TestGitRunnerCommitUsesProjectIDTemplate(t *testing.T) {
	root := t.TempDir()
	runGit(t, root, "init", "-q")
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("a"), 0600); err != nil {
		t.Fatal(err)
	}
	r := newGitRunnerWithBinary(t, "/usr/bin/git", root, "author", "author@example.invalid")
	if err := r.Add(context.Background(), []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Commit(context.Background(), "arbitrary message"); err == nil {
		t.Fatal("accepted arbitrary commit message")
	}
	if err := r.Commit(context.Background(), "project-1"); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("/usr/bin/git", "log", "-1", "--format=%s")
	cmd.Dir = root
	msg, err := cmd.Output()
	if err != nil || strings.TrimSpace(string(msg)) != "supervisor: verified changes in project-1" {
		t.Fatalf("message %q: %v", msg, err)
	}
}

func TestGitRunnerFixedOperations(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	runGit(t, dir, "init", "-q")
	if err := os.WriteFile(filepath.Join(dir, "a"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	hooks := filepath.Join(dir, ".git", "hooks", "pre-commit")
	if err := os.WriteFile(hooks, []byte("#!/bin/sh\nexit 99\n"), 0700); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	if err := os.WriteFile(filepath.Join(home, ".gitconfig"), []byte("[user]\n name = evil\n email = evil@example.invalid\n"), 0600); err != nil {
		t.Fatal(err)
	}
	xdgGit := filepath.Join(home, ".config", "git")
	if err := os.MkdirAll(xdgGit, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(xdgGit, "ignore"), []byte("a\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(xdgGit, "attributes"), []byte("* filter=evil\n"), 0600); err != nil {
		t.Fatal(err)
	}
	r := newGitRunnerWithBinary(t, "/usr/bin/git", dir, "good", "good@example.invalid")
	r.parentEnv = []string{"PATH=/usr/bin:/bin", "HOME=" + home}
	status, err := r.Status(ctx)
	if err != nil || !strings.Contains(string(status), "a") {
		t.Fatalf("status %q: %v", status, err)
	}
	attrs, err := r.CheckAttrFilter(ctx, []string{"a"})
	if err != nil || !strings.Contains(string(attrs), "unspecified") {
		t.Fatalf("user XDG attributes affected check-attr: %q %v", attrs, err)
	}
	if err := r.Add(ctx, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	tree, err := r.WriteTree(ctx)
	if err != nil || len(strings.TrimSpace(string(tree))) != 40 {
		t.Fatalf("tree %q: %v", tree, err)
	}
	if err := r.Commit(ctx, "verified"); err != nil {
		t.Fatalf("hook ran or commit failed: %v", err)
	}
	head, err := r.RevParseHead(ctx)
	if err != nil || len(strings.TrimSpace(string(head))) != 40 {
		t.Fatalf("head %q: %v", head, err)
	}
	if err := r.CatFileExists(ctx, strings.TrimSpace(string(head))); err != nil {
		t.Fatal(err)
	}
	for name, call := range map[string]func() ([]byte, error){"git dir": func() ([]byte, error) { return r.RevParseGitDir(ctx) }, "common dir": func() ([]byte, error) { return r.RevParseGitCommonDir(ctx) }, "rev list": func() ([]byte, error) {
		return r.RevListSince(ctx, strings.TrimSpace(string(head)), strings.TrimSpace(string(head)))
	}, "diff cached": func() ([]byte, error) { return r.DiffCached(ctx) }, "symbolic ref": func() ([]byte, error) { return r.SymbolicRef(ctx) }} {
		t.Run(name, func(t *testing.T) {
			if _, err := call(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGitRunnerRemoteOperationsAndPushValidation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	bare := t.TempDir()
	runGit(t, dir, "init", "-q", "-b", "work")
	runGit(t, bare, "init", "-q", "--bare")
	_ = os.WriteFile(filepath.Join(dir, "a"), []byte("x"), 0600)
	r := newGitRunnerWithBinary(t, "/usr/bin/git", dir, "author", "author@example.invalid")
	r.parentEnv = []string{"PATH=/usr/bin:/bin", "HOME=" + t.TempDir()}
	if err := r.Add(ctx, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	if err := r.Commit(ctx, "verified"); err != nil {
		t.Fatal(err)
	}
	runGit(t, dir, "remote", "add", "origin", bare)
	for key, value := range map[string]string{
		"core.gitProxy":       "proxy.example.invalid",
		"http.proxy":          "http://proxy.example.invalid:8080",
		"credential.helper":   "invalid-helper",
		"remote.origin.proxy": "http://proxy.example.invalid:8080",
	} {
		runGit(t, dir, "config", key, value)
	}
	marker := filepath.Join(dir, "uploadpack-ran")
	malicious := "touch " + marker + "; git-upload-pack"
	runGit(t, dir, "config", "remote.origin.uploadpack", malicious)
	configured, err := r.ConfigGetUploadpack(ctx, "origin")
	if err != nil || strings.TrimSpace(string(configured)) != malicious {
		t.Fatalf("uploadpack config not visible: %q %v", configured, err)
	}
	fetch, push, err := r.RemoteURLs(ctx, "origin")
	if err != nil || strings.TrimSpace(string(fetch)) != bare || strings.TrimSpace(string(push)) != bare {
		t.Fatalf("urls %q %q %v", fetch, push, err)
	}
	if out, err := r.LsRemote(ctx, "origin", "refs/heads/work"); err != nil || len(strings.TrimSpace(string(out))) != 0 {
		t.Fatalf("expected empty remote ref, got %q: %v", out, err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("configured uploadpack executed: %v", err)
	}
	head, err := r.RevParseHead(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Push(ctx, "origin", "work", strings.TrimSpace(string(head))); err != nil {
		t.Fatal(err)
	}
	out, err := r.LsRemote(ctx, "origin", "refs/heads/work")
	if err != nil || len(strings.TrimSpace(string(out))) < 40 {
		t.Fatalf("ls-remote %q: %v", out, err)
	}
	for _, pair := range [][2]string{{"-evil", "work"}, {"origin", "HEAD"}, {"origin", "refs/heads/work"}, {"origin", "bad name"}} {
		if err := ValidatePushArgv([]string{"/usr/bin/git", "push", "--force"}, pair[0], pair[1], strings.TrimSpace(string(head))); err == nil {
			t.Fatalf("accepted unsafe push %v", pair)
		}
	}
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	c := exec.Command("/usr/bin/git", args...)
	c.Dir = dir
	if out, err := c.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
}

func TestGitRunnerTimeoutKillsProcessGroup(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "slow-git")
	marker := filepath.Join(dir, "survived")
	if err := os.WriteFile(fake, []byte("#!/bin/sh\n/bin/sh -c 'sleep 1; touch \""+marker+"\"' &\nwait\n"), 0700); err != nil {
		t.Fatal(err)
	}
	r := newGitRunnerWithBinary(t, fake, dir, "", "")
	_, err := r.run(context.Background(), "git_local", 20_000_000, false, "status")
	if !errors.Is(err, ErrGitTimeout) {
		t.Fatalf("want git_timeout, got %v", err)
	}
	time.Sleep(1200 * time.Millisecond)
	if _, statErr := os.Stat(marker); !os.IsNotExist(statErr) {
		t.Fatal("grandchild survived timeout")
	}
}

func TestGitRunnerHardeningIsPresentAndValidatedPushArgvIsExecutedUnchanged(t *testing.T) {
	dir := t.TempDir()
	runGit(t, dir, "init", "-q")
	log := filepath.Join(dir, "argv.log")
	fake := filepath.Join(dir, "git-recorder")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" >> '" + log + "'\nprintf '\\n' >> '" + log + "'\ncase \"$*\" in *--show-toplevel*) printf '%s\\n' '" + dir + "';; esac\nexit 0\n"
	if err := os.WriteFile(fake, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	r := newGitRunnerWithBinary(t, fake, dir, "", " ")
	r.stateDir = filepath.Join(dir, "state")
	if err := os.Mkdir(r.stateDir, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Status(context.Background()); err != nil {
		t.Fatal(err)
	}
	sha := stringOf('b', 40)
	if err := r.Push(context.Background(), "origin", "work", sha); err != nil {
		t.Fatal(err)
	}
	if _, err := r.LsRemote(context.Background(), "origin", "refs/heads/work"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	sections := strings.Split(strings.TrimSpace(string(data)), "\n\n")
	if len(sections) != 4 {
		t.Fatalf("expected four operations recorded: %q", data)
	}
	for _, section := range sections {
		for _, arg := range strings.Fields(SupervisorGitHardeningArgs) {
			if !strings.Contains("\n"+section+"\n", "\n"+arg+"\n") {
				t.Fatalf("hardening argument %q missing from %q", arg, section)
			}
		}
	}
	want := strings.Join(BuildPushArgv("origin", "work", sha), "\n") + "\n"
	if sections[2] != strings.TrimSpace(want) {
		t.Fatalf("push argv changed: got %q want %q", sections[2], strings.TrimSpace(want))
	}
	lsRemoteWant := append(strings.Fields(SupervisorGitHardeningArgs), "-c", "remote.origin.proxy=", "ls-remote", "--upload-pack=git-upload-pack", "--end-of-options", "origin", "refs/heads/work")
	if sections[3] != strings.Join(lsRemoteWant, "\n") {
		t.Fatalf("ls-remote argv changed: got %q want %q", sections[3], lsRemoteWant)
	}
}

func TestGitRunnerFixedInputsAndNotFound(t *testing.T) {
	dir := t.TempDir()
	runGit(t, dir, "init", "-q")
	r := newGitRunnerWithBinary(t, "/usr/bin/git", dir, "", " ")
	r.stateDir = t.TempDir()
	if _, err := r.RevListSince(context.Background(), "--output=/tmp/unsafe", strings.Repeat("b", 40)); err == nil {
		t.Fatal("accepted option as revision")
	}
	for _, head := range []string{"HEAD", strings.Repeat("B", 40), strings.Repeat("b", 39)} {
		if _, err := r.RevListSince(context.Background(), strings.Repeat("a", 40), head); err == nil {
			t.Fatalf("accepted invalid head %q", head)
		}
	}
	if err := r.CatFileExists(context.Background(), "--help"); err == nil {
		t.Fatal("accepted option as sha")
	}
	if _, err := r.ConfigGetUploadpack(context.Background(), "-bad"); err == nil {
		t.Fatal("accepted option as remote")
	}
	if _, err := r.ConfigGetUploadpack(context.Background(), "origin"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing config should be not_found: %v", err)
	}
	if _, err := r.RevParseToplevel(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestGitRunnerRevListSinceUsesInspectedHead(t *testing.T) {
	home := t.TempDir()
	dir := t.TempDir()
	pushPolicyGit(t, dir, home, "init", "-q", "--object-format=sha1", "-b", "topic")
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("base"), 0600); err != nil {
		t.Fatal(err)
	}
	pushPolicyGit(t, dir, home, "add", "file")
	pushPolicyGit(t, dir, home, "-c", "user.name=Setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "base")
	base := pushPolicyGit(t, dir, home, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(dir, "file"), []byte("next"), 0600); err != nil {
		t.Fatal(err)
	}
	pushPolicyGit(t, dir, home, "add", "file")
	pushPolicyGit(t, dir, home, "-c", "user.name=Setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "next")
	r := newGitRunnerWithBinary(t, "/usr/bin/git", dir, "", "")
	if out, err := r.RevListSince(context.Background(), base, base); err != nil || len(out) != 0 {
		t.Fatalf("rev-list followed moving HEAD: %q %v", out, err)
	}
}
