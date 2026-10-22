package gitops

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
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

const gitTimingTimeout = 500 * time.Millisecond
const gitTimingWatchdog = 15 * time.Second

func awaitGitTiming[T any](t *testing.T, ch <-chan T, operation string) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(gitTimingWatchdog):
		t.Fatalf("watchdog waiting for %s", operation)
		var zero T
		return zero
	}
}

func holdGitTimingFIFO(t *testing.T, path string) *os.File {
	t.Helper()
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	held, err := os.OpenFile(path, os.O_RDWR|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	return held
}

// EOF cannot release cat while the holding descriptor is open. exec keeps
// Output/Wait responsible for exactly one directly owned child.
func hangingGitTimingBinary(t *testing.T, dir string) string {
	t.Helper()
	fifo := filepath.Join(dir, "release")
	holdGitTimingFIFO(t, fifo)
	binary := filepath.Join(dir, "git-slow")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexec /bin/cat < '"+fifo+"'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	return binary
}

func TestGitRunnerTimeoutClassification(t *testing.T) {
	dir := t.TempDir()
	r := newGitRunnerWithBinary(t, hangingGitTimingBinary(t, dir), dir, "", "")
	ctx, cancel := context.WithTimeout(context.Background(), gitTimingWatchdog)
	defer cancel()
	_, err := r.run(ctx, proc.GitEnvLocal, gitTimingTimeout, false, "status")
	if ctx.Err() != nil {
		t.Fatalf("watchdog ended: %v", ctx.Err())
	}
	if !errors.Is(err, ErrGitTimeout) {
		t.Fatalf("want git_timeout, got %v", err)
	}
}

// The helper is selected only in subprocesses. It reports kernel PID/PGID and
// stays alive behind a FIFO gate, with no finite lifetime or external clock.
func TestGitRunnerTimeoutFixture(t *testing.T) {
	args := os.Args
	if len(args) < 5 || args[len(args)-5] != "timing-process-group" {
		return
	}
	fifo, gate, role := args[len(args)-4], args[len(args)-3], args[len(args)-2]
	// The trailing argument protects this protocol from go test's own flags.
	if args[len(args)-1] != "fixture" {
		return
	}
	var child *exec.Cmd
	if role == "parent" {
		child = exec.Command(args[0], "-test.run=^TestGitRunnerTimeoutFixture$", "--", "timing-process-group", fifo, gate, "grandchild", "fixture")
		child.Env = proc.SafeChildEnv()
		if err := child.Start(); err != nil {
			os.Exit(2)
		}
	}
	pgid, err := syscall.Getpgid(os.Getpid())
	if err != nil {
		os.Exit(3)
	}
	output, err := os.OpenFile(fifo, os.O_WRONLY, 0)
	if err != nil {
		os.Exit(4)
	}
	if _, err := fmt.Fprintf(output, "%s %d %d READY\n", role, os.Getpid(), pgid); err != nil {
		os.Exit(5)
	}
	_ = output.Close()
	input, err := os.Open(gate)
	if err != nil {
		os.Exit(6)
	}
	_, _ = input.Read(make([]byte, 1))
	_ = input.Close()
	if child != nil {
		_ = child.Process.Kill()
		_ = child.Wait()
	}
	os.Exit(0)
}

type gitTimingIdentity struct {
	role      string
	pid, pgid int
}

func parseGitTimingIdentity(line string) (gitTimingIdentity, error) {
	fields := strings.Fields(line)
	if !strings.HasSuffix(line, "\n") || len(fields) != 4 || fields[3] != "READY" || (fields[0] != "parent" && fields[0] != "grandchild") {
		return gitTimingIdentity{}, fmt.Errorf("invalid READY record=%q", line)
	}
	pid, e1 := strconv.Atoi(fields[1])
	pgid, e2 := strconv.Atoi(fields[2])
	if e1 != nil || e2 != nil || pid <= 1 || pgid <= 1 {
		return gitTimingIdentity{}, fmt.Errorf("invalid process identity=%q", line)
	}
	return gitTimingIdentity{fields[0], pid, pgid}, nil
}

func TestGitRunnerTimeoutKillsProcessGroup(t *testing.T) {
	t.Run("reject invalid identities", func(t *testing.T) {
		for _, line := range []string{"", "parent 123 123", "parent 123 123 READY", "parent x 123 READY\n", "parent 1 123 READY\n", "grandchild 124 0 READY\n", "other 123 123 READY\n", "parent 123 123 WAIT\n"} {
			if _, err := parseGitTimingIdentity(line); err == nil {
				t.Fatalf("accepted %q", line)
			}
		}
	})

	dir := t.TempDir()
	fifo, gate := filepath.Join(dir, "ready"), filepath.Join(dir, "release")
	held := holdGitTimingFIFO(t, fifo)
	release := holdGitTimingFIFO(t, gate)
	input, err := os.Open(fifo)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	fake := filepath.Join(dir, "slow-git")
	script := "#!/bin/sh\nexec '" + binary + "' '-test.run=^TestGitRunnerTimeoutFixture$' -- timing-process-group '" + fifo + "' '" + gate + "' parent fixture\n"
	if err := os.WriteFile(fake, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	records := make(chan string, 2)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		reader := bufio.NewReader(input)
		for i := 0; i < 2; i++ {
			line, err := reader.ReadString('\n')
			if err != nil {
				records <- "read error: " + err.Error()
				return
			}
			records <- line
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	exited := make(chan struct{})
	r := newGitRunnerWithBinary(t, fake, dir, "", "")
	go func() {
		defer close(exited)
		_, err := r.run(ctx, proc.GitEnvLocal, time.Hour, false, "status")
		result <- err
	}()
	pids := map[string]int{}
	var group, killGroup int
	defer func() {
		cancel()
		// Only the validated fixture group may be signalled.
		if killGroup > 1 {
			_ = syscall.Kill(-killGroup, syscall.SIGKILL)
		}
		_ = release.Close()
		awaitGitTiming(t, exited, "runner cleanup")
		_ = held.Close()
		awaitGitTiming(t, readDone, "READY reader cleanup")
		_ = input.Close()
	}()
	for i := 0; i < 2; i++ {
		line := awaitGitTiming(t, records, "child/grandchild READY")
		identity, err := parseGitTimingIdentity(line)
		if err != nil {
			t.Fatal(err)
		}
		pgid := identity.pgid
		if group != 0 && group != pgid {
			t.Fatalf("group differs: %q", line)
		}
		group = pgid
		if _, duplicate := pids[identity.role]; duplicate {
			t.Fatalf("duplicate role: %q", line)
		}
		pids[identity.role] = identity.pid
	}
	if group <= 1 || group == syscall.Getpgrp() || group != pids["parent"] || pids["grandchild"] == group {
		t.Fatalf("identities=%v pgid=%d", pids, group)
	}
	killGroup = group
	watcher, err := proc.WatchExitWithoutReaping(pids["grandchild"])
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	watchCtx, stopWatch := context.WithTimeout(context.Background(), gitTimingWatchdog)
	defer stopWatch()
	cancel() // registration happened while both processes were alive
	if err := awaitGitTiming(t, result, "direct child Wait"); !errors.Is(err, context.Canceled) {
		t.Errorf("runner error=%v, want context.Canceled", err)
	}
	if err := watcher.Wait(watchCtx); err != nil {
		t.Errorf("grandchild exit was not observed: %v", err)
	} else {
		// The grandchild exited and the runner has reaped its direct child.
		killGroup = 0
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
