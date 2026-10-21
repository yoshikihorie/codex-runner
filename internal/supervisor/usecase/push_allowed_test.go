package usecase

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/gitops"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

const pushTestBase = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const pushTestHead = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

type pushFakeGit struct {
	dir         string
	ref         string
	refErr      error
	fetchURLs   string
	pushURLs    string
	receive     bool
	upload      bool
	vcs         bool
	remoteOut   string
	remoteEmpty bool
	remoteErr   error
	urlErr      error
	catErr      error
	commits     string
	commitsErr  error
	noCommits   bool
	head        string
	pushErr     error
	pushRemote  string
	pushBranch  string
	pushSHA     string
	revListBase string
	revListHead string
	lsRemote    string
	calls       []string
	onCall      func(string)
}

func (g *pushFakeGit) call(name string) {
	g.calls = append(g.calls, name)
	if g.onCall != nil {
		g.onCall(name)
	}
}
func (g *pushFakeGit) SymbolicRef(context.Context) ([]byte, error) {
	g.call("symbolic-ref")
	if g.refErr != nil {
		return nil, g.refErr
	}
	if g.ref != "" {
		return []byte(g.ref), nil
	}
	return []byte("refs/heads/topic\n"), nil
}
func (g *pushFakeGit) RemoteURLs(context.Context, string) ([]byte, []byte, error) {
	g.call("remote get-url")
	if g.urlErr != nil {
		return nil, nil, g.urlErr
	}
	if g.fetchURLs != "" || g.pushURLs != "" {
		fetch, push := g.fetchURLs, g.pushURLs
		if fetch == "" {
			fetch = "url\n"
		}
		if push == "" {
			push = "url\n"
		}
		return []byte(fetch), []byte(push), nil
	}
	return []byte("url\n"), []byte("url\n"), nil
}
func (g *pushFakeGit) ConfigGetReceivepack(context.Context, string) ([]byte, error) {
	g.call("receivepack")
	if g.receive {
		return []byte("custom\n"), nil
	}
	return nil, gitops.ErrNotFound
}
func (g *pushFakeGit) ConfigGetUploadpack(context.Context, string) ([]byte, error) {
	g.call("uploadpack")
	if g.upload {
		return []byte("custom\n"), nil
	}
	return nil, gitops.ErrNotFound
}
func (g *pushFakeGit) ConfigGetVcs(context.Context, string) ([]byte, error) {
	g.call("vcs")
	if g.vcs {
		return []byte("custom\n"), nil
	}
	return nil, gitops.ErrNotFound
}
func (g *pushFakeGit) RevParseGitCommonDir(context.Context) ([]byte, error) {
	g.call("common-dir")
	return []byte(g.dir), nil
}
func (g *pushFakeGit) RevParseGitDir(context.Context) ([]byte, error) {
	g.call("git-dir")
	return []byte(g.dir), nil
}
func (g *pushFakeGit) LsRemote(_ context.Context, remote, _ string) ([]byte, error) {
	g.call("ls-remote")
	g.lsRemote = remote
	if g.remoteErr != nil {
		return nil, g.remoteErr
	}
	if g.remoteEmpty {
		return nil, nil
	}
	if g.remoteOut != "" {
		return []byte(g.remoteOut), nil
	}
	return []byte(pushTestBase + "\trefs/heads/topic\n"), nil
}
func (g *pushFakeGit) CatFileExists(context.Context, string) error {
	g.call("cat-file")
	return g.catErr
}
func (g *pushFakeGit) RevListSince(_ context.Context, baseSHA, headSHA string) ([]byte, error) {
	g.call("rev-list")
	g.revListBase, g.revListHead = baseSHA, headSHA
	if g.commitsErr != nil {
		return nil, g.commitsErr
	}
	if g.noCommits {
		return nil, nil
	}
	if g.commits != "" {
		return []byte(g.commits), nil
	}
	return []byte(pushTestHead + "\n"), nil
}
func (g *pushFakeGit) RevParseHead(context.Context) ([]byte, error) {
	g.call("rev-parse HEAD")
	if g.head != "" {
		return []byte(g.head), nil
	}
	return []byte(pushTestHead + "\n"), nil
}
func (g *pushFakeGit) Push(_ context.Context, remote, branch, inspectedSHA string) error {
	g.call("push")
	g.pushRemote, g.pushBranch = remote, branch
	g.pushSHA = inspectedSHA
	return g.pushErr
}

type pushTestSessions struct {
	value   supervisor.ManagedSession
	saveErr error
}

func (s *pushTestSessions) Load(supervisor.SessionID) (supervisor.ManagedSession, error) {
	return s.value, nil
}
func (s *pushTestSessions) Save(v supervisor.ManagedSession) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	s.value = v
	return nil
}

type pushFakeProjects struct {
	shas []string
	err  error
}

func (p pushFakeProjects) Load(string) (store.ProjectRecord, error) {
	return store.ProjectRecord{SupervisorCommitSHAs: p.shas}, p.err
}

func pushFakeFixture(t *testing.T) (*PushToAllowedRemoteUseCase, *pushFakeGit, *pushTestSessions, *[]store.Event) {
	t.Helper()
	dir := t.TempDir()
	fp := gitops.GitConfigFingerprintFromEntries(nil).SHA256
	id, _ := supervisor.NewSessionID("push-test-1")
	sessions := &pushTestSessions{value: supervisor.ManagedSession{SessionID: id, ProjectID: "sample", GitConfigFingerprint: fp}}
	git := &pushFakeGit{dir: dir}
	events := &[]store.Event{}
	u := &PushToAllowedRemoteUseCase{Git: git, Sessions: sessions, Projects: pushFakeProjects{shas: []string{pushTestHead}}, Stop: NewStopState(t.TempDir()), Allowlist: []gitops.PushAllowlistEntry{{Remote: "origin", URL: "url", Branch: "topic"}}, Record: func(e store.Event) error { *events = append(*events, e); return nil }}
	return u, git, sessions, events
}
func pushRunFake(t *testing.T, u *PushToAllowedRemoteUseCase, s *pushTestSessions) {
	t.Helper()
	if err := u.Run(context.Background(), s.value, CommitLocksHeld{session: s.value.SessionID}); err != nil {
		t.Fatal(err)
	}
}

func TestPushToAllowedRemote_SCN13_FastForward(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	pushRunFake(t, u, s)
	if len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindPushSucceeded || (*events)[0].CommitSHA != pushTestHead || (*events)[0].Remote != "origin" || (*events)[0].Branch != "topic" || g.pushRemote != "origin" || g.pushBranch != "topic" || g.pushSHA != pushTestHead || g.calls[len(g.calls)-1] != "push" {
		t.Fatalf("calls=%v events=%+v", g.calls, *events)
	}
}
func TestPushToAllowedRemote_SCN58_HeadMovesAfterInspection(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[failed], func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			if failed {
				g.pushErr = errors.New("push failed")
			}
			g.onCall = func(name string) {
				if name == "rev-list" {
					g.head = strings.Repeat("c", 40)
				}
			}
			pushRunFake(t, u, s)
			if g.revListBase != pushTestBase || g.revListHead != pushTestHead || g.pushSHA != pushTestHead || (*events)[0].CommitSHA != pushTestHead {
				t.Fatalf("rev-list range=%q..%q push sha=%q event=%+v", g.revListBase, g.revListHead, g.pushSHA, *events)
			}
			if failed && s.value.LastPushFailedHead != pushTestHead {
				t.Fatalf("failed head=%q", s.value.LastPushFailedHead)
			}
			headReads := 0
			for _, call := range g.calls {
				if call == "rev-parse HEAD" {
					headReads++
				}
			}
			if headReads != 1 {
				t.Fatalf("HEAD reads=%d", headReads)
			}
		})
	}
}

func TestPushToAllowedRemote_SCN59_ConfigChangesBeforeGitExecution(t *testing.T) {
	for _, tc := range []struct {
		name, trigger string
		wantRemote    bool
	}{
		{"before ls-remote", "symbolic-ref", false},
		{"before push", "ls-remote", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			refReads := 0
			g.onCall = func(name string) {
				if name == "symbolic-ref" {
					refReads++
				}
				if name == tc.trigger && (name != "symbolic-ref" || refReads == 2) {
					if err := os.WriteFile(filepath.Join(g.dir, "config"), []byte("[core]\n\tfoo = changed\n"), 0600); err != nil {
						t.Fatal(err)
					}
				}
			}
			pushRunFake(t, u, s)
			remoteCalled := strings.Contains(strings.Join(g.calls, ","), "ls-remote")
			if remoteCalled != tc.wantRemote || g.pushSHA != "" || len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindPushBlocked || (*events)[0].Code != pushRejectedCode || (*events)[0].Reason != store.ReasonGitConfigChanged {
				t.Fatalf("calls=%v events=%+v", g.calls, *events)
			}
		})
	}
}

func TestPushToAllowedRemote_SCN59_URLChangesBeforePush(t *testing.T) {
	for _, tc := range []struct {
		name, fetch, push string
	}{
		{"fetch", "other\n", "url\n"},
		{"push", "url\n", "other\n"},
		{"count", "url\nother\n", "url\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			g.onCall = func(name string) {
				if name == "ls-remote" {
					g.fetchURLs, g.pushURLs = tc.fetch, tc.push
				}
			}
			pushRunFake(t, u, s)
			if g.lsRemote == "" || g.pushSHA != "" || len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindPushBlocked || (*events)[0].Code != pushRejectedCode || (*events)[0].Reason != store.ReasonURLMismatch {
				t.Fatalf("calls=%v events=%+v", g.calls, *events)
			}
		})
	}
}

func TestPushToAllowedRemote_URLRecheckGitFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		reason store.EventReason
	}{
		{"git failed", errors.New("remote get-url failed"), store.ReasonGitFailed},
		{"output too large", gitops.ErrGitOutputTooLarge, store.ReasonGitOutputTooLarge},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			g.onCall = func(name string) {
				if name == "ls-remote" {
					g.urlErr = tc.err
				}
			}
			pushRunFake(t, u, s)
			if g.lsRemote == "" || g.pushSHA != "" || len(*events) != 1 || (*events)[0].Reason != tc.reason {
				t.Fatalf("calls=%v events=%+v", g.calls, *events)
			}
		})
	}
}

func TestPushToAllowedRemote_BranchChangesAfterHeadRead(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	g.onCall = func(name string) {
		if name == "rev-parse HEAD" {
			g.ref = "refs/heads/other\n"
		}
	}
	pushRunFake(t, u, s)
	if len(*events) != 1 || (*events)[0].Reason != store.ReasonNotInAllowlist || g.pushSHA != "" || g.revListHead != "" {
		t.Fatalf("calls=%v events=%+v", g.calls, *events)
	}
}

func TestPushToAllowedRemote_ProjectRecordLoadFailureIsForeign(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	u.Projects = pushFakeProjects{err: errors.New("corrupt project record")}
	pushRunFake(t, u, s)
	if len(*events) != 1 || (*events)[0].Reason != store.ReasonForeignCommits || (*events)[0].Count != 1 || strings.Contains(strings.Join(g.calls, ","), "push") {
		t.Fatalf("calls=%v events=%+v", g.calls, *events)
	}
}
func TestPushToAllowedRemote_CallerCancellationHasNoSideEffects(t *testing.T) {
	for _, stage := range []string{"ls-remote", "push"} {
		t.Run(stage, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			notifications := 0
			u.Notify = func(supervisor.SupervisorEventKind, supervisor.SessionID) { notifications++ }
			g.onCall = func(name string) {
				if name == stage {
					cancel()
				}
			}
			if stage == "ls-remote" {
				g.remoteErr = context.Canceled
			} else {
				g.pushErr = context.Canceled
			}
			err := u.Run(ctx, s.value, CommitLocksHeld{session: s.value.SessionID})
			if !errors.Is(err, context.Canceled) || len(*events) != 0 || s.value.LastPushFailedHead != "" || notifications != 0 {
				t.Fatalf("err=%v events=%+v failed=%q notifications=%d", err, *events, s.value.LastPushFailedHead, notifications)
			}
		})
	}
}
func TestPushToAllowedRemote_CallerDeadlineHasNoSideEffects(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	g.pushErr = context.DeadlineExceeded
	g.onCall = func(name string) {
		if name == "push" {
			<-ctx.Done()
		}
	}
	notifications := 0
	u.Notify = func(supervisor.SupervisorEventKind, supervisor.SessionID) { notifications++ }
	err := u.Run(ctx, s.value, CommitLocksHeld{session: s.value.SessionID})
	if !errors.Is(err, context.DeadlineExceeded) || len(*events) != 0 || s.value.LastPushFailedHead != "" || notifications != 0 || g.pushRemote != "origin" {
		t.Fatalf("err=%v events=%+v failed=%q notifications=%d", err, *events, s.value.LastPushFailedHead, notifications)
	}
}
func TestPushToAllowedRemote_InternalPushTimeoutStillFails(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	g.pushErr = gitops.ErrGitTimeout
	pushRunFake(t, u, s)
	if len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindPushFailed || s.value.LastPushFailedHead != pushTestHead {
		t.Fatalf("events=%+v failed=%q", *events, s.value.LastPushFailedHead)
	}
}
func TestPushToAllowedRemote_StopBeforeEvent(t *testing.T) {
	for _, stage := range []string{"ls-remote", "push"} {
		t.Run(stage, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			g.onCall = func(name string) {
				if name == stage {
					u.Stop.SetSession(s.value.SessionID, true)
				}
			}
			if stage == "push" {
				g.pushErr = errors.New("failed")
			} else {
				g.remoteErr = errors.New("failed")
			}
			pushRunFake(t, u, s)
			if len(*events) != 0 || s.value.LastPushFailedHead != "" {
				t.Fatalf("events=%+v failed=%q", *events, s.value.LastPushFailedHead)
			}
		})
	}
}
func TestPushToAllowedRemote_StopDuringRecordSkipsNotification(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	g.remoteErr = errors.New("remote failed")
	notifications := 0
	u.Record = func(e store.Event) error {
		*events = append(*events, e)
		u.Stop.SetSession(s.value.SessionID, true)
		return nil
	}
	u.Notify = func(supervisor.SupervisorEventKind, supervisor.SessionID) { notifications++ }
	pushRunFake(t, u, s)
	if len(*events) != 1 || notifications != 0 {
		t.Fatalf("events=%+v notifications=%d", *events, notifications)
	}
}
func TestPushToAllowedRemote_AmbiguousBranchRemoteIsRejected(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	u.Allowlist = append(u.Allowlist, gitops.PushAllowlistEntry{Remote: "backup", URL: "url", Branch: "topic"})
	pushRunFake(t, u, s)
	if len(*events) != 1 || (*events)[0].Reason != store.ReasonNotInAllowlist || strings.Contains(strings.Join(g.calls, ","), "ls-remote") || strings.Contains(strings.Join(g.calls, ","), "push") {
		t.Fatalf("calls=%v events=%+v", g.calls, *events)
	}
}
func TestPushToAllowedRemote_URLCheckedRemoteMatchesUsedRemote(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	u.Allowlist = []gitops.PushAllowlistEntry{{Remote: "backup", URL: "other", Branch: "topic"}, {Remote: "origin", URL: "url", Branch: "topic"}}
	pushRunFake(t, u, s)
	if len(*events) != 1 || (*events)[0].Reason != store.ReasonNotInAllowlist || g.lsRemote != "" || g.pushRemote != "" {
		t.Fatalf("calls=%v events=%+v ls=%q push=%q", g.calls, *events, g.lsRemote, g.pushRemote)
	}
}
func TestPushToAllowedRemote_SaveFailureStillRecordsPushFailed(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	g.pushErr = errors.New("failed")
	s.saveErr = errors.New("save failed")
	notifications := 0
	u.Notify = func(supervisor.SupervisorEventKind, supervisor.SessionID) { notifications++ }
	err := u.Run(context.Background(), s.value, CommitLocksHeld{session: s.value.SessionID})
	if !errors.Is(err, s.saveErr) || len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindPushFailed || notifications != 1 {
		t.Fatalf("err=%v events=%+v notifications=%d", err, *events, notifications)
	}
}
func TestPushToAllowedRemote_SCN21_InitialGlobalStopAndUnreadableStop(t *testing.T) {
	for _, mode := range []string{"global", "unreadable"} {
		t.Run(mode, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			if mode == "global" {
				u.Stop.SetGlobal(true)
			} else {
				root := filepath.Join(t.TempDir(), "not-a-directory")
				if err := os.WriteFile(root, nil, 0600); err != nil {
					t.Fatal(err)
				}
				u.Stop = NewStopState(root)
			}
			pushRunFake(t, u, s)
			if len(g.calls) != 0 || len(*events) != 0 {
				t.Fatalf("calls=%v events=%+v", g.calls, *events)
			}
		})
	}
}
func TestPushToAllowedRemote_SCN28_MultiplePushURLs(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	g.pushURLs = "url\nother\n"
	pushRunFake(t, u, s)
	if len(*events) != 1 || (*events)[0].Reason != store.ReasonMultipleURLs || g.lsRemote != "" || g.pushRemote != "" {
		t.Fatalf("calls=%v events=%+v", g.calls, *events)
	}
}
func TestPushToAllowedRemote_SCN47_RemoteAndCatFileFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*pushFakeGit)
		reason store.EventReason
	}{
		{"multiple remote rows", func(g *pushFakeGit) {
			g.remoteOut = pushTestBase + "\trefs/heads/topic\n" + pushTestHead + "\trefs/heads/topic\n"
		}, store.ReasonRemoteHeadUnavailable},
		{"remote command failure", func(g *pushFakeGit) { g.remoteErr = errors.New("network failed") }, store.ReasonRemoteHeadUnavailable},
		{"remote timeout", func(g *pushFakeGit) { g.remoteErr = gitops.ErrGitTimeout }, store.ReasonRemoteHeadUnavailable},
		{"cat-file failure", func(g *pushFakeGit) { g.catErr = errors.New("cat-file failed") }, store.ReasonGitFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			tc.change(g)
			pushRunFake(t, u, s)
			if len(*events) != 1 || (*events)[0].Reason != tc.reason || g.pushRemote != "" {
				t.Fatalf("calls=%v events=%+v", g.calls, *events)
			}
		})
	}
}
func TestPushToAllowedRemote_SCN52_InvalidCurrentRefs(t *testing.T) {
	for _, ref := range []string{"refs/heads/HEAD", "refs/heads/refs/topic"} {
		t.Run(ref, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			g.ref = ref
			pushRunFake(t, u, s)
			if len(*events) != 1 || (*events)[0].Reason != store.ReasonArgvNotAllowed || g.pushRemote != "" || g.lsRemote != "" {
				t.Fatalf("calls=%v events=%+v", g.calls, *events)
			}
		})
	}
}
func TestPushToAllowedRemote_GitOutputTooLarge(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	g.urlErr = gitops.ErrGitOutputTooLarge
	pushRunFake(t, u, s)
	if len(*events) != 1 || (*events)[0].Reason != store.ReasonGitOutputTooLarge || g.lsRemote != "" || g.pushRemote != "" {
		t.Fatalf("calls=%v events=%+v", g.calls, *events)
	}
}
func TestPushToAllowedRemote_SCN43_IncludeAndFingerprintReadFailure(t *testing.T) {
	for _, mode := range []string{"include", "read failure"} {
		t.Run(mode, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			config := filepath.Join(g.dir, "config")
			if mode == "include" {
				if err := os.WriteFile(config, []byte("[include]\n path = other\n"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Mkdir(config, 0700); err != nil {
				t.Fatal(err)
			}
			pushRunFake(t, u, s)
			if len(*events) != 1 || (*events)[0].Reason != store.ReasonGitConfigInclude || g.lsRemote != "" || g.pushRemote != "" {
				t.Fatalf("calls=%v events=%+v", g.calls, *events)
			}
		})
	}
}
func TestPushToAllowedRemote_SCN27_HooksFingerprintChanged(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	hooks := filepath.Join(g.dir, "hooks")
	if err := os.Mkdir(hooks, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(hooks, "pre-push"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	pushRunFake(t, u, s)
	if len(*events) != 1 || (*events)[0].Reason != store.ReasonGitConfigChanged || g.lsRemote != "" || g.pushRemote != "" {
		t.Fatalf("calls=%v events=%+v", g.calls, *events)
	}
}
func TestPushToAllowedRemote_SCN20_EmptyAllowlist(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	u.Allowlist = nil
	pushRunFake(t, u, s)
	if len(g.calls) != 0 || len(*events) != 0 {
		t.Fatalf("calls=%v events=%v", g.calls, *events)
	}
}
func TestPushToAllowedRemote_SCN17_SymbolicRefFailure(t *testing.T) {
	for _, err := range []error{errors.New("git failed"), gitops.ErrGitTimeout} {
		t.Run(err.Error(), func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			g.refErr = err
			pushRunFake(t, u, s)
			if len(*events) != 1 || (*events)[0].Reason != store.ReasonGitFailed || strings.Contains(strings.Join(g.calls, ","), "ls-remote") {
				t.Fatalf("calls=%v events=%+v", g.calls, *events)
			}
		})
	}
}
func TestPushToAllowedRemote_SCN17_DetachedHead(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	g.refErr = gitops.ErrNotFound
	pushRunFake(t, u, s)
	if len(*events) != 1 || (*events)[0].Reason != store.ReasonDetachedHead {
		t.Fatalf("events=%+v", *events)
	}
}
func TestPushToAllowedRemote_SCN19_FailedHeadSuppressedOnce(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	g.pushErr = errors.New("rejected")
	pushRunFake(t, u, s)
	if s.value.LastPushFailedHead != pushTestHead || (*events)[0].Kind != supervisor.EventKindPushFailed {
		t.Fatalf("session=%+v events=%+v", s.value, *events)
	}
	pushRunFake(t, u, s)
	pushRunFake(t, u, s)
	count := 0
	for _, e := range *events {
		if e.Kind == supervisor.EventKindPushSuppressedSameHead {
			count++
		}
	}
	if count != 1 || strings.Count(strings.Join(g.calls, ","), "push") != 1 {
		t.Fatalf("calls=%v events=%+v", g.calls, *events)
	}
}

func TestCommitAfterCommit_SCN45_PushEvaluatedAfterCommitAndNoChanges(t *testing.T) {
	for _, noChanges := range []bool{false, true} {
		name := "commit_created"
		if noChanges {
			name = "no_changes"
		}
		t.Run(name, func(t *testing.T) {
			commit, sessions, verifier, root, events := commitTestFixture(t)
			base := commitTestGit(t, root, "rev-parse", "HEAD")
			if noChanges {
				if err := commit.Execute(context.Background(), sessions.value.SessionID); err != nil {
					t.Fatal(err)
				}
			}
			git := &pushFakeGit{dir: filepath.Join(root, ".git"), remoteOut: base + "\trefs/heads/topic\n"}
			push := &PushToAllowedRemoteUseCase{
				Git: git, Sessions: sessions, Projects: commit.Projects.(*store.ProjectRecordStore),
				Stop: commit.Stop, Allowlist: []gitops.PushAllowlistEntry{{Remote: "origin", URL: "url", Branch: "topic"}},
				Record: commit.Record,
			}
			commit.AfterCommit = func(ctx context.Context, s supervisor.ManagedSession, held CommitLocksHeld) error {
				git.head = commitTestGit(t, root, "rev-parse", "HEAD")
				git.commits = git.head + "\n"
				return push.Run(ctx, s, held)
			}
			if err := commit.Execute(context.Background(), sessions.value.SessionID); err != nil {
				t.Fatal(err)
			}
			if strings.Count(strings.Join(git.calls, ","), "push") != 1 {
				t.Fatalf("push calls=%v", git.calls)
			}
			want := supervisor.EventKindCommitCreated
			if noChanges {
				want = supervisor.EventKindNoChanges
			}
			found, pushed := false, false
			for _, event := range *events {
				found = found || event.Kind == want
				pushed = pushed || event.Kind == supervisor.EventKindPushSucceeded
			}
			if !found || !pushed || verifier.calls != 1 {
				t.Fatalf("events=%+v verify calls=%d", *events, verifier.calls)
			}
		})
	}
}
func TestCommitAfterCommit_SCN19_NoChangesSuppressesFailedHead(t *testing.T) {
	commit, sessions, _, root, events := commitTestFixture(t)
	remoteHead := commitTestGit(t, root, "rev-parse", "HEAD")
	if err := commit.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	base := commitTestGit(t, root, "rev-parse", "HEAD")
	git := &pushFakeGit{dir: filepath.Join(root, ".git"), remoteOut: remoteHead + "\trefs/heads/topic\n", head: base, commits: base + "\n", pushErr: errors.New("rejected")}
	push := &PushToAllowedRemoteUseCase{Git: git, Sessions: sessions, Projects: commit.Projects.(*store.ProjectRecordStore), Stop: commit.Stop, Allowlist: []gitops.PushAllowlistEntry{{Remote: "origin", URL: "url", Branch: "topic"}}, Record: commit.Record}
	commit.AfterCommit = func(ctx context.Context, s supervisor.ManagedSession, held CommitLocksHeld) error {
		return push.Run(ctx, s, held)
	}
	if err := commit.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if sessions.value.LastPushFailedHead != base {
		t.Fatalf("failed head=%q", sessions.value.LastPushFailedHead)
	}
	git.pushErr = nil
	if err := commit.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if strings.Count(strings.Join(git.calls, ","), "push") != 1 {
		t.Fatalf("push retried: %v", git.calls)
	}
	failed, suppressed, noChanges := 0, 0, 0
	for _, event := range *events {
		switch event.Kind {
		case supervisor.EventKindPushFailed:
			failed++
		case supervisor.EventKindPushSuppressedSameHead:
			suppressed++
		case supervisor.EventKindNoChanges:
			noChanges++
		}
	}
	if failed != 1 || suppressed != 1 || noChanges != 1 {
		t.Fatalf("events=%+v", *events)
	}
}
func TestPushToAllowedRemote_SCN21_StopBeforeRemoteAndPush(t *testing.T) {
	for _, at := range []string{"rev-parse HEAD", "rev-list"} {
		t.Run(at, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			g.onCall = func(name string) {
				if name == at {
					u.Stop.SetSession(s.value.SessionID, true)
				}
			}
			pushRunFake(t, u, s)
			if strings.Contains(strings.Join(g.calls, ","), "push") || len(*events) != 0 {
				t.Fatalf("calls=%v events=%+v", g.calls, *events)
			}
			if at == "rev-parse HEAD" && strings.Contains(strings.Join(g.calls, ","), "ls-remote") {
				t.Fatalf("ls-remote after stop: %v", g.calls)
			}
		})
	}
}
func TestPushToAllowedRemote_SCN53_LocalRejectSkipsRemote(t *testing.T) {
	u, g, s, events := pushFakeFixture(t)
	config := filepath.Join(g.dir, "config")
	if err := os.WriteFile(config, []byte("[remote \"origin\"]\n\tvcs = helper\n"), 0600); err != nil {
		t.Fatal(err)
	}
	entries, _, err := gitops.BuildGitConfigFingerprintEntries(g.dir, g.dir)
	if err != nil {
		t.Fatal(err)
	}
	s.value.GitConfigFingerprint = gitops.GitConfigFingerprintFromEntries(entries).SHA256
	g.upload = true
	pushRunFake(t, u, s)
	if strings.Contains(strings.Join(g.calls, ","), "ls-remote") || strings.Contains(strings.Join(g.calls, ","), "push") || len(*events) != 1 || (*events)[0].Reason != store.ReasonRemoteHelperConfigured {
		t.Fatalf("calls=%v events=%+v", g.calls, *events)
	}
}

func pushBareGit(t *testing.T, dir, home string, args ...string) string {
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
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}
func pushBarePolicyFixture(t *testing.T, publishBase bool) (string, string, string, *PushToAllowedRemoteUseCase, *pushTestSessions, *[]store.Event) {
	t.Helper()
	home, root := t.TempDir(), t.TempDir()
	bare := filepath.Join(t.TempDir(), "remote.git")
	pushBareGit(t, root, home, "init", "-q", "--object-format=sha1", "-b", "topic")
	pushBareGit(t, root, home, "init", "-q", "--object-format=sha1", "--bare", bare)
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("base"), 0600); err != nil {
		t.Fatal(err)
	}
	pushBareGit(t, root, home, "add", "file")
	pushBareGit(t, root, home, "-c", "user.name=Setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "base")
	pushBareGit(t, root, home, "remote", "add", "origin", bare)
	base := pushBareGit(t, root, home, "rev-parse", "HEAD")
	if publishBase {
		pushBareGit(t, root, home, "push", "origin", "refs/heads/topic:refs/heads/topic")
	}
	entries, _, err := gitops.BuildGitConfigFingerprintEntries(filepath.Join(root, ".git"), filepath.Join(root, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := supervisor.NewSessionID("push-policy-bare-1")
	sessions := &pushTestSessions{value: supervisor.ManagedSession{SessionID: id, ProjectID: "sample", GitConfigFingerprint: gitops.GitConfigFingerprintFromEntries(entries).SHA256}}
	state := t.TempDir()
	projects := store.NewProjectRecordStore(state)
	if err := projects.AppendCommit("sample", base); err != nil {
		t.Fatal(err)
	}
	git := gitops.NewGitRunner(root, "", "", state)
	git.SetParentEnv([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "TMPDIR=" + home, "GIT_CONFIG_GLOBAL=" + os.DevNull})
	events := &[]store.Event{}
	u := &PushToAllowedRemoteUseCase{Git: git, Sessions: sessions, Projects: projects, Stop: NewStopState(state), Allowlist: []gitops.PushAllowlistEntry{{Remote: "origin", URL: bare, Branch: "topic"}}, Record: func(e store.Event) error { *events = append(*events, e); return nil }}
	return root, bare, home, u, sessions, events
}
func TestPushToAllowedRemote_SCN38_LocalTrackingRefDoesNotPermitInitialPush(t *testing.T) {
	root, _, home, u, sessions, events := pushBarePolicyFixture(t, false)
	pushBareGit(t, root, home, "update-ref", "refs/remotes/origin/topic", "HEAD")
	pushRunFake(t, u, sessions)
	if len(*events) != 1 || (*events)[0].Reason != store.ReasonNoTrackingRef {
		t.Fatalf("events=%+v", *events)
	}
}
func TestPushToAllowedRemote_SCN41_ChangedTrackingRefDoesNotPermitForeignCommit(t *testing.T) {
	root, bare, home, u, sessions, events := pushBarePolicyFixture(t, true)
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	pushBareGit(t, root, home, "add", "file")
	pushBareGit(t, root, home, "-c", "user.name=Other", "-c", "user.email=other@example.invalid", "commit", "-q", "-m", "foreign")
	pushBareGit(t, root, home, "update-ref", "refs/remotes/origin/topic", "HEAD")
	remoteBefore := pushBareGit(t, root, home, "--git-dir", bare, "rev-parse", "refs/heads/topic")
	pushRunFake(t, u, sessions)
	if len(*events) != 1 || (*events)[0].Reason != store.ReasonForeignCommits || (*events)[0].Count != 1 {
		t.Fatalf("events=%+v", *events)
	}
	if after := pushBareGit(t, root, home, "--git-dir", bare, "rev-parse", "refs/heads/topic"); after != remoteBefore {
		t.Fatalf("remote moved to %q from %q", after, remoteBefore)
	}
}
func TestPushToAllowedRemote_SCN13_BareFastForwardAndNoChanges(t *testing.T) {
	home := t.TempDir()
	root := t.TempDir()
	bare := filepath.Join(t.TempDir(), "remote.git")
	pushBareGit(t, root, home, "init", "-q", "--object-format=sha1", "-b", "topic")
	pushBareGit(t, root, home, "init", "-q", "--object-format=sha1", "--bare", bare)
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("base"), 0600); err != nil {
		t.Fatal(err)
	}
	pushBareGit(t, root, home, "add", "file")
	pushBareGit(t, root, home, "-c", "user.name=Setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "base")
	pushBareGit(t, root, home, "remote", "add", "origin", bare)
	pushBareGit(t, root, home, "push", "origin", "refs/heads/topic:refs/heads/topic")
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("next"), 0600); err != nil {
		t.Fatal(err)
	}
	pushBareGit(t, root, home, "add", "file")
	pushBareGit(t, root, home, "-c", "user.name=Setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "next")
	head := pushBareGit(t, root, home, "rev-parse", "HEAD")
	if len(head) != 40 {
		t.Fatalf("unexpected HEAD length=%d value=%q", len(head), head)
	}
	entries, _, err := gitops.BuildGitConfigFingerprintEntries(filepath.Join(root, ".git"), filepath.Join(root, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := supervisor.NewSessionID("push-bare-1")
	sessions := &pushTestSessions{value: supervisor.ManagedSession{SessionID: id, ProjectID: "sample", GitConfigFingerprint: gitops.GitConfigFingerprintFromEntries(entries).SHA256}}
	state := t.TempDir()
	projects := store.NewProjectRecordStore(state)
	if err := projects.AppendCommit("sample", head); err != nil {
		t.Fatal(err)
	}
	git := gitops.NewGitRunner(root, "", "", state)
	git.SetParentEnv([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "TMPDIR=" + home, "GIT_CONFIG_GLOBAL=" + os.DevNull})
	events := []store.Event{}
	u := &PushToAllowedRemoteUseCase{Git: git, Sessions: sessions, Projects: projects, Stop: NewStopState(state), Allowlist: []gitops.PushAllowlistEntry{{Remote: "origin", URL: bare, Branch: "topic"}}, Record: func(e store.Event) error { events = append(events, e); return nil }}
	if err := u.Run(context.Background(), sessions.value, CommitLocksHeld{session: id}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Kind != supervisor.EventKindPushSucceeded || events[0].CommitSHA != head {
		t.Fatalf("events=%+v", events)
	}
	if got := pushBareGit(t, root, home, "--git-dir", bare, "rev-parse", "refs/heads/topic"); got != head {
		t.Fatalf("remote head=%s want=%s", got, head)
	}
	if err := u.Run(context.Background(), sessions.value, CommitLocksHeld{session: id}); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("zero unpushed commits recorded: %+v", events)
	}
}

type pushRaceGit struct {
	pushGit
	before func()
	pushes int
}

func (g *pushRaceGit) Push(ctx context.Context, remote, branch, inspectedSHA string) error {
	g.pushes++
	if g.before != nil {
		g.before()
		g.before = nil
	}
	return g.pushGit.Push(ctx, remote, branch, inspectedSHA)
}
func TestPushToAllowedRemote_SCN19_BareNonFastForwardNoRepair(t *testing.T) {
	home := t.TempDir()
	root := t.TempDir()
	bare := filepath.Join(t.TempDir(), "remote.git")
	pushBareGit(t, root, home, "init", "-q", "--object-format=sha1", "-b", "topic")
	pushBareGit(t, root, home, "init", "-q", "--object-format=sha1", "--bare", bare)
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("base"), 0600); err != nil {
		t.Fatal(err)
	}
	pushBareGit(t, root, home, "add", "file")
	pushBareGit(t, root, home, "-c", "user.name=Setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "base")
	pushBareGit(t, root, home, "remote", "add", "origin", bare)
	pushBareGit(t, root, home, "push", "origin", "refs/heads/topic:refs/heads/topic")
	other := t.TempDir()
	pushBareGit(t, other, home, "clone", "-q", bare, ".")
	pushBareGit(t, other, home, "checkout", "-q", "-b", "topic", "origin/topic")
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("local"), 0600); err != nil {
		t.Fatal(err)
	}
	pushBareGit(t, root, home, "add", "file")
	pushBareGit(t, root, home, "-c", "user.name=Setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "local")
	head := pushBareGit(t, root, home, "rev-parse", "HEAD")
	entries, _, err := gitops.BuildGitConfigFingerprintEntries(filepath.Join(root, ".git"), filepath.Join(root, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	id, _ := supervisor.NewSessionID("push-race-1")
	sessions := &pushTestSessions{value: supervisor.ManagedSession{SessionID: id, ProjectID: "sample", GitConfigFingerprint: gitops.GitConfigFingerprintFromEntries(entries).SHA256}}
	state := t.TempDir()
	projects := store.NewProjectRecordStore(state)
	if err := projects.AppendCommit("sample", head); err != nil {
		t.Fatal(err)
	}
	realGit := gitops.NewGitRunner(root, "", "", state)
	realGit.SetParentEnv([]string{"PATH=" + os.Getenv("PATH"), "HOME=" + home, "TMPDIR=" + home, "GIT_CONFIG_GLOBAL=" + os.DevNull})
	remoteHead := ""
	race := &pushRaceGit{pushGit: realGit, before: func() {
		if err := os.WriteFile(filepath.Join(other, "file"), []byte("remote"), 0600); err != nil {
			t.Fatal(err)
		}
		pushBareGit(t, other, home, "add", "file")
		pushBareGit(t, other, home, "-c", "user.name=Setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "remote")
		pushBareGit(t, other, home, "push", "origin", "refs/heads/topic:refs/heads/topic")
		remoteHead = pushBareGit(t, other, home, "rev-parse", "HEAD")
	}}
	events := []store.Event{}
	u := &PushToAllowedRemoteUseCase{Git: race, Sessions: sessions, Projects: projects, Stop: NewStopState(state), Allowlist: []gitops.PushAllowlistEntry{{Remote: "origin", URL: bare, Branch: "topic"}}, Record: func(e store.Event) error { events = append(events, e); return nil }}
	if err := u.Run(context.Background(), sessions.value, CommitLocksHeld{session: id}); err != nil {
		t.Fatal(err)
	}
	if race.pushes != 1 || len(events) != 1 || events[0].Kind != supervisor.EventKindPushFailed || sessions.value.LastPushFailedHead != head {
		t.Fatalf("pushes=%d events=%+v failed=%s", race.pushes, events, sessions.value.LastPushFailedHead)
	}
	if got := pushBareGit(t, root, home, "--git-dir", bare, "rev-parse", "refs/heads/topic"); got != remoteHead {
		t.Fatalf("remote changed: %s want %s", got, remoteHead)
	}
	if err := u.Run(context.Background(), sessions.value, CommitLocksHeld{session: id}); err != nil {
		t.Fatal(err)
	}
	if race.pushes != 1 {
		t.Fatalf("retried after failure: %d", race.pushes)
	}
}

func TestPushToAllowedRemote_SCN14_LocalPolicyStopsRemote(t *testing.T) {
	cases := []struct {
		name   string
		change func(*PushToAllowedRemoteUseCase, *pushFakeGit)
		reason store.EventReason
	}{
		{"other branch", func(_ *PushToAllowedRemoteUseCase, g *pushFakeGit) { g.ref = "refs/heads/other\n" }, store.ReasonNotInAllowlist},
		{"protected", func(u *PushToAllowedRemoteUseCase, _ *pushFakeGit) { u.ProtectedBranches = []string{"topic"} }, store.ReasonProtectedBranch},
		{"url mismatch", func(_ *PushToAllowedRemoteUseCase, g *pushFakeGit) { g.pushURLs = "other\n" }, store.ReasonURLMismatch},
		{"multiple urls", func(_ *PushToAllowedRemoteUseCase, g *pushFakeGit) {
			g.fetchURLs = "url\nother\n"
			g.pushURLs = "url\n"
		}, store.ReasonMultipleURLs},
		{"receivepack", func(_ *PushToAllowedRemoteUseCase, g *pushFakeGit) { g.receive = true }, store.ReasonReceivepackConfigured},
		{"uploadpack", func(_ *PushToAllowedRemoteUseCase, g *pushFakeGit) { g.upload = true }, store.ReasonRemoteHelperConfigured},
		{"vcs", func(_ *PushToAllowedRemoteUseCase, g *pushFakeGit) { g.vcs = true }, store.ReasonRemoteHelperConfigured},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			tc.change(u, g)
			pushRunFake(t, u, s)
			if len(*events) != 1 || (*events)[0].Reason != tc.reason || strings.Contains(strings.Join(g.calls, ","), "ls-remote") || strings.Contains(strings.Join(g.calls, ","), "push") {
				t.Fatalf("calls=%v events=%+v", g.calls, *events)
			}
		})
	}
}
func TestPushToAllowedRemote_SCN29_RemoteAndCommitPolicy(t *testing.T) {
	cases := []struct {
		name    string
		change  func(*PushToAllowedRemoteUseCase, *pushFakeGit)
		reason  store.EventReason
		noEvent bool
	}{
		{"remote unavailable", func(_ *PushToAllowedRemoteUseCase, g *pushFakeGit) { g.remoteErr = gitops.ErrGitTimeout }, store.ReasonRemoteHeadUnavailable, false},
		{"initial push", func(_ *PushToAllowedRemoteUseCase, g *pushFakeGit) { g.remoteEmpty = true }, store.ReasonNoTrackingRef, false},
		{"remote not local", func(_ *PushToAllowedRemoteUseCase, g *pushFakeGit) { g.catErr = gitops.ErrNotFound }, store.ReasonRemoteHeadNotLocal, false},
		{"foreign commit", func(_ *PushToAllowedRemoteUseCase, g *pushFakeGit) { g.commits = pushTestBase + "\n" }, store.ReasonForeignCommits, false},
		{"no unpushed commit", func(_ *PushToAllowedRemoteUseCase, g *pushFakeGit) { g.noCommits = true }, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			tc.change(u, g)
			pushRunFake(t, u, s)
			if tc.noEvent {
				if len(*events) != 0 {
					t.Fatalf("events=%+v", *events)
				}
			} else if len(*events) != 1 || (*events)[0].Reason != tc.reason {
				t.Fatalf("events=%+v", *events)
			}
			if tc.name == "foreign commit" && (*events)[0].Count != 1 {
				t.Fatalf("foreign count=%d", (*events)[0].Count)
			}
			if strings.Contains(strings.Join(g.calls, ","), "push") {
				t.Fatalf("push after rejection: %v", g.calls)
			}
		})
	}
}
func TestPushToAllowedRemote_SCN27_ConfigFingerprint(t *testing.T) {
	for _, name := range []string{"config", "config.worktree", "info/exclude", "info/attributes"} {
		t.Run(name, func(t *testing.T) {
			u, g, s, events := pushFakeFixture(t)
			p := filepath.Join(g.dir, name)
			if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
				t.Fatal(err)
			}
			data := []byte("changed\n")
			if name == "config" || name == "config.worktree" {
				data = []byte("[core]\n\tfoo = bar\n")
			}
			if err := os.WriteFile(p, data, 0600); err != nil {
				t.Fatal(err)
			}
			pushRunFake(t, u, s)
			if len(*events) != 1 || (*events)[0].Reason != store.ReasonGitConfigChanged || strings.Contains(strings.Join(g.calls, ","), "ls-remote") {
				t.Fatalf("calls=%v events=%+v", g.calls, *events)
			}
		})
	}
}
