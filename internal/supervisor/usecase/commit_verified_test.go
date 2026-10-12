package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/gitops"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type commitTestSessions struct {
	value supervisor.ManagedSession
	phase supervisor.GitPhase
}

func (s *commitTestSessions) SetGitPhase(_ supervisor.SessionID, phase supervisor.GitPhase) {
	s.phase = phase
}
func (s *commitTestSessions) SetGitPhaseIfEligible(_ supervisor.SessionID, phase supervisor.GitPhase) (bool, error) {
	if s.value.State != supervisor.SessionIdle && s.value.State != supervisor.SessionAwaitingRecommendation || s.value.Rotation != nil {
		return false, nil
	}
	s.phase = phase
	return true, nil
}

func (s *commitTestSessions) Load(supervisor.SessionID) (supervisor.ManagedSession, error) {
	return s.value, nil
}
func (s *commitTestSessions) Save(v supervisor.ManagedSession) error { s.value = v; return nil }

type commitTestVerifier struct {
	calls int
	code  int
	after func()
}

func (v *commitTestVerifier) Run(context.Context, []string, string, string) (int, error) {
	v.calls++
	if v.after != nil {
		v.after()
	}
	return v.code, nil
}

type commitInterceptGit struct {
	commitGit
	afterAdd func()
}

type commitBlobResetFailGit struct {
	commitGit
	afterAdd func()
	resetErr error
}

func (g commitBlobResetFailGit) Add(ctx context.Context, paths []string) error {
	if err := g.commitGit.Add(ctx, paths); err != nil {
		return err
	}
	g.afterAdd()
	return nil
}
func (g commitBlobResetFailGit) ResetPaths(context.Context, []string) error { return g.resetErr }

type commitRejectGit struct {
	commitGit
	calls int
}

type commitUnavailableGit struct{ commitGit }

func (commitUnavailableGit) RevParseToplevel(context.Context) ([]byte, error) {
	return nil, errors.New("git_unavailable")
}

func (g *commitRejectGit) Commit(context.Context, string) error { g.calls++; return os.ErrPermission }

type commitTreeChangeGit struct {
	commitGit
	calls int
	root  string
	t     *testing.T
}

func (g *commitTreeChangeGit) WriteTree(ctx context.Context) ([]byte, error) {
	g.calls++
	if g.calls == 2 {
		if err := os.WriteFile(filepath.Join(g.root, "owned", "other.txt"), []byte("injected"), 0600); err != nil {
			g.t.Fatal(err)
		}
		commitTestGit(g.t, g.root, "add", "--", "owned/other.txt")
	}
	return g.commitGit.WriteTree(ctx)
}

func (g commitInterceptGit) Add(ctx context.Context, paths []string) error {
	if err := g.commitGit.Add(ctx, paths); err != nil {
		return err
	}
	if g.afterAdd != nil {
		g.afterAdd()
	}
	return nil
}

func commitTestGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	bin, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL="+os.DevNull)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

func commitTestFixture(t *testing.T) (*CommitVerifiedChangesUseCase, *commitTestSessions, *commitTestVerifier, string, *[]store.Event) {
	t.Helper()
	root := t.TempDir()
	commitTestGit(t, root, "init", "-q")
	if err := os.Mkdir(filepath.Join(root, "owned"), 0700); err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(root, "owned", "file.txt")
	if err := os.WriteFile(file, []byte("base"), 0600); err != nil {
		t.Fatal(err)
	}
	commitTestGit(t, root, "add", "--", "owned/file.txt")
	commitTestGit(t, root, "-c", "user.name=setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "base")
	if err := os.WriteFile(file, []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	gitDir := filepath.Join(root, ".git")
	entries, _, err := gitops.BuildGitConfigFingerprintEntries(gitDir, gitDir)
	if err != nil {
		t.Fatal(err)
	}
	configFP := gitops.GitConfigFingerprintFromEntries(entries).SHA256
	stateDir := t.TempDir()
	id, _ := supervisor.NewSessionID("commit-test-1")
	sessions := &commitTestSessions{value: supervisor.ManagedSession{SessionID: id, ProjectID: "sample", State: supervisor.SessionIdle, StateSince: time.Now().Add(-time.Hour), GitConfigFingerprint: configFP}}
	verifier := &commitTestVerifier{}
	events := &[]store.Event{}
	u := &CommitVerifiedChangesUseCase{
		Git: gitops.NewGitRunner(root, "supervisor", "supervisor@example.invalid", stateDir), Verify: verifier,
		Sessions: sessions, Projects: store.NewProjectRecordStore(stateDir), Stop: NewStopState(stateDir), Locker: gitops.NewRepoLocker(),
		CWD: root, OwnedPaths: []string{"owned"}, VerifyCommand: []string{filepath.Join(t.TempDir(), "verify")},
		Record: func(e store.Event) error { *events = append(*events, e); return nil },
	}
	return u, sessions, verifier, root, events
}

func TestCommit_VerifiedChangesOnlyOwnedPaths(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	calledAfterCommit := false
	u.AfterCommit = func(_ context.Context, _ supervisor.ManagedSession, locks CommitLocksHeld) error {
		calledAfterCommit = true
		if locks.session != sessions.value.SessionID {
			t.Fatal("wrong lock token")
		}
		if sessions.phase != supervisor.GitPhaseEvaluating {
			t.Fatalf("session evaluation lock boundary lost: phase=%q", sessions.phase)
		}
		if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
			t.Fatalf("reentrant evaluation was not skipped: %v", err)
		}
		if release, ok := u.Locker.TryLock(filepath.Join(root, ".git")); ok {
			release()
			t.Error("repository lock released before push evaluation boundary")
		}
		return nil
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if !calledAfterCommit {
		t.Fatal("push evaluation boundary not called")
	}
	if verifier.calls != 1 {
		t.Fatalf("verify calls = %d", verifier.calls)
	}
	if got := commitTestGit(t, root, "log", "-1", "--format=%s"); got != "supervisor: verified changes in sample" {
		t.Fatalf("subject = %q", got)
	}
	if len(*events) == 0 || (*events)[len(*events)-1].Kind != supervisor.EventKindCommitCreated {
		t.Fatalf("events = %+v", *events)
	}
	if sessions.value.LastVerifiedFingerprint == "" {
		t.Fatal("verified fingerprint missing")
	}
	record, err := u.Projects.(*store.ProjectRecordStore).Load("sample")
	if err != nil || len(record.SupervisorCommitSHAs) != 1 || record.SupervisorCommitSHAs[0] != commitTestGit(t, root, "rev-parse", "HEAD") {
		t.Fatalf("project record=%+v err=%v", record, err)
	}
}

func TestCommit_StageBlobMismatchRollsBack(t *testing.T) {
	u, sessions, _, root, events := commitTestFixture(t)
	u.Git = commitInterceptGit{commitGit: u.Git, afterAdd: func() {
		if err := os.WriteFile(filepath.Join(root, "owned", "file.txt"), []byte("changed again"), 0600); err != nil {
			t.Fatal(err)
		}
		commitTestGit(t, root, "add", "--", "owned/file.txt")
	}}
	head := commitTestGit(t, root, "rev-parse", "HEAD")
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if got := commitTestGit(t, root, "rev-parse", "HEAD"); got != head {
		t.Fatal("unexpected commit")
	}
	if got := commitTestGit(t, root, "diff", "--cached", "--name-only"); got != "" {
		t.Fatalf("stage not reset: %q", got)
	}
	if len(*events) == 0 || (*events)[len(*events)-1].Reason != store.ReasonTreeMismatchBeforeCommit {
		t.Fatalf("events = %+v", *events)
	}
}

func TestCommit_BlobMismatchRollbackFailureRecordsCurrentFingerprint(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	rollbackErr := errors.New("reset failure")
	u.Git = commitBlobResetFailGit{commitGit: u.Git, resetErr: rollbackErr, afterAdd: func() {
		if err := os.WriteFile(filepath.Join(root, "owned", "file.txt"), []byte("changed again"), 0600); err != nil {
			t.Fatal(err)
		}
		commitTestGit(t, root, "add", "--", "owned/file.txt")
	}}
	if err := u.Execute(context.Background(), sessions.value.SessionID); !errors.Is(err, rollbackErr) {
		t.Fatalf("rollback failure not returned: %v; events=%+v; verify=%d", err, *events, verifier.calls)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 1 || sessions.value.LastFailureFingerprint == "" {
		t.Fatalf("same current tree retried: calls=%d fingerprint=%q", verifier.calls, sessions.value.LastFailureFingerprint)
	}
}

func TestCommit_VerifyFailedNoCommit(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	verifier.code = 1
	head := commitTestGit(t, root, "rev-parse", "HEAD")
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if got := commitTestGit(t, root, "rev-parse", "HEAD"); got != head {
		t.Fatal("unexpected commit")
	}
	if len(*events) == 0 || (*events)[len(*events)-1].Kind != supervisor.EventKindVerifyFailed {
		t.Fatalf("events = %+v", *events)
	}
	if sessions.value.LastFailureFingerprint == "" {
		t.Fatal("failure fingerprint missing")
	}
}

func TestCommit_TreeChangedDuringVerifyNoCommit(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	verifier.after = func() {
		if err := os.WriteFile(filepath.Join(root, "owned", "file.txt"), []byte("changed during verify"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if len(*events) == 0 || (*events)[len(*events)-1].Kind != supervisor.EventKindTreeChangedDuringVerify {
		t.Fatalf("events = %+v", *events)
	}
}

func TestCommit_SameFingerprintNotReverified(t *testing.T) {
	u, sessions, verifier, _, _ := commitTestFixture(t)
	verifier.code = 1
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 1 {
		t.Fatalf("verify calls = %d", verifier.calls)
	}
}

func TestCommit_BlobIDUsesGitBlobFormat(t *testing.T) {
	if got := gitBlobID([]byte("hello\n")); got != "ce013625030ba8dba906f756967f9e9ca394464a" {
		t.Fatalf("blob ID = %q", got)
	}
}

func TestCommit_NoChangesRecordedOnce(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	if err := os.WriteFile(filepath.Join(root, "owned", "file.txt"), []byte("base"), 0600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
			t.Fatal(err)
		}
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindNoChanges {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_OutOfScopeUncommittedAborts(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	outside := filepath.Join(root, "outside.txt")
	if err := os.WriteFile(outside, []byte("user"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Reason != store.ReasonOutOfScopeUncommitted {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
	if got := commitTestGit(t, root, "status", "--porcelain"); !strings.Contains(got, "outside.txt") {
		t.Fatalf("outside changed: %q", got)
	}
}

func TestCommit_StagedOutOfScopeAborts(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	if err := os.WriteFile(filepath.Join(root, "outside.txt"), []byte("user"), 0600); err != nil {
		t.Fatal(err)
	}
	commitTestGit(t, root, "add", "--", "outside.txt")
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Reason != store.ReasonStagedOutOfScope {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
	if got := commitTestGit(t, root, "diff", "--cached", "--name-only"); got != "outside.txt" {
		t.Fatalf("user stage changed: %q", got)
	}
}

func TestCommit_OwnedPathsUnsetNoAction(t *testing.T) {
	u, sessions, verifier, _, events := commitTestFixture(t)
	u.OwnedPaths = nil
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 0 {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_VerifyCommandUnsetNoCommit(t *testing.T) {
	u, sessions, verifier, _, events := commitTestFixture(t)
	u.VerifyCommand = nil
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 0 {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_SensitivePathBlocked(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	if err := os.WriteFile(filepath.Join(root, "owned", ".ENV.local"), []byte("opaque"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindSensitivePathBlocked {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_PathEscapeAborts(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	u.OwnedPaths = []string{filepath.Join("..", filepath.Base(root))}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Reason != store.ReasonPathEscape {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_StopStateSkips(t *testing.T) {
	u, sessions, verifier, _, events := commitTestFixture(t)
	u.Stop.SetSession(sessions.value.SessionID, true)
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 0 {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_RepoBusySkips(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	release, ok := u.Locker.TryLock(filepath.Join(root, ".git"))
	if !ok {
		t.Fatal("lock")
	}
	defer release()
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindRepoBusy {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_GitattributesFilterAborts(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	if err := os.WriteFile(filepath.Join(root, ".gitattributes"), []byte("owned/file.txt filter=unsafe\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// Track the attributes file so it is not an unrelated uncommitted path.
	commitTestGit(t, root, "add", "--", ".gitattributes")
	commitTestGit(t, root, "-c", "user.name=setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "attributes")
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Reason != store.ReasonGitattributesFilter {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_GitConfigIncludeAborts(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	f, err := os.OpenFile(filepath.Join(root, ".git", "config"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("\n[include]\n path = other\n"); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Reason != store.ReasonGitConfigInclude {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_GitConfigOrHooksChangedAborts(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	if err := os.WriteFile(filepath.Join(root, ".git", "hooks", "pre-commit"), []byte("exit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Reason != store.ReasonGitConfigChanged {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_GitConfigFilesChangedAborts(t *testing.T) {
	for _, relative := range []string{"config", "config.worktree"} {
		t.Run(relative, func(t *testing.T) {
			u, sessions, verifier, root, events := commitTestFixture(t)
			if err := os.WriteFile(filepath.Join(root, ".git", relative), []byte("[core]\n\tbare = false\n"), 0600); err != nil {
				t.Fatal(err)
			}
			if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
				t.Fatal(err)
			}
			if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Reason != store.ReasonGitConfigChanged {
				t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
			}
		})
	}
}

func TestCommit_InfoExcludeChangedAborts(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	if err := os.WriteFile(filepath.Join(root, ".git", "info", "exclude"), []byte("outside.txt\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Reason != store.ReasonGitConfigChanged {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_DeletedFileStagesAbsence(t *testing.T) {
	u, sessions, _, root, events := commitTestFixture(t)
	if err := os.Remove(filepath.Join(root, "owned", "file.txt")); err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if got := commitTestGit(t, root, "ls-tree", "HEAD", "owned/file.txt"); got != "" {
		t.Fatalf("deleted file staged: %q", got)
	}
	if len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindCommitCreated {
		t.Fatalf("events=%+v", *events)
	}
}

func TestCommit_HooksDisabledAndRejectedNoRetry(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	hook := filepath.Join(root, ".git", "hooks", "pre-commit")
	if err := os.WriteFile(hook, []byte("#!/bin/sh\nexit 1\n"), 0700); err != nil {
		t.Fatal(err)
	}
	entries, _, err := gitops.BuildGitConfigFingerprintEntries(filepath.Join(root, ".git"), filepath.Join(root, ".git"))
	if err != nil {
		t.Fatal(err)
	}
	sessions.value.GitConfigFingerprint = gitops.GitConfigFingerprintFromEntries(entries).SHA256
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 1 || len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindCommitCreated {
		t.Fatalf("hooks ran: calls=%d events=%+v", verifier.calls, *events)
	}
	u2, sessions2, _, _, events2 := commitTestFixture(t)
	reject := &commitRejectGit{commitGit: u2.Git}
	u2.Git = reject
	for i := 0; i < 2; i++ {
		if err := u2.Execute(context.Background(), sessions2.value.SessionID); err != nil {
			t.Fatal(err)
		}
	}
	if reject.calls != 1 || len(*events2) != 2 || (*events2)[0].Reason != store.ReasonCommitRejected {
		t.Fatalf("reject retried: calls=%d events=%+v", reject.calls, *events2)
	}
}

func TestCommit_TreeMismatchBeforeCommit(t *testing.T) {
	u, sessions, _, root, events := commitTestFixture(t)
	wrapper := &commitTreeChangeGit{commitGit: u.Git, root: root, t: t}
	u.Git = wrapper
	head := commitTestGit(t, root, "rev-parse", "HEAD")
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if got := commitTestGit(t, root, "rev-parse", "HEAD"); got != head {
		t.Fatal("unexpected commit")
	}
	if len(*events) != 1 || (*events)[0].Reason != store.ReasonTreeMismatchBeforeCommit {
		t.Fatalf("events=%+v", *events)
	}
}

func TestCommit_RenamePartialAborts(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	commitTestGit(t, root, "mv", "owned/file.txt", "outside.txt")
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Reason != store.ReasonRenamePartial {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_SymlinkBlobUsesLinkTarget(t *testing.T) {
	u, sessions, _, root, events := commitTestFixture(t)
	if err := os.Symlink("file.txt", filepath.Join(root, "owned", "link")); err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if got := commitTestGit(t, root, "ls-tree", "HEAD", "owned/link"); !strings.HasPrefix(got, "120000 blob ") {
		t.Fatalf("symlink = %q", got)
	}
	if len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindCommitCreated {
		t.Fatalf("events=%+v", *events)
	}
}

func TestCommit_GitBinaryUnresolvedAborts(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	u.Git = commitUnavailableGit{u.Git}
	head := commitTestGit(t, root, "rev-parse", "HEAD")
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || commitTestGit(t, root, "rev-parse", "HEAD") != head || len(*events) != 1 || (*events)[0].Reason != store.ReasonGitUnavailable {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_StopDuringVerifySkipsStage(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	verifier.after = func() { u.Stop.SetSession(sessions.value.SessionID, true) }
	head := commitTestGit(t, root, "rev-parse", "HEAD")
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 1 || commitTestGit(t, root, "rev-parse", "HEAD") != head || commitTestGit(t, root, "diff", "--cached", "--name-only") != "" || len(*events) != 0 {
		t.Fatalf("calls=%d events=%+v", verifier.calls, *events)
	}
}

func TestCommit_NoOutputRecorded(t *testing.T) {
	u, sessions, _, _, events := commitTestFixture(t)
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(*events)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"changed", "supervisor: verified changes", "verify"} {
		if strings.Contains(string(data), secret) {
			t.Fatalf("unexpected output %q in event", secret)
		}
	}
}

func TestCommit_VerifiedResultCanonical(t *testing.T) {
	t.Run("passed", func(t *testing.T) {
		u, sessions, _, _, _ := commitTestFixture(t)
		if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
			t.Fatal(err)
		}
		if sessions.value.LastVerifiedResult != "passed" {
			t.Fatalf("result=%q", sessions.value.LastVerifiedResult)
		}
	})
	t.Run("failed", func(t *testing.T) {
		u, sessions, verifier, _, _ := commitTestFixture(t)
		verifier.code = 1
		if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
			t.Fatal(err)
		}
		if sessions.value.LastVerifiedResult != "failed" {
			t.Fatalf("result=%q", sessions.value.LastVerifiedResult)
		}
	})
}

func TestCommit_NoChangesStillEvaluatesPush(t *testing.T) {
	u, sessions, _, root, _ := commitTestFixture(t)
	if err := os.WriteFile(filepath.Join(root, "owned", "file.txt"), []byte("base"), 0600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	u.AfterCommit = func(context.Context, supervisor.ManagedSession, CommitLocksHeld) error { calls++; return nil }
	for i := 0; i < 2; i++ {
		if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("push evaluations = %d", calls)
	}
}

func TestCommit_SensitiveEventRecordsPatternOnly(t *testing.T) {
	u, sessions, _, root, events := commitTestFixture(t)
	if err := os.WriteFile(filepath.Join(root, "owned", ".ENV.local"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if len(*events) != 1 || (*events)[0].Pattern != ".env*" {
		t.Fatalf("event = %+v", *events)
	}
	data, _ := json.Marshal(*events)
	if strings.Contains(string(data), ".ENV.local") {
		t.Fatal("path leaked")
	}
}

func TestCommit_SymlinkEscapeIsPathEscape(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "owned", "escape")); err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 0 || len(*events) != 1 || (*events)[0].Reason != store.ReasonPathEscape {
		t.Fatalf("events = %+v", *events)
	}
}

type commitStopGit struct {
	commitGit
	afterAdd, afterLs, afterDiff, afterTree   func()
	addCalls, lsCalls, treeCalls, commitCalls int
}

func (g *commitStopGit) Add(ctx context.Context, paths []string) error {
	g.addCalls++
	err := g.commitGit.Add(ctx, paths)
	if g.afterAdd != nil {
		g.afterAdd()
	}
	return err
}
func (g *commitStopGit) LsFilesStage(ctx context.Context, paths []string) ([]byte, error) {
	g.lsCalls++
	data, err := g.commitGit.LsFilesStage(ctx, paths)
	if g.afterLs != nil {
		g.afterLs()
	}
	return data, err
}
func (g *commitStopGit) DiffCached(ctx context.Context) ([]byte, error) {
	data, err := g.commitGit.DiffCached(ctx)
	if g.afterDiff != nil {
		g.afterDiff()
	}
	return data, err
}
func (g *commitStopGit) WriteTree(ctx context.Context) ([]byte, error) {
	g.treeCalls++
	data, err := g.commitGit.WriteTree(ctx)
	if g.afterTree != nil {
		g.afterTree()
	}
	return data, err
}
func (g *commitStopGit) Commit(ctx context.Context, project string) error {
	g.commitCalls++
	return g.commitGit.Commit(ctx, project)
}

func TestCommit_StopRecheckedAtEveryMutation(t *testing.T) {
	for _, point := range []string{"after_verify", "after_add", "after_10b", "before_tree", "before_commit"} {
		t.Run(point, func(t *testing.T) {
			u, sessions, verifier, root, _ := commitTestFixture(t)
			id := sessions.value.SessionID
			stop := func() { u.Stop.SetSession(id, true) }
			if point == "after_verify" {
				verifier.after = stop
			}
			g := &commitStopGit{commitGit: u.Git}
			switch point {
			case "after_add":
				g.afterAdd = stop
			case "after_10b":
				g.afterLs = stop
			case "before_tree":
				g.afterDiff = stop
			case "before_commit":
				g.afterTree = func() {
					if g.treeCalls == 2 {
						stop()
					}
				}
			}
			u.Git = g
			head := commitTestGit(t, root, "rev-parse", "HEAD")
			if err := u.Execute(context.Background(), id); err != nil {
				t.Fatal(err)
			}
			if got := commitTestGit(t, root, "rev-parse", "HEAD"); got != head {
				t.Fatalf("STOP at %s did not stop commit", point)
			}
			wantAdd, wantLs, wantTree := 1, 1, 0
			switch point {
			case "after_verify":
				wantAdd = 0
				wantLs = 0
			case "after_add":
				wantLs = 0
			case "before_commit":
				wantTree = 2
			}
			if g.addCalls != wantAdd || g.lsCalls != wantLs || g.treeCalls != wantTree || g.commitCalls != 0 {
				t.Fatalf("STOP at %s: add=%d ls-files=%d tree=%d commit=%d", point, g.addCalls, g.lsCalls, g.treeCalls, g.commitCalls)
			}
		})
	}
}

type commitLsFailGit struct {
	commitGit
	resetErr error
}

func (g commitLsFailGit) LsFilesStage(context.Context, []string) ([]byte, error) {
	return nil, errors.New("ls failure")
}
func (g commitLsFailGit) ResetPaths(ctx context.Context, paths []string) error {
	if g.resetErr != nil {
		return g.resetErr
	}
	return g.commitGit.ResetPaths(ctx, paths)
}

func TestCommit_LsFailureAndRollbackFailureSuppressesRetry(t *testing.T) {
	u, sessions, verifier, _, events := commitTestFixture(t)
	rollbackErr := errors.New("reset failure")
	u.Git = commitLsFailGit{commitGit: u.Git, resetErr: rollbackErr}
	if err := u.Execute(context.Background(), sessions.value.SessionID); !errors.Is(err, rollbackErr) {
		t.Fatalf("rollback failure not returned: %v", err)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if sessions.value.LastFailureFingerprint == "" || sessions.value.LastFailureReason != supervisor.FailureGitFailed || verifier.calls != 1 {
		t.Fatalf("session=%+v verify=%d events=%+v", sessions.value, verifier.calls, *events)
	}
}

func TestCommit_ModeSwapSameBlobDetected(t *testing.T) {
	u, sessions, verifier, root, events := commitTestFixture(t)
	target := filepath.Join(root, "owned", "changed")
	if err := os.WriteFile(target, []byte("target"), 0600); err != nil {
		t.Fatal(err)
	}
	commitTestGit(t, root, "add", "--", "owned/changed")
	commitTestGit(t, root, "-c", "user.name=setup", "-c", "user.email=setup@example.invalid", "commit", "-q", "-m", "target")
	verifier.after = func() {
		file := filepath.Join(root, "owned", "file.txt")
		if err := os.Remove(file); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("changed", file); err != nil {
			t.Fatal(err)
		}
	}
	head := commitTestGit(t, root, "rev-parse", "HEAD")
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if commitTestGit(t, root, "rev-parse", "HEAD") != head || len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindTreeChangedDuringVerify {
		t.Fatalf("events=%+v", *events)
	}
}

func TestCommit_SameSessionExecutionSerializedAndPhaseCleared(t *testing.T) {
	u, sessions, verifier, _, _ := commitTestFixture(t)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	verifier.after = func() { close(entered); <-release }
	go func() { done <- u.Execute(context.Background(), sessions.value.SessionID) }()
	<-entered
	if sessions.phase != supervisor.GitPhaseEvaluating {
		t.Fatalf("phase=%q", sessions.phase)
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if verifier.calls != 1 || sessions.phase != supervisor.GitPhaseNone {
		t.Fatalf("calls=%d phase=%q", verifier.calls, sessions.phase)
	}
}

type commitNoPhaseSessions struct{ SessionRecordStore }

func TestCommit_RequiresGitPhaseStore(t *testing.T) {
	u, sessions, verifier, _, _ := commitTestFixture(t)
	u.Sessions = commitNoPhaseSessions{sessions}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err == nil || !strings.Contains(err.Error(), "git phase") {
		t.Fatalf("missing phase store accepted: %v", err)
	}
	if verifier.calls != 0 {
		t.Fatalf("verification started: %d", verifier.calls)
	}
}

func TestCommit_PhaseCompletionRequestsAutoContinue(t *testing.T) {
	u, sessions, _, _, _ := commitTestFixture(t)
	calls := 0
	u.AfterGitPhase = func(id supervisor.SessionID, at time.Time) {
		calls++
		if id != sessions.value.SessionID || !at.Equal(sessions.value.StateSince) || sessions.phase != supervisor.GitPhaseNone {
			t.Fatalf("re-evaluation before phase completion: phase=%q id=%q", sessions.phase, id)
		}
	}
	if err := u.Execute(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("re-evaluations=%d", calls)
	}
}

func TestCommit_GitPhaseDispatcherConcurrentRegistration(t *testing.T) {
	u := &CommitVerifiedChangesUseCase{}
	id := supervisor.SessionID("dispatch-session")
	since := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			called := make(chan time.Time, 1)
			unregister := u.RegisterGitPhase(id, func(at time.Time) {
				select {
				case called <- at:
				default:
				}
			})
			u.AfterGitPhase(id, since)
			select {
			case got := <-called:
				if !got.Equal(since) {
					t.Errorf("since=%v", got)
				}
			case <-time.After(time.Second):
				t.Error("notification lost")
			}
			unregister()
		}()
	}
	wg.Wait()
}

func TestCommit_GitPhaseDispatcherRegistersDuringExecute(t *testing.T) {
	u, sessions, _, _, _ := commitTestFixture(t)
	id := sessions.value.SessionID
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			unregister := u.RegisterGitPhase(id, func(time.Time) {})
			unregister()
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		if err := u.Execute(context.Background(), id); err != nil {
			t.Error(err)
		}
	}()
	close(start)
	wg.Wait()
}

func TestCommit_RealSessionStorePersistsFailureAndSuppressesRetry(t *testing.T) {
	u, fake, verifier, _, events := commitTestFixture(t)
	real := store.NewSessionStore(t.TempDir())
	v := fake.value
	v.CLI = supervisor.CLIClaude
	v.Generation = 1
	v.StopScope = supervisor.StopScopeNone
	v.RestartTimes = []time.Time{}
	v.SendGate.NextSeq = 1
	if err := real.Save(v); err != nil {
		t.Fatal(err)
	}
	u.Sessions = real
	u.Git = commitLsFailGit{commitGit: u.Git}
	for i := 0; i < 2; i++ {
		if err := u.Execute(context.Background(), v.SessionID); err != nil {
			t.Fatal(err)
		}
	}
	got, err := real.Load(v.SessionID)
	if err != nil || got.LastFailureFingerprint == "" || got.LastFailureReason != supervisor.FailureGitFailed || verifier.calls != 1 {
		t.Fatalf("got=%+v err=%v verify=%d events=%+v", got, err, verifier.calls, *events)
	}
}
