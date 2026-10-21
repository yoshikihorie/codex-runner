package gitops

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
)

const (
	gitOperationTimeout           = 30 * time.Second  // SUPERVISOR_GIT_OP_TIMEOUT_SECONDS
	gitPushTimeout                = 120 * time.Second // SUPERVISOR_PUSH_TIMEOUT_SECONDS
	gitOutputMaxBytes             = 1 << 20
	timeoutKillGrace              = 10 * time.Second                   // TIMEOUT_KILL_GRACE_SECONDS
	supervisorCommitMessagePrefix = "supervisor: verified changes in " // SUPERVISOR_COMMIT_MESSAGE_TEMPLATE
)

var (
	ErrGitTimeout         = errors.New("git_timeout")
	ErrGitOutputTooLarge  = errors.New("git_output_too_large")
	ErrPushArgvNotAllowed = errors.New("argv_not_allowed")
	ErrGitUnavailable     = errors.New("git_unavailable")
)

// PushRunError marks a failed git invocation separately from the argv gate.
type PushRunError struct{ Cause error }

func (e *PushRunError) Error() string { return e.Cause.Error() }
func (e *PushRunError) Unwrap() error { return e.Cause }

// PushErrorReason is the event classification for a GitRunner.Push error.
func PushErrorReason(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, ErrPushArgvNotAllowed) {
		return "argv_not_allowed"
	}
	return "push_failed"
}

type GitRunner struct {
	git, dir, authorName, authorEmail string
	stateDir                          string
	passSSHAuthSock                   bool
	parentEnv                         []string
	pushTimeout                       time.Duration
}

type gitExitError struct{ code int }

func (e gitExitError) Error() string { return fmt.Sprintf("git_exit_%d", e.code) }

func NewGitRunner(dir, authorName, authorEmail, stateDirectory string) *GitRunner {
	git, _ := proc.FindGitBinary()
	return &GitRunner{git: git, dir: dir, authorName: authorName, authorEmail: authorEmail, parentEnv: os.Environ(), stateDir: stateDirectory, passSSHAuthSock: true, pushTimeout: gitPushTimeout}
}

func (r *GitRunner) SetPassSSHAuthSock(pass bool) { r.passSSHAuthSock = pass }

// SetParentEnv supplies an isolated parent environment for tests and embedding.
func (r *GitRunner) SetParentEnv(env []string) { r.parentEnv = append([]string(nil), env...) }

// SetPushTimeout overrides the canonical limit when a short test deadline is needed.
func (r *GitRunner) SetPushTimeout(timeout time.Duration) { r.pushTimeout = timeout }

func (r *GitRunner) effectivePushTimeout() time.Duration {
	if r.pushTimeout > 0 {
		return r.pushTimeout
	}
	return gitPushTimeout
}

func (r *GitRunner) run(ctx context.Context, purpose proc.GitEnvPurpose, timeout time.Duration, literal bool, args ...string) ([]byte, error) {
	return r.runCommand(ctx, purpose, timeout, literal, true, args...)
}

func (r *GitRunner) runCommand(ctx context.Context, purpose proc.GitEnvPurpose, timeout time.Duration, literal bool, capture bool, args ...string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if r.git == "" {
		return nil, fmt.Errorf("git_unavailable")
	}
	env, err := proc.GitOpsEnv(purpose, purpose == proc.GitEnvPush && r.passSSHAuthSock, r.parentEnv, r.stateDir)
	if err != nil {
		return nil, fmt.Errorf("git_unavailable")
	}
	parentCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	full := append([]string{}, strings.Fields(SupervisorGitHardeningArgs)...)
	if literal {
		full = append(full, "--literal-pathspecs")
	}
	full = append(full, args...)
	cmd := exec.CommandContext(ctx, r.git, full...)
	cmd.Dir = r.dir
	cmd.WaitDelay = 2 * time.Second
	cmd.Cancel = func() error { return terminateProcessGroup(cmd.Process, timeoutKillGrace) }
	cmd.Env = env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr limitedBuffer
	if capture {
		stdout.max = gitOutputMaxBytes
		stderr.max = gitOutputMaxBytes
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr
	}
	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("git_unavailable")
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err = <-done:
	case <-ctx.Done():
		_ = terminateProcessGroup(cmd.Process, timeoutKillGrace)
		<-done
		if parentErr := parentCtx.Err(); parentErr != nil {
			return nil, parentErr
		}
		return nil, ErrGitTimeout
	}
	if capture && (stdout.exceeded || stderr.exceeded) {
		return nil, ErrGitOutputTooLarge
	}
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			return nil, gitExitError{code: exit.ExitCode()}
		}
		return nil, fmt.Errorf("git_failed")
	}
	return append([]byte(nil), stdout.Bytes()...), nil
}

type limitedBuffer struct {
	bytes.Buffer
	max      int
	exceeded bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remain := b.max - b.Len()
	if remain > 0 {
		if len(p) > remain {
			_, _ = b.Buffer.Write(p[:remain])
			b.exceeded = true
		} else {
			_, _ = b.Buffer.Write(p)
		}
	} else if n > 0 {
		b.exceeded = true
	}
	return n, nil
}

func (r *GitRunner) Status(ctx context.Context) ([]byte, error) {
	return r.runAtToplevel(ctx, false, "status", "--porcelain=v1", "-z", "--untracked-files=all")
}
func (r *GitRunner) CheckAttrFilter(ctx context.Context, paths []string) ([]byte, error) {
	return r.runAtToplevel(ctx, true, append([]string{"check-attr", "filter", "-z", "--"}, paths...)...)
}
func (r *GitRunner) Add(ctx context.Context, paths []string) error {
	_, e := r.runAtToplevel(ctx, true, append([]string{"add", "--"}, paths...)...)
	return e
}
func (r *GitRunner) LsFilesStage(ctx context.Context, paths []string) ([]byte, error) {
	return r.runAtToplevel(ctx, true, append([]string{"ls-files", "-s", "-z", "--"}, paths...)...)
}
func (r *GitRunner) ResetPaths(ctx context.Context, paths []string) error {
	_, e := r.runAtToplevel(ctx, true, append([]string{"reset", "-q", "--"}, paths...)...)
	return e
}
func (r *GitRunner) WriteTree(ctx context.Context) ([]byte, error) {
	return r.runAtToplevel(ctx, false, "write-tree")
}
func (r *GitRunner) DiffCached(ctx context.Context) ([]byte, error) {
	return r.runAtToplevel(ctx, true, "diff", "--cached", "--name-only", "-z")
}
func (r *GitRunner) Commit(ctx context.Context, projectID string) error {
	if !validProjectID(projectID) {
		return fmt.Errorf("argv_not_allowed")
	}
	_, e := r.runAtToplevel(ctx, false, "-c", "user.name="+r.authorName, "-c", "user.email="+r.authorEmail, "commit", "-m", supervisorCommitMessagePrefix+projectID)
	return e
}
func (r *GitRunner) RevParseHead(ctx context.Context) ([]byte, error) {
	return r.run(ctx, proc.GitEnvLocal, gitOperationTimeout, false, "rev-parse", "--verify", "HEAD")
}
func (r *GitRunner) RevParseGitDir(ctx context.Context) ([]byte, error) {
	return r.revParseDirectory(ctx, "--git-dir")
}
func (r *GitRunner) RevParseGitCommonDir(ctx context.Context) ([]byte, error) {
	return r.revParseDirectory(ctx, "--git-common-dir")
}

func (r *GitRunner) revParseDirectory(ctx context.Context, flag string) ([]byte, error) {
	out, err := r.run(ctx, proc.GitEnvLocal, gitOperationTimeout, false, "rev-parse", flag)
	if err != nil {
		return nil, err
	}
	path := strings.TrimSpace(string(out))
	if !filepath.IsAbs(path) {
		path = filepath.Join(r.dir, path)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	return []byte(path + "\n"), nil
}

func (r *GitRunner) runAtToplevel(ctx context.Context, literal bool, args ...string) ([]byte, error) {
	root, err := r.RevParseToplevel(ctx)
	if err != nil {
		return nil, err
	}
	rootPath := strings.TrimSpace(string(root))
	if !filepath.IsAbs(rootPath) {
		return nil, fmt.Errorf("git_unavailable")
	}
	return r.run(ctx, proc.GitEnvLocal, gitOperationTimeout, literal, append([]string{"-C", rootPath}, args...)...)
}

func validProjectID(id string) bool {
	if id == "" || strings.HasPrefix(id, "-") || strings.HasSuffix(id, "-") {
		return false
	}
	for _, c := range id {
		if c != '-' && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
			return false
		}
	}
	return !strings.Contains(id, "--")
}
func (r *GitRunner) RevListSince(ctx context.Context, baseSHA, headSHA string) ([]byte, error) {
	if !shaPattern.MatchString(baseSHA) || !shaPattern.MatchString(headSHA) {
		return nil, fmt.Errorf("argv_not_allowed")
	}
	return r.run(ctx, proc.GitEnvLocal, gitOperationTimeout, false, "rev-list", "--end-of-options", baseSHA+".."+headSHA)
}
func (r *GitRunner) RemoteURLs(ctx context.Context, remote string) (fetch, push []byte, err error) {
	if !validRemote(remote) {
		err = fmt.Errorf("argv_not_allowed")
		return
	}
	fetch, err = r.run(ctx, proc.GitEnvLocal, gitOperationTimeout, false, "remote", "get-url", "--all", "--", remote)
	if err != nil {
		return
	}
	push, err = r.run(ctx, proc.GitEnvLocal, gitOperationTimeout, false, "remote", "get-url", "--push", "--all", "--", remote)
	return
}
func (r *GitRunner) ConfigGetReceivepack(ctx context.Context, remote string) ([]byte, error) {
	return r.configRemote(ctx, remote, "receivepack")
}
func (r *GitRunner) ConfigGetUploadpack(ctx context.Context, remote string) ([]byte, error) {
	return r.configRemote(ctx, remote, "uploadpack")
}
func (r *GitRunner) ConfigGetVcs(ctx context.Context, remote string) ([]byte, error) {
	return r.configRemote(ctx, remote, "vcs")
}
func (r *GitRunner) configRemote(ctx context.Context, remote, key string) ([]byte, error) {
	if !validRemote(remote) {
		return nil, fmt.Errorf("argv_not_allowed")
	}
	out, err := r.run(ctx, proc.GitEnvLocal, gitOperationTimeout, false, "config", "--get-all", "remote."+remote+"."+key)
	var exit gitExitError
	if errors.As(err, &exit) && exit.code == 1 {
		return nil, ErrNotFound
	}
	return out, err
}
func (r *GitRunner) SymbolicRef(ctx context.Context) ([]byte, error) {
	out, err := r.run(ctx, proc.GitEnvLocal, gitOperationTimeout, false, "symbolic-ref", "--quiet", "HEAD")
	var exit gitExitError
	if errors.As(err, &exit) && exit.code == 1 {
		return nil, ErrNotFound
	}
	return out, err
}
func (r *GitRunner) LsRemote(ctx context.Context, remote, ref string) ([]byte, error) {
	if !validRemote(remote) || strings.HasPrefix(ref, "-") {
		return nil, fmt.Errorf("argv_not_allowed")
	}
	return r.run(ctx, proc.GitEnvPush, r.effectivePushTimeout(), false, "-c", "remote."+remote+".proxy=", "ls-remote", "--upload-pack=git-upload-pack", "--end-of-options", remote, ref)
}
func (r *GitRunner) CatFileExists(ctx context.Context, sha string) error {
	sha = strings.ToLower(sha)
	if !shaPattern.MatchString(sha) {
		return fmt.Errorf("argv_not_allowed")
	}
	_, e := r.run(ctx, proc.GitEnvLocal, gitOperationTimeout, false, "cat-file", "-e", "--end-of-options", sha+"^{commit}")
	var exit gitExitError
	if errors.As(e, &exit) && exit.code == 1 {
		return ErrNotFound
	}
	return e
}
func (r *GitRunner) RevParseToplevel(ctx context.Context) ([]byte, error) {
	return r.run(ctx, proc.GitEnvLocal, gitOperationTimeout, false, "rev-parse", "--show-toplevel")
}
func (r *GitRunner) Push(ctx context.Context, remote, branch, inspectedSHA string) error {
	if r.git == "" {
		return &PushRunError{Cause: ErrGitUnavailable}
	}
	argv := append([]string{r.git}, BuildPushArgv(remote, branch, inspectedSHA)...)
	if err := ValidatePushArgv(argv, remote, branch, inspectedSHA); err != nil {
		return err
	}
	if err := r.runExact(ctx, proc.GitEnvPush, r.effectivePushTimeout(), argv); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &PushRunError{Cause: err}
	}
	return nil
}

func (r *GitRunner) runExact(ctx context.Context, purpose proc.GitEnvPurpose, timeout time.Duration, argv []string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	env, err := proc.GitOpsEnv(purpose, purpose == proc.GitEnvPush && r.passSSHAuthSock, r.parentEnv, r.stateDir)
	if err != nil {
		return fmt.Errorf("git_unavailable")
	}
	parentCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir, cmd.Env = r.dir, env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 2 * time.Second
	cmd.Cancel = func() error { return terminateProcessGroup(cmd.Process, timeoutKillGrace) }
	cmd.Stdout, cmd.Stderr = nil, nil
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("git_unavailable")
	}
	err = cmd.Wait()
	if ctx.Err() != nil {
		if parentCtx.Err() != nil {
			return parentCtx.Err()
		}
		return ErrGitTimeout
	}
	if err != nil {
		return fmt.Errorf("git_failed")
	}
	return nil
}

var ErrNotFound = errors.New("not_found")

func terminateProcessGroup(p *os.Process, grace time.Duration) error {
	if p == nil {
		return nil
	}
	_ = syscall.Kill(-p.Pid, syscall.SIGTERM)
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(-p.Pid, 0); err != nil {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = syscall.Kill(-p.Pid, syscall.SIGKILL)
	return nil
}
