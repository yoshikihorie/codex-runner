package execution

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
)

type launchTestLogs struct {
	logs *contract.ExecutionLogs
	err  error
}

func (f launchTestLogs) OpenExecutionLogs(domain.TaskID) (*contract.ExecutionLogs, error) {
	return f.logs, f.err
}

func launchTestID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260808-120000-a1b2-launch")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func launchTestParams(t *testing.T, lock *os.File) LaunchParams {
	t.Helper()
	return LaunchParams{TaskID: launchTestID(t), SandboxMode: "workspace-write", WorkingDir: t.TempDir(), PromptText: "prompt", TaskDirPath: t.TempDir(), AllowResume: true, LivenessLockFile: lock, CodexBinaryPath: "/bin/echo", Model: "test-model"}
}

func launchTestLogsFor(t *testing.T) *contract.ExecutionLogs {
	t.Helper()
	stdout, err := os.Create(filepath.Join(t.TempDir(), "stdout.log"))
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := os.Create(filepath.Join(t.TempDir(), "stderr.log"))
	if err != nil {
		t.Fatal(err)
	}
	return &contract.ExecutionLogs{Stdout: stdout, Stderr: stderr}
}

func launchTestLock(t *testing.T) *os.File {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "task.lock"))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestBuildLaunchArgs(t *testing.T) {
	reasoning := "high"
	p := launchTestParams(t, nil)
	p.ReasoningEffort = &reasoning
	for _, tt := range []struct {
		name   string
		pty    bool
		head   string
		prefix []string
	}{
		{"pty", true, scriptBinaryPath, []string{"-q", "/dev/null", stdbufBinaryPath, "-oL", "/bin/echo"}},
		{"no pty", false, stdbufBinaryPath, []string{"-oL", "/bin/echo"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p.PTYEnabled = tt.pty
			head, args := buildLaunchArgs(p)
			if head != tt.head || !reflect.DeepEqual(args[:len(tt.prefix)], tt.prefix) {
				t.Fatalf("head,args = %q,%q", head, args)
			}
			if strings.Contains(strings.Join(args, " "), "--ephemeral") {
				t.Fatal("--ephemeral present")
			}
			if !strings.Contains(strings.Join(args, " "), "-c model_reasoning_effort=high") {
				t.Fatal("reasoning effort missing")
			}
		})
	}
	p.ReasoningEffort = nil
	_, args := buildLaunchArgs(p)
	if strings.Contains(strings.Join(args, " "), "model_reasoning_effort") {
		t.Fatal("unexpected reasoning effort")
	}
}

func TestBuildLaunchArgsTerminatesOptionsBeforePrompt(t *testing.T) {
	for _, prompt := range []string{
		"normal prompt",
		"-",
		"--help",
		"--dangerously-bypass-approvals-and-sandbox",
	} {
		for _, pty := range []bool{false, true} {
			name := "non-pty"
			if pty {
				name = "pty"
			}
			t.Run(prompt+"/"+name, func(t *testing.T) {
				p := launchTestParams(t, nil)
				p.PromptText = prompt
				p.PTYEnabled = pty

				_, args := buildLaunchArgs(p)
				want := []string{"--", prompt}
				if got := args[len(args)-len(want):]; !slices.Equal(got, want) {
					t.Fatalf("argument suffix = %q, want %q", got, want)
				}
			})
		}
	}
}

func TestProcessRunnerLaunchMapsErrorsAndClosesLock(t *testing.T) {
	original := launchNewSession
	t.Cleanup(func() { launchNewSession = original })
	launchNewSession = func(_ context.Context, _ string, _ []string, lock *os.File, _ io.Writer, _ io.Writer, _ ...string) (*exec.Cmd, error) {
		_ = lock.Close()
		return nil, errors.New("start failed")
	}
	// The compile-time shape of the seam is checked by assigning the real function below.
	_ = original
	for _, tt := range []struct {
		name string
		pty  bool
		want error
	}{{"pty", true, domain.ErrPTYAllocationFailed}, {"non-pty", false, domain.ErrChildProcessLaunchFailed}} {
		t.Run(tt.name, func(t *testing.T) {
			lock := launchTestLock(t)
			p := launchTestParams(t, lock)
			p.PTYEnabled = tt.pty
			_, err := NewProcessRunner(launchTestLogs{logs: launchTestLogsFor(t)}).Launch(context.Background(), p)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if _, statErr := lock.Stat(); statErr == nil {
				t.Fatal("lock remains open")
			}
		})
	}
}

func TestProcessRunnerLaunchDetachesLaunchContext(t *testing.T) {
	type contextKey struct{}

	original := launchNewSession
	t.Cleanup(func() { launchNewSession = original })
	var captured context.Context
	launchNewSession = func(ctx context.Context, _ string, _ []string, lock *os.File, _ io.Writer, _ io.Writer, _ ...string) (*exec.Cmd, error) {
		captured = ctx
		_ = lock.Close()
		return &exec.Cmd{Process: &os.Process{Pid: 12345}}, nil
	}

	for _, tt := range []struct {
		name       string
		newContext func() (context.Context, context.CancelFunc)
	}{
		{
			name: "cancel",
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.WithValue(context.Background(), contextKey{}, "expected"))
			},
		},
		{
			name: "deadline",
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.WithValue(context.Background(), contextKey{}, "expected"), time.Now().Add(time.Hour))
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			captured = nil
			parent, cancel := tt.newContext()
			defer cancel()

			launched, err := NewProcessRunner(launchTestLogs{logs: launchTestLogsFor(t)}).Launch(parent, launchTestParams(t, launchTestLock(t)))
			if err != nil {
				t.Fatal(err)
			}
			if launched.Handle == nil {
				t.Fatal("launch handle is nil")
			}
			if captured == nil {
				t.Fatal("launch context was not captured")
			}

			cancel()
			if got := captured.Value(contextKey{}); got != "expected" {
				t.Fatalf("value = %v, want expected", got)
			}
			if captured.Err() != nil {
				t.Fatalf("context error = %v, want nil", captured.Err())
			}
			if captured.Done() != nil {
				t.Fatal("context Done is non-nil")
			}
			if deadline, ok := captured.Deadline(); ok {
				t.Fatalf("unexpected deadline: %v", deadline)
			}
		})
	}
}

func TestProcessRunnerLaunchRejectsMissingTaskDirectoryBeforeDependencies(t *testing.T) {
	original := launchNewSession
	t.Cleanup(func() { launchNewSession = original })
	called := false
	launchNewSession = func(ctx context.Context, name string, env []string, lock *os.File, stdout io.Writer, _ io.Writer, args ...string) (*exec.Cmd, error) {
		called = true
		return nil, nil
	}
	lock := launchTestLock(t)
	p := launchTestParams(t, lock)
	p.TaskDirPath = filepath.Join(t.TempDir(), "missing")
	if _, err := NewProcessRunner(launchTestLogs{}).Launch(context.Background(), p); err == nil || called {
		t.Fatalf("err=%v called=%t", err, called)
	}
	if _, statErr := lock.Stat(); statErr == nil {
		t.Fatal("lock remains open")
	}
}

func TestProcessRunnerLaunchRejectsNilLivenessLockBeforeClosing(t *testing.T) {
	original := launchNewSession
	t.Cleanup(func() { launchNewSession = original })
	called := false
	launchNewSession = func(context.Context, string, []string, *os.File, io.Writer, io.Writer, ...string) (*exec.Cmd, error) {
		called = true
		return nil, nil
	}
	p := launchTestParams(t, nil)
	p.TaskDirPath = filepath.Join(t.TempDir(), "missing")
	if _, err := NewProcessRunner(launchTestLogs{}).Launch(context.Background(), p); err == nil {
		t.Fatal("nil liveness lock was accepted")
	}
	if called {
		t.Fatal("launch started with a nil liveness lock")
	}
}

func TestProcessRunnerLaunchRejectsFileAsTaskDirectory(t *testing.T) {
	original := launchNewSession
	t.Cleanup(func() { launchNewSession = original })
	called := false
	launchNewSession = func(ctx context.Context, name string, env []string, lock *os.File, stdout io.Writer, _ io.Writer, args ...string) (*exec.Cmd, error) {
		called = true
		return nil, nil
	}
	lock := launchTestLock(t)
	p := launchTestParams(t, lock)
	regularFile := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(regularFile, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	p.TaskDirPath = regularFile
	if _, err := NewProcessRunner(launchTestLogs{}).Launch(context.Background(), p); err == nil || called {
		t.Fatalf("err=%v called=%t", err, called)
	}
	if _, statErr := lock.Stat(); statErr == nil {
		t.Fatal("lock remains open")
	}
}

func TestProcessRunnerLaunchReturnsContractErrorWithoutStarting(t *testing.T) {
	original := launchNewSession
	t.Cleanup(func() { launchNewSession = original })
	called := false
	launchNewSession = func(ctx context.Context, name string, env []string, lock *os.File, stdout io.Writer, _ io.Writer, args ...string) (*exec.Cmd, error) {
		called = true
		return nil, nil
	}
	lock := launchTestLock(t)
	_, err := NewProcessRunner(launchTestLogs{err: errors.New("open logs")}).Launch(context.Background(), launchTestParams(t, lock))
	if !errors.Is(err, domain.ErrContractWriteFailed) || called {
		t.Fatalf("err=%v called=%t", err, called)
	}
	if _, statErr := lock.Stat(); statErr == nil {
		t.Fatal("lock remains open")
	}
}

func TestProcessWaiterReturnsExitCode(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "exit 3")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	code, err := (&processWaiter{cmd: cmd}).Wait()
	if code != 3 || err != nil {
		t.Fatalf("code,err = %d,%v", code, err)
	}
}

func TestProcessWaiterNormalizesSignaledExitCode(t *testing.T) {
	cmd := exec.Command("/bin/sh", "-c", "kill -KILL $$")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	code, err := (&processWaiter{cmd: cmd}).Wait()
	want := shellSignalExitBase + int(syscall.SIGKILL)
	if code != want || err != nil {
		t.Fatalf("code,err = %d,%v; want %d,nil", code, err, want)
	}
}

func TestProcessRunnerCapturesTimestampAndDomainHandle(t *testing.T) {
	originalLaunch, originalNow := launchNewSession, now
	t.Cleanup(func() { launchNewSession, now = originalLaunch, originalNow })
	fixed := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	now = func() time.Time { return fixed }
	var gotName string
	var gotEnv []string
	var gotArgs []string
	launchNewSession = func(ctx context.Context, name string, env []string, lock *os.File, stdout io.Writer, _ io.Writer, args ...string) (*exec.Cmd, error) {
		gotName, gotEnv, gotArgs = name, env, args
		cmd := exec.Command("/bin/echo")
		cmd.Stdout = stdout
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		_ = lock.Close()
		return cmd, nil
	}
	logs := launchTestLogsFor(t)
	p := launchTestParams(t, launchTestLock(t))
	launched, err := NewProcessRunner(launchTestLogs{logs: logs}).Launch(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if launched.Handle.ProcessStartedAt != fixed || launched.Handle.PID <= 0 {
		t.Fatalf("handle = %#v", launched.Handle)
	}
	if _, ok := interface{}(launched.Handle).(*domain.ProcessHandle); !ok {
		t.Fatal("handle is not domain.ProcessHandle")
	}
	if _, err := launched.Waiter.Wait(); err != nil {
		t.Fatal(err)
	}
	output, err := os.ReadFile(logs.Stdout.Name())
	if err != nil || string(output) != "\n" {
		t.Fatalf("stdout = %q, err = %v", output, err)
	}
	wantName, wantArgs := buildLaunchArgs(p)
	if gotName != wantName {
		t.Fatalf("name = %q, want %q", gotName, wantName)
	}
	if !slices.Equal(gotArgs, wantArgs) {
		t.Fatalf("args = %q, want %q", gotArgs, wantArgs)
	}
	if !slices.Equal(gotEnv, proc.SafeChildEnv()) {
		t.Fatalf("env = %q, want %q", gotEnv, proc.SafeChildEnv())
	}
}

func TestProcessRunnerLaunchPassesPTYArguments(t *testing.T) {
	original := launchNewSession
	t.Cleanup(func() { launchNewSession = original })
	var gotName string
	var gotArgs []string
	launchNewSession = func(ctx context.Context, name string, env []string, lock *os.File, stdout io.Writer, _ io.Writer, args ...string) (*exec.Cmd, error) {
		gotName, gotArgs = name, args
		cmd := exec.Command("/bin/echo")
		cmd.Stdout = stdout
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		_ = lock.Close()
		return cmd, nil
	}
	p := launchTestParams(t, launchTestLock(t))
	p.PTYEnabled = true
	launched, err := NewProcessRunner(launchTestLogs{logs: launchTestLogsFor(t)}).Launch(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := launched.Waiter.Wait(); err != nil {
		t.Fatal(err)
	}
	wantName, wantArgs := buildLaunchArgs(p)
	if gotName != wantName {
		t.Fatalf("name = %q, want %q", gotName, wantName)
	}
	if !slices.Equal(gotArgs, wantArgs) {
		t.Fatalf("args = %q, want %q", gotArgs, wantArgs)
	}
}

var _ ProcessRunner = (*processRunner)(nil)

func TestProcessRunnerSignalDelegatesArgumentsAndError(t *testing.T) {
	originalTerminate, originalKill := sendTerminate, sendKill
	t.Cleanup(func() { sendTerminate, sendKill = originalTerminate, originalKill })

	for _, tt := range []struct {
		name string
		send func(ProcessRunner, int) error
		want error
	}{
		{
			name: "terminate",
			send: func(runner ProcessRunner, pid int) error { return runner.SendTerminate(pid) },
			want: errors.New("terminate failed"),
		},
		{
			name: "kill",
			send: func(runner ProcessRunner, pid int) error { return runner.SendKill(pid) },
			want: errors.New("kill failed"),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const wantPID = 12345
			terminateCalls, killCalls := 0, 0
			var terminatePID, killPID int
			sendTerminate = func(pid int) error {
				terminateCalls++
				terminatePID = pid
				return tt.want
			}
			sendKill = func(pid int) error {
				killCalls++
				killPID = pid
				return tt.want
			}

			err := tt.send(NewProcessRunner(launchTestLogs{}), wantPID)
			if err != tt.want {
				t.Fatalf("error = %v, want same error %v", err, tt.want)
			}
			if tt.name == "terminate" && (terminateCalls != 1 || killCalls != 0 || terminatePID != wantPID) {
				t.Fatalf("terminate calls,pid; kill calls = %d,%d; %d", terminateCalls, terminatePID, killCalls)
			}
			if tt.name == "kill" && (killCalls != 1 || terminateCalls != 0 || killPID != wantPID) {
				t.Fatalf("kill calls,pid; terminate calls = %d,%d; %d", killCalls, killPID, terminateCalls)
			}
		})
	}
}

func TestResumeLaunchArgs(t *testing.T) {
	id := launchTestID(t)
	params := recovery.ResumeLaunchParams{TaskID: id, CodexBinaryPath: "/usr/local/bin/codex", SessionID: "session-id", OutputLastMessagePath: "/tmp/codex-tasks/last-message.md"}
	if got, want := buildResumeArgs(params), []string{"exec", "resume", "session-id", "--output-last-message", "/tmp/codex-tasks/last-message.md"}; !slices.Equal(got, want) {
		t.Fatalf("args = %q, want %q", got, want)
	}
}

func TestResumeLauncherDoesNotLaunchWhenContextCanceledAfterLockAcquisition(t *testing.T) {
	originalFlock := flockFunc
	t.Cleanup(func() { flockFunc = originalFlock })
	originalLaunch := launchNewSession
	t.Cleanup(func() { launchNewSession = originalLaunch })

	id, err := domain.NewTaskID("impl-20260814-120000-a1b2-cancel")
	if err != nil {
		t.Fatal(err)
	}
	taskDir := filepath.Join(taskPlacementRoot, id.String())
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(taskDir, "task.lock")
	lock, err := os.Create(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var acquired *os.File
	flockFunc = func(f *os.File, _ int) error {
		acquired = f
		cancel()
		return nil
	}
	launchCalls := 0
	launchNewSession = func(context.Context, string, []string, *os.File, io.Writer, io.Writer, ...string) (*exec.Cmd, error) {
		launchCalls++
		return nil, nil
	}

	params := recovery.ResumeLaunchParams{TaskID: id, CodexBinaryPath: "/usr/local/bin/codex", SessionID: "session-id", OutputLastMessagePath: filepath.Join(taskDir, "last-message.md")}
	err = NewResumeLauncher(&timeoutProcessFake{}).LaunchAndWait(ctx, params)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if launchCalls != 0 {
		t.Fatalf("launch calls = %d, want 0", launchCalls)
	}
	if acquired == nil {
		t.Fatal("flock did not receive the acquired lock")
	}
	if _, statErr := acquired.Stat(); statErr == nil {
		t.Fatal("acquired lock remained open after cancellation")
	}
}

func TestResumeLauncherRejectsEvictedWorktreeBeforeContextCancellation(t *testing.T) {
	originalLaunch := launchNewSession
	t.Cleanup(func() { launchNewSession = originalLaunch })
	id, err := domain.NewTaskID("impl-20260814-120002-a1b2-evicted")
	if err != nil {
		t.Fatal(err)
	}
	taskDir := filepath.Join(taskPlacementRoot, id.String())
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(taskDir) })
	if err := os.WriteFile(filepath.Join(taskDir, "task.lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(worktreeEvictionMarkerPath(id), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	launchCalls := 0
	launchNewSession = func(context.Context, string, []string, *os.File, io.Writer, io.Writer, ...string) (*exec.Cmd, error) {
		launchCalls++
		return nil, nil
	}
	var logOutput bytes.Buffer
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	params := recovery.ResumeLaunchParams{TaskID: id, CodexBinaryPath: "/usr/local/bin/codex", SessionID: "session-id", OutputLastMessagePath: filepath.Join(taskDir, "last-message.md")}
	err = NewResumeLauncher(&timeoutProcessFake{}, slog.New(slog.NewTextHandler(&logOutput, nil))).LaunchAndWait(ctx, params)
	if !errors.Is(err, ErrWorktreeEvicted) || errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want eviction error only", err)
	}
	if launchCalls != 0 {
		t.Fatalf("launch calls = %d, want 0", launchCalls)
	}
	other, openErr := os.OpenFile(filepath.Join(taskDir, "task.lock"), os.O_RDWR, 0)
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer other.Close()
	if lockErr := syscall.Flock(int(other.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); lockErr != nil {
		t.Fatalf("resume lock remained held after eviction rejection: %v", lockErr)
	}
	if !strings.Contains(logOutput.String(), "task_id="+id.String()) || !strings.Contains(logOutput.String(), worktreeEvictionMarkerPath(id)) || !strings.Contains(logOutput.String(), ErrWorktreeEvicted.Error()) {
		t.Fatalf("eviction rejection log = %q", logOutput.String())
	}
}

func TestResumeLauncherRejectsMarkerInspectionPermissionFailure(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("permission checks are not reliable for root")
	}
	originalLaunch := launchNewSession
	originalFlock := flockFunc
	t.Cleanup(func() {
		launchNewSession = originalLaunch
		flockFunc = originalFlock
	})
	id, err := domain.NewTaskID("impl-20260814-120006-a1b2-markereacces")
	if err != nil {
		t.Fatal(err)
	}
	taskDir := filepath.Join(taskPlacementRoot, id.String())
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.Chmod(taskDir, 0o700)
		_ = os.RemoveAll(taskDir)
	})
	lockPath := filepath.Join(taskDir, "task.lock")
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	flockFunc = func(f *os.File, how int) error {
		if err := originalFlock(f, how); err != nil {
			return err
		}
		return os.Chmod(taskDir, 0o000)
	}
	launchCalls := 0
	launchNewSession = func(context.Context, string, []string, *os.File, io.Writer, io.Writer, ...string) (*exec.Cmd, error) {
		launchCalls++
		return nil, nil
	}
	var logOutput bytes.Buffer
	params := recovery.ResumeLaunchParams{TaskID: id, CodexBinaryPath: "/usr/local/bin/codex", SessionID: "session-id", OutputLastMessagePath: filepath.Join(taskDir, "last-message.md")}
	err = NewResumeLauncher(&timeoutProcessFake{}, slog.New(slog.NewTextHandler(&logOutput, nil))).LaunchAndWait(context.Background(), params)
	if !errors.Is(err, syscall.EACCES) {
		t.Fatalf("error = %v, want EACCES", err)
	}
	if launchCalls != 0 {
		t.Fatalf("launch calls = %d, want 0", launchCalls)
	}
	if err := os.Chmod(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	other, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := syscall.Flock(int(other.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("resume lock remained held after marker inspection failure: %v", err)
	}
	if !strings.Contains(logOutput.String(), "task_id="+id.String()) || !strings.Contains(logOutput.String(), worktreeEvictionMarkerPath(id)) || !strings.Contains(logOutput.String(), "permission denied") {
		t.Fatalf("marker inspection log = %q", logOutput.String())
	}
}

func TestLimitedWriterLenReportsCapturedBytesWithinLimit(t *testing.T) {
	writer := &limitedWriter{limit: resumeStderrBufferMaxBytes}
	if _, err := writer.Write(bytes.Repeat([]byte("x"), resumeStderrBufferMaxBytes+1)); err != nil {
		t.Fatal(err)
	}
	if got := writer.Len(); got != resumeStderrBufferMaxBytes {
		t.Fatalf("captured bytes = %d, want %d", got, resumeStderrBufferMaxBytes)
	}
}

func TestResumeLauncherLogsCapturedStderrMetadataOnLaunchFailure(t *testing.T) {
	original := launchNewSession
	t.Cleanup(func() { launchNewSession = original })
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logOutput, nil))
	const sensitiveStderr = "resume-secret-value-7a930b"
	launchNewSession = func(_ context.Context, _ string, _ []string, _ *os.File, _ io.Writer, stderr io.Writer, _ ...string) (*exec.Cmd, error) {
		_, _ = stderr.Write([]byte("\x1b[31m" + sensitiveStderr + "\x1b[0m"))
		return nil, errors.New("launch failed")
	}
	id := launchTestID(t)
	taskDir := filepath.Join(taskPlacementRoot, id.String())
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := os.Create(filepath.Join(taskDir, "task.lock"))
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	params := recovery.ResumeLaunchParams{TaskID: id, CodexBinaryPath: "/usr/local/bin/codex", SessionID: "session-id", OutputLastMessagePath: filepath.Join(taskDir, "last-message.md")}
	if err := NewResumeLauncher(&timeoutProcessFake{}, logger).LaunchAndWait(context.Background(), params); err == nil {
		t.Fatal("launch failure was accepted")
	}
	log := logOutput.String()
	if !strings.Contains(log, "task_id="+id.String()) || !strings.Contains(log, "error=\"launch failed\"") || !strings.Contains(log, "stderr_bytes=") {
		t.Fatal("operational log is missing resume stderr metadata")
	}
	if strings.Contains(log, "stderr_summary=") || strings.Contains(log, sensitiveStderr) || strings.Contains(log, "\\x1b") {
		t.Fatal("operational log contains captured stderr content")
	}
}

func TestResumeLauncherLogsCapturedStderrMetadataForNonZeroExit(t *testing.T) {
	var logOutput bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logOutput, nil))
	launcher := &resumeLauncher{logger: logger}
	params := recovery.ResumeLaunchParams{TaskID: launchTestID(t)}
	stderr := &limitedWriter{limit: resumeStderrBufferMaxBytes}
	const sensitiveStderr = "non-zero-secret-value-3d5f4e"
	if _, err := stderr.Write([]byte(sensitiveStderr)); err != nil {
		t.Fatal(err)
	}
	launcher.logStderr(params, errors.New("resume process exited with code 1"), stderr)
	log := logOutput.String()
	if !strings.Contains(log, "task_id="+params.TaskID.String()) || !strings.Contains(log, "stderr_bytes=") {
		t.Fatal("operational log is missing captured-byte metadata")
	}
	if strings.Contains(log, "stderr_summary=") || strings.Contains(log, sensitiveStderr) {
		t.Fatal("operational log contains captured stderr content")
	}
}

func TestResumeLauncherDoesNotLogEmptyCapturedStderr(t *testing.T) {
	var logOutput bytes.Buffer
	launcher := &resumeLauncher{logger: slog.New(slog.NewTextHandler(&logOutput, nil))}
	launcher.logStderr(recovery.ResumeLaunchParams{TaskID: launchTestID(t)}, errors.New("resume failed"), &limitedWriter{limit: resumeStderrBufferMaxBytes})
	if logOutput.Len() != 0 {
		t.Fatal("empty captured stderr produced an operational warning")
	}
}

type resumeSignalRunnerFake struct {
	terminatePIDs []int
	killPIDs      []int
	terminateErr  error
	killErr       error
	onTerminate   func()
	onKill        func()
}

func (*resumeSignalRunnerFake) Launch(context.Context, LaunchParams) (*LaunchedProcess, error) {
	return nil, errors.New("unused")
}

func (f *resumeSignalRunnerFake) SendTerminate(pid int) error {
	f.terminatePIDs = append(f.terminatePIDs, pid)
	if f.onTerminate != nil {
		f.onTerminate()
	}
	return f.terminateErr
}

func (f *resumeSignalRunnerFake) SendKill(pid int) error {
	f.killPIDs = append(f.killPIDs, pid)
	if f.onKill != nil {
		f.onKill()
	}
	return f.killErr
}

func TestResumeLauncherCancellationPrefersAlreadyCompletedWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waitResult := make(chan resumeWaitOutcome, 1)
	waitResult <- resumeWaitOutcome{err: errors.New("wait failed")}
	runner := &resumeSignalRunnerFake{}
	launcher := &resumeLauncher{runner: runner, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), killGrace: time.Millisecond}
	err := launcher.finishContextCancellation(ctx, &exec.Cmd{Process: &os.Process{Pid: 4321}}, waitResult, &limitedWriter{limit: resumeStderrBufferMaxBytes}, recovery.ResumeLaunchParams{TaskID: launchTestID(t)})
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "wait failed") {
		t.Fatalf("error = %v, want joined cancellation and wait error", err)
	}
	if len(runner.terminatePIDs) != 0 || len(runner.killPIDs) != 0 {
		t.Fatal("signals were sent after a completed wait result")
	}
}

func TestResumeLauncherCancellationSkipsKillWhenTerminateCompletesWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waitResult := make(chan resumeWaitOutcome, 1)
	runner := &resumeSignalRunnerFake{}
	runner.onTerminate = func() { waitResult <- resumeWaitOutcome{} }
	launcher := &resumeLauncher{runner: runner, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), killGrace: time.Millisecond}
	err := launcher.finishContextCancellation(ctx, &exec.Cmd{Process: &os.Process{Pid: 4321}}, waitResult, &limitedWriter{limit: resumeStderrBufferMaxBytes}, recovery.ResumeLaunchParams{TaskID: launchTestID(t)})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if !slices.Equal(runner.terminatePIDs, []int{4321}) || len(runner.killPIDs) != 0 {
		t.Fatalf("terminate pids = %v, kill pids = %v", runner.terminatePIDs, runner.killPIDs)
	}
}

func TestResumeLauncherCancellationKillsOnlyAfterGraceAndWaits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waitResult := make(chan resumeWaitOutcome, 1)
	terminateErr := errors.New("terminate failed")
	waitErr := errors.New("wait failed")
	runner := &resumeSignalRunnerFake{terminateErr: terminateErr}
	runner.onKill = func() { waitResult <- resumeWaitOutcome{err: waitErr} }
	launcher := &resumeLauncher{runner: runner, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), killGrace: time.Millisecond}
	err := launcher.finishContextCancellation(ctx, &exec.Cmd{Process: &os.Process{Pid: 4321}}, waitResult, &limitedWriter{limit: resumeStderrBufferMaxBytes}, recovery.ResumeLaunchParams{TaskID: launchTestID(t)})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, terminateErr) || !errors.Is(err, waitErr) {
		t.Fatalf("error = %v, want cancellation, terminate, and wait errors", err)
	}
	if !slices.Equal(runner.terminatePIDs, []int{4321}) || !slices.Equal(runner.killPIDs, []int{4321}) {
		t.Fatalf("terminate pids = %v, kill pids = %v", runner.terminatePIDs, runner.killPIDs)
	}
}

func TestResumeLauncherCancellationReturnsImmediatelyWhenKillFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waitResult := make(chan resumeWaitOutcome)
	killErr := errors.New("kill failed")
	runner := &resumeSignalRunnerFake{killErr: killErr}
	launcher := &resumeLauncher{runner: runner, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), killGrace: time.Millisecond}
	err := launcher.finishContextCancellation(ctx, &exec.Cmd{Process: &os.Process{Pid: 4321}}, waitResult, &limitedWriter{limit: resumeStderrBufferMaxBytes}, recovery.ResumeLaunchParams{TaskID: launchTestID(t)})
	if !errors.Is(err, context.Canceled) || !errors.Is(err, killErr) {
		t.Fatalf("error = %v, want cancellation and kill error", err)
	}
	if !slices.Equal(runner.terminatePIDs, []int{4321}) || !slices.Equal(runner.killPIDs, []int{4321}) {
		t.Fatalf("terminate pids = %v, kill pids = %v", runner.terminatePIDs, runner.killPIDs)
	}
}

func TestResumeLauncherCancellationReturnsReapTimeoutAfterKill(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	waitResult := make(chan resumeWaitOutcome)
	runner := &resumeSignalRunnerFake{}
	launcher := &resumeLauncher{runner: runner, logger: slog.New(slog.NewTextHandler(io.Discard, nil)), killGrace: time.Millisecond}
	err := launcher.finishContextCancellation(ctx, &exec.Cmd{Process: &os.Process{Pid: 4321}}, waitResult, &limitedWriter{limit: resumeStderrBufferMaxBytes}, recovery.ResumeLaunchParams{TaskID: launchTestID(t)})
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "resume process was not reaped after SIGKILL") {
		t.Fatalf("error = %v, want cancellation and reap timeout", err)
	}
	if !slices.Equal(runner.terminatePIDs, []int{4321}) || !slices.Equal(runner.killPIDs, []int{4321}) {
		t.Fatalf("terminate pids = %v, kill pids = %v", runner.terminatePIDs, runner.killPIDs)
	}
}
