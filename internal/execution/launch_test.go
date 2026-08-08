package execution

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/proc"
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

func TestProcessRunnerLaunchMapsErrorsAndClosesLock(t *testing.T) {
	original := launchNewSession
	t.Cleanup(func() { launchNewSession = original })
	launchNewSession = func(_ context.Context, _ string, _ []string, lock *os.File, _ io.Writer, _ ...string) (*exec.Cmd, error) {
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

func TestProcessRunnerLaunchRejectsMissingTaskDirectoryBeforeDependencies(t *testing.T) {
	original := launchNewSession
	t.Cleanup(func() { launchNewSession = original })
	called := false
	launchNewSession = func(ctx context.Context, name string, env []string, lock *os.File, stdout io.Writer, args ...string) (*exec.Cmd, error) {
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

func TestProcessRunnerLaunchRejectsFileAsTaskDirectory(t *testing.T) {
	original := launchNewSession
	t.Cleanup(func() { launchNewSession = original })
	called := false
	launchNewSession = func(ctx context.Context, name string, env []string, lock *os.File, stdout io.Writer, args ...string) (*exec.Cmd, error) {
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
	launchNewSession = func(ctx context.Context, name string, env []string, lock *os.File, stdout io.Writer, args ...string) (*exec.Cmd, error) {
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

func TestProcessRunnerCapturesTimestampAndDomainHandle(t *testing.T) {
	originalLaunch, originalNow := launchNewSession, now
	t.Cleanup(func() { launchNewSession, now = originalLaunch, originalNow })
	fixed := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	now = func() time.Time { return fixed }
	var gotName string
	var gotEnv []string
	var gotArgs []string
	launchNewSession = func(ctx context.Context, name string, env []string, lock *os.File, stdout io.Writer, args ...string) (*exec.Cmd, error) {
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
	launchNewSession = func(ctx context.Context, name string, env []string, lock *os.File, stdout io.Writer, args ...string) (*exec.Cmd, error) {
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
