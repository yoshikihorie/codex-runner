package contract_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/execution/usecase"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type goldenWaiter struct {
	code  int
	calls int
}

func (w *goldenWaiter) Wait() (int, error) { w.calls++; return w.code, nil }

type goldenRunner struct {
	writer   contract.ContractWriter
	taskDir  string
	taskID   domain.TaskID
	family   domain.Subcommand
	rawExit  int
	model    string
	prompt   string
	launches int
	waiter   *goldenWaiter
}

func (r *goldenRunner) Launch(_ context.Context, p execution.LaunchParams) (*execution.LaunchedProcess, error) {
	r.launches++
	if p.TaskID != r.taskID || p.Subcommand != r.family || p.TaskDirPath != r.taskDir || !p.AllowResume || p.Model != r.model || p.PromptText != r.prompt || p.SandboxMode != "" || p.WorkingDir != "" || p.PTYEnabled || p.CodexBinaryPath != "" || p.ReasoningEffort != nil || p.LivenessLockFile != nil {
		return nil, errors.New("invalid golden launch parameters")
	}
	logs, err := r.writer.OpenExecutionLogs(p.TaskID)
	if err != nil {
		return nil, err
	}
	if _, err = logs.Stdout.WriteString("stdout\n"); err == nil {
		_, err = logs.Stderr.WriteString("stderr\n")
	}
	if closeErr := logs.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	if !recoveryRequired(r.rawExit) {
		if err := os.WriteFile(filepath.Join(r.taskDir, "last-message.md"), []byte("final answer\n"), 0o600); err != nil {
			return nil, err
		}
	}
	r.waiter = &goldenWaiter{code: r.rawExit}
	return &execution.LaunchedProcess{Handle: &domain.ProcessHandle{PID: 4242, ProcessStartedAt: time.Date(2026, 8, 22, 1, 2, 4, 0, time.FixedZone("JST", 9*3600))}, Waiter: r.waiter}, nil
}
func (*goldenRunner) SendTerminate(int) error { return nil }
func (*goldenRunner) SendKill(int) error      { return nil }

type goldenResumeLauncher struct {
	taskDir         string
	success         bool
	taskID          domain.TaskID
	sessionID       string
	codexBinaryPath string
	calls           int
	params          []recovery.ResumeLaunchParams
}

func (l *goldenResumeLauncher) LaunchAndWait(_ context.Context, p recovery.ResumeLaunchParams) error {
	l.calls++
	l.params = append(l.params, p)
	if p.TaskID != l.taskID || p.SessionID != l.sessionID || p.CodexBinaryPath != l.codexBinaryPath || p.OutputLastMessagePath != filepath.Join(l.taskDir, "last-message.md") {
		return errors.New("invalid golden resume launch parameters")
	}
	if !l.success {
		return errors.New("deterministic resume failure")
	}
	return os.WriteFile(p.OutputLastMessagePath, []byte("recovered answer\n"), 0o600)
}

func recoveryRequired(rawExit int) bool {
	return domain.NewExitCode(rawExit).Class() == domain.ExitCodeClassTimeout
}

type goldenSlot struct{}

func (goldenSlot) ReleaseAndAdvance(context.Context, domain.TaskID, time.Time) {}

type goldenMetrics struct{}

func (goldenMetrics) Execute(context.Context, metrics.RecordTaskMetricsInput) metrics.RecordTaskMetricsOutput {
	return metrics.RecordTaskMetricsOutput{}
}

type goldenDisarmer struct{}

func (goldenDisarmer) Disarm(domain.TaskID) {}

type goldenTracker struct{}

func (goldenTracker) LeaveStalled(domain.TaskID, time.Time) int { return 0 }
func (goldenTracker) TakeTotal(domain.TaskID) int               { return 0 }

func TestOutputContractGolden(t *testing.T) {
	cases := []struct {
		name          string
		family        domain.Subcommand
		rawExit       int
		resumeSuccess bool
	}{{"research-normal", domain.SubcommandResearch, 0, false}, {"research-recovered", domain.SubcommandResearch, 124, true}, {"research-recovery-failed", domain.SubcommandResearch, 137, false}, {"review-normal", domain.SubcommandReview, 0, false}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			runGoldenScenario(t, tc.name, tc.family, tc.rawExit, tc.resumeSuccess)
		})
	}
}

func runGoldenScenario(t *testing.T, scenario string, family domain.Subcommand, rawExit int, resumeSuccess bool) {
	t.Helper()
	root := t.TempDir()
	now := time.Date(2026, 8, 22, 1, 2, 3, 0, time.FixedZone("JST", 9*3600))
	clock := domain.ClockFunc(func() time.Time { return now })
	id, err := domain.NewTaskID(string(family) + "-20260822-010203-a1b2-golden-test")
	if err != nil {
		t.Fatal(err)
	}
	slug, err := domain.NewSlug("golden-test")
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := domain.NewTask(id, family, slug, nil, now, 1)
	if err != nil {
		t.Fatal(err)
	}
	timeout, err := domain.ResolveTimeout(nil)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := store.NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := tasks.Reserve(id); err != nil {
		t.Fatal(err)
	}
	writer := contract.NewFileContractWriter(root, clock)
	reader := store.NewFileContractReader(root)
	dir := filepath.Join(root, id.String())
	if err := usecase.NewRecordTaskStartingUseCase(tasks, writer).Execute(context.Background(), task, timeout, "test-model", nil, domain.ExecutionRouteDaemon, "golden prompt\n", now); err != nil {
		t.Fatal(err)
	}
	if family == domain.SubcommandReview {
		if err := writer.WriteReviewInput(id, []byte("diff input\n")); err != nil {
			t.Fatal(err)
		}
		if err := writer.WriteCombinedPrompt(id, []byte("golden prompt\n---\n## Input\n\ndiff input\n")); err != nil {
			t.Fatal(err)
		}
	}
	m, err := loadManifest(scenario)
	if err != nil {
		t.Fatal(err)
	}
	if m.Scenario != scenario {
		t.Fatalf("manifest scenario=%q does not match test scenario=%q", m.Scenario, scenario)
	}
	if m.SubcommandFamily != string(family) {
		t.Fatalf("manifest subcommand_family=%q does not match test family=%q", m.SubcommandFamily, family)
	}
	baseline := map[string][]byte{}
	for _, f := range m.Files {
		if f.Immutable {
			b, e := os.ReadFile(filepath.Join(dir, f.Name))
			if e != nil {
				t.Fatal(e)
			}
			baseline[f.Name] = b
		}
	}
	runner := &goldenRunner{writer: writer, taskDir: dir, taskID: id, family: family, rawExit: rawExit, model: "test-model", prompt: "golden prompt\n"}
	launched, err := usecase.NewLaunchWithPTYUseCase(runner).Execute(context.Background(), execution.LaunchParams{TaskID: id, Subcommand: family, TaskDirPath: dir, AllowResume: true, Model: "test-model", PromptText: "golden prompt\n"})
	if err != nil {
		t.Fatal(err)
	}
	if runner.launches != 1 {
		t.Fatalf("launch calls=%d, want 1", runner.launches)
	}
	if err := usecase.NewRecordTaskProcessUseCase(tasks, writer).Execute(context.Background(), task, launched.Handle, now); err != nil {
		t.Fatal(err)
	}
	mu := store.NewTaskMutex()
	liveness := execution.NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(domain.TaskID) string { return filepath.Join(dir, "task.lock") })
	confirmed, err := usecase.NewConfirmTaskRunningUseCase(tasks, mu, liveness, writer).Execute(context.Background(), id, now)
	if err != nil || confirmed.State != domain.StateRunning {
		t.Fatalf("running confirmation=%+v err=%v", confirmed, err)
	}
	observed, err := launched.Waiter.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if runner.waiter.calls != 1 || observed != rawExit {
		t.Fatalf("wait calls=%d code=%d", runner.waiter.calls, observed)
	}
	needsRecovery := recoveryRequired(observed)
	if needsRecovery != m.RecoveryExpected {
		t.Fatalf("recovery branch=%v does not match manifest recovery_expected=%v", needsRecovery, m.RecoveryExpected)
	}
	if !needsRecovery {
		pathLocks := store.NewPathLockFileStore(t.TempDir())
		out, err := execution.NewFinalizeTaskUseCase(tasks, writer, reader, clock, mu, goldenSlot{}, goldenDisarmer{}, execution.NewReleasePathLockUseCase(pathLocks), goldenMetrics{}, goldenTracker{}).Execute(context.Background(), execution.FinalizeTaskInput{TaskID: id, RawExitCode: observed, OccurredAt: now})
		if err != nil || out.ResultState != domain.StateCompleted {
			t.Fatalf("finalize=%+v err=%v", out, err)
		}
	} else {
		session, err := domain.NewSessionRef("123e4567-e89b-12d3-a456-426614174000", now, false)
		if err != nil {
			t.Fatal(err)
		}
		snapshot, err := tasks.Load(id)
		if err != nil {
			t.Fatal(err)
		}
		timedOut, err := snapshot.Restore()
		if err != nil {
			t.Fatal(err)
		}
		events, err := timedOut.MarkTimedOut(&session, now)
		if err != nil {
			t.Fatal(err)
		}
		updated, err := snapshot.WithTask(timedOut, now)
		if err != nil {
			t.Fatal(err)
		}
		if err := tasks.Save(id, updated); err != nil {
			t.Fatal(err)
		}
		for _, event := range events {
			if err := writer.AppendEvent(id, event); err != nil {
				t.Fatal(err)
			}
		}
		launcher := &goldenResumeLauncher{taskDir: dir, success: resumeSuccess, taskID: id, sessionID: session.SessionID(), codexBinaryPath: "/usr/bin/false"}
		recoverer := recovery.NewResumeRecoverer(launcher, reader, "/usr/bin/false", root, clock)
		partial := recovery.NewSavePartialOutputUseCase(reader, writer)
		out, err := recovery.NewRecoverViaResumeUseCase(tasks, writer, recoverer, partial, goldenSlot{}, goldenMetrics{}, goldenTracker{}, mu, clock).Execute(context.Background(), recovery.RecoverViaResumeInput{TaskID: id, SessionRef: &session, Origin: domain.RecoveryOriginTimeout, OccurredAt: now})
		if err != nil {
			t.Fatal(err)
		}
		if launcher.calls != 1 {
			t.Fatalf("resume launcher calls=%d, want 1", launcher.calls)
		}
		if len(launcher.params) != 1 || launcher.params[0].TaskID != id {
			t.Fatal("resume launch params were not recorded")
		}
		if resumeSuccess && (!out.Succeeded || out.FinalState != domain.StateRecovered) {
			t.Fatalf("recovery success output=%+v", out)
		}
		if !resumeSuccess && (out.Succeeded || out.FinalState != domain.StateTimeoutLost) {
			t.Fatalf("recovery failure output=%+v", out)
		}
	}
	if err := verifyOutputDirectory(dir, m, baseline); err != nil {
		t.Fatal(err)
	}
}

func TestOutputContractRecoveryBranchClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  int
		want bool
	}{
		{"success", 0, false}, {"failure-one", 1, false}, {"failure-two", 2, false}, {"timeout-canonical", 6, true}, {"timeout-deadline", 124, true}, {"cancelled", 130, false}, {"timeout-kill", 137, true}, {"failure-max", 255, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := recoveryRequired(tc.raw); got != tc.want {
				t.Fatalf("recoveryRequired(%d)=%v, want %v", tc.raw, got, tc.want)
			}
		})
	}
}
