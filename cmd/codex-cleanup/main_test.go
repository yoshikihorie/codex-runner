package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/execution"
)

var fixedNow = func() time.Time { return time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC) }

type fakeCleanupUseCase struct {
	planCandidates []execution.WorktreeCandidate
	planErr        error
	executeOutput  execution.EvictWorkDirOutput
	executeErr     error
	planInputs     []execution.EvictWorkDirInput
	executeInputs  []execution.EvictWorkDirInput
	confirmedPaths [][]string
}

type fakeCleanupLogsUseCase struct {
	planOutput    execution.EvictLogsOutput
	planErr       error
	executeOutput execution.EvictLogsOutput
	executeErr    error
	confirmed     [][]execution.LogDeletionCandidate
}

func (f *fakeCleanupLogsUseCase) Plan(_ context.Context, _ execution.EvictLogsInput) (execution.EvictLogsOutput, error) {
	return f.planOutput, f.planErr
}

func (f *fakeCleanupLogsUseCase) Execute(_ context.Context, _ execution.EvictLogsInput, candidates []execution.LogDeletionCandidate) (execution.EvictLogsOutput, error) {
	f.confirmed = append(f.confirmed, append([]execution.LogDeletionCandidate(nil), candidates...))
	return f.executeOutput, f.executeErr
}

func (f *fakeCleanupUseCase) Plan(_ context.Context, in execution.EvictWorkDirInput) ([]execution.WorktreeCandidate, []execution.WorktreeSkipped, error) {
	f.planInputs = append(f.planInputs, in)
	return f.planCandidates, nil, f.planErr
}

func (f *fakeCleanupUseCase) Execute(_ context.Context, in execution.EvictWorkDirInput, paths []string) (execution.EvictWorkDirOutput, error) {
	f.executeInputs = append(f.executeInputs, in)
	pathsCopy := make([]string, len(paths))
	copy(pathsCopy, paths)
	f.confirmedPaths = append(f.confirmedPaths, pathsCopy)
	return f.executeOutput, f.executeErr
}

type errorReader struct{ err error }

func (r errorReader) Read(_ []byte) (int, error) { return 0, r.err }

func candidates(paths ...string) []execution.WorktreeCandidate {
	result := make([]execution.WorktreeCandidate, 0, len(paths))
	for _, path := range paths {
		result = append(result, execution.WorktreeCandidate{Path: path})
	}
	return result
}

func TestRunCleanupConfirmedPathsAndMessages(t *testing.T) {
	t.Setenv("CODEX_RUNNER_LANG", "en")
	uc := &fakeCleanupUseCase{
		planCandidates: candidates("/tmp/a", "/tmp/b", "/tmp/c"),
		executeOutput:  execution.EvictWorkDirOutput{Deleted: []string{"/tmp/a", "/tmp/b"}},
	}
	var stdout bytes.Buffer
	out, err := runCleanup(context.Background(), uc, nil, strings.NewReader("y\n"), &stdout, fixedNow)
	if err != nil {
		t.Fatalf("runCleanup() error = %v", err)
	}
	if got, want := len(uc.executeInputs), 1; got != want {
		t.Fatalf("Execute calls = %d, want %d", got, want)
	}
	if got, want := uc.confirmedPaths[0], []string{"/tmp/a", "/tmp/b", "/tmp/c"}; !sameStrings(got, want) {
		t.Fatalf("confirmed paths = %q, want %q", got, want)
	}
	if got, want := out.Deleted, []string{"/tmp/a", "/tmp/b"}; !sameStrings(got, want) {
		t.Fatalf("Deleted = %q, want %q", got, want)
	}
	got := stdout.String()
	if !strings.Contains(got, "This will delete 3 working directories. Continue? [y/N]") {
		t.Fatalf("confirmation = %q", got)
	}
	if !strings.Contains(got, "Deleted 2 working directories.") {
		t.Fatalf("completion = %q", got)
	}
}

func TestRunCleanupCancelsUnlessResponseIsExactY(t *testing.T) {
	for _, response := range []string{"Y\n", "yes\n", "n\n", "\n", ""} {
		t.Run(strings.TrimSpace(response), func(t *testing.T) {
			uc := &fakeCleanupUseCase{planCandidates: candidates("/tmp/a")}
			out, err := runCleanup(context.Background(), uc, nil, strings.NewReader(response), io.Discard, fixedNow)
			if err != nil {
				t.Fatalf("runCleanup() error = %v", err)
			}
			if len(uc.executeInputs) != 0 || len(out.Deleted) != 0 {
				t.Fatalf("cleanup was executed for response %q", response)
			}
		})
	}
}

func TestReadConfirmation(t *testing.T) {
	for _, test := range []struct {
		name  string
		input string
		want  bool
	}{
		{"empty EOF", "", false},
		{"empty line", "\n", false},
		{"exact y", "y\n", true},
		{"y without newline", "y", true},
		{"whitespace is not y", " y \n", false},
		{"input over limit without newline", strings.Repeat("y", maxConfirmationInputBytes+1), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := readConfirmation(strings.NewReader(test.input))
			if err != nil || got != test.want {
				t.Fatalf("readConfirmation(%q) = %v, %v; want %v, nil", test.input, got, err, test.want)
			}
		})
	}
	if _, err := readConfirmation(errorReader{err: errors.New("read failed")}); err == nil {
		t.Fatal("readConfirmation() error = nil, want error")
	}
}

func TestReadConfirmationReaderDiscardsOverlongLineBeforeNextConfirmation(t *testing.T) {
	input := strings.Repeat("x", maxConfirmationInputBytes+bufio.MaxScanTokenSize) + "\n" + "y\n"
	reader := bufio.NewReader(strings.NewReader(input))

	first, err := readConfirmationReader(reader)
	if err != nil || first {
		t.Fatalf("first confirmation = %v, %v; want false, nil", first, err)
	}
	second, err := readConfirmationReader(reader)
	if err != nil || !second {
		t.Fatalf("second confirmation = %v, %v; want true, nil", second, err)
	}
}

func TestRunCleanupExecutesOnUnterminatedY(t *testing.T) {
	uc := &fakeCleanupUseCase{planCandidates: candidates("/tmp/a")}
	_, err := runCleanup(context.Background(), uc, nil, strings.NewReader("y"), io.Discard, fixedNow)
	if err != nil || len(uc.executeInputs) != 1 {
		t.Fatalf("error = %v, Execute calls = %d", err, len(uc.executeInputs))
	}
}

func TestRunCleanupReadErrorDoesNotExecute(t *testing.T) {
	uc := &fakeCleanupUseCase{planCandidates: candidates("/tmp/a")}
	_, err := runCleanup(context.Background(), uc, nil, errorReader{err: errors.New("read failed")}, io.Discard, fixedNow)
	if err == nil || len(uc.executeInputs) != 0 {
		t.Fatalf("error = %v, Execute calls = %d", err, len(uc.executeInputs))
	}
}

func TestRunCleanupZeroCandidatesStillConfirms(t *testing.T) {
	t.Setenv("CODEX_RUNNER_LANG", "ja")
	uc := &fakeCleanupUseCase{planCandidates: []execution.WorktreeCandidate{}}
	var stdout bytes.Buffer
	_, err := runCleanup(context.Background(), uc, nil, strings.NewReader("y\n"), &stdout, fixedNow)
	if err != nil || len(uc.executeInputs) != 1 || uc.confirmedPaths[0] == nil || len(uc.confirmedPaths[0]) != 0 {
		t.Fatalf("error = %v, execute = %d, paths = %#v", err, len(uc.executeInputs), uc.confirmedPaths)
	}
	if !strings.Contains(stdout.String(), "0件の作業用ディレクトリを削除します。よろしいですか。 [y/N]") {
		t.Fatalf("confirmation = %q", stdout.String())
	}
}

func TestRunCleanupFlagsPropagate(t *testing.T) {
	uc := &fakeCleanupUseCase{planCandidates: candidates("/tmp/a")}
	_, err := runCleanup(context.Background(), uc, []string{"--force", "--max-age=12"}, strings.NewReader("y\n"), io.Discard, fixedNow)
	if err != nil {
		t.Fatal(err)
	}
	if !uc.planInputs[0].Force || !uc.executeInputs[0].Force || uc.planInputs[0].MaxAgeDays != 12 || uc.executeInputs[0].MaxAgeDays != 12 {
		t.Fatalf("inputs = plan %#v, execute %#v", uc.planInputs[0], uc.executeInputs[0])
	}
	if uc.planInputs[0].Trigger != execution.TriggerExplicit || !uc.planInputs[0].OccurredAt.Equal(fixedNow()) {
		t.Fatalf("plan input = %#v", uc.planInputs[0])
	}
}

func TestRunCleanupDefaultForceIsFalse(t *testing.T) {
	uc := &fakeCleanupUseCase{planCandidates: candidates("/tmp/a")}
	_, err := runCleanup(context.Background(), uc, nil, strings.NewReader("n\n"), io.Discard, fixedNow)
	if err != nil || uc.planInputs[0].Force {
		t.Fatalf("error = %v, input = %#v", err, uc.planInputs[0])
	}
}

func TestRunCleanupPlanErrorsAreFailClosed(t *testing.T) {
	want := errors.New("maxAgeDays out of range")
	uc := &fakeCleanupUseCase{planErr: want}
	var stdout bytes.Buffer
	_, err := runCleanup(context.Background(), uc, []string{"--max-age=-1"}, strings.NewReader("y\n"), &stdout, fixedNow)
	if !errors.Is(err, want) || stdout.Len() != 0 || len(uc.executeInputs) != 0 {
		t.Fatalf("error = %v, stdout = %q, Execute calls = %d", err, stdout.String(), len(uc.executeInputs))
	}
}

func TestRunCleanupExecuteError(t *testing.T) {
	uc := &fakeCleanupUseCase{planCandidates: candidates("/tmp/a"), executeErr: context.Canceled}
	_, err := runCleanup(context.Background(), uc, nil, strings.NewReader("y\n"), io.Discard, fixedNow)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestCleanupMessagesAndLocaleResolution(t *testing.T) {
	t.Setenv("CODEX_RUNNER_LANG", "ja")
	if got := formatCleanupMessage(resolveCleanupLocale(), "info.cleanup.confirm", 4); got != "4件の作業用ディレクトリを削除します。よろしいですか。" {
		t.Fatalf("ja confirmation = %q", got)
	}
	if got := formatCleanupMessage("ja", "info.cleanup.completed", 4); got != "4件の作業用ディレクトリを削除しました。" {
		t.Fatalf("ja completion = %q", got)
	}
	t.Setenv("CODEX_RUNNER_LANG", "en")
	if got := formatCleanupMessage(resolveCleanupLocale(), "info.cleanup.completed", 4); got != "Deleted 4 working directories." {
		t.Fatalf("en completion = %q", got)
	}
	t.Setenv("CODEX_RUNNER_LANG", "fr_FR")
	t.Setenv("LC_ALL", "en_US.UTF-8")
	t.Setenv("LANG", "ja_JP.UTF-8")
	if got := resolveCleanupLocale(); got != "en" {
		t.Fatalf("locale = %q, want en", got)
	}
}

func TestRunMainErrorsReturnOne(t *testing.T) {
	for _, args := range [][]string{{"--max-age=-1"}, {"--max-age=366"}, {"--max-age=abc"}} {
		uc := &fakeCleanupUseCase{planErr: errors.New("maxAgeDays out of range")}
		if got := runMain(context.Background(), uc, args, strings.NewReader("y\n"), io.Discard, io.Discard, fixedNow); got != 1 {
			t.Fatalf("runMain(%q) = %d, want 1", args, got)
		}
	}
}

func TestRunMainFlagErrorWrittenOnce(t *testing.T) {
	var stderr bytes.Buffer
	got := runMain(context.Background(), &fakeCleanupUseCase{}, []string{"--max-age=abc"}, strings.NewReader("y\n"), io.Discard, &stderr, fixedNow)
	if got != 1 || strings.Count(stderr.String(), "invalid value") != 1 {
		t.Fatalf("exit = %d, stderr = %q", got, stderr.String())
	}
}

func TestRunCleanupRejectsPositionalArguments(t *testing.T) {
	uc := &fakeCleanupUseCase{}
	_, err := runCleanup(context.Background(), uc, []string{"extra-arg"}, strings.NewReader("y\n"), io.Discard, fixedNow)
	if err == nil || len(uc.executeInputs) != 0 {
		t.Fatalf("error = %v, Execute calls = %d", err, len(uc.executeInputs))
	}
}

func TestRunCleanupWithLogsSharesConfirmationReader(t *testing.T) {
	t.Setenv("CODEX_RUNNER_LANG", "en")
	worktrees := &fakeCleanupUseCase{planCandidates: candidates("/tmp/a"), executeOutput: execution.EvictWorkDirOutput{Deleted: []string{"/tmp/a"}}}
	logs := &fakeCleanupLogsUseCase{
		planOutput:    execution.EvictLogsOutput{Candidates: []execution.LogDeletionCandidate{{Path: "/tmp/log", Category: execution.LogCategoryMonthlyMetrics}}},
		executeOutput: execution.EvictLogsOutput{Deleted: []string{"/tmp/log"}},
	}
	var stdout bytes.Buffer
	_, logOutput, err := runCleanupWithLogs(context.Background(), worktrees, logs, nil, strings.NewReader("y\ny\n"), &stdout, fixedNow)
	if err != nil || len(logs.confirmed) != 1 || len(logOutput.Deleted) != 1 {
		t.Fatalf("error = %v, confirmed = %#v, logs = %#v", err, logs.confirmed, logOutput)
	}
	if !strings.Contains(stdout.String(), "This will delete 1 log files. Continue?") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunCleanupLogsCancelsUnlessResponseIsExactY(t *testing.T) {
	for _, response := range []string{"n\n", "\n", ""} {
		logs := &fakeCleanupLogsUseCase{planOutput: execution.EvictLogsOutput{Candidates: []execution.LogDeletionCandidate{{Path: "/tmp/log", Category: execution.LogCategoryMonthlyMetrics}}}}
		_, err := runCleanupLogs(context.Background(), logs, io.Discard, bufio.NewReader(strings.NewReader(response)), fixedNow)
		if err != nil || len(logs.confirmed) != 0 {
			t.Fatalf("response %q: error = %v, confirmed = %#v", response, err, logs.confirmed)
		}
	}
}

func sameStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
