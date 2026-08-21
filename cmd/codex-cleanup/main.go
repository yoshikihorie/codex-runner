package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/config"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type cleanupUseCase interface {
	Plan(context.Context, execution.EvictWorkDirInput) ([]execution.WorktreeCandidate, []execution.WorktreeSkipped, error)
	Execute(context.Context, execution.EvictWorkDirInput, []string) (execution.EvictWorkDirOutput, error)
}

type cleanupLogsUseCase interface {
	Plan(context.Context, execution.EvictLogsInput) (execution.EvictLogsOutput, error)
	Execute(context.Context, execution.EvictLogsInput, []execution.LogDeletionCandidate) (execution.EvictLogsOutput, error)
}

var _ cleanupUseCase = (*execution.EvictWorkDirUseCase)(nil)

const maxConfirmationInputBytes = 64

// メッセージ文言は message-catalog.md:145-146（info.cleanup.confirm / info.cleanup.completed）
// から転記。値を変更する場合は先に message-catalog.md を更新し、本コード側へ同期すること
// （AGENTS.md「値を直書きしない」の「転記元をコメントで明示する」条項に従う。10-shared/
// message-catalog.md §1 machine-canon 規約: message-catalog.yaml が値の正典）。
var cleanupMessages = map[string]map[string]string{
	"ja": {
		"info.cleanup.confirm":   "{count}件の作業用ディレクトリを削除します。よろしいですか。",
		"info.cleanup.completed": "{count}件の作業用ディレクトリを削除しました。",
		// message-catalog.md:172-173（info.cleanup.logsConfirm / info.cleanup.logsCompleted）から転記。
		"info.cleanup.logsConfirm":   "{count}件のログファイルを削除します。よろしいですか。",
		"info.cleanup.logsCompleted": "{count}件のログファイルを削除しました。",
	},
	"en": {
		"info.cleanup.confirm":       "This will delete {count} working directories. Continue?",
		"info.cleanup.completed":     "Deleted {count} working directories.",
		"info.cleanup.logsConfirm":   "This will delete {count} log files. Continue?",
		"info.cleanup.logsCompleted": "Deleted {count} log files.",
	},
}

func newCleanupLogsUseCase() (*execution.EvictLogsUseCase, error) {
	cfg, err := config.LoadDefault()
	if err != nil {
		return nil, err
	}
	paths, err := execution.DefaultLogPaths()
	if err != nil {
		return nil, err
	}
	paths.SocketPath = cfg.SocketPath()
	locks := execution.NewCheckLivenessUseCase(domain.LivenessLockFunc(store.TryAcquireLiveness), execution.DefaultLockPathResolver)
	policy := execution.LogEvictionPolicy{
		RotationMaxSize:  cfg.LogRotationMaxSizeBytes(),
		RotationInterval: time.Duration(cfg.LogRotationIntervalSeconds()) * time.Second,
		RetentionDays:    cfg.LogRotationRetentionDays(),
		RetentionCount:   cfg.LogRotationRetentionCount(),
		Compress:         cfg.LogRotationCompress(),
		MetricsRetention: cfg.MetricsRetentionMonths(),
	}
	return execution.NewEvictLogsUseCase(store.NewFileLogStore(nil), locks, policy, paths)
}

func newCleanupUseCase() (*execution.EvictWorkDirUseCase, error) {
	root, err := execution.DefaultWorktreeRoot()
	if err != nil {
		return nil, err
	}
	locks := execution.NewCheckLivenessUseCase(
		domain.LivenessLockFunc(store.TryAcquireLiveness),
		execution.DefaultLockPathResolver,
	)
	return execution.NewEvictWorkDirUseCase(store.NewWorktreeFileStore(), locks, root)
}

func runCleanup(ctx context.Context, uc cleanupUseCase, args []string, stdin io.Reader, stdout io.Writer, now func() time.Time) (execution.EvictWorkDirOutput, error) {
	return runCleanupWithReader(ctx, uc, args, bufio.NewReader(stdin), stdout, now)
}

func runCleanupWithReader(ctx context.Context, uc cleanupUseCase, args []string, stdin *bufio.Reader, stdout io.Writer, now func() time.Time) (execution.EvictWorkDirOutput, error) {
	flags := flag.NewFlagSet("codex-cleanup", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	force := flags.Bool("force", false, "include changed working directories")
	maxAge := flags.Int("max-age", 0, "maximum age in days")
	if err := flags.Parse(args); err != nil {
		return execution.EvictWorkDirOutput{}, err
	}
	if flags.NArg() != 0 {
		return execution.EvictWorkDirOutput{}, fmt.Errorf("unexpected positional arguments: %q", flags.Args())
	}

	in := execution.EvictWorkDirInput{
		Trigger:    execution.TriggerExplicit,
		Force:      *force,
		MaxAgeDays: *maxAge,
		OccurredAt: now(),
	}
	candidates, _, err := uc.Plan(ctx, in)
	if err != nil {
		return execution.EvictWorkDirOutput{}, err
	}

	locale := resolveCleanupLocale()
	if _, err := fmt.Fprintf(stdout, "%s [y/N] ", formatCleanupMessage(locale, "info.cleanup.confirm", len(candidates))); err != nil {
		return execution.EvictWorkDirOutput{}, fmt.Errorf("write cleanup confirmation: %w", err)
	}
	confirmed, err := readConfirmationReader(stdin)
	if err != nil {
		return execution.EvictWorkDirOutput{}, err
	}
	if !confirmed {
		return execution.EvictWorkDirOutput{}, nil
	}

	confirmedPaths := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		confirmedPaths = append(confirmedPaths, candidate.Path)
	}
	out, err := uc.Execute(ctx, in, confirmedPaths)
	if err != nil {
		return execution.EvictWorkDirOutput{}, err
	}
	if _, err := fmt.Fprintln(stdout, formatCleanupMessage(locale, "info.cleanup.completed", len(out.Deleted))); err != nil {
		return execution.EvictWorkDirOutput{}, fmt.Errorf("write cleanup completion: %w", err)
	}
	return out, nil
}

func runCleanupLogs(ctx context.Context, uc cleanupLogsUseCase, stdout io.Writer, stdin *bufio.Reader, now func() time.Time) (execution.EvictLogsOutput, error) {
	in := execution.EvictLogsInput{Trigger: execution.TriggerExplicit, OccurredAt: now()}
	planned, err := uc.Plan(ctx, in)
	if err != nil {
		return execution.EvictLogsOutput{}, err
	}
	locale := resolveCleanupLocale()
	if _, err := fmt.Fprintf(stdout, "%s [y/N] ", formatCleanupMessage(locale, "info.cleanup.logsConfirm", len(planned.Candidates))); err != nil {
		return execution.EvictLogsOutput{}, fmt.Errorf("write logs cleanup confirmation: %w", err)
	}
	confirmed, err := readConfirmationReader(stdin)
	if err != nil {
		return execution.EvictLogsOutput{}, err
	}
	if !confirmed {
		return planned, nil
	}
	out, err := uc.Execute(ctx, in, planned.Candidates)
	if err != nil {
		return out, err
	}
	if _, err := fmt.Fprintln(stdout, formatCleanupMessage(locale, "info.cleanup.logsCompleted", len(out.Deleted))); err != nil {
		return execution.EvictLogsOutput{}, fmt.Errorf("write logs cleanup completion: %w", err)
	}
	return out, nil
}

func readConfirmation(r io.Reader) (bool, error) {
	return readConfirmationReader(bufio.NewReader(r))
}

func readConfirmationReader(r *bufio.Reader) (bool, error) {
	line := make([]byte, 0, maxConfirmationInputBytes)
	tooLong := false

	for {
		fragment, err := r.ReadSlice('\n')

		if !tooLong {
			if len(line)+len(fragment) > maxConfirmationInputBytes {
				tooLong = true
			} else {
				line = append(line, fragment...)
			}
		}

		switch {
		case err == nil:
			goto done
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF):
			goto done
		default:
			return false, fmt.Errorf("read cleanup confirmation: %w", err)
		}
	}

done:
	if tooLong || len(line) == 0 {
		return false, nil
	}
	value := strings.TrimSuffix(string(line), "\n")
	value = strings.TrimSuffix(value, "\r")
	return value == "y", nil
}

func runCleanupWithLogs(ctx context.Context, worktrees cleanupUseCase, logs cleanupLogsUseCase, args []string, stdin io.Reader, stdout io.Writer, now func() time.Time) (execution.EvictWorkDirOutput, execution.EvictLogsOutput, error) {
	reader := bufio.NewReader(stdin)
	worktreeOutput, err := runCleanupWithReader(ctx, worktrees, args, reader, stdout, now)
	if err != nil {
		return worktreeOutput, execution.EvictLogsOutput{}, err
	}
	logOutput, err := runCleanupLogs(ctx, logs, stdout, reader, now)
	return worktreeOutput, logOutput, err
}

func resolveCleanupLocale() string {
	for _, candidate := range []string{os.Getenv("CODEX_RUNNER_LANG"), os.Getenv("LC_ALL"), os.Getenv("LANG")} {
		language := strings.ToLower(strings.Split(strings.Split(candidate, ".")[0], "_")[0])
		if language == "en" || language == "ja" {
			return language
		}
	}
	return "ja"
}

func formatCleanupMessage(locale, key string, count int) string {
	return strings.ReplaceAll(cleanupMessages[locale][key], "{count}", strconv.Itoa(count))
}

func runMain(ctx context.Context, uc cleanupUseCase, args []string, stdin io.Reader, stdout io.Writer, stderr io.Writer, now func() time.Time) int {
	if _, err := runCleanup(ctx, uc, args, stdin, stdout, now); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func main() {
	uc, err := newCleanupUseCase()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	logs, err := newCleanupLogsUseCase()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, _, err := runCleanupWithLogs(context.Background(), uc, logs, os.Args[1:], os.Stdin, os.Stdout, time.Now); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
