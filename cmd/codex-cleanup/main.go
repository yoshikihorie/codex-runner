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

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type cleanupUseCase interface {
	Plan(context.Context, execution.EvictWorkDirInput) ([]execution.WorktreeCandidate, []execution.WorktreeSkipped, error)
	Execute(context.Context, execution.EvictWorkDirInput, []string) (execution.EvictWorkDirOutput, error)
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
	},
	"en": {
		"info.cleanup.confirm":   "This will delete {count} working directories. Continue?",
		"info.cleanup.completed": "Deleted {count} working directories.",
	},
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
	confirmed, err := readConfirmation(stdin)
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

func readConfirmation(r io.Reader) (bool, error) {
	limited := io.LimitReader(r, maxConfirmationInputBytes)
	line, err := bufio.NewReader(limited).ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return false, fmt.Errorf("read cleanup confirmation: %w", err)
	}
	if errors.Is(err, io.EOF) && line == "" {
		return false, nil
	}
	line = strings.TrimSuffix(line, "\n")
	line = strings.TrimSuffix(line, "\r")
	return line == "y", nil
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
	os.Exit(runMain(context.Background(), uc, os.Args[1:], os.Stdin, os.Stdout, os.Stderr, time.Now))
}
