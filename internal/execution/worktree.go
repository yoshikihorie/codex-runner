package execution

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const (
	WorktreeRetentionDaysDefault = 7
	cleanupMaxAgeMinDays         = 1
	cleanupMaxAgeMaxDays         = 365
	worktreeRootDirName          = ".codex-worktrees-cli"

	TriggerExplicit  = "explicit"
	TriggerAutomatic = "automatic"
)

const worktreeRootPermission os.FileMode = 0o700

// WorktreeCreator creates one atomic worktree copy.
type WorktreeCreator interface {
	Create(ctx context.Context, sourceDir string, destinationDir string) error
}

type CreateWorktreeInput struct {
	TaskID           domain.TaskID
	SourceWorkingDir string
}

type CreateWorktreeOutput struct{ WorkingDir string }

type CreateWorktreeUseCase struct {
	creator WorktreeCreator
	root    string
}

func NewCreateWorktreeUseCase(creator WorktreeCreator, root string) (*CreateWorktreeUseCase, error) {
	if creator == nil {
		return nil, fmt.Errorf("worktree creator must not be nil")
	}
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, fmt.Errorf("worktree root must be a normalized absolute path")
	}
	return &CreateWorktreeUseCase{creator: creator, root: root}, nil
}

func (uc *CreateWorktreeUseCase) Execute(ctx context.Context, in CreateWorktreeInput) (CreateWorktreeOutput, error) {
	if err := ctx.Err(); err != nil {
		return CreateWorktreeOutput{}, err
	}
	if in.TaskID.String() == "" || in.SourceWorkingDir == "" || !filepath.IsAbs(in.SourceWorkingDir) || filepath.Clean(in.SourceWorkingDir) != in.SourceWorkingDir {
		return CreateWorktreeOutput{}, fmt.Errorf("worktree source and task ID are required")
	}
	if err := os.Mkdir(uc.root, worktreeRootPermission); err != nil && !errors.Is(err, fs.ErrExist) {
		return CreateWorktreeOutput{}, fmt.Errorf("create worktree root: %w", err)
	}
	rootInfo, err := os.Lstat(uc.root)
	if err != nil {
		return CreateWorktreeOutput{}, err
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() {
		return CreateWorktreeOutput{}, fmt.Errorf("worktree root must be a directory and not a symlink")
	}
	sourceInfo, err := os.Lstat(in.SourceWorkingDir)
	if err != nil {
		return CreateWorktreeOutput{}, err
	}
	if sourceInfo.Mode()&os.ModeSymlink != 0 || !sourceInfo.IsDir() {
		return CreateWorktreeOutput{}, fmt.Errorf("worktree source must be a directory and not a symlink")
	}
	destination := filepath.Join(uc.root, in.TaskID.String())
	if err := uc.creator.Create(ctx, in.SourceWorkingDir, destination); err != nil {
		return CreateWorktreeOutput{}, err
	}
	return CreateWorktreeOutput{WorkingDir: destination}, nil
}

type WorktreeCandidate struct {
	Path    string
	TaskID  domain.TaskID
	AgeDays int
}

type WorktreeSkipReason string

const (
	WorktreeSkipHasGitChanges      WorktreeSkipReason = "has_git_changes"
	WorktreeSkipStillAlive         WorktreeSkipReason = "still_alive"
	WorktreeSkipBelowAgeThreshold  WorktreeSkipReason = "below_age_threshold"
	WorktreeSkipInvalidTaskID      WorktreeSkipReason = "invalid_task_id"
	WorktreeSkipSymlink            WorktreeSkipReason = "symlink"
	WorktreeSkipSymlinkCheckFailed WorktreeSkipReason = "symlink_check_failed"
	WorktreeSkipRemoveFailed       WorktreeSkipReason = "remove_failed"
)

type WorktreeSkipped struct {
	Path   string
	Reason WorktreeSkipReason
}

type EvictWorkDirInput struct {
	Trigger    string
	Force      bool
	MaxAgeDays int
	OccurredAt time.Time
}

type EvictWorkDirOutput struct {
	Candidates []WorktreeCandidate
	Deleted    []string
	Skipped    []WorktreeSkipped
}

// WorktreeStore is the execution-layer boundary for worktree filesystem operations.
type WorktreeStore interface {
	ListTopLevel(root string) ([]string, error)
	IsSymlink(path string) (bool, error)
	HasGitChanges(path string) (bool, error)
	ModTime(path string) (time.Time, error)
	AgeDays(path string, now time.Time) (int, error)
	Remove(path string) error
}

type EvictWorkDirUseCase struct {
	store  WorktreeStore
	locks  *CheckLivenessUseCase
	root   string
	logger *slog.Logger
}

func NewEvictWorkDirUseCase(store WorktreeStore, locks *CheckLivenessUseCase, root string, loggers ...*slog.Logger) (*EvictWorkDirUseCase, error) {
	if store == nil || locks == nil {
		return nil, fmt.Errorf("store and locks must not be nil")
	}
	if root == "" {
		return nil, fmt.Errorf("root must not be empty")
	}
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &EvictWorkDirUseCase{store: store, locks: locks, root: root, logger: logger}, nil
}

// DefaultWorktreeRoot resolves the conventional worktree placement root.
func DefaultWorktreeRoot() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home directory: %w", err)
	}
	return filepath.Join(home, worktreeRootDirName), nil
}

func validateCommonInput(in EvictWorkDirInput) error {
	if in.Trigger != TriggerExplicit && in.Trigger != TriggerAutomatic {
		return fmt.Errorf("invalid trigger: %q", in.Trigger)
	}
	if in.Trigger == TriggerAutomatic && in.Force {
		return fmt.Errorf("automatic trigger must not set force=true")
	}
	if in.MaxAgeDays != 0 && (in.MaxAgeDays < cleanupMaxAgeMinDays || in.MaxAgeDays > cleanupMaxAgeMaxDays) {
		return fmt.Errorf("maxAgeDays out of range [%d, %d]: %d", cleanupMaxAgeMinDays, cleanupMaxAgeMaxDays, in.MaxAgeDays)
	}
	if in.OccurredAt.IsZero() {
		return fmt.Errorf("occurredAt must not be zero")
	}
	return nil
}

func validateExecuteInput(in EvictWorkDirInput, confirmedPaths []string) error {
	if err := validateCommonInput(in); err != nil {
		return err
	}
	if in.Trigger == TriggerAutomatic && confirmedPaths != nil {
		return fmt.Errorf("automatic trigger must not receive confirmedPaths")
	}
	if in.Trigger == TriggerExplicit && confirmedPaths == nil {
		return fmt.Errorf("explicit trigger requires confirmedPaths (bypassing confirmation is not allowed)")
	}
	return nil
}

func (uc *EvictWorkDirUseCase) validateWithinRoot(path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("worktree path must be absolute: %q", path)
	}
	clean := filepath.Clean(path)
	if clean != path {
		return fmt.Errorf("worktree path must be normalized: %q", path)
	}
	rel, err := filepath.Rel(uc.root, clean)
	if err != nil {
		return fmt.Errorf("worktree path is not under root: %w", err)
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || strings.Contains(rel, string(filepath.Separator)) {
		return fmt.Errorf("worktree path is not a direct child of root: %q", path)
	}
	return nil
}

func (uc *EvictWorkDirUseCase) evaluateCandidate(ctx context.Context, path string, in EvictWorkDirInput) (*WorktreeCandidate, *WorktreeSkipped, error) {
	if err := uc.validateWithinRoot(path); err != nil {
		uc.logger.Error("reject worktree path outside root", "path", path, "error", err)
		return nil, nil, nil
	}
	isLink, err := uc.store.IsSymlink(path)
	if err != nil {
		uc.logger.Error("check worktree symlink", "path", path, "error", err)
		return nil, &WorktreeSkipped{Path: path, Reason: WorktreeSkipSymlinkCheckFailed}, nil
	}
	if isLink {
		return nil, &WorktreeSkipped{Path: path, Reason: WorktreeSkipSymlink}, nil
	}
	taskID, err := domain.NewTaskID(filepath.Base(path))
	if err != nil {
		return nil, &WorktreeSkipped{Path: path, Reason: WorktreeSkipInvalidTaskID}, nil
	}
	hasChanges, err := uc.store.HasGitChanges(path)
	if err != nil {
		uc.logger.Error("check worktree git changes", "path", path, "error", err)
		return nil, nil, nil
	}
	ageDays, err := uc.store.AgeDays(path, in.OccurredAt)
	if err != nil {
		uc.logger.Error("compute worktree age", "path", path, "error", err)
		return nil, nil, nil
	}
	if hasChanges {
		mtime, err := uc.store.ModTime(path)
		if err != nil {
			uc.logger.Error("get worktree mtime", "path", path, "error", err)
			return nil, nil, nil
		}
		maxAgeDays := in.MaxAgeDays
		if maxAgeDays == 0 {
			maxAgeDays = WorktreeRetentionDaysDefault
		}
		if in.OccurredAt.Sub(mtime) < time.Duration(maxAgeDays)*24*time.Hour {
			return nil, &WorktreeSkipped{Path: path, Reason: WorktreeSkipBelowAgeThreshold}, nil
		}
		if !in.Force {
			return nil, &WorktreeSkipped{Path: path, Reason: WorktreeSkipHasGitChanges}, nil
		}
	}
	dead, err := uc.locks.Execute(ctx, taskID)
	switch {
	case err == nil && !dead:
		return nil, &WorktreeSkipped{Path: path, Reason: WorktreeSkipStillAlive}, nil
	case err == nil || errors.Is(err, domain.ErrTaskNotFound):
		return &WorktreeCandidate{Path: path, TaskID: taskID, AgeDays: ageDays}, nil, nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return nil, nil, err
	default:
		uc.logger.Error("check worktree liveness", "path", path, "task_id", taskID.String(), "error", err)
		return nil, nil, nil
	}
}

func (uc *EvictWorkDirUseCase) Plan(ctx context.Context, in EvictWorkDirInput) ([]WorktreeCandidate, []WorktreeSkipped, error) {
	if err := validateCommonInput(in); err != nil {
		return nil, nil, err
	}
	paths, err := uc.store.ListTopLevel(uc.root)
	if err != nil {
		return nil, nil, fmt.Errorf("list worktree root: %w", err)
	}
	candidates := make([]WorktreeCandidate, 0, len(paths))
	skipped := make([]WorktreeSkipped, 0)
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return candidates, skipped, err
		}
		candidate, skip, err := uc.evaluateCandidate(ctx, path, in)
		if err != nil {
			return candidates, skipped, err
		}
		if candidate != nil {
			candidates = append(candidates, *candidate)
		}
		if skip != nil {
			skipped = append(skipped, *skip)
		}
	}
	if err := ctx.Err(); err != nil {
		return candidates, skipped, err
	}
	return candidates, skipped, nil
}

func (uc *EvictWorkDirUseCase) Execute(ctx context.Context, in EvictWorkDirInput, confirmedPaths []string) (EvictWorkDirOutput, error) {
	if err := validateExecuteInput(in, confirmedPaths); err != nil {
		return EvictWorkDirOutput{}, err
	}
	if confirmedPaths == nil {
		candidates, skipped, err := uc.Plan(ctx, in)
		if err != nil {
			return EvictWorkDirOutput{}, err
		}
		deleted, deleteSkipped, err := uc.deleteAll(ctx, candidates)
		skipped = append(skipped, deleteSkipped...)
		return EvictWorkDirOutput{Candidates: candidates, Deleted: deleted, Skipped: skipped}, err
	}
	out := EvictWorkDirOutput{}
	seen := make(map[string]struct{}, len(confirmedPaths))
	for _, path := range confirmedPaths {
		if _, duplicate := seen[path]; duplicate {
			continue
		}
		seen[path] = struct{}{}
		if err := ctx.Err(); err != nil {
			return out, err
		}
		candidate, skip, err := uc.evaluateCandidate(ctx, path, in)
		if err != nil {
			return out, err
		}
		if skip != nil {
			out.Skipped = append(out.Skipped, *skip)
			continue
		}
		if candidate == nil {
			continue
		}
		out.Candidates = append(out.Candidates, *candidate)
		if err := uc.store.Remove(candidate.Path); err != nil {
			uc.logger.Error("remove worktree", "path", candidate.Path, "error", err)
			out.Skipped = append(out.Skipped, WorktreeSkipped{Path: candidate.Path, Reason: WorktreeSkipRemoveFailed})
			continue
		}
		out.Deleted = append(out.Deleted, candidate.Path)
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	return out, nil
}

func (uc *EvictWorkDirUseCase) deleteAll(ctx context.Context, candidates []WorktreeCandidate) (deleted []string, skipped []WorktreeSkipped, err error) {
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return deleted, skipped, err
		}
		if err := uc.store.Remove(candidate.Path); err != nil {
			uc.logger.Error("remove worktree", "path", candidate.Path, "error", err)
			skipped = append(skipped, WorktreeSkipped{Path: candidate.Path, Reason: WorktreeSkipRemoveFailed})
			continue
		}
		deleted = append(deleted, candidate.Path)
	}
	if err := ctx.Err(); err != nil {
		return deleted, skipped, err
	}
	return deleted, skipped, nil
}
