package store

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
)

var findGitBinary = proc.FindGitBinary

// WorktreeFileStore provides filesystem-backed worktree operations.
type WorktreeFileStore struct{}

// NewWorktreeFileStore constructs a filesystem-backed worktree store.
func NewWorktreeFileStore() *WorktreeFileStore { return &WorktreeFileStore{} }

// ListTopLevel returns normalized absolute paths for root's direct children only.
func (s *WorktreeFileStore) ListTopLevel(root string) ([]string, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read worktree root: %w", err)
	}
	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		paths = append(paths, filepath.Join(root, entry.Name()))
	}
	return paths, nil
}

// IsSymlink reports whether path itself is a symbolic link without following it.
func (s *WorktreeFileStore) IsSymlink(path string) (bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return false, fmt.Errorf("lstat worktree: %w", err)
	}
	return info.Mode()&os.ModeSymlink != 0, nil
}

// HasGitChanges reports uncommitted changes or commits not merged into a known base branch.
func (s *WorktreeFileStore) HasGitChanges(path string) (bool, error) {
	status, err := runGit(path, "status", "--porcelain")
	if err != nil {
		return false, fmt.Errorf("git status: %w", err)
	}
	if len(bytes.TrimSpace(status)) != 0 {
		return true, nil
	}
	for _, base := range []string{"main", "dev", "master"} {
		mergeBase, err := runGit(path, "merge-base", "HEAD", base)
		if err != nil {
			continue
		}
		diff, err := runGit(path, "diff", "--name-only", string(bytes.TrimSpace(mergeBase))+"..HEAD")
		if err != nil {
			return false, fmt.Errorf("git diff from %s: %w", base, err)
		}
		return len(bytes.TrimSpace(diff)) != 0, nil
	}
	// Without a known base branch, conservatively retain the worktree.
	return true, nil
}

func runGit(path string, args ...string) ([]byte, error) {
	gitPath, err := findGitBinary()
	if err != nil {
		return nil, err
	}
	commandArgs := append([]string{"-C", path}, args...)
	cmd := exec.Command(gitPath, commandArgs...)
	cmd.Env = proc.SafeChildEnv()
	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	return output, nil
}

// ModTime returns path's filesystem modification time.
func (s *WorktreeFileStore) ModTime(path string) (time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, fmt.Errorf("stat worktree: %w", err)
	}
	return info.ModTime(), nil
}

// AgeDays returns whole elapsed 24-hour periods since path's modification time.
func (s *WorktreeFileStore) AgeDays(path string, now time.Time) (int, error) {
	mtime, err := s.ModTime(path)
	if err != nil {
		return 0, err
	}
	return int(now.Sub(mtime) / (24 * time.Hour)), nil
}

// Remove removes one previously validated worktree path. Removing a missing path succeeds.
func (s *WorktreeFileStore) Remove(path string) error {
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove worktree: %w", err)
	}
	return nil
}
