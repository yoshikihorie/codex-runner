package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"

	"github.com/yoshikihorie/codex-runner/internal/proc"
)

var (
	findGitBinary      = proc.FindGitBinary
	openWorktreeFile   = os.Open
	chmodWorktreePath  = os.Chmod
	removeWorktreeTree = os.RemoveAll
	gitCommandTimeout  = 30 * time.Second
)

// WorktreeFileStore provides filesystem-backed worktree operations.
type WorktreeFileStore struct{}

const renameExclusive = 0x00000004   // Darwin RENAME_EXCL.
const sysRenameatxNP = 488           // Darwin SYS_RENAMEATX_NP.
const atFileSystemRoot = ^uintptr(1) // Darwin AT_FDCWD.

// NewWorktreeFileStore constructs a filesystem-backed worktree store.
func NewWorktreeFileStore() *WorktreeFileStore { return &WorktreeFileStore{} }

// Create copies sourceDir into a temporary sibling and publishes it only when complete.
func (s *WorktreeFileStore) Create(ctx context.Context, sourceDir string, destinationDir string) (err error) {
	if sourceDir == "" || destinationDir == "" || !filepath.IsAbs(sourceDir) || !filepath.IsAbs(destinationDir) {
		return fmt.Errorf("worktree source and destination must be absolute paths")
	}
	sourceInfo, statErr := os.Lstat(sourceDir)
	if statErr != nil {
		return fmt.Errorf("lstat worktree source: %w", statErr)
	}
	if sourceInfo.Mode()&os.ModeSymlink != 0 || !sourceInfo.IsDir() {
		return fmt.Errorf("worktree source must be a directory and not a symbolic link: %s", sourceDir)
	}
	if info, statErr := os.Lstat(destinationDir); statErr == nil || info != nil {
		return fmt.Errorf("%w: worktree destination already exists: %s", fs.ErrExist, destinationDir)
	} else if !os.IsNotExist(statErr) {
		return statErr
	}
	parent := filepath.Dir(destinationDir)
	if err := rejectWorktreeDestinationWithinSource(sourceDir, parent); err != nil {
		return err
	}
	temporary, err := os.MkdirTemp(parent, ".worktree-copy-")
	if err != nil {
		return fmt.Errorf("create worktree temporary directory: %w", err)
	}
	defer func() {
		if err == nil {
			return
		}
		cleanupErr := errors.Join(makeWorktreeTreeRemovable(temporary), removeWorktreeTree(temporary))
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	directories, err := copyWorktreeTree(ctx, sourceDir, temporary)
	if err != nil {
		return err
	}
	for index := len(directories) - 1; index >= 0; index-- {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = chmodWorktreePath(directories[index].path, directories[index].mode); err != nil {
			return fmt.Errorf("restore worktree directory permissions: %w", err)
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = RenameExclusive(temporary, destinationDir); err != nil {
		return fmt.Errorf("publish worktree: %w", err)
	}
	return nil
}

func makeWorktreeTreeRemovable(root string) error {
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.IsDir() {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		return chmodWorktreePath(path, info.Mode().Perm()|0o700)
	})
}

type worktreeDirectoryMode struct {
	path string
	mode os.FileMode
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func rejectWorktreeDestinationWithinSource(source, destinationParent string) error {
	resolvedSource, err := filepath.EvalSymlinks(source)
	if err != nil {
		return fmt.Errorf("resolve worktree source: %w", err)
	}
	resolvedParent, err := filepath.EvalSymlinks(destinationParent)
	if err != nil {
		return fmt.Errorf("resolve worktree destination parent: %w", err)
	}
	rel, err := filepath.Rel(resolvedSource, resolvedParent)
	if err != nil {
		return fmt.Errorf("compare worktree paths: %w", err)
	}
	if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		return fmt.Errorf("worktree destination parent is inside source")
	}
	return nil
}

func copyWorktreeTree(ctx context.Context, source, destination string) ([]worktreeDirectoryMode, error) {
	directories := make([]worktreeDirectoryMode, 0)
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if rel == "." {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			directories = append(directories, worktreeDirectoryMode{destination, info.Mode().Perm()})
			return nil
		}
		target := filepath.Join(destination, rel)
		info, err := entry.Info()
		if err != nil {
			return err
		}
		mode := info.Mode()
		switch {
		case mode&os.ModeSymlink != 0:
			link, err := os.Readlink(path)
			if err != nil {
				return err
			}
			return os.Symlink(link, target)
		case mode.IsDir():
			directories = append(directories, worktreeDirectoryMode{target, mode.Perm()})
			return os.Mkdir(target, mode.Perm()|0o700)
		case mode.IsRegular():
			from, err := openWorktreeFile(path)
			if err != nil {
				return err
			}
			openedInfo, statErr := from.Stat()
			if statErr != nil {
				_ = from.Close()
				return statErr
			}
			if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
				_ = from.Close()
				return fmt.Errorf("worktree source changed while opening: %s", path)
			}
			defer from.Close()
			to, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode.Perm())
			if err != nil {
				return err
			}
			_, copyErr := io.Copy(to, contextReader{ctx: ctx, r: from})
			closeErr := to.Close()
			if copyErr != nil {
				return copyErr
			}
			if closeErr != nil {
				return closeErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			return chmodWorktreePath(target, mode.Perm())
		default:
			return fmt.Errorf("unsupported worktree file type: %s", path)
		}
	})
	return directories, err
}

// RenameExclusive atomically moves source to destination only when destination
// does not already exist.
func RenameExclusive(source, destination string) error {
	from, err := syscall.BytePtrFromString(source)
	if err != nil {
		return err
	}
	to, err := syscall.BytePtrFromString(destination)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall6(sysRenameatxNP, atFileSystemRoot, uintptr(unsafe.Pointer(from)), atFileSystemRoot, uintptr(unsafe.Pointer(to)), uintptr(renameExclusive), 0)
	if errno == 0 {
		return nil
	}
	if errors.Is(errno, syscall.EEXIST) {
		return fs.ErrExist
	}
	return errno
}

// renamexNPExclusive preserves existing internal callers while the exported
// helper is adopted by components outside this package.
func renamexNPExclusive(source, destination string) error {
	return RenameExclusive(source, destination)
}

// ListTopLevel returns normalized absolute paths for root's direct children only.
func (s *WorktreeFileStore) ListTopLevel(root string) ([]string, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("worktree root must be an absolute path: %q", root)
	}
	root = filepath.Clean(root)
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
		count, err := runGit(path, "rev-list", "--count", string(bytes.TrimSpace(mergeBase))+"..HEAD")
		if err != nil {
			return false, fmt.Errorf("git rev-list count from %s: %w", base, err)
		}
		commitCount, err := strconv.ParseUint(string(bytes.TrimSpace(count)), 10, 64)
		if err != nil {
			return false, fmt.Errorf("parse git rev-list count from %s: %w", base, err)
		}
		return commitCount > 0, nil
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
	ctx, cancel := context.WithTimeout(context.Background(), gitCommandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, gitPath, commandArgs...)
	cmd.Env = proc.SafeChildEnv()
	output, err := cmd.Output()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return nil, fmt.Errorf("git command timed out: %w", ctx.Err())
	}
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
