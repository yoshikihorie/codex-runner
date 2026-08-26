package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
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
	findGitBinary                 = proc.FindGitBinary
	openWorktreeFile              = os.Open
	beforeWorktreeTemporaryCreate = func() error { return nil }
	beforeWorktreePublish         = func() error { return nil }
	afterWorktreeFileCopy         = func(string) error { return nil }
	removeWorktreeTreeAtFn        = removeWorktreeTreeAt
	gitCommandTimeout             = 30 * time.Second
)

// WorktreeFileStore provides filesystem-backed worktree operations.
type WorktreeFileStore struct{}

const renameExclusive = 0x00000004   // Darwin RENAME_EXCL.
const sysRenameatxNP = 488           // Darwin SYS_RENAMEATX_NP.
const atFileSystemRoot = ^uintptr(1) // Darwin AT_FDCWD.

const (
	sysOpenat         = 463 // Darwin SYS_OPENAT.
	sysFchmodat       = 467 // Darwin SYS_FCHMODAT.
	sysFstatat        = 470 // Darwin SYS_FSTATAT64 (the inode64 ABI used by Go's Stat_t).
	sysUnlinkat       = 472 // Darwin SYS_UNLINKAT.
	sysSymlinkat      = 474 // Darwin SYS_SYMLINKAT.
	sysMkdirat        = 475 // Darwin SYS_MKDIRAT.
	atRemoveDir       = 0x0080
	atSymlinkNoFollow = 0x0020
)

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
	parentPath := filepath.Dir(destinationDir)
	parent, err := openWorktreeDirectory(parentPath)
	if err != nil {
		return fmt.Errorf("open worktree destination parent: %w", err)
	}
	defer parent.Close()
	if err := rejectWorktreeDestinationWithinSource(sourceDir, parentPath); err != nil {
		return err
	}
	if err := beforeWorktreeTemporaryCreate(); err != nil {
		return err
	}
	temporaryName, temporary, err := createWorktreeTemporary(parent.Fd())
	if err != nil {
		return fmt.Errorf("create worktree temporary directory: %w", err)
	}
	defer temporary.Close()
	defer func() {
		if err == nil {
			return
		}
		cleanupErr := removeWorktreeTreeAtFn(parent.Fd(), temporaryName, temporary.Fd())
		if cleanupErr != nil {
			err = errors.Join(err, cleanupErr)
		}
	}()
	directories, err := copyWorktreeTree(ctx, sourceDir, int(temporary.Fd()))
	if err != nil {
		closeWorktreeDirectories(directories)
		return err
	}
	defer closeWorktreeDirectories(directories)
	for index := len(directories) - 1; index >= 0; index-- {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = syscall.Fchmod(directories[index].fd, uint32(directories[index].mode)); err != nil {
			return fmt.Errorf("restore worktree directory permissions: %w", err)
		}
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = beforeWorktreePublish(); err != nil {
		return err
	}
	currentParent, err := openWorktreeDirectory(parentPath)
	if err != nil {
		return fmt.Errorf("worktree destination parent changed: %w", err)
	}
	defer currentParent.Close()
	if !sameWorktreeDirectory(parent, currentParent) {
		return fmt.Errorf("worktree destination parent changed")
	}
	if err = renameAtExclusive(parent.Fd(), temporaryName, filepath.Base(destinationDir)); err != nil {
		return fmt.Errorf("publish worktree: %w", err)
	}
	return nil
}

type worktreeDirectoryMode struct {
	fd   int
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

func copyWorktreeTree(ctx context.Context, source string, temporaryFD int) ([]worktreeDirectoryMode, error) {
	directories := make([]worktreeDirectoryMode, 0)
	rootFD, err := syscall.Dup(temporaryFD)
	if err != nil {
		return nil, err
	}
	directories = append(directories, worktreeDirectoryMode{fd: rootFD})
	err = filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
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
			directories[0].mode = info.Mode().Perm()
			return nil
		}
		parentFD, err := worktreeOutputParent(temporaryFD, filepath.Dir(rel))
		if err != nil {
			return err
		}
		defer syscall.Close(parentFD)
		name := filepath.Base(rel)
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
			return symlinkAt(link, parentFD, name)
		case mode.IsDir():
			if err := mkdirAt(parentFD, name, mode.Perm()|0o700); err != nil {
				return err
			}
			fd, err := openAt(parentFD, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
			if err != nil {
				return err
			}
			directories = append(directories, worktreeDirectoryMode{fd: fd, mode: mode.Perm()})
			return nil
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
			toFD, err := openAt(parentFD, name, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, uint32(mode.Perm()))
			if err != nil {
				return err
			}
			to := os.NewFile(uintptr(toFD), name)
			defer to.Close()
			copyDigest := sha256.New()
			_, copyErr := io.Copy(io.MultiWriter(to, copyDigest), contextReader{ctx: ctx, r: from})
			if copyErr != nil {
				_ = to.Close()
				return copyErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := afterWorktreeFileCopy(path); err != nil {
				return err
			}
			if _, err := from.Seek(0, io.SeekStart); err != nil {
				return err
			}
			verifyDigest := sha256.New()
			if _, err := io.Copy(verifyDigest, contextReader{ctx: ctx, r: from}); err != nil {
				return err
			}
			finalInfo, err := from.Stat()
			if err != nil {
				return err
			}
			if !finalInfo.Mode().IsRegular() || !os.SameFile(openedInfo, finalInfo) || openedInfo.Size() != finalInfo.Size() || !openedInfo.ModTime().Equal(finalInfo.ModTime()) || !bytes.Equal(copyDigest.Sum(nil), verifyDigest.Sum(nil)) {
				_ = to.Close()
				return fmt.Errorf("worktree source changed during copy: %s", path)
			}
			if err := syscall.Fchmod(toFD, uint32(mode.Perm())); err != nil {
				_ = to.Close()
				return err
			}
			return to.Close()
		default:
			return fmt.Errorf("unsupported worktree file type: %s", path)
		}
	})
	return directories, err
}

func closeWorktreeDirectories(directories []worktreeDirectoryMode) {
	for _, directory := range directories {
		_ = syscall.Close(directory.fd)
	}
}

func openWorktreeDirectory(path string) (*os.File, error) {
	return os.OpenFile(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
}

func sameWorktreeDirectory(left, right *os.File) bool {
	leftInfo, leftErr := left.Stat()
	rightInfo, rightErr := right.Stat()
	return leftErr == nil && rightErr == nil && os.SameFile(leftInfo, rightInfo)
}

func createWorktreeTemporary(parentFD uintptr) (string, *os.File, error) {
	for attempts := 0; attempts < 100; attempts++ {
		name, err := worktreeTemporaryName()
		if err != nil {
			return "", nil, err
		}
		if err := mkdirAt(int(parentFD), name, 0o700); err != nil {
			if errors.Is(err, syscall.EEXIST) {
				continue
			}
			return "", nil, err
		}
		fd, err := openAt(int(parentFD), name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return "", nil, err
		}
		var named syscall.Stat_t
		var opened syscall.Stat_t
		if err := fstatAt(int(parentFD), name, &named, atSymlinkNoFollow); err != nil {
			_ = syscall.Close(fd)
			return "", nil, err
		}
		if err := syscall.Fstat(fd, &opened); err != nil || named.Dev != opened.Dev || named.Ino != opened.Ino {
			_ = syscall.Close(fd)
			if err != nil {
				return "", nil, err
			}
			return "", nil, fmt.Errorf("worktree temporary changed while opening")
		}
		return name, os.NewFile(uintptr(fd), name), nil
	}
	return "", nil, fmt.Errorf("create unique worktree temporary directory")
}

func worktreeTemporaryName() (string, error) {
	bytes := make([]byte, 12)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return ".worktree-copy-" + hex.EncodeToString(bytes), nil
}

func worktreeOutputParent(rootFD int, relativeDirectory string) (int, error) {
	if relativeDirectory == "." {
		fd, err := syscall.Dup(rootFD)
		return fd, err
	}
	fd, err := syscall.Dup(rootFD)
	if err != nil {
		return 0, err
	}
	for _, component := range strings.Split(relativeDirectory, string(filepath.Separator)) {
		next, err := openAt(fd, component, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
		_ = syscall.Close(fd)
		if err != nil {
			return 0, err
		}
		fd = next
	}
	return fd, nil
}

func openAt(dirFD int, name string, flags int, mode uint32) (int, error) {
	path, err := syscall.BytePtrFromString(name)
	if err != nil {
		return 0, err
	}
	r0, _, errno := syscall.Syscall6(sysOpenat, uintptr(dirFD), uintptr(unsafe.Pointer(path)), uintptr(flags), uintptr(mode), 0, 0)
	if errno != 0 {
		return 0, errno
	}
	return int(r0), nil
}

func mkdirAt(dirFD int, name string, mode os.FileMode) error {
	path, err := syscall.BytePtrFromString(name)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall(sysMkdirat, uintptr(dirFD), uintptr(unsafe.Pointer(path)), uintptr(mode))
	if errno != 0 {
		return errno
	}
	return nil
}

func fstatAt(dirFD int, name string, stat *syscall.Stat_t, flags int) error {
	path, err := syscall.BytePtrFromString(name)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall6(sysFstatat, uintptr(dirFD), uintptr(unsafe.Pointer(path)), uintptr(unsafe.Pointer(stat)), uintptr(flags), 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func symlinkAt(target string, dirFD int, name string) error {
	link, err := syscall.BytePtrFromString(target)
	if err != nil {
		return err
	}
	path, err := syscall.BytePtrFromString(name)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall6(sysSymlinkat, uintptr(unsafe.Pointer(link)), uintptr(dirFD), uintptr(unsafe.Pointer(path)), 0, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

func renameAtExclusive(parentFD uintptr, source, destination string) error {
	from, err := syscall.BytePtrFromString(source)
	if err != nil {
		return err
	}
	to, err := syscall.BytePtrFromString(destination)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall6(sysRenameatxNP, parentFD, uintptr(unsafe.Pointer(from)), parentFD, uintptr(unsafe.Pointer(to)), uintptr(renameExclusive), 0)
	if errno == 0 {
		return nil
	}
	if errors.Is(errno, syscall.EEXIST) {
		return fs.ErrExist
	}
	return errno
}

func removeWorktreeTreeAt(parentFD uintptr, name string, rootFD uintptr) error {
	if err := syscall.Fchmod(int(rootFD), 0o700); err != nil {
		return err
	}
	readFD, err := syscall.Dup(int(rootFD))
	if err != nil {
		return err
	}
	root := os.NewFile(uintptr(readFD), name)
	entries, err := root.ReadDir(-1)
	closeErr := root.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	for _, entry := range entries {
		if entry.IsDir() {
			childFD, openErr := openAt(int(rootFD), entry.Name(), syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
			if openErr != nil {
				return openErr
			}
			if removeErr := removeWorktreeTreeAt(uintptr(rootFD), entry.Name(), uintptr(childFD)); removeErr != nil {
				_ = syscall.Close(childFD)
				return removeErr
			}
			_ = syscall.Close(childFD)
			continue
		}
		if unlinkErr := unlinkAt(int(rootFD), entry.Name(), 0); unlinkErr != nil {
			return unlinkErr
		}
	}
	return unlinkAt(int(parentFD), name, atRemoveDir)
}

func unlinkAt(dirFD int, name string, flags int) error {
	path, err := syscall.BytePtrFromString(name)
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall(sysUnlinkat, uintptr(dirFD), uintptr(unsafe.Pointer(path)), uintptr(flags))
	if errno != 0 {
		return errno
	}
	return nil
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
