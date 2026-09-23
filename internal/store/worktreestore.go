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
	readWorktreeLink              = os.Readlink
	walkWorktreeTree              = filepath.WalkDir
	createWorktreeTemporaryFn     = createWorktreeTemporary
	openWorktreeOutputFile        = openAt
	copyWorktreeFileData          = io.Copy
	worktreeSetPermissions        = syscall.Fchmod
	beforeWorktreeTemporaryCreate = func() error { return nil }
	beforeWorktreePublish         = func() error { return nil }
	afterWorktreeFileCopy         = func(string) error { return nil }
	removeWorktreeTreeAtFn        = removeWorktreeTreeAt
	removeTreeFchmod              = syscall.Fchmod
	gitCommandTimeout             = 30 * time.Second
	worktreeRetryWait             = waitForWorktreeRetry
)

// FD-exec-07 §17: one initial copy and three retries, with at most 100 ms of waiting.
const (
	worktreeCopyMaxAttempts = 4
	worktreeRetryFirst      = 10 * time.Millisecond
	worktreeRetrySecond     = 30 * time.Millisecond
	worktreeRetryThird      = 60 * time.Millisecond
)

var (
	worktreeRetryDelays      = [...]time.Duration{worktreeRetryFirst, worktreeRetrySecond, worktreeRetryThird}
	errWorktreeSourceChanged = errors.New("worktree source changed during copy")
)

type worktreeSourceRetryError struct{ cause error }

func (e worktreeSourceRetryError) Error() string { return e.cause.Error() }
func (e worktreeSourceRetryError) Unwrap() error { return e.cause }

type worktreeCleanupError struct{ cause error }

func (e worktreeCleanupError) Error() string { return e.cause.Error() }
func (e worktreeCleanupError) Unwrap() error { return e.cause }

func retryMissingWorktreeSource(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return worktreeSourceRetryError{cause: err}
	}
	return err
}

func waitForWorktreeRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ctx.Err()
	}
}

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
func (s *WorktreeFileStore) Create(ctx context.Context, sourceDir string, destinationDir string) error {
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
	for attempt := 0; attempt < worktreeCopyMaxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if attempt > 0 {
			if err := worktreeRetryWait(ctx, worktreeRetryDelays[attempt-1]); err != nil {
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		err := createWorktreeCopyAttempt(ctx, sourceDir, destinationDir, parentPath, parent)
		if err == nil {
			return nil
		}
		var cleanupErr worktreeCleanupError
		if errors.As(err, &cleanupErr) {
			return err
		}
		var sourceErr worktreeSourceRetryError
		if !errors.As(err, &sourceErr) {
			return err
		}
		if attempt == worktreeCopyMaxAttempts-1 {
			return fmt.Errorf("worktree copy failed after %d attempts: %w", worktreeCopyMaxAttempts, err)
		}
	}
	return fmt.Errorf("worktree copy attempts exhausted")
}

func createWorktreeCopyAttempt(ctx context.Context, sourceDir, destinationDir, parentPath string, parent *os.File) (err error) {
	if err := beforeWorktreeTemporaryCreate(); err != nil {
		return err
	}
	temporaryName, temporary, err := createWorktreeTemporaryFn(parent.Fd())
	if err != nil {
		return fmt.Errorf("create worktree temporary directory: %w", err)
	}
	var directories []worktreeDirectoryMode
	defer func() {
		closeWorktreeDirectories(directories)
		if err != nil {
			cleanupErr := removeWorktreeTreeAtFn(parent.Fd(), temporaryName, temporary.Fd())
			if cleanupErr != nil {
				err = errors.Join(err, worktreeCleanupError{cause: cleanupErr})
			}
		}
		_ = temporary.Close()
	}()
	directories, err = copyWorktreeTree(ctx, sourceDir, int(temporary.Fd()))
	if err != nil {
		return err
	}
	for index := len(directories) - 1; index >= 0; index-- {
		if err = ctx.Err(); err != nil {
			return err
		}
		if err = worktreeSetPermissions(directories[index].fd, uint32(directories[index].mode)); err != nil {
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
	err = walkWorktreeTree(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return retryMissingWorktreeSource(walkErr)
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
				return retryMissingWorktreeSource(err)
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
			return retryMissingWorktreeSource(err)
		}
		mode := info.Mode()
		switch {
		case mode&os.ModeSymlink != 0:
			link, err := readWorktreeLink(path)
			if err != nil {
				return retryMissingWorktreeSource(err)
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
				return retryMissingWorktreeSource(err)
			}
			openedInfo, statErr := from.Stat()
			if statErr != nil {
				_ = from.Close()
				return statErr
			}
			if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
				_ = from.Close()
				return worktreeSourceRetryError{cause: fmt.Errorf("%w while opening: %s", errWorktreeSourceChanged, path)}
			}
			defer from.Close()
			toFD, err := openWorktreeOutputFile(parentFD, name, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, uint32(mode.Perm()))
			if err != nil {
				return err
			}
			to := os.NewFile(uintptr(toFD), name)
			defer to.Close()
			copyDigest := sha256.New()
			_, copyErr := copyWorktreeFileData(io.MultiWriter(to, copyDigest), contextReader{ctx: ctx, r: from})
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
				return worktreeSourceRetryError{cause: fmt.Errorf("%w: %s", errWorktreeSourceChanged, path)}
			}
			if err := worktreeSetPermissions(toFD, uint32(mode.Perm())); err != nil {
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
	if err := removeTreeContentsAt(int(rootFD), 1, 0, nil); err != nil {
		return err
	}
	return unlinkAt(int(parentFD), name, atRemoveDir)
}

// removeTreeContentsAt removes entries below rootFD through descriptor-relative
// operations.  It deliberately leaves rootFD itself in place so callers can
// preserve protocol files until their own final unlink sequence.
func removeTreeContentsAt(rootFD, depth, maxDepth int, preserve func(string) bool) error {
	if maxDepth > 0 && depth > maxDepth {
		return fmt.Errorf("tree delete depth exceeds %d", maxDepth)
	}
	if err := removeTreeFchmod(rootFD, 0o700); err != nil {
		return err
	}
	entries, err := readDirFD(rootFD, "tree delete")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		name := entry.Name()
		if preserve != nil && preserve(name) {
			continue
		}
		var st syscall.Stat_t
		if err := fstatAt(rootFD, name, &st, atSymlinkNoFollow); err != nil {
			return err
		}
		if st.Mode&syscall.S_IFMT == syscall.S_IFDIR {
			childFD, err := openAt(rootFD, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
			if err != nil {
				return err
			}
			err = removeTreeContentsAt(childFD, depth+1, maxDepth, nil)
			closeErr := syscall.Close(childFD)
			if err != nil {
				return err
			}
			if closeErr != nil {
				return closeErr
			}
			if err := unlinkAt(rootFD, name, atRemoveDir); err != nil {
				return err
			}
			continue
		}
		if err := unlinkAt(rootFD, name, 0); err != nil {
			return err
		}
	}
	return nil
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
