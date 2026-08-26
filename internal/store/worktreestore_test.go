package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
)

func TestWorktreeFileStoreCreateCopiesFilesDirectoriesAndLinks(t *testing.T) {
	source := t.TempDir()
	destination := filepath.Join(t.TempDir(), "published")
	if err := os.Mkdir(filepath.Join(source, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(source, "empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(source, "nested"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "nested", "run"), []byte("content"), 0o751); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, ".git", "config"), []byte("git"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nested/run", filepath.Join(source, "relative-link")); err != nil {
		t.Fatal(err)
	}
	if err := NewWorktreeFileStore().Create(context.Background(), source, destination); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(destination, "nested", "run"))
	if err != nil || string(contents) != "content" {
		t.Fatalf("copied contents=%q err=%v", contents, err)
	}
	if info, err := os.Stat(filepath.Join(destination, "nested", "run")); err != nil || info.Mode().Perm() != 0o751 {
		t.Fatalf("copied mode=%v err=%v", info.Mode(), err)
	}
	if _, err := os.Stat(filepath.Join(destination, ".git", "config")); err != nil {
		t.Fatalf("dot directory was not copied: %v", err)
	}
	if info, err := os.Stat(filepath.Join(destination, "empty")); err != nil || !info.IsDir() {
		t.Fatalf("empty directory was not preserved: %v", err)
	}
	if link, err := os.Readlink(filepath.Join(destination, "relative-link")); err != nil || link != "nested/run" {
		t.Fatalf("link=%q err=%v", link, err)
	}
}

func TestContextReaderStopsBeforeTheNextReadAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reader := contextReader{ctx: ctx, r: bytes.NewReader([]byte("content"))}
	buffer := make([]byte, 3)
	if _, err := reader.Read(buffer); err != nil {
		t.Fatal(err)
	}
	cancel()
	if _, err := reader.Read(buffer); !errors.Is(err, context.Canceled) {
		t.Fatalf("Read() error=%v, want context.Canceled", err)
	}
}

type observingWorktreeContext struct {
	context.Context
	cancel context.CancelFunc
	onErr  func()
}

func (c *observingWorktreeContext) Err() error {
	if c.Context.Err() == nil && c.onErr != nil {
		c.onErr()
	}
	return c.Context.Err()
}

func temporaryWorktreeSibling(t *testing.T, parent string) string {
	t.Helper()
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), ".worktree-copy-") {
			return filepath.Join(parent, entry.Name())
		}
	}
	return ""
}

func TestWorktreeFileStoreCreateCancelsDuringDirectoryPermissionRestore(t *testing.T) {
	source := t.TempDir()
	parent := t.TempDir()
	destination := filepath.Join(parent, "published")
	first := filepath.Join(source, "first")
	second := filepath.Join(first, "second")
	firstMode := os.FileMode(0o555)
	if err := os.MkdirAll(second, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(first, firstMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(second, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(second, 0o700); err != nil {
			t.Errorf("restore second directory permissions: %v", err)
		}
		if err := os.Chmod(first, 0o700); err != nil {
			t.Errorf("restore first directory permissions: %v", err)
		}
	})
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	var observed string
	ctx := &observingWorktreeContext{Context: base, cancel: cancel, onErr: func() {
		temporary := temporaryWorktreeSibling(t, parent)
		if temporary == "" {
			return
		}
		firstInfo, firstErr := os.Stat(filepath.Join(temporary, "first"))
		secondInfo, secondErr := os.Stat(filepath.Join(temporary, "first", "second"))
		if firstErr == nil && secondErr == nil && firstInfo.Mode().Perm() == firstMode.Perm()|0o700 && secondInfo.Mode().Perm() == 0o500 {
			observed = temporary
			cancel()
		}
	}}
	if err := NewWorktreeFileStore().Create(ctx, source, destination); !errors.Is(err, context.Canceled) {
		t.Fatalf("Create() error=%v, want context.Canceled", err)
	}
	if observed == "" {
		t.Fatal("did not observe cancellation between directory permission restores")
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("destination was published: %v", err)
	}
}

func TestWorktreeFileStoreCreateCancelsImmediatelyBeforeRename(t *testing.T) {
	source := t.TempDir()
	parent := t.TempDir()
	destination := filepath.Join(parent, "published")
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(source, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chmod(source, 0o700); err != nil {
			t.Errorf("restore source directory permissions: %v", err)
		}
	})
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	var observed string
	ctx := &observingWorktreeContext{Context: base, cancel: cancel, onErr: func() {
		temporary := temporaryWorktreeSibling(t, parent)
		if temporary == "" {
			return
		}
		info, statErr := os.Stat(temporary)
		if statErr == nil && info.Mode().Perm() == 0o555 {
			observed = temporary
			cancel()
		}
	}}
	if err := NewWorktreeFileStore().Create(ctx, source, destination); !errors.Is(err, context.Canceled) {
		t.Fatalf("Create() error=%v, want context.Canceled", err)
	}
	if observed == "" {
		t.Fatal("did not observe the pre-rename worktree")
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("destination was published: %v", err)
	}
	if _, err := os.Stat(observed); !os.IsNotExist(err) {
		t.Fatalf("temporary copy was not removed: %v", err)
	}
}

func TestWorktreeFileStoreCreateRejectsNonDirectoryOrSymlinkSource(t *testing.T) {
	parent := t.TempDir()
	regularFile := filepath.Join(parent, "source-file")
	if err := os.WriteFile(regularFile, []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(parent, "source-directory")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(parent, "source-link")
	if err := os.Symlink(directory, symlink); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{regularFile, symlink} {
		destinationParent := t.TempDir()
		destination := filepath.Join(destinationParent, "published")
		if err := NewWorktreeFileStore().Create(context.Background(), source, destination); err == nil {
			t.Fatalf("Create(%q) accepted invalid source", source)
		}
		if _, err := os.Stat(destination); !os.IsNotExist(err) {
			t.Fatalf("destination was created for %q: %v", source, err)
		}
		if temporary := temporaryWorktreeSibling(t, destinationParent); temporary != "" {
			t.Fatalf("temporary copy was created for %q: %s", source, temporary)
		}
	}
}

func TestWorktreeFileStoreCreateJoinsCleanupErrors(t *testing.T) {
	originalChmod := chmodWorktreePath
	originalRemoveAll := removeWorktreeTree
	chmodErr := errors.New("chmod cleanup failed")
	removeErr := errors.New("remove cleanup failed")
	chmodWorktreePath = func(string, os.FileMode) error { return chmodErr }
	removeWorktreeTree = func(string) error { return removeErr }
	t.Cleanup(func() {
		chmodWorktreePath = originalChmod
		removeWorktreeTree = originalRemoveAll
	})
	source := t.TempDir()
	destination := filepath.Join(t.TempDir(), "published")
	err := NewWorktreeFileStore().Create(context.Background(), source, destination)
	if !errors.Is(err, chmodErr) || !errors.Is(err, removeErr) {
		t.Fatalf("Create() error=%v, want both cleanup errors", err)
	}
}

func TestWorktreeFileStoreCreateRejectsFileReplacedWithSymlinkBeforeOpen(t *testing.T) {
	source := t.TempDir()
	destinationParent := t.TempDir()
	destination := filepath.Join(destinationParent, "published")
	sourceFile := filepath.Join(source, "source-file")
	externalFile := filepath.Join(t.TempDir(), "external-file")
	if err := os.WriteFile(sourceFile, []byte("source"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(externalFile, []byte("external"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalOpen := openWorktreeFile
	openWorktreeFile = func(path string) (*os.File, error) {
		if path == sourceFile {
			if err := os.Rename(sourceFile, sourceFile+".original"); err != nil {
				return nil, err
			}
			if err := os.Symlink(externalFile, sourceFile); err != nil {
				return nil, err
			}
		}
		return originalOpen(path)
	}
	t.Cleanup(func() { openWorktreeFile = originalOpen })
	if err := NewWorktreeFileStore().Create(context.Background(), source, destination); err == nil {
		t.Fatal("Create() succeeded after source file replacement")
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("destination was published: %v", err)
	}
	if temporary := temporaryWorktreeSibling(t, destinationParent); temporary != "" {
		t.Fatalf("temporary copy was not removed: %s", temporary)
	}
}

func TestWorktreeFileStoreCreateCancelsDuringRegularFileCopy(t *testing.T) {
	source := t.TempDir()
	parent := t.TempDir()
	destination := filepath.Join(parent, "published")
	contents := bytes.Repeat([]byte("copy"), 128*1024)
	if err := os.WriteFile(filepath.Join(source, "large-file"), contents, 0o600); err != nil {
		t.Fatal(err)
	}
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	var observedTemporary string
	var errCalls int
	ctx := &observingWorktreeContext{Context: base, cancel: cancel, onErr: func() {
		errCalls++
		temporary := temporaryWorktreeSibling(t, parent)
		if temporary == "" || observedTemporary != "" {
			return
		}
		info, err := os.Stat(filepath.Join(temporary, "large-file"))
		if err == nil && info.Size() > 0 && info.Size() < int64(len(contents)) {
			observedTemporary = temporary
			cancel()
		}
	}}
	if err := NewWorktreeFileStore().Create(ctx, source, destination); !errors.Is(err, context.Canceled) {
		t.Fatalf("Create() error=%v, want context.Canceled", err)
	}
	if errCalls < 2 || observedTemporary == "" {
		t.Fatalf("did not observe cancellation before the next regular-file read: Err calls=%d temporary=%q", errCalls, observedTemporary)
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("destination was published: %v", err)
	}
	if _, err := os.Stat(observedTemporary); !os.IsNotExist(err) {
		t.Fatalf("temporary copy was not removed: %v", err)
	}
}

func TestWorktreeFileStoreCreateRestoresRegularFilePermissionsAfterUmask(t *testing.T) {
	source := t.TempDir()
	destination := filepath.Join(t.TempDir(), "published")
	file := filepath.Join(source, "executable")
	if err := os.WriteFile(file, []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(file, 0o751); err != nil {
		t.Fatal(err)
	}
	originalUmask := syscall.Umask(0o077)
	t.Cleanup(func() { syscall.Umask(originalUmask) })
	if err := NewWorktreeFileStore().Create(context.Background(), source, destination); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(destination, "executable"))
	if err != nil {
		t.Fatalf("stat copied executable: %v", err)
	}
	if info.Mode().Perm() != 0o751 {
		t.Fatalf("copied mode=%v err=%v", info.Mode(), err)
	}
}

func TestWorktreeFileStoreCreateRejectsInvalidOrExistingDestination(t *testing.T) {
	source := t.TempDir()
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewWorktreeFileStore()
	for _, tc := range []struct{ source, destination string }{
		{"", filepath.Join(t.TempDir(), "destination")},
		{source, ""},
		{"relative", filepath.Join(t.TempDir(), "destination")},
		{source, "relative"},
	} {
		if err := store.Create(context.Background(), tc.source, tc.destination); err == nil {
			t.Fatalf("Create(%q, %q) accepted invalid paths", tc.source, tc.destination)
		}
	}
	destination := filepath.Join(t.TempDir(), "destination")
	if err := os.Mkdir(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), source, destination); err == nil {
		t.Fatal("existing destination accepted")
	}
	if _, err := os.Stat(destination); err != nil {
		t.Fatalf("existing destination was replaced: %v", err)
	}
}

func TestWorktreeFileStoreFilesystemOperations(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	child := filepath.Join(root, "child")
	if err := os.Mkdir(child, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(child, "file"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewWorktreeFileStore()

	entries, err := store.ListTopLevel(root)
	if err != nil || len(entries) != 1 || entries[0] != child {
		t.Fatalf("ListTopLevel() = %v, %v", entries, err)
	}
	link, err := store.IsSymlink(child)
	if err != nil || link {
		t.Fatalf("IsSymlink() = %v, %v", link, err)
	}
	mtime, err := store.ModTime(child)
	if err != nil || mtime.IsZero() {
		t.Fatalf("ModTime() = %v, %v", mtime, err)
	}
	age, err := store.AgeDays(child, mtime.Add(49*time.Hour))
	if err != nil || age != 2 {
		t.Fatalf("AgeDays() = %d, %v", age, err)
	}
	if err := store.Remove(child); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove(child); err != nil {
		t.Fatalf("Remove() must be idempotent: %v", err)
	}
}

func TestWorktreeFileStoreListTopLevelRejectsNonAbsoluteRoot(t *testing.T) {
	store := NewWorktreeFileStore()
	for _, root := range []string{"relative", "", "."} {
		t.Run(root, func(t *testing.T) {
			paths, err := store.ListTopLevel(root)
			if paths != nil {
				t.Fatalf("ListTopLevel(%q) paths = %v, want nil", root, paths)
			}
			if err == nil || !strings.Contains(err.Error(), "worktree root must be an absolute path") {
				t.Fatalf("ListTopLevel(%q) error = %v, want absolute path error", root, err)
			}
		})
	}
}

func TestWorktreeFileStoreListTopLevelAcceptsRoot(t *testing.T) {
	paths, err := NewWorktreeFileStore().ListTopLevel("/")
	if err != nil {
		t.Fatalf("ListTopLevel(\"/\") error = %v", err)
	}
	for _, path := range paths {
		if !filepath.IsAbs(path) {
			t.Errorf("ListTopLevel(\"/\") path = %q, want absolute path", path)
		}
		if path == "/" {
			t.Errorf("ListTopLevel(\"/\") included root")
		}
	}
}

func TestRunGitIgnoresAmbientPathAndUsesSafeEnvironment(t *testing.T) {
	repo := newGitTestRepository(t, "main")
	fakeDir := t.TempDir()
	marker := filepath.Join(fakeDir, "fake-git-ran")
	fakeGit := filepath.Join(fakeDir, "git")
	if err := os.WriteFile(fakeGit, []byte("#!/bin/sh\n: > \""+marker+"\"\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", fakeDir+":"+os.Getenv("PATH"))
	if changed, err := NewWorktreeFileStore().HasGitChanges(repo); err != nil || changed {
		t.Fatalf("HasGitChanges() = %v, %v", changed, err)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("fake git was executed: stat error = %v", err)
	}

	originalFindGitBinary := findGitBinary
	captureEnv := filepath.Join(t.TempDir(), "capture-env")
	if err := os.WriteFile(captureEnv, []byte("#!/bin/sh\nenv\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	findGitBinary = func() (string, error) { return captureEnv, nil }
	t.Cleanup(func() { findGitBinary = originalFindGitBinary })
	t.Setenv("FAKE_API_KEY", "secret")
	output, err := runGit(repo, "status", "--porcelain")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(output), "FAKE_API_KEY=") {
		t.Fatalf("runGit environment leaked FAKE_API_KEY: %q", output)
	}
	if !strings.Contains(string(output), "PATH="+proc.FixedPath()) {
		t.Fatalf("runGit environment did not use fixed PATH: %q", output)
	}
	if !strings.Contains(string(output), "HOME="+os.Getenv("HOME")) {
		t.Fatalf("runGit environment did not include HOME: %q", output)
	}
}

func TestRunGitReturnsDeadlineExceededWhenCommandTimesOut(t *testing.T) {
	originalFindGitBinary := findGitBinary
	originalTimeout := gitCommandTimeout
	fakeGit := filepath.Join(t.TempDir(), "git")
	if err := os.WriteFile(fakeGit, []byte("#!/bin/sh\nexec /bin/sleep 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	findGitBinary = func() (string, error) { return fakeGit, nil }
	gitCommandTimeout = 20 * time.Millisecond
	t.Cleanup(func() {
		findGitBinary = originalFindGitBinary
		gitCommandTimeout = originalTimeout
	})
	started := time.Now()
	_, err := runGit(t.TempDir(), "status", "--porcelain")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("runGit() error=%v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("runGit() timed out after %v, want less than one second", elapsed)
	}
}

func TestWorktreeFileStoreSymlinkAndMissingPath(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	target := filepath.Join(root, "target")
	link := filepath.Join(root, "link")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	store := NewWorktreeFileStore()
	isLink, err := store.IsSymlink(link)
	if err != nil || !isLink {
		t.Fatalf("IsSymlink(link) = %v, %v", isLink, err)
	}
	missing := filepath.Join(root, "missing")
	if _, err := store.ListTopLevel(missing); err == nil {
		t.Fatal("ListTopLevel(missing) error = nil")
	}
	if _, err := store.ModTime(missing); err == nil {
		t.Fatal("ModTime(missing) error = nil")
	}
	if _, err := store.AgeDays(missing, time.Now()); err == nil {
		t.Fatal("AgeDays(missing) error = nil")
	}
}

func TestWorktreeFileStoreHasGitChanges(t *testing.T) {
	store := NewWorktreeFileStore()
	t.Run("clean main worktree", func(t *testing.T) {
		repo := newGitTestRepository(t, "main")
		changed, err := store.HasGitChanges(repo)
		if err != nil || changed {
			t.Fatalf("HasGitChanges() = %v, %v", changed, err)
		}
	})
	t.Run("uncommitted changes", func(t *testing.T) {
		repo := newGitTestRepository(t, "main")
		if err := os.WriteFile(filepath.Join(repo, "dirty.txt"), []byte("dirty"), 0o600); err != nil {
			t.Fatal(err)
		}
		changed, err := store.HasGitChanges(repo)
		if err != nil || !changed {
			t.Fatalf("HasGitChanges() = %v, %v", changed, err)
		}
	})
	t.Run("commits ahead of base branch", func(t *testing.T) {
		repo := newGitTestRepository(t, "main")
		runGitForTest(t, repo, "checkout", "-b", "feature")
		if err := os.WriteFile(filepath.Join(repo, "feature.txt"), []byte("feature"), 0o600); err != nil {
			t.Fatal(err)
		}
		runGitForTest(t, repo, "add", "feature.txt")
		runGitForTest(t, repo, "commit", "-m", "feature")
		changed, err := store.HasGitChanges(repo)
		if err != nil || !changed {
			t.Fatalf("HasGitChanges() = %v, %v", changed, err)
		}
	})
	t.Run("reverted commits ahead of base branch", func(t *testing.T) {
		repo := newGitTestRepository(t, "main")
		runGitForTest(t, repo, "checkout", "-b", "feature")
		if err := os.WriteFile(filepath.Join(repo, "feature.txt"), []byte("feature"), 0o600); err != nil {
			t.Fatal(err)
		}
		runGitForTest(t, repo, "add", "feature.txt")
		runGitForTest(t, repo, "commit", "-m", "feature")
		runGitForTest(t, repo, "revert", "--no-edit", "HEAD")
		changed, err := store.HasGitChanges(repo)
		if err != nil || !changed {
			t.Fatalf("HasGitChanges() = %v, %v", changed, err)
		}
	})
	t.Run("no known base branch retains worktree", func(t *testing.T) {
		repo := newGitTestRepository(t, "other")
		changed, err := store.HasGitChanges(repo)
		if err != nil || !changed {
			t.Fatalf("HasGitChanges() = %v, %v", changed, err)
		}
	})
	t.Run("non repository returns error", func(t *testing.T) {
		changed, err := store.HasGitChanges(t.TempDir())
		if err == nil || changed {
			t.Fatalf("HasGitChanges() = %v, %v", changed, err)
		}
	})
}

func TestWorktreeFileStoreHasGitChangesReturnsErrorForInvalidRevListOutput(t *testing.T) {
	originalFindGitBinary := findGitBinary
	fakeGit := filepath.Join(t.TempDir(), "git")
	if err := os.WriteFile(fakeGit, []byte("#!/bin/sh\nfor arg in \"$@\"; do\n  case \"$arg\" in\n    status) exit 0 ;;\n    merge-base) printf 'base\\n'; exit 0 ;;\n    rev-list) printf 'not-a-number\\n'; exit 0 ;;\n  esac\ndone\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	findGitBinary = func() (string, error) { return fakeGit, nil }
	t.Cleanup(func() { findGitBinary = originalFindGitBinary })
	changed, err := NewWorktreeFileStore().HasGitChanges(t.TempDir())
	if err == nil || changed {
		t.Fatalf("HasGitChanges() = %v, %v", changed, err)
	}
}

func TestWorktreeFileStoreHasGitChangesReturnsErrorForRevListFailure(t *testing.T) {
	originalFindGitBinary := findGitBinary
	fakeGit := filepath.Join(t.TempDir(), "git")
	if err := os.WriteFile(fakeGit, []byte("#!/bin/sh\nfor arg in \"$@\"; do\n  case \"$arg\" in\n    status) exit 0 ;;\n    merge-base) printf 'base\\n'; exit 0 ;;\n    rev-list) exit 1 ;;\n  esac\ndone\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	findGitBinary = func() (string, error) { return fakeGit, nil }
	t.Cleanup(func() { findGitBinary = originalFindGitBinary })
	changed, err := NewWorktreeFileStore().HasGitChanges(t.TempDir())
	if err == nil || changed {
		t.Fatalf("HasGitChanges() = %v, %v", changed, err)
	}
}

func newGitTestRepository(t *testing.T, branch string) string {
	t.Helper()
	repo := t.TempDir()
	runGitForTest(t, repo, "init")
	runGitForTest(t, repo, "checkout", "-b", branch)
	runGitForTest(t, repo, "config", "user.email", "worktree-test@example.invalid")
	runGitForTest(t, repo, "config", "user.name", "Worktree Test")
	if err := os.WriteFile(filepath.Join(repo, "base.txt"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGitForTest(t, repo, "add", "base.txt")
	runGitForTest(t, repo, "commit", "-m", "base")
	return repo
}

func runGitForTest(t *testing.T, dir string, args ...string) {
	t.Helper()
	command := exec.Command("git", append([]string{"-C", dir}, args...)...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, output)
	}
}
