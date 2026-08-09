package store

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
)

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
