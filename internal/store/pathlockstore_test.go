package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestPathLockFileStoreMissingDirectorySaveListAndDelete(t *testing.T) {
	store := NewPathLockFileStore(t.TempDir() + "/path-locks")
	locks, err := store.List()
	if err != nil || len(locks) != 0 {
		t.Fatalf("initial List = (%v, %v)", locks, err)
	}
	taskID, err := domain.NewTaskID("impl-20260807-120000-a1b2-path-lock")
	if err != nil {
		t.Fatal(err)
	}
	path, err := domain.NewNormalizedPath("/tmp/path-lock-store")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Save(taskID, []domain.NormalizedPath{path}); err != nil {
		t.Fatal(err)
	}
	locks, err = store.List()
	if err != nil || len(locks) != 1 || locks[0].OwnedPaths[0] != path.String() {
		t.Fatalf("List = (%v, %v)", locks, err)
	}
	if err := store.Delete(taskID); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(taskID); err != nil {
		t.Fatal(err)
	}
}

func TestPathLockFileStoreListRejectsNullAndSymlink(t *testing.T) {
	taskID, err := domain.NewTaskID("impl-20260807-120000-a1b2-path-lock")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("null", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, taskID.String()+".json"), []byte("null"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewPathLockFileStore(dir).List(); err == nil {
			t.Fatal("List accepted null path lock")
		}
	})
	t.Run("symlink", func(t *testing.T) {
		dir := t.TempDir()
		target := filepath.Join(t.TempDir(), "target.json")
		if err := os.WriteFile(target, []byte("[]"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, taskID.String()+".json")); err != nil {
			t.Fatal(err)
		}
		if _, err := NewPathLockFileStore(dir).List(); err == nil {
			t.Fatal("List followed path lock symlink")
		}
	})
}

func TestPathLockFileStoreSaveRejectsSymlinkDirectory(t *testing.T) {
	taskID, err := domain.NewTaskID("impl-20260807-120000-a1b2-path-lock")
	if err != nil {
		t.Fatal(err)
	}
	path, err := domain.NewNormalizedPath("/tmp/path-lock-store")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	link := filepath.Join(root, "path-locks")
	if err := os.Symlink(t.TempDir(), link); err != nil {
		t.Fatal(err)
	}
	if err := NewPathLockFileStore(link).Save(taskID, []domain.NormalizedPath{path}); err == nil {
		t.Fatal("Save wrote through path locks directory symlink")
	}
}
