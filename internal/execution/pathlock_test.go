package execution

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type pathLockTestMutex struct{ locked, unlocked bool }

func (m *pathLockTestMutex) Lock() error   { m.locked = true; return nil }
func (m *pathLockTestMutex) Unlock() error { m.unlocked = true; return nil }

type pathLockTestStore struct {
	snapshots []PathLockSnapshot
	saved     bool
	deleted   []domain.TaskID
}

func (s *pathLockTestStore) List() ([]PathLockSnapshot, error) { return s.snapshots, nil }
func (s *pathLockTestStore) Save(domain.TaskID, []domain.NormalizedPath) error {
	s.saved = true
	return nil
}
func (s *pathLockTestStore) Delete(taskID domain.TaskID) error {
	s.deleted = append(s.deleted, taskID)
	return nil
}

func TestAcquirePathLockUseCaseConflictAndEmptyRequest(t *testing.T) {
	first, err := domain.NewTaskID("impl-20260807-120000-a1b2-first")
	if err != nil {
		t.Fatal(err)
	}
	second, err := domain.NewTaskID("impl-20260807-120001-a1b2-second")
	if err != nil {
		t.Fatal(err)
	}
	path, err := domain.NewNormalizedPath("/tmp/path-lock-execution")
	if err != nil {
		t.Fatal(err)
	}
	mutex := &pathLockTestMutex{}
	store := &pathLockTestStore{snapshots: []PathLockSnapshot{{TaskID: first, OwnedPaths: []string{path.String()}}}}
	normalize := func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }
	uc := NewAcquirePathLockUseCase(mutex, store, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), normalize)
	out, err := uc.Execute(context.Background(), AcquirePathLockInput{TaskID: second, RequestedPaths: []string{path.String()}})
	if err != nil || out.Acquired || out.ConflictingTaskID == nil {
		t.Fatalf("conflict result = (%+v, %v)", out, err)
	}
	if !mutex.locked || !mutex.unlocked || store.saved {
		t.Fatal("conflict did not retain the expected mutex/store behavior")
	}
	out, err = uc.Execute(context.Background(), AcquirePathLockInput{TaskID: second})
	if err != nil || !out.Acquired || store.saved {
		t.Fatalf("empty result = (%+v, %v)", out, err)
	}
	if err := uc.Acquire(second, []string{path.String()}); !errors.Is(err, domain.ErrPathLockConflict) {
		t.Fatalf("Acquire error = %v", err)
	}
}

func TestAcquirePathLockUseCaseRepairsOwnerWhenTaskLockIsMissing(t *testing.T) {
	staleTaskID, err := domain.NewTaskID("impl-20260807-120000-a1b2-stale-lock")
	if err != nil {
		t.Fatal(err)
	}
	newTaskID, err := domain.NewTaskID("impl-20260807-120001-a1b2-new-lock")
	if err != nil {
		t.Fatal(err)
	}
	stalePath, err := domain.NewNormalizedPath("/tmp/path-lock-stale")
	if err != nil {
		t.Fatal(err)
	}
	newPath, err := domain.NewNormalizedPath("/tmp/path-lock-new")
	if err != nil {
		t.Fatal(err)
	}
	storeDir := t.TempDir() + "/path-locks"
	pathStore := store.NewPathLockFileStore(storeDir)
	if err := pathStore.Save(staleTaskID, []domain.NormalizedPath{stalePath}); err != nil {
		t.Fatal(err)
	}

	lockPath := taskLockPath(staleTaskID)
	if err := os.MkdirAll("/tmp/codex-tasks/"+staleTaskID.String(), 0o700); err != nil {
		t.Fatal(err)
	}
	lockFile, err := os.Create(lockPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := lockFile.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(lockPath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll("/tmp/codex-tasks/" + staleTaskID.String()) })

	normalize := func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }
	uc := NewAcquirePathLockUseCase(
		&pathLockTestMutex{},
		pathStore,
		domain.LivenessLockFunc(func(string) (bool, error) { return false, fs.ErrNotExist }),
		normalize,
	)
	out, err := uc.Execute(context.Background(), AcquirePathLockInput{TaskID: newTaskID, RequestedPaths: []string{newPath.String()}})
	if err != nil || !out.Acquired {
		t.Fatalf("Execute = (%+v, %v)", out, err)
	}
	locks, err := pathStore.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(locks) != 1 || locks[0].TaskID != newTaskID {
		t.Fatalf("locks after repair = %+v", locks)
	}
}

func TestAcquirePathLockUseCaseDoesNotRepairBeforeAllLivenessChecksSucceed(t *testing.T) {
	staleTaskID, err := domain.NewTaskID("impl-20260807-120000-a1b2-stale-lock")
	if err != nil {
		t.Fatal(err)
	}
	unknownTaskID, err := domain.NewTaskID("impl-20260807-120001-a1b2-unknown-lock")
	if err != nil {
		t.Fatal(err)
	}
	path, err := domain.NewNormalizedPath("/tmp/path-lock-execution")
	if err != nil {
		t.Fatal(err)
	}
	store := &pathLockTestStore{snapshots: []PathLockSnapshot{
		{TaskID: staleTaskID, OwnedPaths: []string{path.String()}},
		{TaskID: unknownTaskID, OwnedPaths: []string{path.String()}},
	}}
	checks := 0
	uc := NewAcquirePathLockUseCase(
		&pathLockTestMutex{},
		store,
		domain.LivenessLockFunc(func(string) (bool, error) {
			checks++
			if checks == 1 {
				return true, nil
			}
			return false, errors.New("liveness I/O failure")
		}),
		func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) },
	)
	if _, err := uc.Execute(context.Background(), AcquirePathLockInput{}); err == nil {
		t.Fatal("Execute succeeded with an indeterminate liveness check")
	}
	if len(store.deleted) != 0 {
		t.Fatalf("deleted before all liveness checks succeeded: %v", store.deleted)
	}
}
