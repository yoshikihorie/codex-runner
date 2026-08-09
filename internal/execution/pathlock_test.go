package execution

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type pathLockTestMutex struct {
	locked, unlocked   bool
	lockErr, unlockErr error
}

func (m *pathLockTestMutex) Lock() error   { m.locked = true; return m.lockErr }
func (m *pathLockTestMutex) Unlock() error { m.unlocked = true; return m.unlockErr }

type pathLockTestStore struct {
	snapshots                   []PathLockSnapshot
	saved                       bool
	savedPaths                  []domain.NormalizedPath
	deleted                     []domain.TaskID
	listErr, saveErr, deleteErr error
}

type pathLockTaskStateReaderFake struct {
	snapshot domain.TaskSnapshot
	err      error
}

func (f pathLockTaskStateReaderFake) Load(domain.TaskID) (domain.TaskSnapshot, error) {
	return f.snapshot, f.err
}

func (s *pathLockTestStore) List() ([]PathLockSnapshot, error) { return s.snapshots, s.listErr }
func (s *pathLockTestStore) Save(_ domain.TaskID, paths []domain.NormalizedPath) error {
	s.saved = true
	s.savedPaths = append([]domain.NormalizedPath(nil), paths...)
	return s.saveErr
}
func (s *pathLockTestStore) Delete(taskID domain.TaskID) error {
	s.deleted = append(s.deleted, taskID)
	return s.deleteErr
}

func TestAcquirePathLockUseCaseDisambiguatesMissingTaskLockWithTaskState(t *testing.T) {
	owner, err := domain.NewTaskID("impl-20260809-120000-a1b2-owner")
	if err != nil {
		t.Fatal(err)
	}
	requester, err := domain.NewTaskID("impl-20260809-120001-a1b2-requester")
	if err != nil {
		t.Fatal(err)
	}
	path, err := domain.NewNormalizedPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		state      domain.TaskState
		loadErr    error
		conflicted bool
	}{
		{name: "queued survives", state: domain.StateQueued, conflicted: true},
		{name: "starting survives", state: domain.StateStarting, conflicted: true},
		{name: "not found is stale", loadErr: domain.ErrTaskNotFound},
		{name: "running is stale", state: domain.StateRunning},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &pathLockTestStore{snapshots: []PathLockSnapshot{{TaskID: owner, OwnedPaths: []string{path.String()}}}}
			uc := NewAcquirePathLockUseCase(&pathLockTestMutex{}, store, domain.LivenessLockFunc(func(string) (bool, error) { return false, fs.ErrNotExist }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, pathLockTaskStateReaderFake{snapshot: domain.TaskSnapshot{State: tc.state}, err: tc.loadErr})
			out, acquireErr := uc.Execute(context.Background(), AcquirePathLockInput{TaskID: requester, RequestedPaths: []string{path.String()}})
			if acquireErr != nil || out.Acquired == tc.conflicted {
				t.Fatalf("Execute=(%+v,%v)", out, acquireErr)
			}
			if tc.conflicted && len(store.deleted) != 0 {
				t.Fatal("live path lock was deleted")
			}
			if !tc.conflicted && len(store.deleted) != 1 {
				t.Fatal("stale path lock was not deleted")
			}
		})
	}
}

func TestAcquirePathLockUseCaseFailsClosedWhenTaskStateReadFails(t *testing.T) {
	owner, _ := domain.NewTaskID("impl-20260809-120000-a1b2-owner")
	requester, _ := domain.NewTaskID("impl-20260809-120001-a1b2-requester")
	path, _ := domain.NewNormalizedPath(t.TempDir())
	store := &pathLockTestStore{snapshots: []PathLockSnapshot{{TaskID: owner, OwnedPaths: []string{path.String()}}}}
	uc := NewAcquirePathLockUseCase(&pathLockTestMutex{}, store, domain.LivenessLockFunc(func(string) (bool, error) { return false, fs.ErrNotExist }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, pathLockTaskStateReaderFake{err: errors.New("task store unavailable")})
	_, err := uc.Execute(context.Background(), AcquirePathLockInput{TaskID: requester, RequestedPaths: []string{path.String()}})
	var liveness *LivenessCheckError
	if !errors.Is(err, domain.ErrPathLockInfraFailure) || errors.As(err, &liveness) || len(store.deleted) != 0 || store.saved {
		t.Fatalf("Execute error=%v deleted=%v saved=%v", err, store.deleted, store.saved)
	}
}

func TestAcquirePathLockUseCaseConflictLogOmitsPath(t *testing.T) {
	owner, _ := domain.NewTaskID("impl-20260809-120000-a1b2-owner")
	requester, _ := domain.NewTaskID("impl-20260809-120001-a1b2-requester")
	path, _ := domain.NewNormalizedPath("/tmp/" + pathLockLogCanary + "/conflict")
	capture := &logCapture{}
	uc := NewAcquirePathLockUseCase(&pathLockTestMutex{}, &pathLockTestStore{snapshots: []PathLockSnapshot{{TaskID: owner, OwnedPaths: []string{path.String()}}}}, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, pathLockTaskStateReaderFake{}, slog.New(capture))
	out, err := uc.Execute(context.Background(), AcquirePathLockInput{TaskID: requester, RequestedPaths: []string{path.String()}})
	if err != nil || out.Acquired || out.ConflictingPath == nil || out.ConflictingPath.String() != path.String() {
		t.Fatalf("Execute=(%+v,%v)", out, err)
	}
	logs := capture.snapshot()
	if len(logs) != 1 || logs[0].attrs["conflicting_task_id"] != owner.String() {
		t.Fatalf("logs=%#v", logs)
	}
	if _, found := logs[0].attrs["conflicting_path"]; found {
		t.Fatal("conflict path was logged")
	}
	requireNoCanaryInLogs(t, logs)
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
	uc := NewAcquirePathLockUseCase(mutex, store, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), normalize, pathLockTaskStateReaderFake{})
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
	if _, err := uc.Acquire(second, []string{path.String()}); !errors.Is(err, domain.ErrPathLockConflict) {
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
		pathLockTaskStateReaderFake{err: domain.ErrTaskNotFound},
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
		pathLockTaskStateReaderFake{},
	)
	if _, err := uc.Execute(context.Background(), AcquirePathLockInput{}); err == nil {
		t.Fatal("Execute succeeded with an indeterminate liveness check")
	}
	if len(store.deleted) != 0 {
		t.Fatalf("deleted before all liveness checks succeeded: %v", store.deleted)
	}
}

func TestAcquirePathLockUseCaseAcquireReturnsTypedConflict(t *testing.T) {
	owner, err := domain.NewTaskID("impl-20260807-120000-a1b2-owner")
	if err != nil {
		t.Fatal(err)
	}
	requester, err := domain.NewTaskID("impl-20260807-120001-a1b2-requester")
	if err != nil {
		t.Fatal(err)
	}
	path, err := domain.NewNormalizedPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	uc := NewAcquirePathLockUseCase(&pathLockTestMutex{}, &pathLockTestStore{snapshots: []PathLockSnapshot{{TaskID: owner, OwnedPaths: []string{path.String()}}}}, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, pathLockTaskStateReaderFake{})
	_, err = uc.Acquire(requester, []string{path.String()})
	if !errors.Is(err, domain.ErrPathLockConflict) {
		t.Fatalf("errors.Is conflict = false: %v", err)
	}
	var conflict *PathLockConflictError
	if !errors.As(err, &conflict) || conflict.TaskID != owner || conflict.Path != path {
		t.Fatalf("conflict=%#v", conflict)
	}
}

func TestAcquirePathLockUseCaseAcquireReturnsTypedLivenessError(t *testing.T) {
	owner, err := domain.NewTaskID("impl-20260807-120000-a1b2-owner")
	if err != nil {
		t.Fatal(err)
	}
	requester, err := domain.NewTaskID("impl-20260807-120001-a1b2-requester")
	if err != nil {
		t.Fatal(err)
	}
	path, err := domain.NewNormalizedPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	original := errors.New("liveness failure")
	store := &pathLockTestStore{snapshots: []PathLockSnapshot{{TaskID: owner, OwnedPaths: []string{path.String()}}}}
	uc := NewAcquirePathLockUseCase(&pathLockTestMutex{}, store, domain.LivenessLockFunc(func(string) (bool, error) { return false, original }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, pathLockTaskStateReaderFake{})
	_, err = uc.Acquire(requester, []string{path.String()})
	var liveness *LivenessCheckError
	if !errors.As(err, &liveness) || liveness.TaskID != owner || !errors.Is(err, original) || errors.Is(err, domain.ErrPathLockInfraFailure) {
		t.Fatalf("liveness error=%v", err)
	}
	if len(store.deleted) != 0 {
		t.Fatalf("liveness failure deleted ownership: %v", store.deleted)
	}
}

func TestAcquirePathLockUseCaseInfrastructureFailuresAreSentinelErrors(t *testing.T) {
	taskID, err := domain.NewTaskID("impl-20260807-120000-a1b2-requester")
	if err != nil {
		t.Fatal(err)
	}
	path, err := domain.NewNormalizedPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storedPath := t.TempDir()
	failure := errors.New("infrastructure failure")
	cases := []struct {
		name      string
		mutex     *pathLockTestMutex
		store     *pathLockTestStore
		normalize normalizePathFunc
		dead      bool
	}{
		{"lock", &pathLockTestMutex{lockErr: failure}, &pathLockTestStore{}, func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, false},
		{"list", &pathLockTestMutex{}, &pathLockTestStore{listErr: failure}, func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, false},
		{"delete", &pathLockTestMutex{}, &pathLockTestStore{snapshots: []PathLockSnapshot{{TaskID: taskID, OwnedPaths: []string{path.String()}}}, deleteErr: failure}, func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, true},
		{"normalize requested", &pathLockTestMutex{}, &pathLockTestStore{}, func(string, bool) (domain.NormalizedPath, error) { return domain.NormalizedPath{}, failure }, false},
		{"normalize stored", &pathLockTestMutex{}, &pathLockTestStore{snapshots: []PathLockSnapshot{{TaskID: taskID, OwnedPaths: []string{storedPath}}}}, func(raw string, _ bool) (domain.NormalizedPath, error) {
			if raw == storedPath {
				return domain.NormalizedPath{}, failure
			}
			return domain.NewNormalizedPath(raw)
		}, false},
		{"save", &pathLockTestMutex{}, &pathLockTestStore{saveErr: failure}, func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			uc := NewAcquirePathLockUseCase(tc.mutex, tc.store, domain.LivenessLockFunc(func(string) (bool, error) { return tc.dead, nil }), tc.normalize, pathLockTaskStateReaderFake{})
			paths, acquireErr := uc.Acquire(taskID, []string{path.String()})
			if !errors.Is(acquireErr, domain.ErrPathLockInfraFailure) || paths != nil {
				t.Fatalf("Acquire=(%v,%v)", paths, acquireErr)
			}
		})
	}
}

func TestAcquirePathLockUseCaseKeepsSuccessAfterUnlockFailure(t *testing.T) {
	taskID, err := domain.NewTaskID("impl-20260807-120000-a1b2-unlock")
	if err != nil {
		t.Fatal(err)
	}
	path, err := domain.NewNormalizedPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mutex := &pathLockTestMutex{unlockErr: errors.New("unlock failure")}
	store := &pathLockTestStore{}
	uc := NewAcquirePathLockUseCase(mutex, store, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, pathLockTaskStateReaderFake{})
	paths, err := uc.Acquire(taskID, []string{path.String()})
	if err != nil || !mutex.unlocked || !store.saved || !reflect.DeepEqual(paths, store.savedPaths) {
		t.Fatalf("Acquire=(%v,%v), saved=%v", paths, err, store.savedPaths)
	}
}

// This is a unit contract: FileMutex cannot deterministically inject Unlock failure externally.
func TestAcquirePathLockUseCaseKeepsEmptySuccessAfterUnlockFailure(t *testing.T) {
	taskID, err := domain.NewTaskID("impl-20260809-120000-a1b2-empty-unlock")
	if err != nil {
		t.Fatal(err)
	}
	mutex := &pathLockTestMutex{unlockErr: errors.New("unlock failure")}
	pathStore := &pathLockTestStore{}
	capture := &logCapture{}
	uc := NewAcquirePathLockUseCase(mutex, pathStore, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, pathLockTaskStateReaderFake{}, slog.New(capture))
	paths, err := uc.Acquire(taskID, nil)
	if err != nil || len(paths) != 0 || pathStore.saved || len(pathStore.deleted) != 0 || !mutex.unlocked {
		t.Fatal("empty acquisition did not retain success after unlock failure")
	}
	logs := capture.snapshot()
	if len(logs) != 1 || logs[0].level != slog.LevelError || logs[0].attrs["task_id"] != taskID.String() || logs[0].attrs["stage"] != "Unlock" {
		t.Fatal("expected one structured unlock warning log")
	}
}

const pathLockLogCanary = "CANARY-SECRET-VALUE-DO-NOT-LOG"

func TestAcquirePathLockUseCaseLogsFailureStages(t *testing.T) {
	requester, err := domain.NewTaskID("impl-20260809-120000-a1b2-log-requester")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := domain.NewTaskID("impl-20260809-120001-a1b2-log-owner")
	if err != nil {
		t.Fatal(err)
	}
	requestedPath := "/tmp/" + pathLockLogCanary + "/requested"
	storedPath := "/tmp/" + pathLockLogCanary + "/stored"
	rawFailure := errors.New("injected failure " + pathLockLogCanary)
	for _, tc := range []struct {
		name  string
		stage string
		build func() (*pathLockTestMutex, *pathLockTestStore, domain.LivenessLock, normalizePathFunc)
	}{
		{"Lock", "Lock", func() (*pathLockTestMutex, *pathLockTestStore, domain.LivenessLock, normalizePathFunc) {
			return &pathLockTestMutex{lockErr: rawFailure}, &pathLockTestStore{}, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }
		}},
		{"List", "List", func() (*pathLockTestMutex, *pathLockTestStore, domain.LivenessLock, normalizePathFunc) {
			return &pathLockTestMutex{}, &pathLockTestStore{listErr: rawFailure}, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }
		}},
		{"Delete", "Delete", func() (*pathLockTestMutex, *pathLockTestStore, domain.LivenessLock, normalizePathFunc) {
			return &pathLockTestMutex{}, &pathLockTestStore{snapshots: []PathLockSnapshot{{TaskID: owner, OwnedPaths: []string{storedPath}}}, deleteErr: rawFailure}, domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }
		}},
		{"normalize-requested", "normalize-requested", func() (*pathLockTestMutex, *pathLockTestStore, domain.LivenessLock, normalizePathFunc) {
			return &pathLockTestMutex{}, &pathLockTestStore{}, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(string, bool) (domain.NormalizedPath, error) { return domain.NormalizedPath{}, rawFailure }
		}},
		{"normalize-stored", "normalize-stored", func() (*pathLockTestMutex, *pathLockTestStore, domain.LivenessLock, normalizePathFunc) {
			return &pathLockTestMutex{}, &pathLockTestStore{snapshots: []PathLockSnapshot{{TaskID: owner, OwnedPaths: []string{storedPath}}}}, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(raw string, _ bool) (domain.NormalizedPath, error) {
				if raw == storedPath {
					return domain.NormalizedPath{}, rawFailure
				}
				return domain.NewNormalizedPath(raw)
			}
		}},
		{"Save", "Save", func() (*pathLockTestMutex, *pathLockTestStore, domain.LivenessLock, normalizePathFunc) {
			return &pathLockTestMutex{}, &pathLockTestStore{saveErr: rawFailure}, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mutex, pathStore, liveness, normalize := tc.build()
			capture := &logCapture{}
			uc := NewAcquirePathLockUseCase(mutex, pathStore, liveness, normalize, pathLockTaskStateReaderFake{}, slog.New(capture))
			_, acquireErr := uc.Acquire(requester, []string{requestedPath})
			if !errors.Is(acquireErr, domain.ErrPathLockInfraFailure) {
				t.Fatal("expected path-lock infrastructure failure")
			}
			requireSafeStageLog(t, capture.snapshot(), requester, tc.stage)
		})
	}

	t.Run("liveness", func(t *testing.T) {
		capture := &logCapture{}
		pathStore := &pathLockTestStore{snapshots: []PathLockSnapshot{{TaskID: owner, OwnedPaths: []string{storedPath}}}}
		uc := NewAcquirePathLockUseCase(&pathLockTestMutex{}, pathStore, domain.LivenessLockFunc(func(string) (bool, error) { return false, rawFailure }), func(raw string, _ bool) (domain.NormalizedPath, error) { return domain.NewNormalizedPath(raw) }, pathLockTaskStateReaderFake{}, slog.New(capture))
		_, acquireErr := uc.Acquire(requester, []string{requestedPath})
		var liveness *LivenessCheckError
		if !errors.As(acquireErr, &liveness) || liveness.TaskID != owner || len(pathStore.deleted) != 0 {
			t.Fatal("expected typed liveness error without ownership deletion")
		}
		logs := capture.snapshot()
		if len(logs) == 0 {
			t.Fatal("expected liveness failure log")
		}
		foundOwner, foundOperation := false, false
		for _, record := range logs {
			if record.level != slog.LevelError || record.attrs["task_id"] != requester.String() {
				continue
			}
			for key, value := range record.attrs {
				if value == owner.String() {
					foundOwner = true
				}
				if key == "operation" && fmt.Sprint(value) != "" {
					foundOperation = true
				}
			}
		}
		if !foundOwner || !foundOperation {
			t.Fatal("liveness log did not identify request, owner, and operation")
		}
		requireNoCanaryInLogs(t, logs)
	})
}

func requireSafeStageLog(t *testing.T, logs []capturedLog, taskID domain.TaskID, stage string) {
	t.Helper()
	if len(logs) == 0 {
		t.Fatal("expected structured error log")
	}
	found := false
	for _, record := range logs {
		if record.level == slog.LevelError && record.attrs["stage"] == stage && record.attrs["task_id"] == taskID.String() {
			if _, ok := record.attrs["error"]; !ok {
				t.Fatal("expected error attribute")
			}
			found = true
		}
	}
	if !found {
		t.Fatal("expected stage log was not found")
	}
	requireNoCanaryInLogs(t, logs)
}

func requireNoCanaryInLogs(t *testing.T, logs []capturedLog) {
	t.Helper()
	for index, record := range logs {
		if containsPathLockCanary(record.msg) {
			t.Fatalf("canary leaked in log message at record %d", index)
		}
		for key, value := range record.attrs {
			if containsPathLockCanary(fmt.Sprint(value)) {
				t.Fatalf("canary leaked in log attribute %q at record %d", key, index)
			}
		}
	}
}

func containsPathLockCanary(value string) bool {
	return strings.Contains(value, pathLockLogCanary)
}
