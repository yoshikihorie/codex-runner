package store

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func storeID(t *testing.T, slug string) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260806-120000-a1b2-" + slug)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func storeSnapshot(t *testing.T, id domain.TaskID, state domain.TaskState) domain.TaskSnapshot {
	t.Helper()
	at := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	requested, pid := 1860, 42
	started, event := at.Add(time.Second), at.Add(2*time.Second)
	code := domain.NewExitCode(1)
	reasoning := "high"
	session, err := domain.NewSessionRef("00112233-4455-6677-8899-aabbccddeeff", at, false)
	if err != nil {
		t.Fatal(err)
	}
	origin := domain.RecoveryOriginTimeout
	v := domain.TaskSnapshot{TaskID: id, Subcommand: domain.SubcommandImpl, PID: &pid, ProcessStartedAt: &started, ResolvedTimeoutSeconds: 1920, RequestedTimeoutSeconds: &requested, Model: "gpt-5", ReasoningEffort: &reasoning, RequestedAt: at, Route: domain.ExecutionRouteDaemon, State: state, StateUpdatedAt: at.Add(3 * time.Second), SessionRef: &session, LastEventAt: &event, ExitCode: &code, Recovered: state == domain.StateRecovered, AdoptedAfterRestart: true, RecoveryOrigin: &origin, SchemaVersion: 1}
	if state != domain.StateRecovered {
		v.RecoveryOrigin = nil
	}
	// Mirrors domain.TaskState.terminal() (internal/domain/state.go): only these
	// states may carry a non-nil ExitCode (see TaskSnapshot.Validate, tasksnapshot.go).
	terminal := state == domain.StateCompleted || state == domain.StateFailed || state == domain.StateRecovered || state == domain.StateTimeoutLost || state == domain.StateKilled || state == domain.StateLost
	if !terminal {
		v.ExitCode = nil
	}
	// Mirrors the `requires` map in TaskSnapshot.Validate (tasksnapshot.go): only
	// these states require PID/ProcessStartedAt to be set; PID and
	// ProcessStartedAt must be set (or nil) together.
	requiresPID := state == domain.StateRunning || state == domain.StateStalled || state == domain.StateTimeout || state == domain.StateRecovering || state == domain.StateRecovered || state == domain.StateTimeoutLost || state == domain.StateLost || state == domain.StateCompleted
	if !requiresPID {
		v.PID, v.ProcessStartedAt, v.LastEventAt = nil, nil, nil
	}
	return v
}
func newReservedStore(t *testing.T) (*FileTaskStore, domain.TaskID) {
	t.Helper()
	s, err := NewFileTaskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id := storeID(t, "store")
	if err := s.Reserve(id); err != nil {
		t.Fatal(err)
	}
	return s, id
}

func TestTaskStoreReserveAndRelease(t *testing.T) {
	s, id := newReservedStore(t)
	if err := s.Reserve(id); !errors.Is(err, os.ErrExist) {
		t.Fatalf("duplicate Reserve = %v", err)
	}
	if err := s.Release(id); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(id); !os.IsNotExist(err) {
		t.Fatalf("second Release = %v", err)
	}
}
func TestTaskStoreReleaseRejectsNonEmptyDirectory(t *testing.T) {
	s, id := newReservedStore(t)
	p, _ := newTaskPaths(s.root, id)
	if err := os.WriteFile(p.taskJSON(), []byte("{}"), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(id); err == nil {
		t.Fatal("Release removed non-empty directory")
	}
}
func TestTaskStoreSaveThenLoadPreservesAllFields(t *testing.T) {
	s, id := newReservedStore(t)
	v := storeSnapshot(t, id, domain.StateRecovered)
	if err := s.Save(id, v); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, v) {
		t.Fatalf("Load = %#v, want %#v", got, v)
	}
}
func TestTaskStoreSaveRejectsTaskIDMismatch(t *testing.T) {
	s, id := newReservedStore(t)
	if err := s.Save(id, storeSnapshot(t, storeID(t, "other"), domain.StateQueued)); err == nil {
		t.Fatal("mismatched TaskID accepted")
	}
}
func TestTaskStoreSaveRejectsInvalidSnapshot(t *testing.T) {
	s, id := newReservedStore(t)
	v := storeSnapshot(t, id, domain.StateQueued)
	v.Model = ""
	if err := s.Save(id, v); err == nil {
		t.Fatal("invalid snapshot accepted")
	}
}
func TestTaskStoreSaveAcceptsFailedWithoutPID(t *testing.T) {
	s, id := newReservedStore(t)
	v := storeSnapshot(t, id, domain.StateFailed)
	if err := s.Save(id, v); err != nil {
		t.Fatal(err)
	}
}
func TestTaskStoreLoadReturnsNotFound(t *testing.T) {
	s, err := NewFileTaskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Load(storeID(t, "missing")); !errors.Is(err, domain.ErrTaskNotFound) {
		t.Fatalf("Load = %v", err)
	}
}
func TestTaskStoreLoadRejectsInvalidSnapshot(t *testing.T) {
	s, id := newReservedStore(t)
	p, _ := newTaskPaths(s.root, id)
	if err := os.WriteFile(p.taskJSON(), []byte(`{"task_id":"`+id.String()+`"}`), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(id); err == nil {
		t.Fatal("invalid snapshot loaded")
	}
}
func TestTaskStoreLoadRejectsTaskIDMismatch(t *testing.T) {
	s, id := newReservedStore(t)
	p, _ := newTaskPaths(s.root, id)
	other := storeID(t, "other")
	b, err := json.Marshal(storeSnapshot(t, other, domain.StateQueued))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p.taskJSON(), b, taskFilePerm); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(id); err == nil {
		t.Fatal("mismatched TaskID loaded")
	}
}
func TestNewFileTaskStoreRecordsTaskIDMismatchAsCorrupted(t *testing.T) {
	root := t.TempDir()
	id, other := storeID(t, "mismatch"), storeID(t, "other")
	if err := os.Mkdir(filepath.Join(root, id.String()), taskDirPerm); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(storeSnapshot(t, other, domain.StateQueued))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, id.String(), "task.json"), b, taskFilePerm); err != nil {
		t.Fatal(err)
	}
	s, err := NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.CorruptedTaskIDs()) != 1 || s.CorruptedTaskIDs()[0] != id {
		t.Fatal("TaskID mismatch was not recorded as corruption")
	}
}
func TestTaskStoreListByStatesSkipsCorruptedSnapshot(t *testing.T) {
	root := t.TempDir()
	id := storeID(t, "corrupt")
	if err := os.Mkdir(filepath.Join(root, id.String()), taskDirPerm); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, id.String(), "task.json"), []byte("{"), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	s, err := NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.CorruptedTaskIDs()) != 1 || s.CorruptedTaskIDs()[0] != id {
		t.Fatal("corruption not recorded")
	}
	got, err := s.ListByStates([]domain.TaskState{domain.StateQueued})
	if err != nil || len(got) != 0 {
		t.Fatalf("list = %#v, %v", got, err)
	}
}
func TestTaskStoreListByStatesFiltersDeduplicatesAndSorts(t *testing.T) {
	s, id := newReservedStore(t)
	ids := []domain.TaskID{id, storeID(t, "list-b"), storeID(t, "list-c")}
	states := []domain.TaskState{domain.StateQueued, domain.StateRunning, domain.StateFailed}
	for i, v := range ids {
		if i > 0 {
			if err := s.Reserve(v); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Save(v, storeSnapshot(t, v, states[i])); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.ListByStates([]domain.TaskState{domain.StateRunning, domain.StateQueued, domain.StateRunning})
	if err != nil || len(got) != 2 || got[0].TaskID.String() > got[1].TaskID.String() {
		t.Fatalf("list = %#v, %v", got, err)
	}
	empty, err := s.ListByStates(nil)
	if err != nil || len(empty) != 0 {
		t.Fatal("empty states returned entries")
	}
}
func TestTaskStoreListByStatesReconstructsAfterRestart(t *testing.T) {
	s, id := newReservedStore(t)
	if err := s.Save(id, storeSnapshot(t, id, domain.StateQueued)); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewFileTaskStore(s.root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restarted.ListByStates([]domain.TaskState{domain.StateQueued})
	if err != nil || len(got) != 1 || got[0].TaskID != id {
		t.Fatalf("restart list = %#v, %v", got, err)
	}
}
func TestTaskStoreListByStatesReflectsConcurrentSave(t *testing.T) {
	s, id := newReservedStore(t)
	v := storeSnapshot(t, id, domain.StateQueued)
	done := make(chan error, 1)
	go func() { done <- s.Save(id, v) }()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	got, err := s.ListByStates([]domain.TaskState{domain.StateQueued})
	if err != nil || len(got) != 1 {
		t.Fatalf("list=%#v err=%v", got, err)
	}
}
func TestTaskStoreConcurrentSaveKeepsDiskAndIndexConsistent(t *testing.T) {
	s, id := newReservedStore(t)
	states := []domain.TaskState{domain.StateQueued, domain.StateStarting, domain.StateFailed}
	var wg sync.WaitGroup
	for _, state := range states {
		state := state
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Save(id, storeSnapshot(t, id, state)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	disk, err := s.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	listed, err := s.ListByStates(states)
	if err != nil || len(listed) != 1 || !reflect.DeepEqual(disk, listed[0]) {
		t.Fatalf("disk=%#v list=%#v err=%v", disk, listed, err)
	}
}
func TestTaskStoreSaveRejectsSymlinkedTaskDir(t *testing.T) {
	s, id := newReservedStore(t)
	p, _ := newTaskPaths(s.root, id)
	target := filepath.Join(s.root, "target")
	if err := os.Mkdir(target, taskDirPerm); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(p.dir()); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, p.dir()); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(id, storeSnapshot(t, id, domain.StateQueued)); err == nil {
		t.Fatal("Save followed symlink")
	}
	if _, err := os.Stat(filepath.Join(target, "task.json")); !os.IsNotExist(err) {
		t.Fatal("Save wrote through symlink")
	}
}
func TestNewFileTaskStoreSkipsSymlinkedTaskDir(t *testing.T) {
	root := t.TempDir()
	id := storeID(t, "symlink")
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, taskDirPerm); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(root, id.String())); err != nil {
		t.Fatal(err)
	}
	s, err := NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(s.CorruptedTaskIDs()) != 1 || s.CorruptedTaskIDs()[0] != id {
		t.Fatal("symlink was not recorded")
	}
}
func TestTaskStoreReserveCreatesDirWithMode0700(t *testing.T) {
	s, err := NewFileTaskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	id := storeID(t, "modes")
	if err := s.Reserve(id); err != nil {
		t.Fatal(err)
	}
	p, _ := newTaskPaths(s.root, id)
	f, err := openTaskDir(p.dir())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Mode().Perm() != taskDirPerm {
		t.Fatalf("mode=%o err=%v", info.Mode().Perm(), err)
	}
}
func TestTaskStoreSaveWritesTaskJSONWithMode0600(t *testing.T) {
	s, id := newReservedStore(t)
	if err := s.Save(id, storeSnapshot(t, id, domain.StateQueued)); err != nil {
		t.Fatal(err)
	}
	p, _ := newTaskPaths(s.root, id)
	f, err := os.Open(p.taskJSON())
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Mode().Perm() != taskFilePerm {
		t.Fatalf("mode=%o err=%v", info.Mode().Perm(), err)
	}
}
