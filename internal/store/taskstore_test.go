package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
	v := domain.TaskSnapshot{TaskID: id, Subcommand: domain.SubcommandImpl, PID: &pid, ProcessStartedAt: &started, ResolvedTimeoutSeconds: 1920, RequestedTimeoutSeconds: &requested, Model: "gpt-5", ReasoningEffort: &reasoning, SandboxMode: "workspace-write", RequestedAt: at, Route: domain.ExecutionRouteDaemon, State: state, StateUpdatedAt: at.Add(3 * time.Second), SessionRef: &session, LastEventAt: &event, ExitCode: &code, Recovered: state == domain.StateRecovered, AdoptedAfterRestart: true, RecoveryOrigin: &origin, SchemaVersion: 2}
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

func TestTaskStoreIsReserved_ReturnsTrueForReservedDirectory(t *testing.T) {
	s, id := newReservedStore(t)
	reserved, err := s.IsReserved(id)
	if err != nil || !reserved {
		t.Fatalf("reserved=%t err=%v", reserved, err)
	}
}

func TestTaskStoreIsReserved_ReturnsFalseOnlyForMissingDirectory(t *testing.T) {
	s, err := NewFileTaskStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	reserved, err := s.IsReserved(storeID(t, "missing-reservation"))
	if err != nil || reserved {
		t.Fatalf("reserved=%t err=%v", reserved, err)
	}
}

func TestTaskStoreIsReserved_RejectsRegularFileAndSymlink(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func(*testing.T, string)
	}{
		{"regular file", func(t *testing.T, path string) {
			if err := os.WriteFile(path, []byte("not a directory"), taskFilePerm); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink", func(t *testing.T, path string) {
			target := filepath.Join(filepath.Dir(path), "target")
			if err := os.Mkdir(target, taskDirPerm); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := NewFileTaskStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			id := storeID(t, "reserved-"+strings.ReplaceAll(tc.name, " ", "-"))
			p, err := newTaskPaths(s.root, id)
			if err != nil {
				t.Fatal(err)
			}
			tc.make(t, p.dir())
			reserved, err := s.IsReserved(id)
			if err == nil || reserved {
				t.Fatalf("reserved=%t err=%v", reserved, err)
			}
		})
	}
}
func TestTaskStoreReleaseRemovesNonEmptyReservationAndIndex(t *testing.T) {
	s, id := newReservedStore(t)
	p, _ := newTaskPaths(s.root, id)
	if err := os.WriteFile(p.taskJSON(), []byte("{}"), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	s.index[id.String()] = storeSnapshot(t, id, domain.StateQueued)
	if err := s.Release(id); err != nil {
		t.Fatalf("Release non-empty reservation: %v", err)
	}
	if _, err := os.Stat(p.dir()); !os.IsNotExist(err) {
		t.Fatalf("reservation directory still exists: %v", err)
	}
	if _, ok := s.index[id.String()]; ok {
		t.Fatal("Release retained reservation index entry")
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
	} else if errors.Is(err, domain.ErrContractWriteFailed) {
		t.Fatalf("validation error classified as contract write failure: %v", err)
	}
}
func TestTaskStoreSaveMarshalFailureIsNotContractWriteFailure(t *testing.T) {
	s, id := newReservedStore(t)
	v := storeSnapshot(t, id, domain.StateQueued)
	v.RequestedAt = time.Date(10000, time.January, 1, 0, 0, 0, 0, time.UTC)
	if err := v.Validate(); err != nil {
		t.Fatalf("test snapshot must pass validation: %v", err)
	}
	err := s.Save(id, v)
	var marshalErr *json.MarshalerError
	if err == nil || !errors.As(err, &marshalErr) {
		t.Fatalf("Save error = %v, want JSON marshal error", err)
	}
	if errors.Is(err, domain.ErrContractWriteFailed) {
		t.Fatalf("marshal error classified as contract write failure: %v", err)
	}
}
func TestTaskStoreSaveWriteAtomicFailureRetainsSentinelAndCause(t *testing.T) {
	s, id := newReservedStore(t)
	p, err := newTaskPaths(s.root, id)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p.taskJSON(), taskDirPerm); err != nil {
		t.Fatal(err)
	}
	err = s.Save(id, storeSnapshot(t, id, domain.StateQueued))
	if !errors.Is(err, domain.ErrContractWriteFailed) {
		t.Fatalf("Save error = %v, want ErrContractWriteFailed", err)
	}
	var linkErr *os.LinkError
	if !errors.As(err, &linkErr) || linkErr.Op != "rename" {
		t.Fatalf("Save error = %v, want underlying rename error", err)
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
func TestTaskStoreLoadReturnsNotFoundWhenTaskJSONMissing(t *testing.T) {
	s, id := newReservedStore(t)
	if _, err := s.Load(id); !errors.Is(err, domain.ErrTaskNotFound) {
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

func TestTaskStoreKeepsAdoptionStateIndexedWhenAnotherSnapshotIsCorrupt_SCNDaemon0139(t *testing.T) {
	root := t.TempDir()
	validID := storeID(t, "scn39-valid")
	corruptID := storeID(t, "scn39-corrupt")
	for _, id := range []domain.TaskID{validID, corruptID} {
		if err := os.Mkdir(filepath.Join(root, id.String()), taskDirPerm); err != nil {
			t.Fatal(err)
		}
	}
	validBody, err := json.Marshal(storeSnapshot(t, validID, domain.StateStarting))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, validID.String(), "task.json"), validBody, taskFilePerm); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, corruptID.String(), "task.json"), []byte("{"), taskFilePerm); err != nil {
		t.Fatal(err)
	}

	s, err := NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := s.CorruptedTaskIDs()
	if len(corrupted) != 1 || corrupted[0] != corruptID {
		t.Fatalf("CorruptedTaskIDs() = %v, want [%s]", corrupted, corruptID)
	}
	got, err := s.ListByStates([]domain.TaskState{domain.StateStarting})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].TaskID != validID || got[0].State != domain.StateStarting {
		t.Fatalf("ListByStates(starting) = %#v, want only %s", got, validID)
	}
	if got[0].TaskID == corruptID {
		t.Fatalf("corrupt task %s remained indexed", corruptID)
	}
}

func TestNewFileTaskStoreDoesNotLogCorruptedSnapshots(t *testing.T) {
	root := t.TempDir()
	id := storeID(t, "warn-corrupted")
	if err := os.Mkdir(filepath.Join(root, id.String()), taskDirPerm); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, id.String(), "task.json"), []byte("{"), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	if _, err := NewFileTaskStore(root); err != nil {
		t.Fatal(err)
	}
	if output.Len() != 0 {
		t.Fatalf("store logged during startup: %q", output.String())
	}
}

func TestNewFileTaskStoreClassifiesStartupCorruption(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, string, domain.TaskID)
	}{
		{name: "task directory open non-ENOENT", setup: func(t *testing.T, root string, id domain.TaskID) {
			if err := os.WriteFile(filepath.Join(root, id.String()), []byte("not a directory"), taskFilePerm); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "task directory open EACCES", setup: func(t *testing.T, root string, id domain.TaskID) {
			dir := filepath.Join(root, id.String())
			if err := os.Mkdir(dir, taskDirPerm); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(dir, 0); err != nil {
				t.Fatal(err)
			}
			// Cleanup runs in LIFO order, so restore permission before t.TempDir removes the tree.
			t.Cleanup(func() {
				if err := os.Chmod(dir, taskDirPerm); err != nil {
					t.Errorf("restore task directory permission: %v", err)
				}
			})
			probe, err := os.Open(dir)
			if err == nil {
				probe.Close()
				t.Skip("permission 0000 does not prevent opening the task directory in this environment")
			}
			if !errors.Is(err, fs.ErrPermission) {
				t.Fatalf("open permission probe = %v, want EACCES", err)
			}
		}},
		{name: "task json IO non-ENOENT", setup: func(t *testing.T, root string, id domain.TaskID) {
			dir := filepath.Join(root, id.String())
			if err := os.Mkdir(dir, taskDirPerm); err != nil {
				t.Fatal(err)
			}
			target := filepath.Join(root, "task-json-symlink-target")
			if err := os.WriteFile(target, []byte("not opened through symlink"), taskFilePerm); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, filepath.Join(dir, "task.json")); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "JSON decode", setup: func(t *testing.T, root string, id domain.TaskID) {
			writeStartupSnapshot(t, root, id, []byte("{"))
		}},
		{name: "snapshot validation", setup: func(t *testing.T, root string, id domain.TaskID) {
			snapshot := storeSnapshot(t, id, domain.StateQueued)
			snapshot.Model = ""
			body, err := json.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			writeStartupSnapshot(t, root, id, body)
		}},
		{name: "task ID mismatch", setup: func(t *testing.T, root string, id domain.TaskID) {
			body, err := json.Marshal(storeSnapshot(t, storeID(t, "table-other"), domain.StateQueued))
			if err != nil {
				t.Fatal(err)
			}
			writeStartupSnapshot(t, root, id, body)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			corruptedID := storeID(t, "table-"+strings.ToLower(strings.ReplaceAll(tc.name, " ", "-")))
			validID := storeID(t, "table-valid")
			validBody, err := json.Marshal(storeSnapshot(t, validID, domain.StateQueued))
			if err != nil {
				t.Fatal(err)
			}
			writeStartupSnapshot(t, root, validID, validBody)
			tc.setup(t, root, corruptedID)
			var output bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })

			store, err := NewFileTaskStore(root)
			if err != nil {
				t.Fatal(err)
			}
			corrupted := store.CorruptedTaskIDs()
			if len(corrupted) != 1 || corrupted[0] != corruptedID {
				t.Fatalf("CorruptedTaskIDs() = %#v, want [%s]", corrupted, corruptedID.String())
			}
			listed, err := store.ListByStates([]domain.TaskState{domain.StateQueued})
			if err != nil || len(listed) != 1 || listed[0].TaskID != validID {
				t.Fatalf("ListByStates() = %#v, %v", listed, err)
			}
			if output.Len() != 0 {
				t.Fatalf("store logged during startup: %q", output.String())
			}
		})
	}
}

func TestNewFileTaskStoreSkipsStartupENOENT(t *testing.T) {
	for _, tc := range []struct {
		name      string
		slug      string
		construct func(*testing.T, string, domain.TaskID) (*FileTaskStore, error)
	}{
		{name: "queued reservation without task json", slug: "enoent-queued", construct: func(t *testing.T, root string, id domain.TaskID) (*FileTaskStore, error) {
			if err := os.Mkdir(filepath.Join(root, id.String()), taskDirPerm); err != nil {
				t.Fatal(err)
			}
			return NewFileTaskStore(root)
		}},
		{name: "task directory removed after ReadDir", slug: "enoent-removed", construct: func(t *testing.T, root string, id domain.TaskID) (*FileTaskStore, error) {
			taskDir := filepath.Join(root, id.String())
			if err := os.Mkdir(taskDir, taskDirPerm); err != nil {
				t.Fatal(err)
			}
			return newFileTaskStore(root, func(path string) ([]os.DirEntry, error) {
				entries, err := os.ReadDir(path)
				if err != nil {
					return nil, err
				}
				if err := os.Remove(taskDir); err != nil {
					t.Fatal(err)
				}
				return entries, nil
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			id := storeID(t, tc.slug)
			validID := storeID(t, tc.slug+"-valid")
			validBody, err := json.Marshal(storeSnapshot(t, validID, domain.StateQueued))
			if err != nil {
				t.Fatal(err)
			}
			writeStartupSnapshot(t, root, validID, validBody)
			var output bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
			t.Cleanup(func() { slog.SetDefault(previous) })

			store, err := tc.construct(t, root, id)
			if err != nil {
				t.Fatal(err)
			}
			corrupted := store.CorruptedTaskIDs()
			for _, corruptedID := range corrupted {
				if corruptedID == id {
					t.Fatalf("CorruptedTaskIDs() contains %s", id.String())
				}
			}
			if len(corrupted) != 0 {
				t.Fatalf("CorruptedTaskIDs() = %#v, want empty", corrupted)
			}
			if len(store.index) != 1 {
				t.Fatalf("index = %#v, want only %s", store.index, validID.String())
			}
			listed, err := store.ListByStates([]domain.TaskState{domain.StateQueued})
			if err != nil || len(listed) != 1 || listed[0].TaskID != validID {
				t.Fatalf("ListByStates() = %#v, %v", listed, err)
			}
			if output.Len() != 0 {
				t.Fatalf("store logged during startup: %q", output.String())
			}
		})
	}
}

func TestNewFileTaskStoreSkipsWrappedSnapshotENOENT(t *testing.T) {
	root := t.TempDir()
	id := storeID(t, "wrapped-enoent")
	if err := os.Mkdir(filepath.Join(root, id.String()), taskDirPerm); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&output, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	store, err := newFileTaskStoreWithSnapshotReader(root, os.ReadDir, func(string) (domain.TaskSnapshot, error) {
		return domain.TaskSnapshot{}, fmt.Errorf("wrapped: %w", fs.ErrNotExist)
	})
	if err != nil {
		t.Fatal(err)
	}
	if corrupted := store.CorruptedTaskIDs(); len(corrupted) != 0 {
		t.Fatalf("CorruptedTaskIDs() = %#v, want empty", corrupted)
	}
	if len(store.index) != 0 {
		t.Fatalf("index = %#v, want empty", store.index)
	}
	if output.Len() != 0 {
		t.Fatalf("store logged during startup: %q", output.String())
	}
}

func TestNewFileTaskStoreStartupDiagnosticsBoundaries(t *testing.T) {
	root := t.TempDir()
	id := storeID(t, "diagnostic-boundaries")
	writeStartupSnapshot(t, root, id, []byte("{"))
	if err := os.Mkdir(filepath.Join(root, "not-a-task-id"), taskDirPerm); err != nil {
		t.Fatal(err)
	}

	store, err := NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := store.CorruptedTaskIDs()
	if len(corrupted) != 1 || corrupted[0] != id {
		t.Fatalf("CorruptedTaskIDs() = %#v", corrupted)
	}
	corrupted[0] = storeID(t, "mutated-copy")
	if got := store.CorruptedTaskIDs(); len(got) != 1 || got[0] != id {
		t.Fatalf("CorruptedTaskIDs() exposed internal storage: %#v", got)
	}

	validBody, err := json.Marshal(storeSnapshot(t, id, domain.StateQueued))
	if err != nil {
		t.Fatal(err)
	}
	writeStartupSnapshot(t, root, id, validBody)
	if got := store.CorruptedTaskIDs(); len(got) != 1 || got[0] != id {
		t.Fatalf("same-generation corruption changed after repair: %#v", got)
	}
	if listed, err := store.ListByStates([]domain.TaskState{domain.StateQueued}); err != nil || len(listed) != 0 {
		t.Fatalf("same-generation index changed after repair: %#v, %v", listed, err)
	}
	restarted, err := NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.CorruptedTaskIDs(); len(got) != 0 {
		t.Fatalf("restart retained repaired corruption: %#v", got)
	}
}

func TestNewFileTaskStoreReturnsRootReadDirError(t *testing.T) {
	root := filepath.Join(t.TempDir(), "regular-file")
	if err := os.WriteFile(root, []byte("not a directory"), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	store, err := NewFileTaskStore(root)
	if err == nil || store != nil {
		t.Fatalf("NewFileTaskStore() = %#v, %v", store, err)
	}
}

func writeStartupSnapshot(t *testing.T, root string, id domain.TaskID, body []byte) {
	t.Helper()
	dir := filepath.Join(root, id.String())
	if err := os.MkdirAll(dir, taskDirPerm); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "task.json"), body, taskFilePerm); err != nil {
		t.Fatal(err)
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
