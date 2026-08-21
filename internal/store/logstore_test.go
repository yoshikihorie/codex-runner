package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestFileLogStoreRotateNowAndLastRotationAt(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "codexd.log")
	if err := os.WriteFile(path, []byte("log"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := NewFileLogStore(nil)
	rotated, err := store.RotateNow(path)
	if err != nil {
		t.Fatalf("RotateNow() error = %v", err)
	}
	if _, err := os.Stat(rotated); err != nil {
		t.Fatalf("rotated file missing: %v", err)
	}
	if info, err := os.Stat(path); err != nil || info.Size() != 0 {
		t.Fatalf("active file = %#v, %v; want empty file", info, err)
	}
	got, err := store.LastRotationAt(path)
	if err != nil || got.IsZero() || time.Since(got) > time.Minute {
		t.Fatalf("LastRotationAt() = %v, %v", got, err)
	}
}

func TestFileLogStoreListRotatedGenerationsIncludesOnlyValidGenerations(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "codexd.log")
	for _, name := range []string{
		"codexd.log.20260821T123456.123456789Z",
		"codexd.log.20260820T123456.123456789Z.gz",
		"codexd.log.manual-backup",
		"codexd.log.20261301T123456.123456789Z",
		"other.log.20260821T123456.123456789Z",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("log"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	got, err := NewFileLogStore(nil).ListRotatedGenerations(path)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		filepath.Join(dir, "codexd.log.20260820T123456.123456789Z.gz"),
		filepath.Join(dir, "codexd.log.20260821T123456.123456789Z"),
	}
	if len(got) != len(want) {
		t.Fatalf("ListRotatedGenerations() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ListRotatedGenerations() = %v, want %v", got, want)
		}
	}
}

func TestFileLogStoreCompressGenerationPreservesModificationTime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "codexd.log.20260821T123456.123456789Z")
	modifiedAt := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
	if err := os.WriteFile(path, []byte("log"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, modifiedAt, modifiedAt); err != nil {
		t.Fatal(err)
	}

	compressed, err := NewFileLogStore(nil).CompressGeneration(path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(compressed)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime().Equal(modifiedAt) {
		t.Fatalf("compressed modification time = %v, want %v", info.ModTime(), modifiedAt)
	}
}

func TestFileLogStoreCompressGenerationRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target.log")
	path := filepath.Join(dir, "codexd.log.20260821T123456.123456789Z")
	if err := os.WriteFile(target, []byte("log"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}

	_, err := NewFileLogStore(nil).CompressGeneration(path)
	if err == nil {
		t.Fatal("CompressGeneration() error = nil, want symlink rejection")
	}
}

func TestFileLogStoreListMonthlyMetricsFilesExcludesUnrelatedNames(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"task-metrics-2026-08.jsonl",
		"task-metrics-2026-08.2.jsonl.gz",
		"task-metrics-2026-00.jsonl",
		"task-metrics-2026-13.jsonl",
		"task-metrics-2000-01.notes.jsonl",
		"task-metrics-2026-08.1.jsonl",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("metric"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	got, err := NewFileLogStore(nil).ListMonthlyMetricsFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		filepath.Join(dir, "task-metrics-2026-08.2.jsonl.gz"),
		filepath.Join(dir, "task-metrics-2026-08.jsonl"),
	}
	if len(got) != len(want) {
		t.Fatalf("ListMonthlyMetricsFiles() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ListMonthlyMetricsFiles() = %v, want %v", got, want)
		}
	}
}

func TestFileLogStoreCompressGenerationReturnsCompressedPathUnchanged(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "codexd.log.20260821T123456.123456789Z.gz")
	if err := os.WriteFile(path, []byte("log"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := NewFileLogStore(nil).CompressGeneration(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != path {
		t.Fatalf("CompressGeneration() = %q, want %q", got, path)
	}
}

func TestFileLogStoreListsOnlySupportedTaskLogs(t *testing.T) {
	root := t.TempDir()
	taskID, err := domain.NewTaskID("impl-20260801-120000-a1b2-example")
	if err != nil {
		t.Fatal(err)
	}
	taskDir := filepath.Join(root, taskID.String())
	if err := os.Mkdir(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"stdout.log", "stderr.log", "events.jsonl", "task.json"} {
		if err := os.WriteFile(filepath.Join(taskDir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	files, err := NewFileLogStore(nil).ListPerTaskLogFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(files[taskID]); got != 3 {
		t.Fatalf("log file count = %d, want 3", got)
	}
}

func TestRollbackRotationDoesNotReplaceNewActiveLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "codexd.log")
	rotated := path + ".20260821T123456.123456789Z"
	if err := os.WriteFile(rotated, []byte("rotated"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("owned-active"), 0o600); err != nil {
		t.Fatal(err)
	}
	activeInfo, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("new-active"), 0o600); err != nil {
		t.Fatal(err)
	}

	err = rollbackRotation(path, rotated, true, activeInfo, errors.New("create active log"))
	if err == nil || !errors.Is(err, os.ErrExist) {
		t.Fatalf("rollbackRotation() error = %v, want ErrExist", err)
	}
	contents, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(contents) != "new-active" {
		t.Fatalf("active log contents = %q, want new active log", contents)
	}
	if _, statErr := os.Stat(rotated); statErr != nil {
		t.Fatalf("rotated log missing: %v", statErr)
	}
}

func TestRenamexNPExclusiveDoesNotReplaceExistingDestination(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "pending.gz")
	destination := filepath.Join(dir, "generation.gz")
	if err := os.WriteFile(source, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(destination, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := renamexNPExclusive(source, destination)
	if err == nil || !errors.Is(err, os.ErrExist) {
		t.Fatalf("renamexNPExclusive() error = %v, want ErrExist", err)
	}
	contents, readErr := os.ReadFile(destination)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(contents) != "existing" {
		t.Fatalf("destination contents = %q, want existing content", contents)
	}
}

func TestFileLogStoreReopenActiveHandleFailsWhenUnavailable(t *testing.T) {
	err := NewFileLogStore(nil).ReopenActiveHandle("/tmp/codexd.log")
	if !errors.Is(err, errReopenUnavailable) {
		t.Fatalf("ReopenActiveHandle() error = %v, want unavailable error", err)
	}
}
