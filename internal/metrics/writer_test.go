package metrics

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const fileMetricsWriterTestMaxFileBytes int64 = 64

func fileMetricsWriterTaskID(t *testing.T, value string) domain.TaskID {
	t.Helper()

	taskID, err := domain.NewTaskID(value)
	if err != nil {
		t.Fatal(err)
	}
	return taskID
}

func TestFileMetricsWriter_AppendsLineWithTrailingLF(t *testing.T) {
	// Arrange
	logsRoot := t.TempDir()
	writer := NewFileMetricsWriter(logsRoot, fileMetricsWriterTestMaxFileBytes)
	line := []byte(`{"task_id":"impl-20260815-120000-abcd-append"}`)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120000-abcd-append"), "2026-08", line)

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(logsRoot, "task-metrics-2026-08.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte(nil), line...), '\n')
	if !bytes.Equal(got, want) {
		t.Fatalf("metrics file = %q, want %q", got, want)
	}
}

func TestFileMetricsWriter_UsesAppendCreateWriteOnlyNoFollowFlagsAnd0600Permission(t *testing.T) {
	// Arrange
	logsRoot := t.TempDir()
	file := &fileMetricsWriterFakeFile{}
	var gotPath string
	var gotFlags int
	var gotPermission os.FileMode
	writer := &FileMetricsWriter{
		logsRoot:     logsRoot,
		maxFileBytes: fileMetricsWriterTestMaxFileBytes,
		stat: func(string) (os.FileInfo, error) {
			return nil, os.ErrNotExist
		},
		open: func(path string, flags int, permission os.FileMode) (metricsWriteCloser, error) {
			gotPath = path
			gotFlags = flags
			gotPermission = permission
			return file, nil
		},
	}

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120001-abcd-open-flags"), "2026-08", []byte(`{"ok":true}`))

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if gotPath != filepath.Join(logsRoot, "task-metrics-2026-08.jsonl") {
		t.Fatalf("OpenFile path = %q", gotPath)
	}
	wantFlags := os.O_APPEND | os.O_CREATE | os.O_WRONLY | syscall.O_NOFOLLOW
	if gotFlags != wantFlags {
		t.Fatalf("OpenFile flags = %d, want %d", gotFlags, wantFlags)
	}
	if gotPermission != 0o600 {
		t.Fatalf("OpenFile permission = %#o, want 0600", gotPermission)
	}
}

func TestFileMetricsWriter_NewFileHas0600Permission(t *testing.T) {
	// Arrange
	logsRoot := t.TempDir()
	writer := NewFileMetricsWriter(logsRoot, fileMetricsWriterTestMaxFileBytes)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120002-abcd-new-permission"), "2026-08", []byte(`{"new":true}`))

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(logsRoot, "task-metrics-2026-08.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("new file permission = %#o, want 0600", got)
	}
}

func TestFileMetricsWriter_PreservesExisting0600FileAndAppends(t *testing.T) {
	// Arrange
	logsRoot := t.TempDir()
	path := filepath.Join(logsRoot, "task-metrics-2026-08.jsonl")
	if err := os.WriteFile(path, []byte("existing\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writer := NewFileMetricsWriter(logsRoot, fileMetricsWriterTestMaxFileBytes)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120003-abcd-existing-permission"), "2026-08", []byte(`{"next":true}`))

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := "existing\n{\"next\":true}\n"; string(got) != want {
		t.Fatalf("metrics file = %q, want %q", got, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("existing file permission = %#o, want 0600", got)
	}
}

func TestFileMetricsWriter_CorrectsExistingFilePermissionTo0600(t *testing.T) {
	// Arrange
	logsRoot := t.TempDir()
	path := filepath.Join(logsRoot, "task-metrics-2026-08.jsonl")
	if err := os.WriteFile(path, []byte("existing\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	writer := NewFileMetricsWriter(logsRoot, fileMetricsWriterTestMaxFileBytes)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120018-abcd-correct-permission"), "2026-08", []byte(`{"next":true}`))

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("existing file permission = %#o, want 0600", got)
	}
}

func TestFileMetricsWriter_SelectsBaseFileWhenBelowLimit(t *testing.T) {
	// Arrange
	logsRoot := t.TempDir()
	basePath := filepath.Join(logsRoot, "task-metrics-2026-08.jsonl")
	if err := os.WriteFile(basePath, []byte("small\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writer := NewFileMetricsWriter(logsRoot, fileMetricsWriterTestMaxFileBytes)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120004-abcd-base-below-limit"), "2026-08", []byte(`{"base":true}`))

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(logsRoot, "task-metrics-2026-08.2.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment 2 stat error = %v, want not exist", err)
	}
}

func TestFileMetricsWriter_SelectsFirstAvailableSegmentAfterFullBase(t *testing.T) {
	// Arrange
	logsRoot := t.TempDir()
	basePath := filepath.Join(logsRoot, "task-metrics-2026-08.jsonl")
	if err := os.WriteFile(basePath, bytes.Repeat([]byte("x"), int(fileMetricsWriterTestMaxFileBytes)), 0o600); err != nil {
		t.Fatal(err)
	}
	writer := NewFileMetricsWriter(logsRoot, fileMetricsWriterTestMaxFileBytes)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120005-abcd-first-segment"), "2026-08", []byte(`{"segment":2}`))

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(logsRoot, "task-metrics-2026-08.2.jsonl")); err != nil {
		t.Fatal(err)
	}
}

func TestFileMetricsWriter_SelectsFirstSegmentBelowLimitWithoutSkipping(t *testing.T) {
	// Arrange
	logsRoot := t.TempDir()
	for _, name := range []string{"task-metrics-2026-08.jsonl", "task-metrics-2026-08.2.jsonl"} {
		if err := os.WriteFile(filepath.Join(logsRoot, name), bytes.Repeat([]byte("x"), int(fileMetricsWriterTestMaxFileBytes)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	segmentThreePath := filepath.Join(logsRoot, "task-metrics-2026-08.3.jsonl")
	if err := os.WriteFile(segmentThreePath, []byte("small\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	writer := NewFileMetricsWriter(logsRoot, fileMetricsWriterTestMaxFileBytes)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120006-abcd-third-segment"), "2026-08", []byte(`{"segment":3}`))

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(segmentThreePath)
	if err != nil {
		t.Fatal(err)
	}
	if want := "small\n{\"segment\":3}\n"; string(got) != want {
		t.Fatalf("segment 3 = %q, want %q", got, want)
	}
	if _, err := os.Stat(filepath.Join(logsRoot, "task-metrics-2026-08.4.jsonl")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("segment 4 stat error = %v, want not exist", err)
	}
}

func TestFileMetricsWriter_ReturnsStatErrorWithoutOpening(t *testing.T) {
	// Arrange
	statErr := errors.New("stat failed")
	openCalled := false
	writer := &FileMetricsWriter{
		logsRoot:     t.TempDir(),
		maxFileBytes: fileMetricsWriterTestMaxFileBytes,
		stat:         func(string) (os.FileInfo, error) { return nil, statErr },
		open: func(string, int, os.FileMode) (metricsWriteCloser, error) {
			openCalled = true
			return nil, errors.New("unexpected open")
		},
	}

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120007-abcd-stat-error"), "2026-08", []byte(`{}`))

	// Assert
	if !errors.Is(err, statErr) {
		t.Fatalf("Append() error = %v, want wrapped stat error", err)
	}
	if openCalled {
		t.Fatal("OpenFile called after Stat failure")
	}
}

func TestFileMetricsWriter_ReturnsOpenErrorWithoutWritingOrClosing(t *testing.T) {
	// Arrange
	openErr := errors.New("open failed")
	writer := &FileMetricsWriter{
		logsRoot:     t.TempDir(),
		maxFileBytes: fileMetricsWriterTestMaxFileBytes,
		stat:         func(string) (os.FileInfo, error) { return nil, os.ErrNotExist },
		open: func(string, int, os.FileMode) (metricsWriteCloser, error) {
			return nil, openErr
		},
	}

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120008-abcd-open-error"), "2026-08", []byte(`{}`))

	// Assert
	if !errors.Is(err, openErr) {
		t.Fatalf("Append() error = %v, want wrapped open error", err)
	}
}

func TestFileMetricsWriter_ReturnsChmodErrorWithoutWritingAndClosesOnce(t *testing.T) {
	// Arrange
	chmodErr := errors.New("chmod failed")
	file := &fileMetricsWriterFakeFile{chmodErr: chmodErr}
	writer := fileMetricsWriterWithFakeFile(t, file)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120019-abcd-chmod-error"), "2026-08", []byte(`{}`))

	// Assert
	if !errors.Is(err, chmodErr) {
		t.Fatalf("Append() error = %v, want wrapped chmod error", err)
	}
	if len(file.writes) != 0 {
		t.Fatalf("Write() calls = %d, want 0", len(file.writes))
	}
	if file.closeCalls != 1 {
		t.Fatalf("Close() calls = %d, want 1", file.closeCalls)
	}
}

func TestFileMetricsWriter_ReturnsWriteErrorAndClosesOnce(t *testing.T) {
	// Arrange
	writeErr := errors.New("write failed")
	file := &fileMetricsWriterFakeFile{writeErr: writeErr}
	writer := fileMetricsWriterWithFakeFile(t, file)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120009-abcd-write-error"), "2026-08", []byte(`{}`))

	// Assert
	if !errors.Is(err, writeErr) {
		t.Fatalf("Append() error = %v, want wrapped write error", err)
	}
	if file.closeCalls != 1 {
		t.Fatalf("Close() calls = %d, want 1", file.closeCalls)
	}
}

func TestFileMetricsWriter_ReturnsShortWriteAndClosesOnce(t *testing.T) {
	// Arrange
	file := &fileMetricsWriterFakeFile{writeCount: 1}
	writer := fileMetricsWriterWithFakeFile(t, file)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120010-abcd-short-write"), "2026-08", []byte(`{}`))

	// Assert
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Append() error = %v, want io.ErrShortWrite", err)
	}
	if file.closeCalls != 1 {
		t.Fatalf("Close() calls = %d, want 1", file.closeCalls)
	}
}

func TestFileMetricsWriter_ReturnsCloseErrorAfterSuccessfulWrite(t *testing.T) {
	// Arrange
	closeErr := errors.New("close failed")
	file := &fileMetricsWriterFakeFile{closeErr: closeErr}
	writer := fileMetricsWriterWithFakeFile(t, file)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120011-abcd-close-error"), "2026-08", []byte(`{}`))

	// Assert
	if !errors.Is(err, closeErr) {
		t.Fatalf("Append() error = %v, want wrapped close error", err)
	}
}

func TestFileMetricsWriter_PreservesWriteErrorWhenCloseAlsoFails(t *testing.T) {
	// Arrange
	writeErr := errors.New("write failed")
	file := &fileMetricsWriterFakeFile{writeErr: writeErr, closeErr: errors.New("close failed")}
	writer := fileMetricsWriterWithFakeFile(t, file)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120012-abcd-write-close-error"), "2026-08", []byte(`{}`))

	// Assert
	if !errors.Is(err, writeErr) {
		t.Fatalf("Append() error = %v, want wrapped write error", err)
	}
}

func TestFileMetricsWriter_WritesPayloadOnceWithoutMutatingInput(t *testing.T) {
	// Arrange
	file := &fileMetricsWriterFakeFile{}
	writer := fileMetricsWriterWithFakeFile(t, file)
	line := make([]byte, 2, 3)
	copy(line, "{}")
	backingArray := line[:cap(line)]
	backingArray[2] = 'x'

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120013-abcd-single-write"), "2026-08", line)

	// Assert
	if err != nil {
		t.Fatal(err)
	}
	if len(file.writes) != 1 {
		t.Fatalf("Write() calls = %d, want 1", len(file.writes))
	}
	if got, want := file.writes[0], []byte("{}\n"); !bytes.Equal(got, want) {
		t.Fatalf("Write() payload = %q, want %q", got, want)
	}
	if got := backingArray[2]; got != 'x' {
		t.Fatalf("line backing array was modified: byte after line = %q, want %q", got, 'x')
	}
}

func TestFileMetricsWriter_RejectsSelectedSymlink(t *testing.T) {
	// Arrange
	logsRoot := t.TempDir()
	targetPath := filepath.Join(logsRoot, "target.jsonl")
	targetContent := []byte("target remains unchanged\n")
	if err := os.WriteFile(targetPath, targetContent, 0o600); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(logsRoot, "task-metrics-2026-08.jsonl")
	if err := os.Symlink(targetPath, linkPath); err != nil {
		t.Fatal(err)
	}
	writer := NewFileMetricsWriter(logsRoot, fileMetricsWriterTestMaxFileBytes)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120014-abcd-symlink"), "2026-08", []byte(`{"link":false}`))

	// Assert
	if !errors.Is(err, syscall.ELOOP) {
		t.Fatalf("Append() error = %v, want syscall.ELOOP", err)
	}
	got, readErr := os.ReadFile(targetPath)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(got, targetContent) {
		t.Fatalf("symlink target = %q, want unchanged %q", got, targetContent)
	}
}

func TestFileMetricsWriter_KeepsMonthsSeparated(t *testing.T) {
	// Arrange
	logsRoot := t.TempDir()
	writer := NewFileMetricsWriter(logsRoot, fileMetricsWriterTestMaxFileBytes)

	// Act
	firstErr := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120015-abcd-july"), "2026-07", []byte(`{"month":7}`))
	secondErr := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120016-abcd-august"), "2026-08", []byte(`{"month":8}`))

	// Assert
	if firstErr != nil || secondErr != nil {
		t.Fatalf("Append() errors = %v, %v", firstErr, secondErr)
	}
	for _, testCase := range []struct{ month, want string }{{"2026-07", "{\"month\":7}\n"}, {"2026-08", "{\"month\":8}\n"}} {
		got, err := os.ReadFile(filepath.Join(logsRoot, "task-metrics-"+testCase.month+".jsonl"))
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != testCase.want {
			t.Fatalf("month %s file = %q, want %q", testCase.month, got, testCase.want)
		}
	}
}

func TestFileMetricsWriter_ConcurrentAppendsProduceDistinctJSONLines(t *testing.T) {
	// Arrange
	const goroutineCount = 16
	logsRoot := t.TempDir()
	writer := NewFileMetricsWriter(logsRoot, 4096)
	start := make(chan struct{})
	results := make(chan error, goroutineCount)
	var waitGroup sync.WaitGroup
	wantTaskIDs := make(map[string]struct{}, goroutineCount)

	for index := 0; index < goroutineCount; index++ {
		taskID := fileMetricsWriterTaskID(t, fmt.Sprintf("impl-20260815-1201%02d-abcd-concurrent", index))
		wantTaskIDs[taskID.String()] = struct{}{}
		line, err := json.Marshal(map[string]string{"task_id": taskID.String()})
		if err != nil {
			t.Fatal(err)
		}
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			results <- writer.Append(taskID, "2026-08", line)
		}()
	}

	// Act
	close(start)
	waitGroup.Wait()
	close(results)

	// Assert
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	content, err := os.ReadFile(filepath.Join(logsRoot, "task-metrics-2026-08.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSuffix(content, []byte{'\n'}), []byte{'\n'})
	if len(lines) != goroutineCount {
		t.Fatalf("JSON line count = %d, want %d", len(lines), goroutineCount)
	}
	gotTaskIDs := make(map[string]int, goroutineCount)
	for _, line := range lines {
		var record struct {
			TaskID string `json:"task_id"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("JSON line %q cannot be unmarshaled: %v", line, err)
		}
		gotTaskIDs[record.TaskID]++
	}
	for taskID := range wantTaskIDs {
		if gotTaskIDs[taskID] != 1 {
			t.Fatalf("task ID %s count = %d, want 1", taskID, gotTaskIDs[taskID])
		}
	}
}

func TestFileMetricsWriter_DoesNotCreateMissingLogsRoot(t *testing.T) {
	// Arrange
	parent := t.TempDir()
	logsRoot := filepath.Join(parent, "missing-logs")
	writer := NewFileMetricsWriter(logsRoot, fileMetricsWriterTestMaxFileBytes)

	// Act
	err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120017-abcd-missing-root"), "2026-08", []byte(`{}`))

	// Assert
	if err == nil {
		t.Fatal("Append() error = nil, want error for missing logs root")
	}
	if _, statErr := os.Stat(logsRoot); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("logs root stat error = %v, want not exist", statErr)
	}
}

func TestFileMetricsWriter_RejectsInvalidMonthWithoutAccessingFiles(t *testing.T) {
	for _, month := range []string{"../escape", "", "2026-13"} {
		t.Run(month, func(t *testing.T) {
			// Arrange
			parent := t.TempDir()
			logsRoot := filepath.Join(parent, "logs")
			if err := os.Mkdir(logsRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			taskID := fileMetricsWriterTaskID(t, "impl-20260815-120020-abcd-invalid-month")
			statCalled := false
			openCalled := false
			writer := &FileMetricsWriter{
				logsRoot:     logsRoot,
				maxFileBytes: fileMetricsWriterTestMaxFileBytes,
				stat: func(string) (os.FileInfo, error) {
					statCalled = true
					return nil, errors.New("unexpected stat")
				},
				open: func(string, int, os.FileMode) (metricsWriteCloser, error) {
					openCalled = true
					return nil, errors.New("unexpected open")
				},
			}

			// Act
			err := writer.Append(taskID, month, []byte(`{}`))

			// Assert
			if err == nil {
				t.Fatal("Append() error = nil, want invalid month error")
			}
			if !bytes.Contains([]byte(err.Error()), []byte(taskID.String())) || !bytes.Contains([]byte(err.Error()), []byte(fmt.Sprintf("%q", month))) {
				t.Fatalf("Append() error = %q, want task ID and month", err)
			}
			if statCalled || openCalled {
				t.Fatalf("file access after invalid month: Stat called = %t, OpenFile called = %t", statCalled, openCalled)
			}
			entries, readErr := os.ReadDir(logsRoot)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("logs root entries = %v, want none", entries)
			}
			parentEntries, readErr := os.ReadDir(parent)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(parentEntries) != 1 || parentEntries[0].Name() != "logs" {
				t.Fatalf("parent entries = %v, want only logs", parentEntries)
			}
		})
	}
}

func TestFileMetricsWriter_RejectsNonPositiveMaxFileBytesWithoutCreatingFiles(t *testing.T) {
	for _, maxFileBytes := range []int64{0, -1} {
		t.Run(fmt.Sprintf("maxFileBytes=%d", maxFileBytes), func(t *testing.T) {
			// Arrange
			logsRoot := t.TempDir()
			writer := NewFileMetricsWriter(logsRoot, maxFileBytes)

			// Act
			err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120022-abcd-invalid-max-file-bytes"), "2026-08", []byte(`{}`))

			// Assert
			if err == nil {
				t.Fatal("Append() error = nil, want error for non-positive maxFileBytes")
			}
			if got, want := err.Error(), "metrics writer: maxFileBytes must be positive"; got != want {
				t.Fatalf("Append() error = %q, want %q", got, want)
			}
			entries, readErr := os.ReadDir(logsRoot)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("logs root entries = %v, want none", entries)
			}
		})
	}
}

func TestFileMetricsWriter_RejectsInvalidMonthWithoutCreatingFiles(t *testing.T) {
	for _, month := range []string{"../escape", "", "2026-13"} {
		t.Run(month, func(t *testing.T) {
			// Arrange
			parent := t.TempDir()
			logsRoot := filepath.Join(parent, "logs")
			if err := os.Mkdir(logsRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			writer := NewFileMetricsWriter(logsRoot, fileMetricsWriterTestMaxFileBytes)

			// Act
			err := writer.Append(fileMetricsWriterTaskID(t, "impl-20260815-120021-abcd-invalid-month-files"), month, []byte(`{}`))

			// Assert
			if err == nil {
				t.Fatal("Append() error = nil, want invalid month error")
			}
			entries, readErr := os.ReadDir(logsRoot)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(entries) != 0 {
				t.Fatalf("logs root entries = %v, want none", entries)
			}
			parentEntries, readErr := os.ReadDir(parent)
			if readErr != nil {
				t.Fatal(readErr)
			}
			if len(parentEntries) != 1 || parentEntries[0].Name() != "logs" {
				t.Fatalf("parent entries = %v, want only logs", parentEntries)
			}
		})
	}
}

func fileMetricsWriterWithFakeFile(t *testing.T, file *fileMetricsWriterFakeFile) *FileMetricsWriter {
	t.Helper()

	return &FileMetricsWriter{
		logsRoot:     t.TempDir(),
		maxFileBytes: fileMetricsWriterTestMaxFileBytes,
		stat:         func(string) (os.FileInfo, error) { return nil, os.ErrNotExist },
		open: func(string, int, os.FileMode) (metricsWriteCloser, error) {
			return file, nil
		},
	}
}

type fileMetricsWriterFakeFile struct {
	writes     [][]byte
	writeCount int
	writeErr   error
	chmodErr   error
	closeCalls int
	closeErr   error
}

func (f *fileMetricsWriterFakeFile) Write(payload []byte) (int, error) {
	f.writes = append(f.writes, append([]byte(nil), payload...))
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	if f.writeCount != 0 {
		return f.writeCount, nil
	}
	return len(payload), nil
}

func (f *fileMetricsWriterFakeFile) Chmod(os.FileMode) error {
	return f.chmodErr
}

func (f *fileMetricsWriterFakeFile) Close() error {
	f.closeCalls++
	return f.closeErr
}
