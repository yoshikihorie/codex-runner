package store

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestFileMetricsReaderListMonthlyFilesFiltersMonths(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"task-metrics-2026-05.jsonl",
		"task-metrics-2026-06.jsonl",
		"task-metrics-2026-07.jsonl",
	} {
		writeMetricsFixture(t, dir, name, "{}\n")
	}

	since, until := "2026-06", "2026-06"
	got, err := NewFileMetricsReader().ListMonthlyFiles(dir, &since, &until)
	if err != nil {
		t.Fatalf("ListMonthlyFiles() error = %v", err)
	}
	want := []string{filepath.Join(dir, "task-metrics-2026-06.jsonl")}
	assertStringSlicesEqual(t, got, want)
}

func TestFileMetricsReaderListMonthlyFilesOrdersSegmentsNumerically(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"task-metrics-2026-08.10.jsonl",
		"task-metrics-2026-08.2.jsonl",
		"task-metrics-2026-08.jsonl",
		"task-metrics-2026-08.3.jsonl",
	} {
		writeMetricsFixture(t, dir, name, "{}\n")
	}

	got, err := NewFileMetricsReader().ListMonthlyFiles(dir, nil, nil)
	if err != nil {
		t.Fatalf("ListMonthlyFiles() error = %v", err)
	}
	want := []string{
		filepath.Join(dir, "task-metrics-2026-08.jsonl"),
		filepath.Join(dir, "task-metrics-2026-08.2.jsonl"),
		filepath.Join(dir, "task-metrics-2026-08.3.jsonl"),
		filepath.Join(dir, "task-metrics-2026-08.10.jsonl"),
	}
	assertStringSlicesEqual(t, got, want)
}

func TestFileMetricsReaderListMonthlyFilesWithoutBounds(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"task-metrics-2025-12.jsonl",
		"task-metrics-2026-01.jsonl",
	} {
		writeMetricsFixture(t, dir, name, "{}\n")
	}

	got, err := NewFileMetricsReader().ListMonthlyFiles(dir, nil, nil)
	if err != nil {
		t.Fatalf("ListMonthlyFiles() error = %v", err)
	}
	want := []string{
		filepath.Join(dir, "task-metrics-2025-12.jsonl"),
		filepath.Join(dir, "task-metrics-2026-01.jsonl"),
	}
	assertStringSlicesEqual(t, got, want)
}

func TestFileMetricsReaderListMonthlyFilesMissingDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	got, err := NewFileMetricsReader().ListMonthlyFiles(dir, nil, nil)
	if err != nil {
		t.Fatalf("ListMonthlyFiles() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("ListMonthlyFiles() = %v, want empty slice", got)
	}
}

func TestFileMetricsReaderOpenMonthlyFileReturnsOpenError(t *testing.T) {
	wantErr := errors.New("open denied")
	reader := NewFileMetricsReader()
	reader.open = func(string, int, os.FileMode) (metricsFile, error) {
		return nil, wantErr
	}

	_, err := reader.OpenMonthlyFile("/tmp/task-metrics-2026-08.jsonl")
	if !errors.Is(err, wantErr) {
		t.Fatalf("OpenMonthlyFile() error = %v, want %v", err, wantErr)
	}
}

func TestFileMetricsReaderOpenMonthlyFileFallsBackToGzip(t *testing.T) {
	dir := t.TempDir()
	path := writeGzipMetricsFixture(t, dir, "task-metrics-2026-08.jsonl.gz", "first\n")

	file, err := NewFileMetricsReader().OpenMonthlyFile(strings.TrimSuffix(path, ".gz"))
	if err != nil {
		t.Fatalf("OpenMonthlyFile() error = %v", err)
	}
	defer file.Close()

	got, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != "first\n" {
		t.Fatalf("ReadAll() = %q, want %q", got, "first\n")
	}
}

func TestFileMetricsReaderListMonthlyFilesSkipsEntryRemovedDuringInfo(t *testing.T) {
	dir := t.TempDir()
	path := writeMetricsFixture(t, dir, "task-metrics-2026-08.jsonl", "{}\n")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}

	reader := NewFileMetricsReader()
	reader.readDir = func(string) ([]os.DirEntry, error) {
		return append(entries, missingInfoMetricsDirEntry{}), nil
	}

	got, err := reader.ListMonthlyFiles(dir, nil, nil)
	if err != nil {
		t.Fatalf("ListMonthlyFiles() error = %v", err)
	}
	assertStringSlicesEqual(t, got, []string{path})
}

func TestFileMetricsReaderOpenMonthlyFileUsesSafeOpenFlags(t *testing.T) {
	reader := NewFileMetricsReader()
	lower := &trackingMetricsFile{Reader: strings.NewReader("{}\n")}

	reader.open = func(_ string, flags int, permission os.FileMode) (metricsFile, error) {
		wantFlags := os.O_RDONLY | syscall.O_NOFOLLOW
		if flags != wantFlags {
			t.Fatalf("flags = %d, want %d", flags, wantFlags)
		}
		if permission != 0 {
			t.Fatalf("permission = %#o, want 0", permission)
		}
		return lower, nil
	}

	file, err := reader.OpenMonthlyFile(filepath.Join(t.TempDir(), "task-metrics-2026-08.jsonl"))
	if err != nil {
		t.Fatalf("OpenMonthlyFile() error = %v", err)
	}
	defer file.Close()
}

func TestFileMetricsReaderOpenMonthlyFileDecompressesGzip(t *testing.T) {
	dir := t.TempDir()
	path := writeGzipMetricsFixture(t, dir, "task-metrics-2026-08.jsonl.gz", "first\nsecond\n")

	file, err := NewFileMetricsReader().OpenMonthlyFile(path)
	if err != nil {
		t.Fatalf("OpenMonthlyFile() error = %v", err)
	}
	defer file.Close()
	got, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(got) != "first\nsecond\n" {
		t.Fatalf("ReadAll() = %q, want %q", got, "first\nsecond\n")
	}
}

func TestFileMetricsReaderListMonthlyFilesIncludesGzipOnlyMonth(t *testing.T) {
	dir := t.TempDir()
	path := writeGzipMetricsFixture(t, dir, "task-metrics-2026-07.jsonl.gz", "{}\n")

	got, err := NewFileMetricsReader().ListMonthlyFiles(dir, nil, nil)
	if err != nil {
		t.Fatalf("ListMonthlyFiles() error = %v", err)
	}
	assertStringSlicesEqual(t, got, []string{path})
}

func TestFileMetricsReaderListMonthlyFilesPrefersGzipForDuplicateSegment(t *testing.T) {
	dir := t.TempDir()
	writeMetricsFixture(t, dir, "task-metrics-2026-07.jsonl", "{}\n")
	gzipPath := writeGzipMetricsFixture(t, dir, "task-metrics-2026-07.jsonl.gz", "{}\n")

	got, err := NewFileMetricsReader().ListMonthlyFiles(dir, nil, nil)
	if err != nil {
		t.Fatalf("ListMonthlyFiles() error = %v", err)
	}
	assertStringSlicesEqual(t, got, []string{gzipPath})
}

func TestFileMetricsReaderListMonthlyFilesSkipsOverflowingSegment(t *testing.T) {
	dir := t.TempDir()
	writeMetricsFixture(t, dir, "task-metrics-2026-07.999999999999999999999999999999999999999.jsonl", "{}\n")

	got, err := NewFileMetricsReader().ListMonthlyFiles(dir, nil, nil)
	if err != nil {
		t.Fatalf("ListMonthlyFiles() error = %v", err)
	}
	assertStringSlicesEqual(t, got, []string{})
}

func TestFileMetricsReaderListMonthlyFilesOrdersCompressedSegmentsNumerically(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{
		"task-metrics-2026-08.10.jsonl.gz",
		"task-metrics-2026-08.2.jsonl.gz",
		"task-metrics-2026-08.jsonl.gz",
	} {
		writeGzipMetricsFixture(t, dir, name, "{}\n")
	}

	got, err := NewFileMetricsReader().ListMonthlyFiles(dir, nil, nil)
	if err != nil {
		t.Fatalf("ListMonthlyFiles() error = %v", err)
	}
	want := []string{
		filepath.Join(dir, "task-metrics-2026-08.jsonl.gz"),
		filepath.Join(dir, "task-metrics-2026-08.2.jsonl.gz"),
		filepath.Join(dir, "task-metrics-2026-08.10.jsonl.gz"),
	}
	assertStringSlicesEqual(t, got, want)
}

func TestFileMetricsReaderListMonthlyFilesFiltersMixedCompression(t *testing.T) {
	dir := t.TempDir()
	writeMetricsFixture(t, dir, "task-metrics-2026-05.jsonl", "{}\n")
	writeGzipMetricsFixture(t, dir, "task-metrics-2026-06.jsonl.gz", "{}\n")
	writeMetricsFixture(t, dir, "task-metrics-2026-07.jsonl", "{}\n")

	since, until := "2026-06", "2026-07"
	got, err := NewFileMetricsReader().ListMonthlyFiles(dir, &since, &until)
	if err != nil {
		t.Fatalf("ListMonthlyFiles() error = %v", err)
	}
	want := []string{
		filepath.Join(dir, "task-metrics-2026-06.jsonl.gz"),
		filepath.Join(dir, "task-metrics-2026-07.jsonl"),
	}
	assertStringSlicesEqual(t, got, want)
}

func TestFileMetricsReaderOpenMonthlyFileClosesGzipAndFile(t *testing.T) {
	lower := &trackingMetricsFile{Reader: bytes.NewReader(gzipBytes(t, "{}\n"))}
	reader := NewFileMetricsReader()
	reader.open = func(string, int, os.FileMode) (metricsFile, error) {
		return lower, nil
	}

	file, err := reader.OpenMonthlyFile("/tmp/task-metrics-2026-08.jsonl.gz")
	if err != nil {
		t.Fatalf("OpenMonthlyFile() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if !lower.closed {
		t.Fatal("lower file was not closed")
	}
}

func TestFileMetricsReaderOpenMonthlyFileClosesFileOnInvalidGzip(t *testing.T) {
	lower := &trackingMetricsFile{Reader: strings.NewReader("not gzip")}
	reader := NewFileMetricsReader()
	reader.open = func(string, int, os.FileMode) (metricsFile, error) {
		return lower, nil
	}

	_, err := reader.OpenMonthlyFile("/tmp/task-metrics-2026-08.jsonl.gz")
	if err == nil {
		t.Fatal("OpenMonthlyFile() error = nil, want invalid gzip error")
	}
	if !lower.closed {
		t.Fatal("lower file was not closed after gzip setup failure")
	}
}

type trackingMetricsFile struct {
	io.Reader
	closed bool
}

type missingInfoMetricsDirEntry struct{}

func (missingInfoMetricsDirEntry) Name() string               { return "task-metrics-2026-08.2.jsonl" }
func (missingInfoMetricsDirEntry) IsDir() bool                { return false }
func (missingInfoMetricsDirEntry) Type() fs.FileMode          { return 0 }
func (missingInfoMetricsDirEntry) Info() (fs.FileInfo, error) { return nil, fs.ErrNotExist }

func (f *trackingMetricsFile) Close() error {
	f.closed = true
	return nil
}

func writeMetricsFixture(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
	return path
}

func writeGzipMetricsFixture(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create(%q) error = %v", path, err)
	}
	gz := gzip.NewWriter(file)
	if _, err := gz.Write([]byte(content)); err != nil {
		t.Fatalf("gzip Write() error = %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip Close() error = %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("Close(%q) error = %v", path, err)
	}
	return path
}

func gzipBytes(t *testing.T, content string) []byte {
	t.Helper()
	var buffer bytes.Buffer
	gz := gzip.NewWriter(&buffer)
	if _, err := gz.Write([]byte(content)); err != nil {
		t.Fatalf("gzip Write() error = %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip Close() error = %v", err)
	}
	return buffer.Bytes()
}

func assertStringSlicesEqual(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length = %d, want %d; got %v, want %v", len(got), len(want), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("element %d = %q, want %q; got %v, want %v", i, got[i], want[i], got, want)
		}
	}
}
