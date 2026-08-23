package store

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
)

type MetricsReader interface {
	ListMonthlyFiles(logsDir string, since, until *string) ([]string, error)
	OpenMonthlyFile(path string) (io.ReadCloser, error)
}

type metricsFile interface {
	io.Reader
	Close() error
}

type FileMetricsReader struct {
	readDir func(string) ([]os.DirEntry, error)
	open    func(string, int, os.FileMode) (metricsFile, error)
}

var _ MetricsReader = (*FileMetricsReader)(nil)

func NewFileMetricsReader() *FileMetricsReader {
	return &FileMetricsReader{
		readDir: os.ReadDir,
		open: func(path string, flags int, permission os.FileMode) (metricsFile, error) {
			return os.OpenFile(path, flags, permission)
		},
	}
}

func (r *FileMetricsReader) ListMonthlyFiles(logsDir string, since, until *string) ([]string, error) {
	if logsDir == "" || !filepath.IsAbs(logsDir) || filepath.Clean(logsDir) != logsDir {
		return nil, fmt.Errorf("metrics directory must be a normalized absolute path: %q", logsDir)
	}
	entries, err := r.readDir(logsDir)
	if errors.Is(err, fs.ErrNotExist) {
		return []string{}, nil
	}
	if err != nil {
		return nil, err
	}

	files := make([]monthlyMetricsFile, 0)
	for _, entry := range entries {
		name := entry.Name()
		if !monthlyMetricsName.MatchString(name) {
			continue
		}
		info, err := entry.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			continue
		}
		month, segment, ok := monthlyMetricsFileKey(name)
		if !ok {
			continue
		}
		if (since != nil && month < *since) || (until != nil && month > *until) {
			continue
		}
		files = append(files, monthlyMetricsFile{
			path:    filepath.Join(logsDir, name),
			month:   month,
			segment: segment,
		})
	}

	sort.Slice(files, func(i, j int) bool {
		if files[i].month != files[j].month {
			return files[i].month < files[j].month
		}
		if files[i].segment != files[j].segment {
			return files[i].segment < files[j].segment
		}
		return strings.HasSuffix(files[i].path, ".gz") &&
			!strings.HasSuffix(files[j].path, ".gz")
	})

	unique := files[:0]
	for _, file := range files {
		if len(unique) > 0 {
			previous := unique[len(unique)-1]
			if previous.month == file.month && previous.segment == file.segment {
				continue
			}
		}
		unique = append(unique, file)
	}
	files = unique

	result := make([]string, len(files))
	for i, file := range files {
		result[i] = file.path
	}
	return result, nil
}

func (r *FileMetricsReader) OpenMonthlyFile(path string) (io.ReadCloser, error) {
	openedPath := path
	file, err := r.open(openedPath, os.O_RDONLY|syscall.O_NOFOLLOW, 0)

	if errors.Is(err, fs.ErrNotExist) {
		switch {
		case strings.HasSuffix(path, ".jsonl.gz"):
			openedPath = strings.TrimSuffix(path, ".gz")
		case strings.HasSuffix(path, ".jsonl"):
			openedPath = path + ".gz"
		default:
			return nil, err
		}
		file, err = r.open(openedPath, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	}
	if err != nil {
		return nil, err
	}
	if !strings.HasSuffix(openedPath, ".gz") {
		return file, nil
	}

	reader, err := gzip.NewReader(file)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	return &gzipMonthlyFile{Reader: reader, file: file}, nil
}

type monthlyMetricsFile struct {
	path    string
	month   string
	segment int
}

func monthlyMetricsFileKey(name string) (string, int, bool) {
	const prefix = "task-metrics-"
	month := name[len(prefix) : len(prefix)+len("2006-01")]
	remainder := strings.TrimSuffix(strings.TrimSuffix(name, ".gz"), ".jsonl")
	remainder = strings.TrimPrefix(remainder, prefix+month)
	if remainder == "" {
		return month, 1, true
	}
	segment, err := strconv.Atoi(strings.TrimPrefix(remainder, "."))
	return month, segment, err == nil
}

type gzipMonthlyFile struct {
	*gzip.Reader
	file metricsFile
}

func (f *gzipMonthlyFile) Close() error {
	// Both Close calls are evaluated before errors.Join runs, so either error
	// cannot prevent the gzip reader and its underlying file from being closed.
	return errors.Join(f.Reader.Close(), f.file.Close())
}
