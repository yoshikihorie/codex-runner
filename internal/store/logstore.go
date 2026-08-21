package store

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const logFilePermission os.FileMode = 0o600

const rotatedTimestampLayout = "20060102T150405.000000000Z"

var (
	errReopenUnavailable = errors.New("active log handle reopen is unavailable")
	monthlyMetricsName   = regexp.MustCompile(
		`^task-metrics-[0-9]{4}-(0[1-9]|1[0-2])(?:\.(?:[2-9]|[1-9][0-9]+))?\.jsonl(?:\.gz)?$`,
	)
)

// FileLogStore implements the filesystem operations used by log eviction.
// ReopenActiveHandle is injected because only the daemon owns its log handle.
type FileLogStore struct {
	reopen func(string) error
	now    func() time.Time
}

func NewFileLogStore(reopen func(string) error) *FileLogStore {
	return &FileLogStore{reopen: reopen, now: time.Now}
}

func validateLogPath(path string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("log path must be a normalized absolute path: %q", path)
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("log path must be a regular non-symlink file: %q", path)
	}
	return nil
}

func (s *FileLogStore) Size(path string) (int64, error) {
	if err := validateLogPath(path); err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

func (s *FileLogStore) RotateNow(path string) (string, error) {
	if err := validateLogPath(path); err != nil {
		return "", err
	}
	now := s.now().UTC()
	rotated := path + "." + now.Format(rotatedTimestampLayout)
	if _, err := os.Lstat(rotated); err == nil {
		return "", fmt.Errorf("rotated generation already exists: %q", rotated)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	if err := renamexNPExclusive(path, rotated); err != nil {
		return "", err
	}
	activeCreated := false
	var activeInfo fs.FileInfo
	rollback := func(cause error) error {
		return rollbackRotation(path, rotated, activeCreated, activeInfo, cause)
	}
	if err := os.Chtimes(rotated, now, now); err != nil {
		return "", rollback(err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY|syscall.O_NOFOLLOW, logFilePermission)
	if err != nil {
		return "", rollback(err)
	}
	activeCreated = true
	activeInfo, err = f.Stat()
	if err != nil {
		return "", rollback(errors.Join(err, f.Close()))
	}
	if err := f.Chmod(logFilePermission); err != nil {
		return "", rollback(errors.Join(err, f.Close()))
	}
	if err := f.Close(); err != nil {
		return "", rollback(err)
	}
	return rotated, nil
}

func rollbackRotation(path, rotated string, activeCreated bool, activeInfo fs.FileInfo, cause error) error {
	if activeCreated {
		current, statErr := os.Lstat(path)
		if statErr != nil || activeInfo == nil || !os.SameFile(activeInfo, current) {
			return errors.Join(cause, statErr, fs.ErrExist)
		}
		if err := os.Remove(path); err != nil {
			return errors.Join(cause, err)
		}
	}
	return errors.Join(cause, renamexNPExclusive(rotated, path))
}

func (s *FileLogStore) ListRotatedGenerations(path string) ([]string, error) {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, fmt.Errorf("log path must be a normalized absolute path: %q", path)
	}
	dir := filepath.Dir(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	for _, entry := range entries {
		name := entry.Name()
		if !isRotatedGeneration(path, name) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink == 0 && info.Mode().IsRegular() {
			result = append(result, filepath.Join(dir, name))
		}
	}
	sort.Strings(result)
	return result, nil
}

func isRotatedGeneration(activePath, name string) bool {
	prefix := filepath.Base(activePath) + "."
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	suffix := strings.TrimPrefix(name, prefix)
	suffix = strings.TrimSuffix(suffix, ".gz")
	_, err := time.Parse(rotatedTimestampLayout, suffix)
	return err == nil
}

func (s *FileLogStore) CompressGeneration(path string) (string, error) {
	if err := validateLogPath(path); err != nil {
		return "", err
	}
	if strings.HasSuffix(path, ".gz") {
		return path, nil
	}
	compressed := path + ".gz"
	if _, err := os.Lstat(compressed); err == nil {
		return "", fmt.Errorf("compressed generation already exists: %q", compressed)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	source, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", err
	}
	defer source.Close()
	sourceInfo, err := source.Stat()
	if err != nil || !sourceInfo.Mode().IsRegular() {
		return "", errors.Join(err, fmt.Errorf("compression source must be regular"))
	}
	temp, err := os.CreateTemp(filepath.Dir(path), ".log-compress-*")
	if err != nil {
		return "", err
	}
	tempPath := temp.Name()
	defer os.Remove(tempPath)
	if err := temp.Chmod(logFilePermission); err != nil {
		_ = temp.Close()
		return "", err
	}
	gz := gzip.NewWriter(temp)
	gz.Header.ModTime = sourceInfo.ModTime()
	_, copyErr := io.Copy(gz, source)
	closeGzipErr := gz.Close()
	closeTempErr := temp.Close()
	if copyErr != nil || closeGzipErr != nil || closeTempErr != nil {
		return "", errors.Join(copyErr, closeGzipErr, closeTempErr)
	}
	if err := os.Chtimes(tempPath, sourceInfo.ModTime(), sourceInfo.ModTime()); err != nil {
		return "", err
	}
	if err := renamexNPExclusive(tempPath, compressed); err != nil {
		return "", fmt.Errorf("publish compressed generation: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return "", errors.Join(err, os.Remove(compressed))
	}
	return compressed, nil
}

func (s *FileLogStore) AgeDays(path string, now time.Time) (int, error) {
	if err := validateLogPath(path); err != nil {
		return 0, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	if now.Before(info.ModTime()) {
		return 0, nil
	}
	return int(now.Sub(info.ModTime()) / (24 * time.Hour)), nil
}

func (s *FileLogStore) ListMonthlyMetricsFiles(dir string) ([]string, error) {
	if dir == "" || !filepath.IsAbs(dir) || filepath.Clean(dir) != dir {
		return nil, fmt.Errorf("metrics directory must be a normalized absolute path: %q", dir)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	result := make([]string, 0)
	for _, entry := range entries {
		name := entry.Name()
		if !monthlyMetricsName.MatchString(name) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink == 0 && info.Mode().IsRegular() {
			result = append(result, filepath.Join(dir, name))
		}
	}
	sort.Strings(result)
	return result, nil
}

func (s *FileLogStore) ListPerTaskLogFiles(root string) (map[domain.TaskID][]string, error) {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return nil, fmt.Errorf("task logs root must be a normalized absolute path: %q", root)
	}
	entries, err := os.ReadDir(root)
	if errors.Is(err, fs.ErrNotExist) {
		return map[domain.TaskID][]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	result := make(map[domain.TaskID][]string)
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			continue
		}
		taskID, err := domain.NewTaskID(entry.Name())
		if err != nil {
			continue
		}
		for _, name := range []string{"stdout.log", "stderr.log", "events.jsonl"} {
			path := filepath.Join(root, entry.Name(), name)
			info, err := os.Lstat(path)
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if info.Mode()&os.ModeSymlink == 0 && info.Mode().IsRegular() {
				result[taskID] = append(result[taskID], path)
			}
		}
	}
	return result, nil
}

func (s *FileLogStore) Remove(path string) error {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("log path must be a normalized absolute path: %q", path)
	}
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("refuse to remove non-regular log file: %q", path)
	}
	return os.Remove(path)
}

func (s *FileLogStore) ReopenActiveHandle(path string) error {
	if s.reopen == nil {
		return errReopenUnavailable
	}
	return s.reopen(path)
}

func (s *FileLogStore) LastRotationAt(path string) (time.Time, error) {
	generations, err := s.ListRotatedGenerations(path)
	if err != nil || len(generations) == 0 {
		return time.Time{}, err
	}
	info, err := os.Stat(generations[len(generations)-1])
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}
