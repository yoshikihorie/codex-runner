package metrics

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const metricsFilePermission os.FileMode = 0o600

var errMaxFileBytesMustBePositive = errors.New("metrics writer: maxFileBytes must be positive")

type MetricsWriter interface {
	Append(taskID domain.TaskID, month string, line []byte) error
}

type metricsWriteCloser interface {
	Write([]byte) (int, error)
	Chmod(os.FileMode) error
	Close() error
}

type FileMetricsWriter struct {
	logsRoot     string
	maxFileBytes int64
	stat         func(string) (os.FileInfo, error)
	open         func(string, int, os.FileMode) (metricsWriteCloser, error)
}

var _ MetricsWriter = (*FileMetricsWriter)(nil)

func NewFileMetricsWriter(logsRoot string, maxFileBytes int64) *FileMetricsWriter {
	return &FileMetricsWriter{
		logsRoot:     logsRoot,
		maxFileBytes: maxFileBytes,
		stat:         os.Stat,
		open: func(path string, flags int, permission os.FileMode) (metricsWriteCloser, error) {
			return os.OpenFile(path, flags, permission)
		},
	}
}

func (w *FileMetricsWriter) Append(taskID domain.TaskID, month string, line []byte) error {
	if w.maxFileBytes <= 0 {
		return errMaxFileBytesMustBePositive
	}

	if err := validateMonth(month); err != nil {
		return fmt.Errorf("validate month %q for task %s: %w", month, taskID.String(), err)
	}

	path, err := w.selectPath(month)
	if err != nil {
		return fmt.Errorf("select metrics file for task %s: %w", taskID.String(), err)
	}

	file, err := w.open(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY|syscall.O_NOFOLLOW, metricsFilePermission)
	if err != nil {
		return fmt.Errorf("open metrics file for task %s: %w", taskID.String(), err)
	}
	if err := file.Chmod(metricsFilePermission); err != nil {
		_ = file.Close()
		return fmt.Errorf("chmod metrics file for task %s: %w", taskID.String(), err)
	}

	payload := make([]byte, len(line)+1)
	copy(payload, line)
	payload[len(line)] = '\n'
	n, writeErr := file.Write(payload)
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("write metrics file for task %s: %w", taskID.String(), writeErr)
	}
	if n != len(payload) {
		return fmt.Errorf("write metrics file for task %s: %w", taskID.String(), io.ErrShortWrite)
	}
	if closeErr != nil {
		return fmt.Errorf("close metrics file for task %s: %w", taskID.String(), closeErr)
	}
	return nil
}

func validateMonth(month string) error {
	parsed, err := time.Parse("2006-01", month)
	if err != nil {
		return err
	}
	if parsed.Format("2006-01") != month {
		return fmt.Errorf("month must use YYYY-MM format")
	}
	return nil
}

func (w *FileMetricsWriter) selectPath(month string) (string, error) {
	for segment := 1; ; segment++ {
		path := w.path(month, segment)
		info, err := w.stat(path)
		if errors.Is(err, os.ErrNotExist) {
			return path, nil
		}
		if err != nil {
			return "", err
		}
		if info.Size() < w.maxFileBytes {
			return path, nil
		}
	}
}

func (w *FileMetricsWriter) path(month string, segment int) string {
	name := fmt.Sprintf("task-metrics-%s.jsonl", month)
	if segment > 1 {
		name = fmt.Sprintf("task-metrics-%s.%d.jsonl", month, segment)
	}
	return filepath.Join(w.logsRoot, name)
}
