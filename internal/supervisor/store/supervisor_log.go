package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// Rotation limits are copied from the canonical validation-rules specification.
const supervisorLogMaxBytes int64 = 10000000
const supervisorLogRetentionCount = 5

// LogLevel is the severity of a structured operational log entry.
type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

// SupervisorLogReason is a restricted reason for an operational log entry.
type SupervisorLogReason string

const (
	LogReasonSequenceRestoreFailed SupervisorLogReason = "sequence_restore_failed"
	LogReasonEventWriteFailed      SupervisorLogReason = "event_write_failed"
	LogReasonRotationFailed        SupervisorLogReason = "rotation_failed"
)

// SupervisorLogEntry contains only structured operational metadata, never message text.
type SupervisorLogEntry struct {
	Level     LogLevel                       `json:"level"`
	Kind      supervisor.SupervisorEventKind `json:"kind"`
	SessionID string                         `json:"session_id,omitempty"`
	Reason    SupervisorLogReason            `json:"reason,omitempty"`
}

// SupervisorLog appends structured operational records and rotates its generations.
type SupervisorLog struct {
	mu        sync.Mutex
	path      string
	maxBytes  int64
	retention int
}

// NewSupervisorLog creates the operational log inside the supplied logs directory.
func NewSupervisorLog(logsDir string, maxBytes int64, retention int) (*SupervisorLog, error) {
	if maxBytes < 1 {
		maxBytes = supervisorLogMaxBytes
	}
	if retention < 1 {
		retention = supervisorLogRetentionCount
	}
	if err := os.MkdirAll(logsDir, 0700); err != nil {
		return nil, err
	}
	if err := os.Chmod(logsDir, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(logsDir, "codexd-supervisor.log")
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_APPEND|syscall.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if err = ensureRegular(file); err == nil {
		err = file.Chmod(0600)
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return &SupervisorLog{path: path, maxBytes: maxBytes, retention: retention}, nil
}

// Record appends one validated structured operational entry.
func (l *SupervisorLog) Record(entry SupervisorLogEntry) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !validLogLevel(entry.Level) || !supervisor.ValidSupervisorEventKind(entry.Kind) || entry.Reason != "" && !validLogReason(entry.Reason) {
		return errors.New("invalid supervisor log entry")
	}
	if entry.SessionID != "" {
		if _, err := supervisor.NewSessionID(entry.SessionID); err != nil {
			return err
		}
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	info, err := regularFileInfo(l.path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if info != nil && info.Size()+int64(len(data)) > l.maxBytes {
		if err = rotateLog(l.path, l.retention); err != nil {
			return err
		}
	}
	fd, err := syscall.Open(l.path, syscall.O_CREAT|syscall.O_APPEND|syscall.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	f := os.NewFile(uintptr(fd), l.path)
	if err = ensureRegular(f); err == nil {
		_, err = f.Write(data)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func validLogLevel(level LogLevel) bool {
	return level == LogLevelDebug || level == LogLevelInfo || level == LogLevelWarn || level == LogLevelError
}

func validLogReason(reason SupervisorLogReason) bool {
	return reason == LogReasonSequenceRestoreFailed || reason == LogReasonEventWriteFailed || reason == LogReasonRotationFailed
}

func regularFileInfo(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("not a regular file: %s", path)
	}
	return info, nil
}

func ensureRegular(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("log target is not a regular file")
	}
	return nil
}

func openNoFollow(path string, flag int) (*os.File, error) {
	fd, err := syscall.Open(path, flag|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if err = ensureRegular(file); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}

func repairPartialTail(path string) error {
	file, err := openNoFollow(path, os.O_RDWR)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || info.Size() == 0 {
		return err
	}
	last := make([]byte, 1)
	if _, err = file.ReadAt(last, info.Size()-1); err != nil {
		return err
	}
	if last[0] == '\n' {
		return nil
	}
	_, err = file.WriteAt([]byte{'\n'}, info.Size())
	return err
}

func rotateLog(path string, retention int) error {
	for generation := retention; generation >= 1; generation-- {
		source := path
		if generation > 1 {
			source = fmt.Sprintf("%s.%d", path, generation-1)
		}
		destination := fmt.Sprintf("%s.%d", path, generation)
		if generation == retention {
			if err := os.Remove(destination); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		if _, err := os.Lstat(source); err == nil {
			if err = os.Rename(source, destination); err != nil {
				return err
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}
