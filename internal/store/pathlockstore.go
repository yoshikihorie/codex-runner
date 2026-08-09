package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const (
	pathLocksDirPerm  = 0o700
	pathLocksFilePerm = 0o600
	pathLocksDirName  = "path-locks"
	pathLocksLockName = "path-locks.lock"
)

// PathLockFileStore persists one path-lock snapshot per task.
type PathLockFileStore struct {
	dir string
}

func NewPathLockFileStore(dir string) *PathLockFileStore {
	return &PathLockFileStore{dir: dir}
}

// List reads every persisted path-lock snapshot. A missing directory means no locks exist yet.
func (s *PathLockFileStore) List() ([]domain.PathLockSnapshot, error) {
	entries, err := os.ReadDir(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return []domain.PathLockSnapshot{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read path locks directory: %w", err)
	}

	locks := make([]domain.PathLockSnapshot, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		taskID, err := domain.NewTaskID(strings.TrimSuffix(entry.Name(), ".json"))
		if err != nil {
			return nil, fmt.Errorf("invalid path lock filename %q: %w", entry.Name(), err)
		}
		path := filepath.Join(s.dir, entry.Name())
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("stat path lock %q: %w", entry.Name(), err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("path lock %q is a symbolic link", entry.Name())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read path lock %q: %w", entry.Name(), err)
		}
		var ownedPaths []string
		if err := json.Unmarshal(data, &ownedPaths); err != nil {
			return nil, fmt.Errorf("decode path lock %q: %w", entry.Name(), err)
		}
		if strings.TrimSpace(string(data)) == "null" {
			return nil, fmt.Errorf("path lock %q is null", entry.Name())
		}
		locks = append(locks, domain.PathLockSnapshot{TaskID: taskID, OwnedPaths: ownedPaths})
	}
	return locks, nil
}

// Save atomically persists normalized paths for taskID.
func (s *PathLockFileStore) Save(taskID domain.TaskID, paths []domain.NormalizedPath) error {
	if err := os.MkdirAll(s.dir, pathLocksDirPerm); err != nil {
		return fmt.Errorf("create path locks directory: %w", err)
	}
	info, err := os.Lstat(s.dir)
	if err != nil {
		return fmt.Errorf("stat path locks directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("path locks directory is a symbolic link")
	}
	if err := os.Chmod(s.dir, pathLocksDirPerm); err != nil {
		return fmt.Errorf("secure path locks directory: %w", err)
	}
	data, err := json.Marshal(paths)
	if err != nil {
		return fmt.Errorf("encode path lock: %w", err)
	}
	if err := WriteAtomic(s.path(taskID), data, pathLocksFilePerm); err != nil {
		return fmt.Errorf("write path lock: %w", err)
	}
	return nil
}

// Delete removes taskID's snapshot. Missing snapshots are already released.
func (s *PathLockFileStore) Delete(taskID domain.TaskID) error {
	err := os.Remove(s.path(taskID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete path lock: %w", err)
	}
	return nil
}

func (s *PathLockFileStore) path(taskID domain.TaskID) string {
	return filepath.Join(s.dir, taskID.String()+".json")
}

// DefaultPathLocksDir returns the standard per-task path lock directory.
func DefaultPathLocksDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home directory: %w", err)
	}
	return filepath.Join(home, ".claude", "run", pathLocksDirName), nil
}

// DefaultPathLocksMutexPath returns the standard path lock mutex location.
func DefaultPathLocksMutexPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve user home directory: %w", err)
	}
	return filepath.Join(home, ".claude", "run", pathLocksLockName), nil
}
