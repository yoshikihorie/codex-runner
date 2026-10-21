package store

import (
	"os"
	"path/filepath"
)

// WriteAtomic replaces path only after the complete data has reached a same-directory temporary file.
func WriteAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	// Sync the directory entry after rename; failure is best effort because replacement succeeded.
	_ = syncDir(dir)
	return nil
}

// SyncDir syncs a directory after publishing a file. Callers should ignore errors
// when publication has already succeeded, since directory sync is best effort.
func SyncDir(dir string) error {
	return syncDir(dir)
}

// syncDir opens and syncs dir to help preserve rename across power loss.
var syncDir = func(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}
