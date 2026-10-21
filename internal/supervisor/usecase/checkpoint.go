package usecase

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// Values copied from validation-rules.md: SUPERVISOR_CHECKPOINT_*.
const (
	checkpointRelativePath     = ".docs/temp/checkpoint.md"
	checkpointHeadLineCount    = 40
	checkpointMaxBytes         = 1048576
	checkpointGenerationMarker = "<!-- CHECKPOINT-GEN -->" // FD-sup-04 §13 PT-17.
)

var errCheckpointUnavailable = errors.New("checkpoint cannot be inspected")

type CheckpointInspector struct{ CWD string }

// Inspect returns only comparison metadata. Errors deliberately omit path and content.
func (i CheckpointInspector) Inspect() (supervisor.RotationCheckpointSnapshot, error) {
	var empty supervisor.RotationCheckpointSnapshot
	unavailable := supervisor.RotationCheckpointSnapshot{Undeterminable: true}
	root, err := filepath.EvalSymlinks(i.CWD)
	if err != nil {
		return unavailable, errCheckpointUnavailable
	}
	path := filepath.Join(i.CWD, checkpointRelativePath)
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if errors.Is(err, os.ErrNotExist) {
		return empty, nil
	}
	if err != nil {
		return unavailable, errCheckpointUnavailable
	}
	rel, err := filepath.Rel(root, parent)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return unavailable, errCheckpointUnavailable
	}
	resolvedPath := filepath.Join(parent, filepath.Base(path))
	fd, err := syscall.Open(resolvedPath, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if errors.Is(err, syscall.ENOENT) {
		return empty, nil
	}
	if err != nil {
		return unavailable, errCheckpointUnavailable
	}
	f := os.NewFile(uintptr(fd), resolvedPath)
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > checkpointMaxBytes {
		return unavailable, errCheckpointUnavailable
	}
	data, err := io.ReadAll(io.LimitReader(f, checkpointMaxBytes))
	if err != nil {
		return unavailable, errCheckpointUnavailable
	}
	info, err = f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > checkpointMaxBytes {
		return unavailable, errCheckpointUnavailable
	}
	hash := sha256.Sum256(checkpointHead(data))
	mtime := info.ModTime().UTC()
	return supervisor.RotationCheckpointSnapshot{Exists: true, Mtime: &mtime, HeadHash: hex.EncodeToString(hash[:])}, nil
}

func checkpointHead(data []byte) []byte {
	lines := strings.SplitAfter(string(data), "\n")
	first, second := -1, -1
	for n, line := range lines {
		if strings.TrimRight(strings.TrimSuffix(line, "\n"), " \t\r") == checkpointGenerationMarker {
			if first < 0 {
				first = n
			} else {
				second = n
				break
			}
		}
	}
	start, end := 0, len(lines)
	if first >= 0 {
		start = first + 1
		if second >= 0 {
			end = second
		}
	} else if end > checkpointHeadLineCount {
		end = checkpointHeadLineCount
	}
	return []byte(strings.Join(lines[start:end], ""))
}

// CheckpointUpdated is the two file based conditions of stage 3.
func CheckpointUpdated(before, after supervisor.RotationCheckpointSnapshot, issuedAt time.Time) bool {
	if before.Undeterminable || after.Undeterminable {
		return false
	}
	if !after.Exists || after.Mtime == nil || !after.Mtime.After(issuedAt) {
		return false
	}
	return !before.Exists || before.HeadHash != after.HeadHash
}
