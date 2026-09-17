package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const (
	// TaskPlacementEvictionMarkerName is the shared death-lease protocol
	// marker used by task-placement and worktree eviction.
	TaskPlacementEvictionMarkerName       = ".eviction-marker"
	taskPlacementMarkerName               = TaskPlacementEvictionMarkerName
	taskPlacementLockName                 = "task.lock"
	taskPlacementMaxDepth                 = 64
	taskPlacementUnknownBytes       int64 = -1
)

// These descriptor-relative seams retain the production syscall behavior while
// allowing safety tests to verify that rejected paths cannot be written.
var (
	taskPlacementOpenAt   = openAt
	taskPlacementUnlinkAt = unlinkAt
	taskPlacementFstat    = syscall.Fstat
)

// TaskPlacementPolicy controls retention and capacity eviction. DiskBudgetBytes
// is measured in bytes; a zero value disables capacity eviction.
type TaskPlacementPolicy struct {
	RetentionDays   int
	DiskBudgetBytes int64
	Now             time.Time
}

type TaskPlacementCandidate struct {
	TaskID    domain.TaskID
	SizeBytes int64
	CreatedAt time.Time
	dev       uint64
	ino       uint64
	retention bool
}

// TaskPlacementCleanupFailure describes one candidate that could not be
// removed.  Execute continues with subsequent candidates and returns these
// failures after synchronizing every successful deletion.
type TaskPlacementCleanupFailure struct {
	TaskID domain.TaskID
	Path   string
	Stage  string
	Reason string
	Err    error
}

func (e *TaskPlacementCleanupFailure) Error() string {
	return fmt.Sprintf("task placement cleanup %s for %s: %v", e.Stage, e.TaskID, e.Err)
}

func (e *TaskPlacementCleanupFailure) Unwrap() error { return e.Err }

type TaskPlacementPlan struct {
	Candidates     []TaskPlacementCandidate
	Failures       []*TaskPlacementCleanupFailure
	CurrentBytes   int64
	ProjectedBytes int64
	root           *os.File
	dev            uint64
	ino            uint64
}

func (p *TaskPlacementPlan) Close() error {
	if p == nil || p.root == nil {
		return nil
	}
	err := p.root.Close()
	p.root = nil
	return err
}

// TaskPlacementFileStore performs retention operations through directory FDs.
type TaskPlacementFileStore struct{ root string }

func NewTaskPlacementFileStore(root string) (*TaskPlacementFileStore, error) {
	p, err := domain.NewNormalizedPath(root)
	if err != nil {
		return nil, err
	}
	return &TaskPlacementFileStore{root: p.String()}, nil
}

func (s *TaskPlacementFileStore) Plan(ctx context.Context, policy TaskPlacementPolicy) (*TaskPlacementPlan, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if policy.RetentionDays < 0 || policy.DiskBudgetBytes < 0 || policy.Now.IsZero() {
		return nil, fmt.Errorf("invalid task placement policy")
	}
	root, err := os.OpenFile(s.root, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return &TaskPlacementPlan{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open task placement root: %w", err)
	}
	var rootStat syscall.Stat_t
	if err := taskPlacementFstat(int(root.Fd()), &rootStat); err != nil {
		_ = root.Close()
		return nil, err
	}
	if rootStat.Uid != uint32(os.Geteuid()) || rootStat.Mode&0o777 != 0o700 {
		_ = root.Close()
		return nil, fmt.Errorf("task placement root must be owned by effective uid and mode 0700")
	}
	plan := &TaskPlacementPlan{root: root, dev: uint64(rootStat.Dev), ino: rootStat.Ino}
	entries, err := readDirFD(int(root.Fd()), s.root)
	if err != nil {
		_ = plan.Close()
		return nil, err
	}
	var all []TaskPlacementCandidate
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			_ = plan.Close()
			return nil, err
		}
		// The placement directory is the root of a candidate tree. Its direct
		// children are depth one in the canonical depth definition.
		size, err := placementEntrySize(int(root.Fd()), entry.Name(), 0)
		if err != nil {
			plan.recordFailure(filepath.Join(s.root, entry.Name()), "measure", err)
			plan.CurrentBytes = taskPlacementUnknownBytes
			continue
		}
		if plan.CurrentBytes != taskPlacementUnknownBytes && plan.CurrentBytes > int64(^uint64(0)>>1)-size {
			_ = plan.Close()
			return nil, fmt.Errorf("task placement size overflow")
		}
		if plan.CurrentBytes != taskPlacementUnknownBytes {
			plan.CurrentBytes += size
		}
		candidate, ok, err := s.candidate(int(root.Fd()), entry.Name(), size, policy)
		if err != nil {
			plan.recordFailure(filepath.Join(s.root, entry.Name()), "inspect", err)
		}
		if ok {
			all = append(all, candidate)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].TaskID.String() < all[j].TaskID.String()
		}
		return all[i].CreatedAt.Before(all[j].CreatedAt)
	})
	projected := plan.CurrentBytes
	for _, c := range all {
		if c.retention {
			plan.Candidates = append(plan.Candidates, c)
			if projected != taskPlacementUnknownBytes {
				projected -= c.SizeBytes
			}
		}
	}
	if projected != taskPlacementUnknownBytes && projected > policy.DiskBudgetBytes && policy.DiskBudgetBytes > 0 {
		for _, c := range all {
			if !c.retention && projected > policy.DiskBudgetBytes {
				plan.Candidates = append(plan.Candidates, c)
				projected -= c.SizeBytes
			}
		}
	}
	plan.ProjectedBytes = projected
	return plan, nil
}

func (p *TaskPlacementPlan) recordFailure(path, stage string, err error) {
	id, _ := domain.NewTaskID(filepath.Base(path))
	p.Failures = append(p.Failures, &TaskPlacementCleanupFailure{
		TaskID: id,
		Path:   path,
		Stage:  stage,
		Reason: taskPlacementFailureReason(err),
		Err:    err,
	})
}

func (s *TaskPlacementFileStore) candidate(rootFD int, name string, size int64, policy TaskPlacementPolicy) (TaskPlacementCandidate, bool, error) {
	id, err := domain.NewTaskID(name)
	if err != nil {
		return TaskPlacementCandidate{}, false, err
	}
	fd, err := taskPlacementOpenAt(rootFD, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return TaskPlacementCandidate{}, false, err
	}
	defer syscall.Close(fd)
	var st syscall.Stat_t
	if err := taskPlacementFstat(fd, &st); err != nil {
		return TaskPlacementCandidate{}, false, err
	}
	dirStat := st
	lease, err := taskPlacementOpenAt(fd, taskPlacementLockName, syscall.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return TaskPlacementCandidate{}, false, nil
		}
		return TaskPlacementCandidate{}, false, err
	}
	if err := taskPlacementFstat(lease, &st); err != nil {
		_ = syscall.Close(lease)
		return TaskPlacementCandidate{}, false, err
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFREG {
		_ = syscall.Close(lease)
		return TaskPlacementCandidate{}, false, fmt.Errorf("task lock is not a regular file")
	}
	if err := syscall.Flock(lease, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = syscall.Close(lease)
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return TaskPlacementCandidate{}, false, nil
		}
		return TaskPlacementCandidate{}, false, err
	}
	defer syscall.Close(lease)
	terminal, err := placementSnapshotTerminal(fd)
	if err != nil {
		return TaskPlacementCandidate{}, false, err
	}
	if !terminal {
		return TaskPlacementCandidate{}, false, nil
	}
	created, err := taskPlacementCreatedAt(id)
	if err != nil {
		return TaskPlacementCandidate{}, false, err
	}
	age := policy.Now.Sub(created)
	if age < 0 {
		return TaskPlacementCandidate{}, false, nil
	}
	return TaskPlacementCandidate{TaskID: id, SizeBytes: size, CreatedAt: created, dev: uint64(dirStat.Dev), ino: dirStat.Ino, retention: age >= time.Duration(policy.RetentionDays)*24*time.Hour}, true, nil
}

func (s *TaskPlacementFileStore) Execute(ctx context.Context, plan *TaskPlacementPlan) ([]domain.TaskID, error) {
	if plan == nil || plan.root == nil {
		return nil, nil
	}
	var st syscall.Stat_t
	if err := taskPlacementFstat(int(plan.root.Fd()), &st); err != nil {
		return nil, fmt.Errorf("recheck planned task placement root: %w", err)
	}
	if uint64(st.Dev) != plan.dev || st.Ino != plan.ino {
		return nil, fmt.Errorf("planned task placement root identity changed")
	}
	current, err := os.OpenFile(s.root, os.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open current task placement root: %w", err)
	}
	defer current.Close()
	if err := taskPlacementFstat(int(current.Fd()), &st); err != nil {
		return nil, fmt.Errorf("recheck current task placement root: %w", err)
	}
	if uint64(st.Dev) != plan.dev || st.Ino != plan.ino {
		return nil, fmt.Errorf("current task placement root identity changed")
	}
	deleted := make([]domain.TaskID, 0, len(plan.Candidates))
	failures := make([]error, 0, len(plan.Failures)+len(plan.Candidates)+1)
	for _, failure := range plan.Failures {
		failures = append(failures, failure)
	}
	for _, candidate := range plan.Candidates {
		if err := ctx.Err(); err != nil {
			return deleted, errors.Join(append(failures, err)...)
		}
		ok, err := deletePlacementAt(int(plan.root.Fd()), candidate)
		if err != nil {
			failures = append(failures, &TaskPlacementCleanupFailure{
				TaskID: candidate.TaskID, Path: filepath.Join(s.root, candidate.TaskID.String()),
				Stage: "delete", Reason: taskPlacementFailureReason(err), Err: err,
			})
			continue
		}
		if ok {
			deleted = append(deleted, candidate.TaskID)
		}
	}
	return deleted, errors.Join(failures...)
}

func deletePlacementAt(rootFD int, c TaskPlacementCandidate) (deleted bool, retErr error) {
	name := c.TaskID.String()
	if !safeEntryName(name) {
		return false, nil
	}
	fd, err := taskPlacementOpenAt(rootFD, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	defer syscall.Close(fd)
	var st syscall.Stat_t
	if err := taskPlacementFstat(fd, &st); err != nil {
		return false, err
	} else if uint64(st.Dev) != c.dev || st.Ino != c.ino {
		return false, fmt.Errorf("task placement candidate identity changed")
	}
	lockFD, err := taskPlacementOpenAt(fd, taskPlacementLockName, syscall.O_RDWR|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	defer syscall.Close(lockFD)
	if err := taskPlacementFstat(lockFD, &st); err != nil {
		return false, err
	}
	if st.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return false, fmt.Errorf("task lock is not a regular file")
	}
	if err := syscall.Flock(lockFD, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return false, nil
		}
		return false, err
	}
	terminal, err := placementSnapshotTerminal(fd)
	if err != nil {
		return false, err
	}
	if !terminal {
		return false, nil
	}
	if err := ensureTaskPlacementMarker(fd); err != nil {
		return false, err
	}
	markerUnlinked := false
	defer func() {
		if retErr != nil && markerUnlinked {
			retErr = errors.Join(retErr, restoreTaskPlacementMarker(fd))
		}
	}()
	if err := removePlacementContents(fd, 0); err != nil {
		return false, err
	}
	if err := taskPlacementUnlinkAt(fd, taskPlacementMarkerName, 0); err != nil {
		return false, err
	}
	markerUnlinked = true
	var currentLockStat syscall.Stat_t
	if err := fstatAt(fd, taskPlacementLockName, &currentLockStat, atSymlinkNoFollow); err != nil {
		return false, err
	}
	if currentLockStat.Mode&syscall.S_IFMT != syscall.S_IFREG || currentLockStat.Dev != st.Dev || currentLockStat.Ino != st.Ino {
		return false, fmt.Errorf("task lock identity changed")
	}
	if err := taskPlacementUnlinkAt(fd, taskPlacementLockName, 0); err != nil {
		return false, err
	}
	if err := taskPlacementUnlinkAt(rootFD, name, atRemoveDir); err != nil {
		return false, err
	}
	return true, nil
}

func taskPlacementFailureReason(err error) string {
	switch {
	case errors.Is(err, syscall.EACCES):
		return "access_denied"
	case errors.Is(err, syscall.EMFILE), errors.Is(err, syscall.ENFILE):
		return "file_descriptor_limit"
	case errors.Is(err, syscall.ELOOP):
		return "symlink_loop"
	case errors.Is(err, syscall.ENOTDIR):
		return "not_directory"
	default:
		return "delete_failed"
	}
}

func removePlacementContents(fd, depth int) error {
	return removeTreeContentsAt(fd, depth, taskPlacementMaxDepth, func(name string) bool {
		return name == taskPlacementLockName || name == taskPlacementMarkerName
	})
}

func ensureTaskPlacementMarker(dirFD int) error {
	marker, err := taskPlacementOpenAt(dirFD, taskPlacementMarkerName, syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err == nil {
		return syscall.Close(marker)
	}
	if !errors.Is(err, fs.ErrExist) {
		return err
	}
	var markerStat syscall.Stat_t
	if err := fstatAt(dirFD, taskPlacementMarkerName, &markerStat, atSymlinkNoFollow); err != nil {
		return err
	}
	if markerStat.Mode&syscall.S_IFMT != syscall.S_IFREG {
		return fmt.Errorf("task placement marker is not a regular file")
	}
	return nil
}

func restoreTaskPlacementMarker(dirFD int) error {
	return ensureTaskPlacementMarker(dirFD)
}

func placementSnapshotTerminal(dirFD int) (bool, error) {
	fd, err := taskPlacementOpenAt(dirFD, "task.json", syscall.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	f := os.NewFile(uintptr(fd), "task.json")
	defer f.Close()
	var snapshot struct {
		State domain.TaskState `json:"state"`
	}
	if err := json.NewDecoder(f).Decode(&snapshot); err != nil {
		return false, err
	}
	return snapshot.State.IsTerminal(), nil
}

func taskPlacementCreatedAt(id domain.TaskID) (time.Time, error) {
	parts := strings.Split(id.String(), "-")
	if len(parts) < 4 {
		return time.Time{}, fmt.Errorf("invalid task ID")
	}
	return time.ParseInLocation("20060102 150405", parts[1]+" "+parts[2], time.Local)
}
func safeEntryName(name string) bool {
	return name != "" && name != "." && name != ".." && !strings.Contains(name, "/")
}
func readDirFD(fd int, name string) ([]os.DirEntry, error) {
	duplicate, err := syscall.Dup(fd)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(duplicate), name)
	entries, err := f.ReadDir(-1)
	closeErr := f.Close()
	if err != nil {
		return nil, err
	}
	return entries, closeErr
}
func placementEntrySize(parentFD int, name string, depth int) (int64, error) {
	if depth > taskPlacementMaxDepth {
		return 0, fmt.Errorf("task placement depth exceeds %d", taskPlacementMaxDepth)
	}
	var st syscall.Stat_t
	if err := fstatAt(parentFD, name, &st, atSymlinkNoFollow); err != nil {
		return 0, err
	}
	total := int64(st.Blocks) * 512
	if st.Mode&syscall.S_IFMT != syscall.S_IFDIR {
		return total, nil
	}
	fd, err := taskPlacementOpenAt(parentFD, name, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return 0, err
	}
	defer syscall.Close(fd)
	entries, err := readDirFD(fd, name)
	if err != nil {
		return 0, err
	}
	for _, entry := range entries {
		child, err := placementEntrySize(fd, entry.Name(), depth+1)
		if err != nil {
			return 0, err
		}
		if total > int64(^uint64(0)>>1)-child {
			return 0, fmt.Errorf("task placement size overflow")
		}
		total += child
	}
	return total, nil
}
