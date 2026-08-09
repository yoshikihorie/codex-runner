package execution

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"runtime"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// PathLockMutex serializes the complete path-lock acquisition sequence.
type PathLockMutex interface {
	Lock() error
	Unlock() error
}

// PathLockSnapshot is a persisted lock read with unnormalized path strings.
// The alias keeps the execution-layer contract while avoiding a store-to-execution import cycle.
type PathLockSnapshot = domain.PathLockSnapshot

// PathLockStore persists path lock snapshots.
type PathLockStore interface {
	List() ([]PathLockSnapshot, error)
	Save(taskID domain.TaskID, paths []domain.NormalizedPath) error
	Delete(taskID domain.TaskID) error
}

// PathLockTaskStateReader reads persisted task state for liveness disambiguation.
type PathLockTaskStateReader interface {
	Load(domain.TaskID) (domain.TaskSnapshot, error)
}

type AcquirePathLockInput struct {
	TaskID         domain.TaskID
	RequestedPaths []string
}

type AcquirePathLockOutput struct {
	Acquired          bool
	ConflictingTaskID *domain.TaskID
	ConflictingPath   *domain.NormalizedPath
	NormalizedPaths   []domain.NormalizedPath
}

type PathLockConflictError struct {
	TaskID domain.TaskID
	Path   domain.NormalizedPath
}

func (e *PathLockConflictError) Error() string {
	return fmt.Sprintf("path lock conflict: task %s owns %s", e.TaskID, e.Path)
}
func (e *PathLockConflictError) Unwrap() error { return domain.ErrPathLockConflict }

type LivenessCheckError struct {
	TaskID domain.TaskID
	Err    error
}

func (e *LivenessCheckError) Error() string {
	return fmt.Sprintf("liveness check failed for task %s: %v", e.TaskID, e.Err)
}
func (e *LivenessCheckError) Unwrap() error { return e.Err }

type ReleasePathLockInput struct {
	TaskID domain.TaskID
}

type normalizePathFunc func(raw string, isMacOS bool) (domain.NormalizedPath, error)

// AcquirePathLockUseCase acquires ownership of requested paths for a task.
type AcquirePathLockUseCase struct {
	mutex       PathLockMutex
	store       PathLockStore
	liveness    domain.LivenessLock
	normalizeFn normalizePathFunc
	tasks       PathLockTaskStateReader
	logger      *slog.Logger
}

// NewAcquirePathLockUseCase constructs an acquirer. logger is optional and defaults to slog.Default.
func NewAcquirePathLockUseCase(mutex PathLockMutex, store PathLockStore, liveness domain.LivenessLock, normalizeFn normalizePathFunc, tasks PathLockTaskStateReader, loggers ...*slog.Logger) *AcquirePathLockUseCase {
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &AcquirePathLockUseCase{mutex: mutex, store: store, liveness: liveness, normalizeFn: normalizeFn, tasks: tasks, logger: logger}
}

// Execute atomically checks, repairs, and creates a path-lock snapshot.
//
// Known limitation: NormalizePath resolves symbolic links before this method persists ownership,
// but the current output schema cannot return that normalized path to the caller. A link changed
// between normalization and the caller's write can therefore make ownership differ from the write target.
func (uc *AcquirePathLockUseCase) Execute(_ context.Context, in AcquirePathLockInput) (out AcquirePathLockOutput, err error) {
	if err = uc.mutex.Lock(); err != nil {
		uc.logger.Error("acquire path lock mutex", "task_id", in.TaskID.String(), "stage", "Lock", "error", ErrorTypeName(err))
		return AcquirePathLockOutput{}, fmt.Errorf("%w: %v", domain.ErrPathLockInfraFailure, err)
	}
	defer func() {
		if unlockErr := uc.mutex.Unlock(); unlockErr != nil {
			uc.logger.Error("release path lock mutex", "task_id", in.TaskID.String(), "stage", "Unlock", "error", ErrorTypeName(unlockErr))
			if err != nil {
				err = errors.Join(err, unlockErr)
			}
		}
	}()

	snapshots, err := uc.store.List()
	if err != nil {
		uc.logger.Error("list path locks", "task_id", in.TaskID.String(), "stage", "List", "error", ErrorTypeName(err))
		return AcquirePathLockOutput{}, fmt.Errorf("%w: %v", domain.ErrPathLockInfraFailure, err)
	}
	survivors := make([]PathLockSnapshot, 0, len(snapshots))
	staleTaskIDs := make([]domain.TaskID, 0, len(snapshots))
	for _, snapshot := range snapshots {
		dead, livenessErr := uc.liveness.TryAcquire(taskLockPath(snapshot.TaskID))
		if errors.Is(livenessErr, fs.ErrNotExist) {
			task, loadErr := uc.tasks.Load(snapshot.TaskID)
			switch {
			case errors.Is(loadErr, domain.ErrTaskNotFound):
				dead = true
			case loadErr != nil:
				uc.logger.Error("read task state for path lock liveness", "task_id", in.TaskID.String(), "confirmed_task_id", snapshot.TaskID.String(), "stage", "load_task_state", "error", ErrorTypeName(loadErr))
				return AcquirePathLockOutput{}, fmt.Errorf("%w: %v", domain.ErrPathLockInfraFailure, loadErr)
			case task.State == domain.StateQueued || task.State == domain.StateStarting:
				dead = false
			default:
				dead = true
			}
		} else if livenessErr != nil {
			uc.logger.Error("check path lock liveness", "task_id", in.TaskID.String(), "confirmed_task_id", snapshot.TaskID.String(), "operation", "liveness_check", "error", ErrorTypeName(livenessErr))
			return AcquirePathLockOutput{}, &LivenessCheckError{TaskID: snapshot.TaskID, Err: livenessErr}
		}
		if dead {
			staleTaskIDs = append(staleTaskIDs, snapshot.TaskID)
			continue
		}
		survivors = append(survivors, snapshot)
	}
	for _, taskID := range staleTaskIDs {
		if deleteErr := uc.store.Delete(taskID); deleteErr != nil {
			uc.logger.Error("delete stale path lock", "task_id", in.TaskID.String(), "stage", "Delete", "error", ErrorTypeName(deleteErr))
			return AcquirePathLockOutput{}, fmt.Errorf("%w: %v", domain.ErrPathLockInfraFailure, deleteErr)
		}
		uc.logger.Info("removed stale path lock", "task_id", taskID.String())
	}

	if len(in.RequestedPaths) == 0 {
		return AcquirePathLockOutput{Acquired: true, NormalizedPaths: []domain.NormalizedPath{}}, nil
	}

	requested, err := uc.normalizeAll(in.RequestedPaths)
	if err != nil {
		uc.logger.Error("normalize requested path locks", "task_id", in.TaskID.String(), "stage", "normalize-requested", "error", ErrorTypeName(err))
		return AcquirePathLockOutput{}, fmt.Errorf("%w: %v", domain.ErrPathLockInfraFailure, err)
	}
	active := make([]*domain.PathLock, 0, len(survivors))
	for _, snapshot := range survivors {
		ownedPaths, normalizeErr := uc.normalizeAll(snapshot.OwnedPaths)
		if normalizeErr != nil {
			uc.logger.Error("normalize stored path locks", "task_id", in.TaskID.String(), "stage", "normalize-stored", "error", ErrorTypeName(normalizeErr))
			return AcquirePathLockOutput{}, fmt.Errorf("%w: %v", domain.ErrPathLockInfraFailure, normalizeErr)
		}
		active = append(active, &domain.PathLock{TaskID: snapshot.TaskID, OwnedPaths: ownedPaths})
	}

	lock, err := domain.Acquire(in.TaskID, requested, active, time.Now())
	if errors.Is(err, domain.ErrPathLockConflict) {
		conflictingTaskID, conflictingPath := findConflict(requested, active)
		uc.logger.Info("path lock conflict", "task_id", in.TaskID.String(), "conflicting_task_id", conflictingTaskID.String())
		return AcquirePathLockOutput{Acquired: false, ConflictingTaskID: &conflictingTaskID, ConflictingPath: &conflictingPath}, nil
	}
	if err != nil {
		return AcquirePathLockOutput{}, err
	}
	if err := uc.store.Save(lock.TaskID, lock.OwnedPaths); err != nil {
		uc.logger.Error("save path lock", "task_id", in.TaskID.String(), "stage", "Save", "error", ErrorTypeName(err))
		return AcquirePathLockOutput{}, fmt.Errorf("%w: %v", domain.ErrPathLockInfraFailure, err)
	}
	normalized := make([]domain.NormalizedPath, len(lock.OwnedPaths))
	copy(normalized, lock.OwnedPaths)
	return AcquirePathLockOutput{Acquired: true, NormalizedPaths: normalized}, nil
}

// Acquire adapts Execute to the PathLockAcquirer boundary.
func (uc *AcquirePathLockUseCase) Acquire(taskID domain.TaskID, rawPaths []string) ([]domain.NormalizedPath, error) {
	out, err := uc.Execute(context.Background(), AcquirePathLockInput{TaskID: taskID, RequestedPaths: rawPaths})
	if err != nil {
		return nil, err
	}
	if !out.Acquired {
		return nil, &PathLockConflictError{TaskID: *out.ConflictingTaskID, Path: *out.ConflictingPath}
	}
	return out.NormalizedPaths, nil
}

func (uc *AcquirePathLockUseCase) normalizeAll(rawPaths []string) ([]domain.NormalizedPath, error) {
	normalized := make([]domain.NormalizedPath, 0, len(rawPaths))
	for _, raw := range rawPaths {
		path, err := uc.normalizeFn(raw, runtime.GOOS == "darwin")
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, path)
	}
	return normalized, nil
}

func findConflict(requested []domain.NormalizedPath, active []*domain.PathLock) (domain.TaskID, domain.NormalizedPath) {
	for _, lock := range active {
		for _, owned := range lock.OwnedPaths {
			for _, path := range requested {
				if path.String() == owned.String() {
					return lock.TaskID, path
				}
			}
		}
	}
	return domain.TaskID{}, domain.NormalizedPath{}
}

// ErrorTypeName returns an error's concrete type without exposing its message.
func ErrorTypeName(err error) string {
	return fmt.Sprintf("%T", err)
}

// ReleasePathLockUseCase removes a task's persisted path lock.
type ReleasePathLockUseCase struct {
	store  PathLockStore
	logger *slog.Logger
}

// NewReleasePathLockUseCase constructs a releaser. logger is optional and defaults to slog.Default.
func NewReleasePathLockUseCase(store PathLockStore, loggers ...*slog.Logger) *ReleasePathLockUseCase {
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &ReleasePathLockUseCase{store: store, logger: logger}
}

// Execute deletes the path-lock snapshot without taking the acquisition mutex.
func (uc *ReleasePathLockUseCase) Execute(_ context.Context, in ReleasePathLockInput) error {
	if err := uc.store.Delete(in.TaskID); err != nil {
		return err
	}
	uc.logger.Info("released path lock", "task_id", in.TaskID.String())
	return nil
}

// Release adapts Execute to the PathLockReleaser boundary.
func (uc *ReleasePathLockUseCase) Release(ctx context.Context, taskID domain.TaskID) error {
	return uc.Execute(ctx, ReleasePathLockInput{TaskID: taskID})
}

func taskLockPath(taskID domain.TaskID) string {
	return DefaultLockPathResolver(taskID)
}
