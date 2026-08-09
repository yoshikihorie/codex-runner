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

type AcquirePathLockInput struct {
	TaskID         domain.TaskID
	RequestedPaths []string
}

type AcquirePathLockOutput struct {
	Acquired          bool
	ConflictingTaskID *domain.TaskID
	ConflictingPath   *domain.NormalizedPath
}

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
	logger      *slog.Logger
}

// NewAcquirePathLockUseCase constructs an acquirer. logger is optional and defaults to slog.Default.
func NewAcquirePathLockUseCase(mutex PathLockMutex, store PathLockStore, liveness domain.LivenessLock, normalizeFn normalizePathFunc, loggers ...*slog.Logger) *AcquirePathLockUseCase {
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &AcquirePathLockUseCase{mutex: mutex, store: store, liveness: liveness, normalizeFn: normalizeFn, logger: logger}
}

// Execute atomically checks, repairs, and creates a path-lock snapshot.
//
// Known limitation: NormalizePath resolves symbolic links before this method persists ownership,
// but the current output schema cannot return that normalized path to the caller. A link changed
// between normalization and the caller's write can therefore make ownership differ from the write target.
func (uc *AcquirePathLockUseCase) Execute(_ context.Context, in AcquirePathLockInput) (out AcquirePathLockOutput, err error) {
	if err = uc.mutex.Lock(); err != nil {
		return AcquirePathLockOutput{}, err
	}
	defer func() {
		if unlockErr := uc.mutex.Unlock(); unlockErr != nil {
			uc.logger.Error("release path lock mutex", "error", unlockErr)
			err = errors.Join(err, unlockErr)
		}
	}()

	snapshots, err := uc.store.List()
	if err != nil {
		return AcquirePathLockOutput{}, err
	}
	survivors := make([]PathLockSnapshot, 0, len(snapshots))
	staleTaskIDs := make([]domain.TaskID, 0, len(snapshots))
	for _, snapshot := range snapshots {
		dead, livenessErr := uc.liveness.TryAcquire(taskLockPath(snapshot.TaskID))
		if errors.Is(livenessErr, fs.ErrNotExist) {
			dead = true
		} else if livenessErr != nil {
			return AcquirePathLockOutput{}, livenessErr
		}
		if dead {
			staleTaskIDs = append(staleTaskIDs, snapshot.TaskID)
			continue
		}
		survivors = append(survivors, snapshot)
	}
	for _, taskID := range staleTaskIDs {
		if deleteErr := uc.store.Delete(taskID); deleteErr != nil {
			return AcquirePathLockOutput{}, deleteErr
		}
		uc.logger.Info("removed stale path lock", "task_id", taskID.String())
	}

	if len(in.RequestedPaths) == 0 {
		return AcquirePathLockOutput{Acquired: true}, nil
	}

	requested, err := uc.normalizeAll(in.RequestedPaths)
	if err != nil {
		return AcquirePathLockOutput{}, err
	}
	active := make([]*domain.PathLock, 0, len(survivors))
	for _, snapshot := range survivors {
		ownedPaths, normalizeErr := uc.normalizeAll(snapshot.OwnedPaths)
		if normalizeErr != nil {
			return AcquirePathLockOutput{}, normalizeErr
		}
		active = append(active, &domain.PathLock{TaskID: snapshot.TaskID, OwnedPaths: ownedPaths})
	}

	lock, err := domain.Acquire(in.TaskID, requested, active, time.Now())
	if errors.Is(err, domain.ErrPathLockConflict) {
		conflictingTaskID, conflictingPath := findConflict(requested, active)
		uc.logger.Info("path lock conflict", "task_id", in.TaskID.String(), "conflicting_task_id", conflictingTaskID.String(), "conflicting_path", conflictingPath.String())
		return AcquirePathLockOutput{Acquired: false, ConflictingTaskID: &conflictingTaskID, ConflictingPath: &conflictingPath}, nil
	}
	if err != nil {
		return AcquirePathLockOutput{}, err
	}
	if err := uc.store.Save(lock.TaskID, lock.OwnedPaths); err != nil {
		uc.logger.Error("save path lock", "task_id", in.TaskID.String(), "error", err)
		return AcquirePathLockOutput{}, err
	}
	return AcquirePathLockOutput{Acquired: true}, nil
}

// Acquire adapts Execute to the PathLockAcquirer boundary.
func (uc *AcquirePathLockUseCase) Acquire(taskID domain.TaskID, rawPaths []string) error {
	out, err := uc.Execute(context.Background(), AcquirePathLockInput{TaskID: taskID, RequestedPaths: rawPaths})
	if err != nil {
		return err
	}
	if !out.Acquired {
		return fmt.Errorf("%w: conflicting task %s at %s", domain.ErrPathLockConflict, out.ConflictingTaskID, out.ConflictingPath)
	}
	return nil
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
