package execution_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	executionusecase "github.com/yoshikihorie/codex-runner/internal/execution/usecase"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
	transportusecase "github.com/yoshikihorie/codex-runner/internal/transport/usecase"
)

type recordedAcquire struct {
	taskID domain.TaskID
	paths  []string
}

type recordingPathLockAcquirer struct {
	inner   *execution.AcquirePathLockUseCase
	records []recordedAcquire
}

func (a *recordingPathLockAcquirer) Acquire(taskID domain.TaskID, paths []string) ([]domain.NormalizedPath, error) {
	a.records = append(a.records, recordedAcquire{taskID: taskID, paths: append([]string(nil), paths...)})
	return a.inner.Acquire(taskID, paths)
}

type pathLockIntegrationFixture struct {
	tasksRoot string
	pathStore *store.PathLockFileStore
	queue     execution.TaskQueue
	registry  execution.ActiveTaskRegistry
	admitter  *recordingAdmitter
	starter   *queueIntegrationStarter
	acquirer  *recordingPathLockAcquirer
	releaser  *execution.ReleasePathLockUseCase
	submit    *transportusecase.SubmitTaskUseCase
}

func newPathLockIntegrationFixture(t *testing.T, maxConcurrent int, liveness domain.LivenessLock) pathLockIntegrationFixture {
	t.Helper()
	tasksRoot, lockRoot, mutexRoot := t.TempDir(), t.TempDir(), t.TempDir()
	tasks, err := store.NewFileTaskStore(tasksRoot)
	if err != nil {
		t.Fatal(err)
	}
	pathStore := store.NewPathLockFileStore(lockRoot)
	acquire := execution.NewAcquirePathLockUseCase(store.NewFileMutex(filepath.Join(mutexRoot, "path-locks.lock")), pathStore, liveness, store.NormalizePath, tasks, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	releaser := execution.NewReleasePathLockUseCase(pathStore, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	queue, registry := execution.NewTaskQueue(), execution.NewActiveTaskRegistry()
	const queueMaxDepth = 10
	recordingAdmit := &recordingAdmitter{inner: executionusecase.NewAdmitTaskUseCase(queue, registry, &sync.Mutex{}, maxConcurrent, queueMaxDepth)}
	starter := &queueIntegrationStarter{}
	recordingAcquire := &recordingPathLockAcquirer{inner: acquire}
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	submit := transportusecase.NewSubmitTaskUseCase(tasks, recordingAcquire, releaser, recordingAdmit, queueMaxDepth, starter, queueIntegrationOptions{model: "gpt-5.6-terra"}, domain.ClockFunc(func() time.Time { return now }), slog.New(slog.NewTextHandler(os.Stderr, nil)))
	return pathLockIntegrationFixture{tasksRoot: tasksRoot, pathStore: pathStore, queue: queue, registry: registry, admitter: recordingAdmit, starter: starter, acquirer: recordingAcquire, releaser: releaser, submit: submit}
}

func implInput(t *testing.T, slug string, path string) transportusecase.SubmitTaskInput {
	t.Helper()
	return transportusecase.SubmitTaskInput{Subcommand: "impl", RawSlug: slug, Prompt: "integration prompt", RawPaths: []string{path}, RawWorkingDir: t.TempDir(), RequestedAt: time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)}
}

func submitHandle(t *testing.T, submit *transportusecase.SubmitTaskUseCase, input transportusecase.SubmitTaskInput) transport.Response {
	t.Helper()
	params, err := json.Marshal(map[string]any{"subcommand": input.Subcommand, "slug": input.RawSlug, "prompt": input.Prompt, "paths": input.RawPaths, "working_dir": input.RawWorkingDir})
	if err != nil {
		t.Fatal(err)
	}
	return submit.Handle(transport.Request{RequestID: "request", Params: params})
}

func requireLockOwner(t *testing.T, pathStore *store.PathLockFileStore, taskID domain.TaskID, path domain.NormalizedPath) {
	t.Helper()
	locks, err := pathStore.List()
	if err != nil || len(locks) != 1 || locks[0].TaskID != taskID || len(locks[0].OwnedPaths) != 1 || locks[0].OwnedPaths[0] != path.String() {
		t.Fatal("unexpected persisted path lock owner")
	}
}

func requirePublicError(t *testing.T, response transport.Response, code, key string, detail map[string]any) {
	t.Helper()
	if response.OK || response.Error == nil || response.Error.Code != code || response.Error.MessageKey != key || len(response.Error.Detail) != len(detail) {
		t.Fatal("unexpected public path-lock error response")
	}
	for name, want := range detail {
		if response.Error.Detail[name] != want {
			t.Fatal("unexpected public error detail")
		}
	}
}

// SCN-proto-01-14 and SCN-proto-01-15.
func TestSubmitPathLockIntegrationRejectsConflictingPath(t *testing.T) {
	fixture := newPathLockIntegrationFixture(t, 2, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }))
	path := t.TempDir()
	normalized, err := store.NormalizePath(path, true)
	if err != nil {
		t.Fatal(err)
	}
	first, err := fixture.submit.Execute(context.Background(), implInput(t, "first", path))
	if err != nil {
		t.Fatal(err)
	}
	requireLockOwner(t, fixture.pathStore, first.TaskID, normalized)
	if len(fixture.admitter.records) != 1 || len(fixture.starter.payloads) != 1 || len(fixture.admitter.records[0].input.NormalizedPaths) != 1 || fixture.admitter.records[0].input.NormalizedPaths[0] != normalized || fixture.admitter.records[0].result.LaunchPayload.NormalizedPaths[0] != normalized || fixture.starter.payloads[0].NormalizedPaths[0] != normalized {
		t.Fatal("normalized path did not reach the successful admission payload")
	}
	response := submitHandle(t, fixture.submit, implInput(t, "conflict", path))
	requirePublicError(t, response, "PATH_LOCK_CONFLICT", "error.pathLock.conflict", map[string]any{"path": normalized.String(), "owner_task_id": first.TaskID.String()})
	if len(fixture.acquirer.records) != 2 || len(fixture.admitter.records) != 1 || fixture.queue.Len() != 0 || len(fixture.starter.payloads) != 1 {
		t.Fatal("conflicting submit crossed an execution boundary")
	}
	conflictID := fixture.acquirer.records[1].taskID
	if _, err := os.Stat(filepath.Join(fixture.tasksRoot, conflictID.String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("conflicting task reservation was not released")
	}
	requireLockOwner(t, fixture.pathStore, first.TaskID, normalized)
}

// SCN-queue-01-12.
func TestSubmitPathLockIntegrationSucceedsAfterReleaseAndRetry(t *testing.T) {
	fixture := newPathLockIntegrationFixture(t, 2, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }))
	path := t.TempDir()
	normalized, err := store.NormalizePath(path, true)
	if err != nil {
		t.Fatal(err)
	}
	first, err := fixture.submit.Execute(context.Background(), implInput(t, "first", path))
	if err != nil {
		t.Fatal(err)
	}
	conflictResponse := submitHandle(t, fixture.submit, implInput(t, "conflict", path))
	requirePublicError(t, conflictResponse, "PATH_LOCK_CONFLICT", "error.pathLock.conflict", map[string]any{"path": normalized.String(), "owner_task_id": first.TaskID.String()})
	if len(fixture.acquirer.records) != 2 {
		t.Fatal("conflict did not delegate exactly once")
	}
	if err := fixture.releaser.Release(context.Background(), first.TaskID); err != nil {
		t.Fatal(err)
	}
	second, err := fixture.submit.Execute(context.Background(), implInput(t, "retry", path))
	if err != nil || second.TaskID == first.TaskID || len(fixture.admitter.records) != 2 || fixture.admitter.records[1].result.LaunchPayload == nil || len(fixture.starter.payloads) != 2 {
		t.Fatal("retry was not immediately admitted after release")
	}
	requireLockOwner(t, fixture.pathStore, second.TaskID, normalized)
	if fixture.admitter.records[1].input.NormalizedPaths[0] != normalized || fixture.admitter.records[1].result.LaunchPayload.NormalizedPaths[0] != normalized || fixture.starter.payloads[1].NormalizedPaths[0] != normalized {
		t.Fatal("retry normalized path did not reach payload")
	}
}

// SCN-queue-01-13.
func TestSubmitPathLockIntegrationRunsNonOverlappingTasksWithinLimit(t *testing.T) {
	fixture := newPathLockIntegrationFixture(t, 2, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }))
	firstPath, secondPath := t.TempDir(), t.TempDir()
	first, err := fixture.submit.Execute(context.Background(), implInput(t, "first", firstPath))
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.submit.Execute(context.Background(), implInput(t, "second", secondPath))
	if err != nil || first.TaskID == second.TaskID || fixture.registry.Size() != 2 || fixture.queue.Len() != 0 || len(fixture.admitter.records) != 2 || len(fixture.starter.payloads) != 2 {
		t.Fatal("non-overlapping tasks were not independently admitted")
	}
	for index, rawPath := range []string{firstPath, secondPath} {
		normalized, normalizeErr := store.NormalizePath(rawPath, true)
		if normalizeErr != nil || fixture.admitter.records[index].input.NormalizedPaths[0] != normalized || fixture.admitter.records[index].result.LaunchPayload.NormalizedPaths[0] != normalized || fixture.starter.payloads[index].NormalizedPaths[0] != normalized {
			t.Fatal("non-overlapping normalized path mismatch")
		}
	}
	locks, err := fixture.pathStore.List()
	if err != nil || len(locks) != 2 {
		t.Fatal("expected two persisted path-lock owners")
	}
}

func TestSubmitPathLockIntegrationMapsLivenessFailureToPublicResponse(t *testing.T) {
	owner, err := domain.NewTaskID("impl-20260809-120000-a1b2-owner")
	if err != nil {
		t.Fatal(err)
	}
	fixture := newPathLockIntegrationFixture(t, 2, domain.LivenessLockFunc(func(string) (bool, error) { return false, errors.New("liveness check failure") }))
	path := t.TempDir()
	normalized, err := store.NormalizePath(path, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.pathStore.Save(owner, []domain.NormalizedPath{normalized}); err != nil {
		t.Fatal(err)
	}
	response := submitHandle(t, fixture.submit, implInput(t, "liveness", path))
	requirePublicError(t, response, "LIVENESS_LOCK_IO_ERROR", "error.liveness.lockIoError", map[string]any{"task_id": owner.String()})
	if len(fixture.acquirer.records) != 1 || len(fixture.admitter.records) != 0 || fixture.queue.Len() != 0 || len(fixture.starter.payloads) != 0 {
		t.Fatal("liveness failure crossed an execution boundary")
	}
	requester := fixture.acquirer.records[0].taskID
	if requester == owner {
		t.Fatal("liveness detail used the requester task ID")
	}
	if _, err := os.Stat(filepath.Join(fixture.tasksRoot, requester.String())); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("liveness-failed task reservation was not released")
	}
	requireLockOwner(t, fixture.pathStore, owner, normalized)
}

func TestSubmitPathLockIntegrationKeepsQueuedOwnerWithoutTaskLock(t *testing.T) {
	fixture := newPathLockIntegrationFixture(t, 1, domain.LivenessLockFunc(func(string) (bool, error) { return false, os.ErrNotExist }))
	owner, err := domain.NewTaskID("impl-20260809-120000-a1b2-queued-owner")
	if err != nil {
		t.Fatal(err)
	}
	path := t.TempDir()
	normalized, err := store.NormalizePath(path, true)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := store.NewFileTaskStore(fixture.tasksRoot)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	if err := tasks.Reserve(owner); err != nil {
		t.Fatal(err)
	}
	if err := tasks.Save(owner, domain.TaskSnapshot{TaskID: owner, Subcommand: domain.SubcommandImpl, ResolvedTimeoutSeconds: 1800, Model: "gpt-5.6-terra", RequestedAt: at, Route: domain.ExecutionRouteDaemon, State: domain.StateQueued, StateUpdatedAt: at, SchemaVersion: 1}); err != nil {
		t.Fatal(err)
	}
	if err := fixture.pathStore.Save(owner, []domain.NormalizedPath{normalized}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join("/tmp/codex-tasks", owner.String(), "task.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("queued test owner unexpectedly has task.lock")
	}
	response := submitHandle(t, fixture.submit, implInput(t, "queued-conflict", path))
	requirePublicError(t, response, "PATH_LOCK_CONFLICT", "error.pathLock.conflict", map[string]any{"path": normalized.String(), "owner_task_id": owner.String()})
	requireLockOwner(t, fixture.pathStore, owner, normalized)
}
