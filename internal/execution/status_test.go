package execution

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type statusStoreStub struct {
	snapshot  domain.TaskSnapshot
	err       error
	snapshots []domain.TaskSnapshot
	errs      []error
	calls     int
}

func (s *statusStoreStub) Load(domain.TaskID) (domain.TaskSnapshot, error) {
	if len(s.errs) > s.calls {
		index := s.calls
		s.calls++
		return s.snapshots[index], s.errs[index]
	}
	s.calls++
	return s.snapshot, s.err
}
func (s *statusStoreStub) Save(domain.TaskID, domain.TaskSnapshot) error { return nil }
func (s *statusStoreStub) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	return nil, nil
}
func (s *statusStoreStub) Reserve(domain.TaskID) error            { return nil }
func (s *statusStoreStub) Release(domain.TaskID) error            { return nil }
func (s *statusStoreStub) IsReserved(domain.TaskID) (bool, error) { return false, nil }

func statusPayload(t *testing.T, suffix string) TaskLaunchPayload {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260813-120000-a1b2-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	slug, err := domain.NewSlug(suffix)
	if err != nil {
		t.Fatal(err)
	}
	requestedAt := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	task, _, err := domain.NewTask(id, domain.SubcommandImpl, slug, nil, requestedAt, 1)
	if err != nil {
		t.Fatal(err)
	}
	timeout, err := domain.ResolveTimeout(nil)
	if err != nil {
		t.Fatal(err)
	}
	return TaskLaunchPayload{Task: task, Model: "gpt-5", ResolvedTimeout: timeout}
}

func TestTaskSnapshotProviderSearchesLaunchingBeforeQueue(t *testing.T) {
	queue := NewTaskQueue()
	payload := statusPayload(t, "provider-shared")
	payload.Model = "queue-model"
	queue.Enqueue(payload)
	registry := NewLaunchingTaskRegistry()
	launchingPayload := payload
	launchingPayload.Model = "launching-model"
	launching, err := domain.NewTaskSnapshotFromAdmission(launchingPayload.Task, launchingPayload.ResolvedTimeout, launchingPayload.Model, nil, domain.ExecutionRouteDaemon, launchingPayload.Task.RequestedAt())
	if err != nil {
		t.Fatal(err)
	}
	registry.Register(launching.TaskID, launching)
	provider := NewTaskSnapshotProvider(&statusStoreStub{err: domain.ErrTaskNotFound}, registry, queue, &sync.Mutex{})

	got, err := provider.Snapshot(launching.TaskID)
	if err != nil || got != launching {
		t.Fatalf("snapshot=%#v err=%v", got, err)
	}
	if got.Model != "launching-model" {
		t.Fatalf("snapshot=%#v; launching snapshot was not preferred", got)
	}
}

func TestTaskSnapshotProviderBuildsQueueSnapshotWithRequestedAt(t *testing.T) {
	payload := statusPayload(t, "provider-queued")
	queue := NewTaskQueue()
	queue.Enqueue(payload)
	provider := NewTaskSnapshotProvider(&statusStoreStub{err: domain.ErrTaskNotFound}, NewLaunchingTaskRegistry(), queue, &sync.Mutex{})

	got, err := provider.Snapshot(payload.Task.ID())
	if err != nil || got.TaskID != payload.Task.ID() || !got.RequestedAt.Equal(payload.Task.RequestedAt()) || !got.StateUpdatedAt.Equal(payload.Task.RequestedAt()) {
		t.Fatalf("snapshot=%#v err=%v", got, err)
	}
}

func TestTaskSnapshotProviderDoesNotFallbackAfterStoreFailure(t *testing.T) {
	storeErr := errors.New("read failed")
	provider := NewTaskSnapshotProvider(&statusStoreStub{err: storeErr}, NewLaunchingTaskRegistry(), NewTaskQueue(), &sync.Mutex{})
	id := statusPayload(t, "provider-error").Task.ID()
	if _, err := provider.Snapshot(id); !errors.Is(err, storeErr) {
		t.Fatalf("err=%v", err)
	}
}

func TestTaskSnapshotProviderQueuePositionExcludesLaunching(t *testing.T) {
	queue := NewTaskQueue()
	payload := statusPayload(t, "position")
	queue.Enqueue(payload)
	registry := NewLaunchingTaskRegistry()
	launchingPayload := statusPayload(t, "position-launching")
	launching, err := domain.NewTaskSnapshotFromAdmission(launchingPayload.Task, launchingPayload.ResolvedTimeout, launchingPayload.Model, nil, domain.ExecutionRouteDaemon, launchingPayload.Task.RequestedAt())
	if err != nil {
		t.Fatal(err)
	}
	registry.Register(launching.TaskID, launching)
	provider := NewTaskSnapshotProvider(&statusStoreStub{}, registry, queue, &sync.Mutex{})
	if position, found, err := provider.QueuePosition(payload.Task.ID()); err != nil || !found || position != 1 {
		t.Fatalf("position=%d found=%t err=%v", position, found, err)
	}
	if position, found, err := provider.QueuePosition(launching.TaskID); err != nil || found || position != 0 {
		t.Fatalf("position=%d found=%t err=%v", position, found, err)
	}
}

func TestTaskSnapshotProviderReturnsNotFoundAfterInMemoryMissWithoutRetryingStore(t *testing.T) {
	payload := statusPayload(t, "provider-miss")
	tasks := &statusStoreStub{err: domain.ErrTaskNotFound}
	provider := NewTaskSnapshotProvider(tasks, NewLaunchingTaskRegistry(), NewTaskQueue(), &sync.Mutex{})
	got, err := provider.Snapshot(payload.Task.ID())
	if !errors.Is(err, domain.ErrTaskNotFound) || got.TaskID.String() != "" || tasks.calls != 1 {
		t.Fatalf("snapshot=%#v err=%v calls=%d", got, err, tasks.calls)
	}
}
