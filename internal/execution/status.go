package execution

import (
	"errors"
	"reflect"
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

type taskSnapshotProvider struct {
	tasks     store.TaskStore
	launching LaunchingTaskRegistry
	queue     TaskQueueReader
	queueMu   *sync.Mutex
}

var _ transport.TaskSnapshotProvider = (*taskSnapshotProvider)(nil)

// NewTaskSnapshotProvider constructs the execution-side implementation of the
// transport status boundary.
func NewTaskSnapshotProvider(tasks store.TaskStore, launching LaunchingTaskRegistry, queue TaskQueueReader, queueMu *sync.Mutex) transport.TaskSnapshotProvider {
	if isNilStatusDependency(tasks) || isNilStatusDependency(launching) || isNilStatusDependency(queue) || isNilStatusDependency(queueMu) {
		panic("task snapshot provider requires non-nil dependencies")
	}
	return &taskSnapshotProvider{tasks: tasks, launching: launching, queue: queue, queueMu: queueMu}
}

func (p *taskSnapshotProvider) Snapshot(taskID domain.TaskID) (domain.TaskSnapshot, error) {
	snapshot, err := p.tasks.Load(taskID)
	if err == nil {
		return snapshot, nil
	}
	if !errors.Is(err, domain.ErrTaskNotFound) {
		return domain.TaskSnapshot{}, err
	}

	p.queueMu.Lock()
	if snapshot, found := p.launching.Lookup(taskID); found {
		p.queueMu.Unlock()
		return snapshot, nil
	}
	payload, _, found := p.queue.Lookup(taskID)
	if !found {
		p.queueMu.Unlock()
		return domain.TaskSnapshot{}, domain.ErrTaskNotFound
	}
	snapshot, err = domain.NewTaskSnapshotFromAdmission(payload.Task, payload.ResolvedTimeout, payload.Model, payload.ReasoningEffort, domain.ExecutionRouteDaemon, payload.Task.RequestedAt())
	p.queueMu.Unlock()
	return snapshot, err
}

func (p *taskSnapshotProvider) QueuePosition(taskID domain.TaskID) (int, bool, error) {
	p.queueMu.Lock()
	defer p.queueMu.Unlock()
	return p.queue.QueuePosition(taskID)
}

func isNilStatusDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}
