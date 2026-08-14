package execution

import (
	"encoding/json"
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

// TaskChangeNotifier provides wakeups for a task whose persisted state changed.
type TaskChangeNotifier interface {
	Subscribe(taskID domain.TaskID) (ch <-chan struct{}, unsubscribe func())
}

type taskChangeBroadcaster struct {
	mu          sync.Mutex
	subscribers map[domain.TaskID]map[uint64]chan struct{}
	nextID      uint64
}

var _ TaskChangeNotifier = (*taskChangeBroadcaster)(nil)

func NewTaskChangeNotifier() *taskChangeBroadcaster {
	return &taskChangeBroadcaster{
		subscribers: make(map[domain.TaskID]map[uint64]chan struct{}),
	}
}

func (b *taskChangeBroadcaster) Subscribe(taskID domain.TaskID) (<-chan struct{}, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.nextID++
	subscriptionID := b.nextID
	ch := make(chan struct{}, 1)
	if b.subscribers[taskID] == nil {
		b.subscribers[taskID] = make(map[uint64]chan struct{})
	}
	b.subscribers[taskID][subscriptionID] = ch

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			delete(b.subscribers[taskID], subscriptionID)
			if len(b.subscribers[taskID]) == 0 {
				delete(b.subscribers, taskID)
			}
		})
	}
}

func (b *taskChangeBroadcaster) notify(taskID domain.TaskID) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, ch := range b.subscribers[taskID] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

type notifyingContractWriter struct {
	contract.ContractWriter
	notifier *taskChangeBroadcaster
}

var _ contract.ContractWriter = (*notifyingContractWriter)(nil)

func NewNotifyingContractWriter(delegate contract.ContractWriter, notifier *taskChangeBroadcaster) contract.ContractWriter {
	return &notifyingContractWriter{ContractWriter: delegate, notifier: notifier}
}

func (w *notifyingContractWriter) AppendEvent(taskID domain.TaskID, event domain.Event) error {
	if err := w.ContractWriter.AppendEvent(taskID, event); err != nil {
		return err
	}
	w.notifier.notify(taskID)
	return nil
}

func (w *notifyingContractWriter) AppendRawEvent(taskID domain.TaskID, eventType string, raw json.RawMessage) error {
	if err := w.ContractWriter.AppendRawEvent(taskID, eventType, raw); err != nil {
		return err
	}
	w.notifier.notify(taskID)
	return nil
}

type notifyingTaskStore struct {
	store.TaskStore
	notifier *taskChangeBroadcaster
}

var _ store.TaskStore = (*notifyingTaskStore)(nil)

func NewNotifyingTaskStore(delegate store.TaskStore, notifier *taskChangeBroadcaster) store.TaskStore {
	return &notifyingTaskStore{TaskStore: delegate, notifier: notifier}
}

func (s *notifyingTaskStore) Save(taskID domain.TaskID, snapshot domain.TaskSnapshot) error {
	if err := s.TaskStore.Save(taskID, snapshot); err != nil {
		return err
	}
	s.notifier.notify(taskID)
	return nil
}
