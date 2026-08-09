package store

import (
	"sync"
	"sync/atomic"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type TaskMutex struct{ locks sync.Map }

type taskLock struct {
	mutex sync.Mutex
	held  atomic.Bool
}

func NewTaskMutex() *TaskMutex { return &TaskMutex{} }
func (m *TaskMutex) Lock(id domain.TaskID) {
	v, _ := m.locks.LoadOrStore(id.String(), &taskLock{})
	lock := v.(*taskLock)
	lock.mutex.Lock()
	lock.held.Store(true)
}
func (m *TaskMutex) Unlock(id domain.TaskID) {
	if v, ok := m.locks.Load(id.String()); ok {
		lock := v.(*taskLock)
		if !lock.held.CompareAndSwap(true, false) {
			panic("TaskMutex: unlock of unlocked task mutex")
		}
		lock.mutex.Unlock()
	}
}
