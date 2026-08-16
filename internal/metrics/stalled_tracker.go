package metrics

import (
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type stalledEntry struct {
	enteredAt time.Time
	totalMs   int
	active    bool
}

// StalledTimeTracker はタスクごとの閉じた stalled 区間をメモリ上で累積する。
type StalledTimeTracker struct {
	mu      sync.Mutex
	entries map[domain.TaskID]stalledEntry
}

func (t *StalledTimeTracker) EnterStalled(taskID domain.TaskID, occurredAt time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.entries == nil {
		t.entries = make(map[domain.TaskID]stalledEntry)
	}
	entry := t.entries[taskID]
	if entry.active {
		return
	}
	entry.enteredAt = occurredAt
	entry.active = true
	t.entries[taskID] = entry
}

func (t *StalledTimeTracker) LeaveStalled(taskID domain.TaskID, occurredAt time.Time) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, found := t.entries[taskID]
	if !found || !entry.active {
		return entry.totalMs
	}
	entry.totalMs += int(occurredAt.Sub(entry.enteredAt).Milliseconds())
	entry.active = false
	t.entries[taskID] = entry
	return entry.totalMs
}

func (t *StalledTimeTracker) TakeTotal(taskID domain.TaskID) int {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry, found := t.entries[taskID]
	if !found {
		return 0
	}
	delete(t.entries, taskID)
	return entry.totalMs
}
