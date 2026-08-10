package recovery

import (
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type PendingEntry struct {
	taskID     domain.TaskID
	signalSent bool
}

type PendingRegistrar interface {
	Register(taskID domain.TaskID, signalSent bool) error
}

type PendingReconciliationSet struct {
	mu      sync.Mutex
	entries map[domain.TaskID]PendingEntry
}

func (s *PendingReconciliationSet) Add(taskID domain.TaskID, signalSent bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.entries == nil {
		s.entries = make(map[domain.TaskID]PendingEntry)
	}
	entry, found := s.entries[taskID]
	if !found {
		s.entries[taskID] = PendingEntry{taskID: taskID, signalSent: signalSent}
		return
	}
	if signalSent && !entry.signalSent {
		entry.signalSent = true
		s.entries[taskID] = entry
	}
}

func (s *PendingReconciliationSet) Register(taskID domain.TaskID, signalSent bool) error {
	s.Add(taskID, signalSent)
	return nil
}

func (s *PendingReconciliationSet) Remove(taskID domain.TaskID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, taskID)
}

func (s *PendingReconciliationSet) List() []PendingEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := make([]PendingEntry, 0, len(s.entries))
	for _, entry := range s.entries {
		entries = append(entries, entry)
	}
	return entries
}

func (s *PendingReconciliationSet) ClaimForSend(taskID domain.TaskID) (claimed bool, found bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.entries[taskID]
	if !found {
		return false, false
	}
	if entry.signalSent {
		return false, true
	}
	entry.signalSent = true
	s.entries[taskID] = entry
	return true, true
}
