package usecase

import (
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
)

// ObservedStateStore holds the last detector result in memory for send gating.
type ObservedStateStore struct {
	mu     sync.RWMutex
	states map[supervisor.SessionID]detect.DetectedState
}

func (s *ObservedStateStore) Set(id supervisor.SessionID, state detect.DetectedState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.states == nil {
		s.states = make(map[supervisor.SessionID]detect.DetectedState)
	}
	s.states[id] = state
}

func (s *ObservedStateStore) Get(id supervisor.SessionID) (detect.DetectedState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.states[id]
	return state, ok
}

func (s *ObservedStateStore) Delete(id supervisor.SessionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.states, id)
}
