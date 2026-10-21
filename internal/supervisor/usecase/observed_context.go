package usecase

import (
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// ObservedContextStore holds validated context usage in memory only.
type ObservedContextStore struct {
	mu     sync.RWMutex
	values map[supervisor.SessionID]int
}

func (s *ObservedContextStore) Set(id supervisor.SessionID, percent *int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if percent == nil {
		delete(s.values, id)
		return
	}
	if s.values == nil {
		s.values = make(map[supervisor.SessionID]int)
	}
	s.values[id] = *percent
}

func (s *ObservedContextStore) Get(id supervisor.SessionID) *int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.values[id]
	if !ok {
		return nil
	}
	return &value
}

func (s *ObservedContextStore) Delete(id supervisor.SessionID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, id)
}
