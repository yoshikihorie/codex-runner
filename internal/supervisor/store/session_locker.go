package store

import (
	"bytes"
	"encoding/json"
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// SessionLocker serializes each session's read-modify-write operations.
type SessionLocker struct {
	mu    sync.Mutex
	locks map[supervisor.SessionID]*sync.Mutex
}

var SharedSessionLocker SessionLocker

func (l *SessionLocker) Lock(id supervisor.SessionID) func() {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = make(map[supervisor.SessionID]*sync.Mutex)
	}
	lock := l.locks[id]
	if lock == nil {
		lock = &sync.Mutex{}
		l.locks[id] = lock
	}
	l.mu.Unlock()
	lock.Lock()
	return lock.Unlock
}

// UpdateSession saves only changes and leaves the record untouched on callback error.
func (s *SessionStore) UpdateSession(id supervisor.SessionID, update func(*supervisor.ManagedSession) error) error {
	unlock := SharedSessionLocker.Lock(id)
	defer unlock()
	current, err := s.Load(id)
	if err != nil {
		return err
	}
	before, err := json.Marshal(current)
	if err != nil {
		return err
	}
	if err := update(&current); err != nil {
		return err
	}
	after, err := json.Marshal(current)
	if err != nil {
		return err
	}
	if bytes.Equal(before, after) {
		return nil
	}
	return s.Save(current)
}
