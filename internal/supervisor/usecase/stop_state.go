package usecase

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// StopState serializes STOP flags and marker-file operations under one lock.
type StopState struct {
	startMu   sync.Mutex
	mu        sync.Mutex
	root      string
	global    bool
	sessions  map[supervisor.SessionID]bool
	onRemoved func(supervisor.SessionID)
}

func NewStopState(stateDir string) *StopState {
	return &StopState{root: stateDir, sessions: make(map[supervisor.SessionID]bool)}
}
func (s *StopState) SetRemovalHandler(fn func(supervisor.SessionID)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onRemoved = fn
}
func (s *StopState) SetGlobal(stopped bool) {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.global = stopped
}
func (s *StopState) SetSession(id supervisor.SessionID, stopped bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if stopped {
		s.sessions[id] = true
	} else {
		delete(s.sessions, id)
	}
}
func (s *StopState) IsStopped(id supervisor.SessionID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.global || s.sessions[id] {
		return true
	}
	if s.markerExistsLocked("") {
		return true
	}
	return id != "" && s.markerExistsLocked(id.String())
}
func (s *StopState) IsGlobalStopped() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.global || s.markerExistsLocked("")
}

// GuardStart serializes spawn with STOP writers without holding the state mutex during I/O.
func (s *StopState) GuardStart(start func() error) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	s.mu.Lock()
	stopped := s.global || s.markerExistsLocked("")
	s.mu.Unlock()
	if stopped {
		return ErrStopActive
	}
	return start()
}
func (s *StopState) IsSessionStopped(id supervisor.SessionID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[id] || s.markerExistsLocked(id.String())
}

// GuardResume keeps the final STOP check and state transition exclusive with Poll/Stop file updates.
func (s *StopState) GuardResume(scope supervisor.SupervisorStopScope, id supervisor.SessionID, transition func() error) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	s.mu.Lock()
	globalStopped := s.global || s.markerExistsLocked("")
	sessionStopped := s.sessions[id] || s.markerExistsLocked(id.String())
	s.mu.Unlock()
	if globalStopped {
		return errors.New("still_stopped_by_global")
	}
	if sessionStopped {
		if scope == supervisor.StopScopeGlobal {
			return errors.New("still_stopped_by_global")
		}
		return errors.New("stop_recreated")
	}
	return transition()
}
func (s *StopState) filePath(id string) string {
	name := "STOP"
	if id != "" {
		name += "." + id
	}
	return filepath.Join(s.root, name)
}
func (s *StopState) markerExistsLocked(id string) bool {
	_, err := os.Lstat(s.filePath(id))
	return err == nil || !errors.Is(err, os.ErrNotExist)
}
func (s *StopState) WriteGlobal() error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	return s.write("")
}
func (s *StopState) WriteSession(id supervisor.SessionID) error {
	if _, err := supervisor.NewSessionID(id.String()); err != nil {
		return err
	}
	return s.write(id.String())
}
func (s *StopState) EnableGlobal() error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeLocked(""); err != nil {
		return err
	}
	s.global = true
	return nil
}
func (s *StopState) EnableSession(id supervisor.SessionID) error {
	if _, err := supervisor.NewSessionID(id.String()); err != nil {
		return err
	}
	s.startMu.Lock()
	defer s.startMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeLocked(id.String()); err != nil {
		return err
	}
	s.sessions[id] = true
	return nil
}
func (s *StopState) write(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked(id)
}
func (s *StopState) writeLocked(id string) error {
	if err := os.MkdirAll(s.root, 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(s.filePath(id), os.O_CREATE|os.O_WRONLY|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
func (s *StopState) ClearGlobal() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(s.filePath("")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.global = false
	return nil
}
func (s *StopState) ClearSession(id supervisor.SessionID) error {
	if _, err := supervisor.NewSessionID(id.String()); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(s.filePath(id.String())); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	delete(s.sessions, id)
	return nil
}

// CheckAndRepair checks global and session markers independently and repairs removed markers.
func (s *StopState) CheckAndRepair(id supervisor.SessionID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.global {
		if !s.markerExistsLocked("") {
			if s.onRemoved != nil {
				s.onRemoved("")
			}
			if err := s.writeLocked(""); err != nil {
				return err
			}
		}
	}
	if id != "" && s.sessions[id] {
		if !s.markerExistsLocked(id.String()) {
			if s.onRemoved != nil {
				s.onRemoved(id)
			}
			if err := s.writeLocked(id.String()); err != nil {
				return err
			}
		}
	}
	return nil
}

// Poll latches markers created outside the process and repairs markers removed outside resume.
func (s *StopState) Poll(id supervisor.SessionID) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.markerExistsLocked("") {
		s.global = true
	}
	if id != "" && s.markerExistsLocked(id.String()) {
		s.sessions[id] = true
	}
	if s.global && !s.markerExistsLocked("") {
		if s.onRemoved != nil {
			s.onRemoved("")
		}
		if err := s.writeLocked(""); err != nil {
			return err
		}
	}
	if id != "" && s.sessions[id] && !s.markerExistsLocked(id.String()) {
		if s.onRemoved != nil {
			s.onRemoved(id)
		}
		if err := s.writeLocked(id.String()); err != nil {
			return err
		}
	}
	return nil
}

// RepairFromSessions restores marker files represented by persisted stop scopes at startup.
func (s *StopState) RepairFromSessions(sessions []supervisor.ManagedSession) error {
	s.startMu.Lock()
	defer s.startMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, session := range sessions {
		if _, err := supervisor.NewSessionID(session.SessionID.String()); err != nil {
			continue
		}
		switch session.StopScope {
		case supervisor.StopScopeGlobal:
			s.global = true
		case supervisor.StopScopeSession:
			s.sessions[session.SessionID] = true
		}
	}
	if s.global && !s.markerExistsLocked("") {
		if s.onRemoved != nil {
			s.onRemoved("")
		}
		if err := s.writeLocked(""); err != nil {
			return err
		}
	}
	for id := range s.sessions {
		if !s.markerExistsLocked(id.String()) {
			if s.onRemoved != nil {
				s.onRemoved(id)
			}
			if err := s.writeLocked(id.String()); err != nil {
				return err
			}
		}
	}
	return nil
}
