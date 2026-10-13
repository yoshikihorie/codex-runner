package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	corestore "github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

const sessionSlugMaxLength = 40        // SLUG_MAX_LENGTH in specifications/10-shared/validation-rules.md.
const sessionSequenceSchemaVersion = 1 // FD-sup-05 §16.2.

type sessionSequenceState struct {
	SchemaVersion       int               `json:"schema_version"`
	SessionSeq          map[string]uint64 `json:"session_seq"`
	SupervisorStartedAt *time.Time        `json:"supervisor_started_at,omitempty"`
}

type SessionSequence struct {
	mu   sync.Mutex
	root string
}

func NewSessionSequence(stateDir string) *SessionSequence { return &SessionSequence{root: stateDir} }

// Next reserves an ID durably before the session is started. A failed start never reuses it.
func (s *SessionSequence) Next(project string) (supervisor.SessionID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.load()
	if err != nil {
		return "", err
	}
	next := state.SessionSeq[project]
	if next == 0 {
		next = 1
	}
	if next == math.MaxUint64 {
		return "", errors.New("SUP_CONFIG_INVALID")
	}
	entries, err := os.ReadDir(filepath.Join(s.root, "sessions"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	occupied := make(map[string]bool, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".json") {
			occupied[strings.TrimSuffix(name, ".json")] = true
		}
		if before, _, ok := strings.Cut(name, ".json.corrupt-"); ok {
			occupied[before] = true
		}
	}
	for occupied[fmt.Sprintf("%s-%d", project, next)] {
		if next == math.MaxUint64 {
			return "", errors.New("SUP_CONFIG_INVALID")
		}
		next++
	}
	name := fmt.Sprintf("%s-%d", project, next)
	if len(name) > sessionSlugMaxLength {
		return "", errors.New("SUP_CONFIG_INVALID")
	}
	id, err := supervisor.NewSessionID(name)
	if err != nil {
		return "", errors.New("SUP_CONFIG_INVALID")
	}
	state.SessionSeq[project] = next + 1
	if err := s.save(state); err != nil {
		return "", err
	}
	return id, nil
}

// MarkStarted records this supervisor generation without resetting any reserved IDs.
func (s *SessionSequence) MarkStarted(at time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, err := s.load()
	if err != nil {
		return err
	}
	at = at.UTC()
	state.SupervisorStartedAt = &at
	return s.save(state)
}

func (s *SessionSequence) load() (sessionSequenceState, error) {
	path := filepath.Join(s.root, "state.json")
	state := sessionSequenceState{SchemaVersion: sessionSequenceSchemaVersion, SessionSeq: map[string]uint64{}}
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() {
			return state, errors.New("SUP_STATE_FILE_CORRUPT")
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return state, err
		}
		if err := json.Unmarshal(data, &state); err != nil || state.SchemaVersion != sessionSequenceSchemaVersion || state.SessionSeq == nil {
			return state, errors.New("SUP_STATE_FILE_CORRUPT")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return state, err
	}
	return state, nil
}

func (s *SessionSequence) save(state sessionSequenceState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return corestore.WriteAtomic(filepath.Join(s.root, "state.json"), append(data, '\n'), 0600)
}
