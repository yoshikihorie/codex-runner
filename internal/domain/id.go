package domain

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"
)

type TaskID struct{ value string }

var taskIDPattern = regexp.MustCompile(`^(impl|review|plan|research|read)-([0-9]{8})-([0-9]{6})-([0-9a-f]{4})-([a-z0-9]+(?:-[a-z0-9]+)*)$`)

func NewTaskID(value string) (TaskID, error) {
	m := taskIDPattern.FindStringSubmatch(value)
	if m == nil {
		return TaskID{}, fmt.Errorf("invalid task id")
	}
	if _, err := time.Parse("20060102 150405", m[2]+" "+m[3]); err != nil {
		return TaskID{}, fmt.Errorf("invalid task id: %w", err)
	}
	if _, err := NewSlug(m[5]); err != nil {
		return TaskID{}, err
	}
	return TaskID{value}, nil
}
func (v TaskID) String() string               { return v.value }
func (v TaskID) MarshalJSON() ([]byte, error) { return json.Marshal(v.value) }
func (v *TaskID) UnmarshalJSON(data []byte) error {
	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := NewTaskID(raw)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}

type SessionRef struct {
	sessionID  string
	capturedAt time.Time
	ephemeral  bool
}

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

func NewSessionRef(id string, at time.Time, ephemeral bool) (SessionRef, error) {
	if !uuidPattern.MatchString(id) || at.IsZero() || ephemeral {
		return SessionRef{}, fmt.Errorf("invalid session reference")
	}
	return SessionRef{id, at, false}, nil
}
func (v SessionRef) SessionID() string     { return v.sessionID }
func (v SessionRef) CapturedAt() time.Time { return v.capturedAt }
func (v SessionRef) Ephemeral() bool       { return v.ephemeral }
func (v SessionRef) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		SessionID  string    `json:"session_id"`
		CapturedAt time.Time `json:"captured_at"`
		Ephemeral  bool      `json:"ephemeral"`
	}{v.sessionID, v.capturedAt, v.ephemeral})
}
func (v *SessionRef) UnmarshalJSON(data []byte) error {
	var raw struct {
		SessionID  string    `json:"session_id"`
		CapturedAt time.Time `json:"captured_at"`
		Ephemeral  bool      `json:"ephemeral"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := NewSessionRef(raw.SessionID, raw.CapturedAt, raw.Ephemeral)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}
