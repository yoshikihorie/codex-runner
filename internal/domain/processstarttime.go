package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

type ProcessStartTime struct {
	pid       int
	startedAt time.Time
}

func NewProcessStartTime(pid int, at time.Time) (ProcessStartTime, error) {
	if pid <= 0 || at.IsZero() {
		return ProcessStartTime{}, fmt.Errorf("invalid process start time")
	}
	return ProcessStartTime{pid, at}, nil
}
func (v ProcessStartTime) PID() int             { return v.pid }
func (v ProcessStartTime) StartedAt() time.Time { return v.startedAt }
func (v ProcessStartTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		PID       int       `json:"pid"`
		StartedAt time.Time `json:"started_at"`
	}{v.pid, v.startedAt})
}
func (v *ProcessStartTime) UnmarshalJSON(data []byte) error {
	var raw struct {
		PID       int       `json:"pid"`
		StartedAt time.Time `json:"started_at"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := NewProcessStartTime(raw.PID, raw.StartedAt)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}
