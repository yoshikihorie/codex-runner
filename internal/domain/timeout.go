package domain

import (
	"encoding/json"
	"fmt"
)

const timeoutMinSeconds = 1800

type Timeout struct {
	requestedSeconds *int
	resolvedSeconds  int
}

func NewTimeout(requested *int, resolved int) (Timeout, error) {
	if resolved < timeoutMinSeconds || (requested != nil && *requested < timeoutMinSeconds) {
		return Timeout{}, fmt.Errorf("invalid timeout")
	}
	var clone *int
	if requested != nil {
		n := *requested
		clone = &n
	}
	return Timeout{clone, resolved}, nil
}
func (v Timeout) RequestedSeconds() *int {
	if v.requestedSeconds == nil {
		return nil
	}
	n := *v.requestedSeconds
	return &n
}
func (v Timeout) ResolvedSeconds() int { return v.resolvedSeconds }
func (v Timeout) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		RequestedSeconds *int `json:"requested_seconds,omitempty"`
		ResolvedSeconds  int  `json:"resolved_seconds"`
	}{v.RequestedSeconds(), v.resolvedSeconds})
}
func (v *Timeout) UnmarshalJSON(data []byte) error {
	var raw struct {
		RequestedSeconds *int `json:"requested_seconds"`
		ResolvedSeconds  int  `json:"resolved_seconds"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := NewTimeout(raw.RequestedSeconds, raw.ResolvedSeconds)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}
