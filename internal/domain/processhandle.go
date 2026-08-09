package domain

import "time"

// ProcessHandle is a reference to the launched lead process for post-hoc diagnosis only.
type ProcessHandle struct {
	PID              int
	ProcessStartedAt time.Time
}
