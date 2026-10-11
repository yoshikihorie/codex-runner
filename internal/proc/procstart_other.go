//go:build !darwin

package proc

import "time"

// ProcessStartTime is unavailable outside Darwin.
func ProcessStartTime(int) (time.Time, bool) { return time.Time{}, false }
