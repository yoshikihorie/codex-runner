//go:build darwin

package proc

import (
	"os"
	"testing"
	"time"
)

func TestProcessStartTimeCurrentPID(t *testing.T) {
	started, ok := ProcessStartTime(os.Getpid())
	if !ok || started.IsZero() || started.After(time.Now()) || time.Since(started) > time.Hour {
		t.Fatalf("current process start time = %v, ok=%t", started, ok)
	}
	if _, ok := ProcessStartTime(0); ok {
		t.Fatal("PID zero accepted")
	}
	if _, ok := ProcessStartTime(1 << 30); ok {
		t.Fatal("nonexistent PID accepted")
	}
	if _, ok := ProcessStartTime(-1); ok {
		t.Fatal("invalid PID accepted")
	}
}
