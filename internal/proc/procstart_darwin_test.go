//go:build darwin

package proc

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestProcessStartTimeCurrentPID(t *testing.T) {

	command := exec.Command("/bin/ps", "-p", strconv.Itoa(os.Getpid()), "-o", "lstart=")
	command.Env = append(SafeChildEnv(), "LC_ALL=C", "TZ=UTC")
	output, err := command.Output()
	if err != nil {
		t.Fatalf("independent ps reference: %v", err)
	}
	reference, err := time.ParseInLocation("Mon Jan _2 15:04:05 2006", strings.TrimSpace(string(output)), time.UTC)
	if err != nil {
		t.Fatalf("ps lstart=%q: %v", output, err)
	}
	// Cross the reference second so returning the current time cannot pass.
	timer := time.NewTimer(time.Until(reference.Add(time.Second)))
	<-timer.C
	timer.Stop()
	started, ok := ProcessStartTime(os.Getpid())
	if !ok || !started.Truncate(time.Second).Equal(reference) {
		t.Fatalf("start=%v ok=%t, ps=%v", started, ok, reference)
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
