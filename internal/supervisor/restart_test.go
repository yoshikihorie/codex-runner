package supervisor

import (
	"testing"
	"time"
)

func TestSCNsup0517RestartBackoff(t *testing.T) {
	for _, tc := range []struct {
		count int
		want  time.Duration
	}{{0, 10 * time.Second}, {1, 20 * time.Second}, {2, 40 * time.Second}, {10, 300 * time.Second}} {
		if got := RestartBackoff(tc.count); got != tc.want {
			t.Errorf("count %d: %v, want %v", tc.count, got, tc.want)
		}
	}
}

func TestSCNsup0518And41RestartLimits(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	times := make([]time.Time, RestartMaxPerDay)
	for i := range times {
		times[i] = now.Add(-23 * time.Hour)
	}
	if got := RestartLimitReason(RestartMaxCount, nil, now); got != StopRestartLimitConsecutive {
		t.Fatalf("consecutive: %q", got)
	}
	if got := RestartLimitReason(0, times, now); got != StopRestartLimitDaily {
		t.Fatalf("daily: %q", got)
	}
	times[0] = now.Add(-24*time.Hour - time.Nanosecond)
	if got := RestartLimitReason(0, times, now); got != "" {
		t.Fatalf("expired entry: %q", got)
	}
}

func TestSCNsup0519StableResetKeepsDailyHistory(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	if RestartStable(now.Add(-RestartStableDuration), now) != true {
		t.Fatal("stable threshold")
	}
	if RestartStable(now.Add(-RestartStableDuration+time.Nanosecond), now) {
		t.Fatal("premature reset")
	}
}
