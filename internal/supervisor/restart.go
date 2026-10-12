package supervisor

import "time"

// Restart limits are copied from specifications/10-shared/validation-rules.md:
// SUPERVISOR_RESTART_MAX_COUNT, SUPERVISOR_RESTART_MAX_PER_DAY,
// SUPERVISOR_RESTART_BACKOFF_BASE_SECONDS, SUPERVISOR_RESTART_BACKOFF_MAX_SECONDS,
// and SUPERVISOR_RESTART_STABLE_SECONDS.
const (
	RestartMaxCount       = 3
	RestartMaxPerDay      = 10
	RestartBackoffBase    = 10 * time.Second
	RestartBackoffMax     = 300 * time.Second
	RestartStableDuration = 600 * time.Second
	RestartWindow         = 24 * time.Hour
)

// RestartBackoff returns the capped delay before the next automatic attempt.
func RestartBackoff(count int) time.Duration {
	delay := RestartBackoffBase
	for i := 0; i < count && delay < RestartBackoffMax; i++ {
		delay *= 2
	}
	if delay > RestartBackoffMax {
		return RestartBackoffMax
	}
	return delay
}

// RestartCountInWindow counts attempts within the rolling 24-hour window.
func RestartCountInWindow(times []time.Time, now time.Time) int {
	start := now.Add(-RestartWindow)
	count := 0
	for _, at := range times {
		if !at.Before(start) && !at.After(now) {
			count++
		}
	}
	return count
}

// RestartLimitReason returns the first reached automatic restart limit.
func RestartLimitReason(count int, times []time.Time, now time.Time) SupervisorStopReason {
	return RestartLimitCounts(count, RestartCountInWindow(times, now))
}

// RestartLimitCounts evaluates the canonical limits for an already counted window.
func RestartLimitCounts(count, daily int) SupervisorStopReason {
	if count >= RestartMaxCount {
		return StopRestartLimitConsecutive
	}
	if daily >= RestartMaxPerDay {
		return StopRestartLimitDaily
	}
	return ""
}

// RestartStable reports whether a restored process has stayed alive long enough.
func RestartStable(started, now time.Time) bool {
	return !started.IsZero() && !now.Before(started) && now.Sub(started) >= RestartStableDuration
}
