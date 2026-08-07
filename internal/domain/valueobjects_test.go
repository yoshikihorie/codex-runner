package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestSlugBoundaries(t *testing.T) {
	for _, value := range []string{"", strings.Repeat("a", 41), "Upper", "a/b", "-a", "a-", "a--b"} {
		if _, err := NewSlug(value); err == nil {
			t.Errorf("NewSlug(%q) unexpectedly succeeded", value)
		}
	}
	if _, err := NewSlug(strings.Repeat("a", 40)); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutBoundariesAndCopiesRequestedValue(t *testing.T) {
	requested := 1800
	for _, tc := range []struct {
		requested *int
		resolved  int
		valid     bool
	}{{nil, 1800, true}, {&requested, 1800, true}, {intPointer(1799), 1800, false}, {intPointer(0), 1800, false}, {intPointer(-1), 1800, false}, {nil, 1799, false}} {
		value, err := NewTimeout(tc.requested, tc.resolved)
		if (err == nil) != tc.valid {
			t.Errorf("NewTimeout(%v, %d): err=%v", tc.requested, tc.resolved, err)
		}
		if tc.valid && tc.requested != nil {
			*tc.requested = 9999
			if *value.RequestedSeconds() != 1800 {
				t.Fatal("requested value was not copied")
			}
		}
	}
}

func TestTaskIDBoundaries(t *testing.T) {
	valid := "impl-20260806-120000-a1b2-example"
	if _, err := NewTaskID(valid); err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"status-20260806-120000-a1b2-example", "impl-20260230-120000-a1b2-example", "impl-20260806-250000-a1b2-example", "impl-20260806-120000-a1b-example", "impl-20260806-120000-a1bz-example", "impl-20260806-120000-a1b2-Upper"} {
		if _, err := NewTaskID(value); err == nil {
			t.Errorf("NewTaskID(%q) unexpectedly succeeded", value)
		}
	}
}

func TestSessionRefAndProcessStartTimeBoundaries(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	if _, err := NewSessionRef("00112233-4455-6677-8899-aabbccddeeff", now, false); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		id        string
		at        time.Time
		ephemeral bool
	}{{"", now, false}, {"not-a-uuid", now, false}, {"00112233-4455-6677-8899-aabbccddeeff", time.Time{}, false}, {"00112233-4455-6677-8899-aabbccddeeff", now, true}} {
		if _, err := NewSessionRef(tc.id, tc.at, tc.ephemeral); err == nil {
			t.Errorf("invalid SessionRef accepted: %+v", tc)
		}
	}
	if _, err := NewProcessStartTime(1, now); err != nil {
		t.Fatal(err)
	}
	for _, pid := range []int{0, -1} {
		if _, err := NewProcessStartTime(pid, now); err == nil {
			t.Errorf("pid %d accepted", pid)
		}
	}
	if _, err := NewProcessStartTime(1, time.Time{}); err == nil {
		t.Error("zero process time accepted")
	}
}

func TestExitCodeClassification(t *testing.T) {
	for raw, want := range map[int]ExitCodeClass{0: ExitCodeClassSuccess, 1: ExitCodeClassFailure, 6: ExitCodeClassTimeout, 124: ExitCodeClassTimeout, 130: ExitCodeClassCancelled, 137: ExitCodeClassTimeout, 999: ExitCodeClassFailure} {
		if got := NewExitCode(raw).Class(); got != want {
			t.Errorf("NewExitCode(%d).Class() = %q, want %q", raw, got, want)
		}
	}
}

func TestNormalizedPathBoundaries(t *testing.T) {
	for _, value := range []string{"/", "/tmp/example"} {
		if _, err := NewNormalizedPath(value); err != nil {
			t.Errorf("valid path %q: %v", value, err)
		}
	}
	for _, value := range []string{"", "tmp/example", "/tmp/../example", "/tmp/example/"} {
		if _, err := NewNormalizedPath(value); err == nil {
			t.Errorf("invalid path %q accepted", value)
		}
	}
}

func TestValueObjectJSONRoundTrip(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	slug, _ := NewSlug("example")
	id, _ := NewTaskID("impl-20260806-120000-a1b2-example")
	timeout, _ := NewTimeout(intPointer(1800), 1800)
	process, _ := NewProcessStartTime(1, now)
	session, _ := NewSessionRef("00112233-4455-6677-8899-aabbccddeeff", now, false)
	path, _ := NewNormalizedPath("/tmp/example")
	values := []any{slug, id, timeout, process, NewExitCode(137), session, path}
	for _, value := range values {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		copy := reflectValuePointer(value)
		if err := json.Unmarshal(data, copy); err != nil {
			t.Fatalf("unmarshal %T: %v", value, err)
		}
		if string(data) != mustJSON(t, copy) {
			t.Fatalf("round trip changed %T", value)
		}
	}
}

func intPointer(value int) *int { return &value }
func mustJSON(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
func reflectValuePointer(value any) any {
	switch value.(type) {
	case Slug:
		return &Slug{}
	case TaskID:
		return &TaskID{}
	case Timeout:
		return &Timeout{}
	case ProcessStartTime:
		return &ProcessStartTime{}
	case ExitCode:
		return &ExitCode{}
	case SessionRef:
		return &SessionRef{}
	case NormalizedPath:
		return &NormalizedPath{}
	default:
		panic("unsupported value object")
	}
}
