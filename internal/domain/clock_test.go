package domain

import (
	"testing"
	"time"
)

func TestClockFunc(t *testing.T) {
	want := time.Unix(1, 0)
	if got := ClockFunc(func() time.Time { return want }).Now(); !got.Equal(want) {
		t.Fatal("ClockFunc did not delegate")
	}
}
