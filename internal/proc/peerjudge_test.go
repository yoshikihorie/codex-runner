package proc

import (
	"os"
	"testing"
)

func TestPeerJudge_SCNsup0544_FailClosed(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	owned := map[int]bool{123: true}
	for _, tc := range []struct {
		name                    string
		tty                     bool
		ancestors               []int
		marker                  bool
		valid                   bool
		wantTTY, wantSupervised bool
	}{
		{"unmanaged_tty", true, []int{1}, false, true, true, false},
		{"owned_ancestor", true, []int{5, 123}, false, true, true, true},
		{"marker", true, []int{1}, true, true, true, true},
		{"no_tty", false, []int{123}, false, true, false, true},
		{"failed", true, []int{123}, true, false, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tty, supervised := JudgePeer(tc.tty, tc.ancestors, owned, tc.marker, tc.valid)
			if tty != tc.wantTTY || supervised != tc.wantSupervised {
				t.Fatalf("tty=%t supervised=%t", tty, supervised)
			}
		})
	}
}
