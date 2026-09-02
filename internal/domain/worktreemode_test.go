package domain

import "testing"

func TestParseWorktreeModeAcceptsKnownValues(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want WorktreeMode
	}{
		{"auto", WorktreeModeAuto},
		{"current", WorktreeModeCurrent},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			got, err := ParseWorktreeMode(tc.raw)
			if err != nil || got != tc.want {
				t.Fatalf("ParseWorktreeMode(%q) = (%q, %v), want (%q, nil)", tc.raw, got, err, tc.want)
			}
		})
	}
}

func TestParseWorktreeModeRejectsUnknownValue(t *testing.T) {
	for _, raw := range []string{"worktree", "AUTO", "", "　"} {
		t.Run(raw, func(t *testing.T) {
			if got, err := ParseWorktreeMode(raw); err == nil {
				t.Fatalf("ParseWorktreeMode(%q) = (%q, nil), want error", raw, got)
			}
		})
	}
}
