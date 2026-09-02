package domain

import "fmt"

// WorktreeMode selects how an impl task's working directory is prepared before launch.
type WorktreeMode string

const (
	// WorktreeModeAuto clones the source working directory into a daemon-managed
	// git worktree and runs codex exec there.
	WorktreeModeAuto WorktreeMode = "auto"
	// WorktreeModeCurrent skips worktree creation and runs codex exec directly in
	// the requested source working directory.
	WorktreeModeCurrent WorktreeMode = "current"
)

// ParseWorktreeMode validates a raw worktree mode value against the allowed set.
func ParseWorktreeMode(value string) (WorktreeMode, error) {
	switch WorktreeMode(value) {
	case WorktreeModeAuto, WorktreeModeCurrent:
		return WorktreeMode(value), nil
	default:
		return "", fmt.Errorf("invalid worktree mode")
	}
}
