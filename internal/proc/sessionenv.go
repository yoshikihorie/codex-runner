package proc

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	sessionTerm           = "xterm-256color" // SUPERVISOR_PTY_TERM, validation-rules.md §3.1
	sessionMarker         = "1"              // SUPERVISOR_CHILD_MARKER_ENV, FD-sup-01 §5.5
	sessionDefaultColumns = 200              // SUPERVISOR_PTY_COLUMNS_DEFAULT, validation-rules.md §3.1
	sessionDefaultRows    = 50               // SUPERVISOR_PTY_ROWS_DEFAULT, validation-rules.md §3.1
)

var strippedSessionEnvNames = map[string]struct{}{
	"ANTHROPIC_API_KEY":            {},
	"OPENAI_API_KEY":               {},
	"ANTHROPIC_AUTH_TOKEN":         {},
	"ANTHROPIC_BASE_URL":           {},
	"CLAUDE_CODE_USE_BEDROCK":      {},
	"CLAUDE_CODE_USE_VERTEX":       {},
	"CLAUDE_CODE_USE_FOUNDRY":      {},
	"AWS_BEARER_TOKEN_BEDROCK":     {},
	"CLAUDECODE":                   {},
	"CLAUDE_CODE_ENTRYPOINT":       {},
	"CLAUDE_CODE_MESSAGING_SOCKET": {},
	"CLAUDE_CODE_MESSAGING_TOKEN":  {},
	"CLAUDE_CODE_EXECPATH":         {},
	"CLAUDE_CODE_SESSION_ID":       {},
	"CLAUDE_CODE_CHILD_SESSION":    {},
	"CLAUDE_CODE_SESSION_ATTENDED": {},
	"CLAUDE_PID":                   {},
	"CLAUDE_EFFORT":                {},
}

// BuildSessionEnv inherits the parent environment except for exact-name removals.
func BuildSessionEnv(parent []string) []string {
	blocked := strippedSessionEnvNames
	filtered := make([]string, 0, len(parent)+4)
	for _, entry := range parent {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			continue
		}
		if _, remove := blocked[key]; remove {
			continue
		}
		filtered = append(filtered, key+"="+value)
	}
	return mergeSessionEnv(filtered, []string{"TERM=" + sessionTerm, "CODEXD_SUPERVISED=" + sessionMarker, "COLUMNS=" + strconv.Itoa(sessionDefaultColumns), "LINES=" + strconv.Itoa(sessionDefaultRows)})
}

func mergeSessionEnv(env, overrides []string) []string {
	values := make(map[string]string, len(env)+len(overrides))
	order := make([]string, 0, len(env)+len(overrides))
	set := func(entry string) {
		key, value, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			return
		}
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = value
	}
	for _, entry := range env {
		set(entry)
	}
	for _, entry := range overrides {
		set(entry)
	}
	result := make([]string, 0, len(order))
	for _, key := range order {
		result = append(result, key+"="+values[key])
	}
	return result
}

// ValidateSessionEnv rejects duplicates, malformed entries, and retained blocked names.
func ValidateSessionEnv(env []string) error {
	blocked := strippedSessionEnvNames
	seen := make(map[string]struct{}, len(env))
	for _, entry := range env {
		key, _, ok := strings.Cut(entry, "=")
		if !ok || key == "" {
			return fmt.Errorf("invalid environment entry")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate environment key %q", key)
		}
		seen[key] = struct{}{}
		if _, forbidden := blocked[key]; forbidden {
			return fmt.Errorf("forbidden environment key %q", key)
		}
	}
	return nil
}
