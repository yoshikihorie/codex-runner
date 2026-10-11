package launch

import (
	"fmt"
	"path/filepath"
	"regexp"
)

const supervisorManagerModel = "claude-opus-5-5" // SUPERVISOR_MANAGER_MODEL, validation-rules.md

var claudeSessionIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// BuildLaunchArgv returns the sole supported managed CLI invocation.
func BuildLaunchArgv(cli, binaryPath, settingsPath, resumeClaudeSessionID string) ([]string, error) {
	if cli != "claude" {
		return nil, fmt.Errorf("SUP_CONFIG_INVALID cli_not_supported: %q", cli)
	}
	if !filepath.IsAbs(binaryPath) {
		return nil, fmt.Errorf("SUP_CONFIG_INVALID relative_path: claude_binary_path")
	}
	if !filepath.IsAbs(settingsPath) {
		return nil, fmt.Errorf("SUP_CONFIG_INVALID relative_path: settings_path")
	}
	argv := []string{binaryPath, "--settings", settingsPath, "--permission-mode", "default", "--model", supervisorManagerModel}
	if resumeClaudeSessionID != "" {
		if !claudeSessionIDPattern.MatchString(resumeClaudeSessionID) {
			return nil, fmt.Errorf("SUP_CONFIG_INVALID claude_session_id_invalid")
		}
		argv = append(argv, "--resume", resumeClaudeSessionID)
	}
	return argv, nil
}

// ValidateProjectModel rejects a project-level model override for managed Claude sessions.
func ValidateProjectModel(model *string) error {
	if model != nil {
		return fmt.Errorf("SUP_CONFIG_INVALID model_not_allowed_for_claude")
	}
	return nil
}
