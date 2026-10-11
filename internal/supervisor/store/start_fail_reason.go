package store

// SupervisorStartFailReason follows specifications/10-shared/published-language/enums.md.
type SupervisorStartFailReason string

const (
	StartFailUserSettings             SupervisorStartFailReason = "user_settings"
	StartFailProjectSettings          SupervisorStartFailReason = "project_settings"
	StartFailProjectLocalSettings     SupervisorStartFailReason = "project_local_settings"
	StartFailClaudeJSON               SupervisorStartFailReason = "claude_json"
	StartFailManaged                  SupervisorStartFailReason = "managed"
	StartFailCodexAuth                SupervisorStartFailReason = "codex_auth"
	StartFailChildEnv                 SupervisorStartFailReason = "child_env"
	StartFailProjectCWDUnavailable    SupervisorStartFailReason = "project_cwd_unavailable"
	StartFailSpawnFailed              SupervisorStartFailReason = "spawn_failed"
	StartFailLaunchArgvInvalid        SupervisorStartFailReason = "launch_argv_invalid"
	StartFailSettingsGenerationFailed SupervisorStartFailReason = "settings_generation_failed"
)

func (r SupervisorStartFailReason) Valid() bool {
	switch r {
	case StartFailUserSettings, StartFailProjectSettings, StartFailProjectLocalSettings, StartFailClaudeJSON, StartFailManaged, StartFailCodexAuth, StartFailChildEnv, StartFailProjectCWDUnavailable, StartFailSpawnFailed, StartFailLaunchArgvInvalid, StartFailSettingsGenerationFailed:
		return true
	}
	return false
}
