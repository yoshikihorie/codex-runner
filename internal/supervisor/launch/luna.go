package launch

import (
	"os"
	"path/filepath"

	"github.com/yoshikihorie/codex-runner/internal/config"
	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// SupervisorLunaCheckReason is copied from specifications/10-shared/published-language/enums.md.
type SupervisorLunaCheckReason string

const (
	LunaHomeUnavailable SupervisorLunaCheckReason = "home_unavailable"
	LunaConfigMissing   SupervisorLunaCheckReason = "config_missing"
	LunaConfigInvalid   SupervisorLunaCheckReason = "config_invalid"
	LunaNotAllowed      SupervisorLunaCheckReason = "not_allowed"
)

// LunaImplAllowed checks the existing user allowlist without modifying it.
func LunaImplAllowed() (bool, SupervisorLunaCheckReason) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return false, LunaHomeUnavailable
	}
	return lunaImplAllowedAt(filepath.Join(home, ".claude", "codexd", "config.toml"))
}

func lunaImplAllowedAt(path string) (bool, SupervisorLunaCheckReason) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return false, LunaConfigMissing
	}
	cfg, err := config.LoadExplicit(path)
	if err != nil {
		return false, LunaConfigInvalid
	}
	if _, allowed := cfg.ResolveModel(domain.SubcommandImpl, stringPointer("gpt-6-luna")); allowed {
		return true, ""
	}
	return false, LunaNotAllowed
}

func stringPointer(value string) *string { return &value }
