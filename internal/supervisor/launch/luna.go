package launch

import (
	"os"
	"path/filepath"

	"github.com/yoshikihorie/codex-runner/internal/config"
	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// LunaImplAllowed checks the existing user allowlist without modifying it.
func LunaImplAllowed() (bool, string) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return false, "HOME unavailable"
	}
	return lunaImplAllowedAt(filepath.Join(home, ".claude", "codexd", "config.toml"))
}

func lunaImplAllowedAt(path string) (bool, string) {
	cfg, err := config.LoadExplicit(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, "config file missing"
		}
		return false, "config invalid or unreadable"
	}
	if _, allowed := cfg.ResolveModel(domain.SubcommandImpl, stringPointer("gpt-6-luna")); allowed {
		return true, "gpt-6-luna impl is allowed"
	}
	return false, "gpt-6-luna impl is not allowed"
}

func stringPointer(value string) *string { return &value }
