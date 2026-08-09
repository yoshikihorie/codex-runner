package store

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// NormalizePath resolves filesystem aliases and validates a normalized absolute path.
func NormalizePath(raw string, isMacOS bool) (domain.NormalizedPath, error) {
	if raw == "" || !filepath.IsAbs(raw) {
		return domain.NormalizedPath{}, fmt.Errorf("path must be a non-empty absolute path")
	}
	resolved, err := filepath.EvalSymlinks(raw)
	if err != nil {
		return domain.NormalizedPath{}, fmt.Errorf("resolve symlinks: %w", err)
	}
	resolved = filepath.Clean(resolved)
	if isMacOS {
		resolved = strings.ToLower(resolved)
	}
	return domain.NewNormalizedPath(resolved)
}
