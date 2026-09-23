package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// NormalizePath resolves filesystem aliases and validates a normalized absolute path.
// Only when resolving the complete path returns ENOENT, it rescues one missing final
// component whose direct parent exists. The parent is resolved from the raw lexical
// representation without cleaning it first, so symlinks are resolved before later
// dot-dot components are applied.
//
// A known TOCTOU remains between checking the final component with os.Lstat and
// resolving its parent with filepath.EvalSymlinks. Fully eliminating that race is
// outside this function's current contract.
func NormalizePath(raw string, isMacOS bool) (domain.NormalizedPath, error) {
	if raw == "" || !filepath.IsAbs(raw) {
		return domain.NormalizedPath{}, fmt.Errorf("path must be a non-empty absolute path")
	}
	resolved, err := filepath.EvalSymlinks(raw)
	if err == nil {
		return validateResolvedPath(resolved, isMacOS)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return domain.NormalizedPath{}, fmt.Errorf("resolve symlinks: %w", err)
	}

	parent, finalName := filepath.Split(raw)
	if finalName == "" || finalName == "." || finalName == ".." {
		return domain.NormalizedPath{}, fmt.Errorf("resolve symlinks: %w", err)
	}
	if _, lstatErr := os.Lstat(raw); lstatErr == nil {
		return domain.NormalizedPath{}, fmt.Errorf("resolve symlinks: %w", err)
	} else if !errors.Is(lstatErr, os.ErrNotExist) {
		return domain.NormalizedPath{}, fmt.Errorf("inspect final path component: %w", lstatErr)
	}

	resolvedParent, parentErr := filepath.EvalSymlinks(parent)
	if parentErr != nil {
		return domain.NormalizedPath{}, fmt.Errorf("resolve parent symlinks: %w", parentErr)
	}
	return validateResolvedPath(filepath.Join(resolvedParent, finalName), isMacOS)
}

func validateResolvedPath(resolved string, isMacOS bool) (domain.NormalizedPath, error) {
	resolved = filepath.Clean(resolved)
	if isMacOS {
		resolved = strings.ToLower(resolved)
	}
	return domain.NewNormalizedPath(resolved)
}
