package proc

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var (
	fixedPathDirs = []string{
		"/usr/bin",
		"/bin",
		"/usr/local/bin",
		"/opt/homebrew/bin",
	}
	gitBinaryCandidates = []string{
		"/usr/bin/git",
		"/bin/git",
		"/usr/local/bin/git",
		"/opt/homebrew/bin/git",
	}
	userHomeDir    = os.UserHomeDir
	allowedEnvKeys = []string{"HOME"}
)

// FixedPath returns the fixed child-process PATH.
func FixedPath() string {
	dirs := append([]string{}, fixedPathDirs...)
	if home, err := userHomeDir(); err == nil && isValidHomeDir(home) {
		dirs = append(dirs, filepath.Join(home, ".npm-global", "bin"))
	}
	return strings.Join(dirs, ":")
}

// isValidHomeDir reports whether value is an absolute path to an existing directory
// (FD-exec-08.md §5.3.3: パス値を持つ変数は絶対パス + 実在確認を要する)。
func isValidHomeDir(value string) bool {
	if !filepath.IsAbs(value) {
		return false
	}
	info, err := os.Stat(value)
	return err == nil && info.IsDir()
}

// SafeChildEnv builds the allowlisted child-process environment.
func SafeChildEnv() []string {
	env := []string{"PATH=" + FixedPath()}
	for _, key := range allowedEnvKeys {
		value, ok := os.LookupEnv(key)
		if !ok {
			continue
		}
		if key == "HOME" && !isValidHomeDir(value) {
			continue
		}
		env = append(env, key+"="+value)
	}
	return env
}

func validateSafeEnv(env []string) error {
	if len(env) == 0 {
		return fmt.Errorf("env must not be empty")
	}
	seen := make(map[string]string, len(env))
	for _, kv := range env {
		key, value, found := strings.Cut(kv, "=")
		if !found {
			return fmt.Errorf("env entry %q is not in KEY=VALUE form", kv)
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("env key %q is duplicated", key)
		}
		seen[key] = value
	}
	pathValue, ok := seen["PATH"]
	if !ok {
		return fmt.Errorf("env is missing PATH")
	}
	if pathValue != FixedPath() {
		return fmt.Errorf("PATH does not match the fixed path list")
	}
	allowed := make(map[string]bool, len(allowedEnvKeys)+1)
	allowed["PATH"] = true
	for _, key := range allowedEnvKeys {
		allowed[key] = true
	}
	for key, value := range seen {
		if !allowed[key] {
			return fmt.Errorf("env key %q is not in the allowlist", key)
		}
		if key == "HOME" && !isValidHomeDir(value) {
			return fmt.Errorf("HOME must be an absolute path to an existing directory")
		}
	}
	return nil
}

// FindGitBinary resolves git from fixed absolute candidates without using PATH.
func FindGitBinary() (string, error) {
	for _, candidate := range gitBinaryCandidates {
		if !filepath.IsAbs(candidate) {
			continue
		}
		info, err := os.Stat(candidate)
		if err == nil && isExecutableRegularFile(info) {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("git binary not found in fixed path list")
}

func isExecutableRegularFile(info os.FileInfo) bool {
	return info.Mode().IsRegular() && info.Mode()&0o111 != 0
}
