package proc

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

const gitStubDirectoryName = "codex-blocked-stubs"

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
	userHomeDir              = os.UserHomeDir
	allowedEnvKeys           = []string{"HOME"}
	gitStubDirectoryOwnerUID = func(info os.FileInfo) (uint32, bool) {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return 0, false
		}
		return stat.Uid, true
	}
)

// GitStubPathStatus reports whether the git-stub directory can safely precede the child PATH.
type GitStubPathStatus struct {
	Directory string
	Eligible  bool
	Reason    string
}

// FixedPath returns the fixed child-process PATH.
func FixedPath() string {
	dirs := append([]string{}, fixedPathDirs...)
	if home, err := userHomeDir(); err == nil && isValidHomeDir(home) {
		if status := GitStubStatus(); status.Eligible {
			dirs = append([]string{status.Directory}, dirs...)
		}
		dirs = append(dirs, filepath.Join(home, ".npm-global", "bin"))
	}
	return strings.Join(dirs, ":")
}

// GitStubStatus enforces the security-spec.md §4 prerequisite and the
// FD-exec-08.md §5.3.3 and 「判断で決めた点」2 decision to retain the git-stub PATH precedence.
func GitStubStatus() GitStubPathStatus {
	home, err := userHomeDir()
	if err != nil {
		return GitStubPathStatus{Reason: "user home directory is unavailable"}
	}
	if !isValidHomeDir(home) {
		return GitStubPathStatus{Reason: "user home directory is invalid"}
	}
	directory := filepath.Join(home, ".claude", "scripts", gitStubDirectoryName)
	info, err := os.Stat(directory)
	if errors.Is(err, fs.ErrNotExist) {
		return GitStubPathStatus{Directory: directory, Reason: "directory does not exist"}
	}
	if err != nil {
		return GitStubPathStatus{Directory: directory, Reason: "directory cannot be inspected"}
	}
	if !info.IsDir() {
		return GitStubPathStatus{Directory: directory, Reason: "path is not a directory"}
	}
	ownerUID, ok := gitStubDirectoryOwnerUID(info)
	if !ok {
		return GitStubPathStatus{Directory: directory, Reason: "directory owner cannot be determined"}
	}
	if ownerUID != uint32(os.Getuid()) {
		return GitStubPathStatus{Directory: directory, Reason: "directory is not owned by the executing user"}
	}
	if info.Mode().Perm()&0o022 != 0 {
		return GitStubPathStatus{Directory: directory, Reason: "directory is writable by group or other"}
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return GitStubPathStatus{Directory: directory, Reason: "directory entries cannot be inspected"}
	}
	// An empty directory has no PATH-overriding entries to audit, so it is safe to retain.
	// Script contents are out of scope because owner and permission checks prevent unauthorized modification.
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			return GitStubPathStatus{Directory: directory, Reason: fmt.Sprintf("entry %q is not a regular file", entry.Name())}
		}
		entryInfo, err := entry.Info()
		if err != nil {
			return GitStubPathStatus{Directory: directory, Reason: fmt.Sprintf("entry %q cannot be inspected", entry.Name())}
		}
		if !entryInfo.Mode().IsRegular() {
			return GitStubPathStatus{Directory: directory, Reason: fmt.Sprintf("entry %q is not a regular file", entry.Name())}
		}
		entryOwnerUID, ok := gitStubDirectoryOwnerUID(entryInfo)
		if !ok {
			return GitStubPathStatus{Directory: directory, Reason: fmt.Sprintf("entry %q owner cannot be determined", entry.Name())}
		}
		if entryOwnerUID != uint32(os.Getuid()) {
			return GitStubPathStatus{Directory: directory, Reason: fmt.Sprintf("entry %q is not owned by the executing user", entry.Name())}
		}
		if entryInfo.Mode().Perm()&0o022 != 0 {
			return GitStubPathStatus{Directory: directory, Reason: fmt.Sprintf("entry %q is writable by group or other", entry.Name())}
		}
	}
	return GitStubPathStatus{Directory: directory, Eligible: true}
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
