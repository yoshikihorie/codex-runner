package proc

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// ExecutableValidationError identifies why an executable failed validation.
type ExecutableValidationError struct {
	Reason string
	Err    error
}

func (e *ExecutableValidationError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Reason, e.Err)
	}
	return e.Reason
}
func (e *ExecutableValidationError) Unwrap() error { return e.Err }

var executableOwnedByCurrentUserOrRoot = func(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (int(stat.Uid) == os.Getuid() || stat.Uid == 0)
}

// ValidateExecutable resolves path symlinks and validates the executable and its parent directory.
func ValidateExecutable(path string) (resolved string, err error) {
	if !filepath.IsAbs(path) {
		return "", &ExecutableValidationError{Reason: "not_absolute"}
	}
	resolved, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", &ExecutableValidationError{Reason: "not_found", Err: err}
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", &ExecutableValidationError{Reason: "not_found", Err: err}
	}
	if !info.Mode().IsRegular() {
		return "", &ExecutableValidationError{Reason: "not_regular"}
	}
	if info.Mode().Perm()&0111 == 0 {
		return "", &ExecutableValidationError{Reason: "not_executable"}
	}
	if !executableOwnedByCurrentUserOrRoot(info) {
		return "", &ExecutableValidationError{Reason: "not_owned"}
	}
	if info.Mode().Perm()&022 != 0 {
		return "", &ExecutableValidationError{Reason: "writable_by_others"}
	}
	parent, err := os.Stat(filepath.Dir(resolved))
	if err != nil {
		return "", &ExecutableValidationError{Reason: "not_found", Err: err}
	}
	if !executableOwnedByCurrentUserOrRoot(parent) {
		return "", &ExecutableValidationError{Reason: "not_owned"}
	}
	if parent.Mode().Perm()&022 != 0 {
		return "", &ExecutableValidationError{Reason: "parent_writable_by_others"}
	}
	return resolved, nil
}
