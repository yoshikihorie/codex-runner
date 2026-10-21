package proc

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestValidateExecutable(t *testing.T) {
	oldOwnerCheck := executableOwnedByCurrentUserOrRoot
	t.Cleanup(func() { executableOwnedByCurrentUserOrRoot = oldOwnerCheck })
	executableOwnedByCurrentUserOrRoot = func(os.FileInfo) bool { return true }

	root := t.TempDir()
	file := filepath.Join(root, "tool")
	if err := os.WriteFile(file, []byte("tool"), 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(file, link); err != nil {
		t.Fatal(err)
	}
	noExec := filepath.Join(root, "noexec")
	if err := os.WriteFile(noExec, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(root, "dir")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	worldFile := filepath.Join(root, "world")
	if err := os.WriteFile(worldFile, nil, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(worldFile, 0o777); err != nil {
		t.Fatal(err)
	}
	worldDir := filepath.Join(root, "worlddir")
	if err := os.Mkdir(worldDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(worldDir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(worldDir, "tool"), nil, 0o700); err != nil {
		t.Fatal(err)
	}
	stickyDir := filepath.Join(root, "stickydir")
	if err := os.Mkdir(stickyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(stickyDir, 0o1777); err != nil {
		t.Fatal(err)
	}
	stickyFile := filepath.Join(stickyDir, "tool")
	if err := os.WriteFile(stickyFile, nil, 0o700); err != nil {
		t.Fatal(err)
	}
	badLink := filepath.Join(root, "badlink")
	if err := os.Symlink(noExec, badLink); err != nil {
		t.Fatal(err)
	}

	cases := []struct{ name, path, reason string }{
		{"valid", file, ""}, {"relative", "tool", "not_absolute"},
		{"missing", filepath.Join(root, "missing"), "not_found"}, {"directory", dir, "not_regular"},
		{"non executable", noExec, "not_executable"}, {"other owner", file, "not_owned"},
		{"root owner", file, ""}, {"0777 file", worldFile, "writable_by_others"},
		{"0777 parent", filepath.Join(worldDir, "tool"), "parent_writable_by_others"},
		{"sticky 1777 parent", stickyFile, "parent_writable_by_others"},
		{"symlink target violates mode", badLink, "not_executable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executableOwnedByCurrentUserOrRoot = func(os.FileInfo) bool { return tc.name != "other owner" }
			if tc.name == "root owner" {
				executableOwnedByCurrentUserOrRoot = func(os.FileInfo) bool { return true }
			}
			_, err := ValidateExecutable(tc.path)
			if tc.reason == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.reason != "" {
				var validationErr *ExecutableValidationError
				if !errors.As(err, &validationErr) || validationErr.Reason != tc.reason {
					t.Fatalf("error=%v, want reason %q", err, tc.reason)
				}
			}
		})
	}
}
