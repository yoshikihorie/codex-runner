package proc

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFixedPathIgnoresAmbientPathAndRelativeHome(t *testing.T) {
	t.Setenv("PATH", "/unsafe/bin")
	originalHomeDir := userHomeDir
	userHomeDir = func() (string, error) { return "relative-home", nil }
	t.Cleanup(func() { userHomeDir = originalHomeDir })

	got := FixedPath()
	if strings.Contains(got, "/unsafe/bin") || strings.Contains(got, "relative-home") {
		t.Fatalf("FixedPath() = %q contains unsafe directory", got)
	}
	if want := strings.Join(fixedPathDirs, ":"); got != want {
		t.Fatalf("FixedPath() = %q, want %q", got, want)
	}
}

func TestFixedPathAppendsAbsoluteNPMGlobalBin(t *testing.T) {
	home := t.TempDir()
	originalHomeDir := userHomeDir
	userHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { userHomeDir = originalHomeDir })

	if got, want := FixedPath(), strings.Join(append(append([]string{}, fixedPathDirs...), filepath.Join(home, ".npm-global", "bin")), ":"); got != want {
		t.Fatalf("FixedPath() = %q, want %q", got, want)
	}
}

func TestFixedPathPrependsExistingGitStubDirectory(t *testing.T) {
	home := t.TempDir()
	stubDir := filepath.Join(home, ".claude", "scripts", gitStubDirectoryName)
	if err := os.MkdirAll(stubDir, 0o700); err != nil {
		t.Fatal(err)
	}
	originalHomeDir := userHomeDir
	userHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { userHomeDir = originalHomeDir })

	wantDirs := append([]string{stubDir}, fixedPathDirs...)
	wantDirs = append(wantDirs, filepath.Join(home, ".npm-global", "bin"))
	if got, want := FixedPath(), strings.Join(wantDirs, ":"); got != want {
		t.Fatalf("FixedPath() = %q, want %q", got, want)
	}
}

func TestFixedPathPrependsLiteralGitStubDirectory(t *testing.T) {
	home := t.TempDir()
	stubDir := filepath.Join(home, ".claude/scripts/codex-blocked-stubs")
	if err := os.MkdirAll(stubDir, 0o700); err != nil {
		t.Fatal(err)
	}
	originalHomeDir := userHomeDir
	userHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { userHomeDir = originalHomeDir })

	if got := strings.Split(FixedPath(), ":")[0]; got != stubDir {
		t.Fatalf("FixedPath() first directory = %q, want literal stub path %q", got, stubDir)
	}
}

func TestFixedPathSkipsGitStubDirectoryThatIsNotDirectory(t *testing.T) {
	home := t.TempDir()
	stubPath := filepath.Join(home, ".claude", "scripts", gitStubDirectoryName)
	if err := os.MkdirAll(filepath.Dir(stubPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stubPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	originalHomeDir := userHomeDir
	userHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { userHomeDir = originalHomeDir })

	if got := FixedPath(); strings.Contains(got, stubPath) {
		t.Fatalf("FixedPath() = %q contains regular-file stub path %q", got, stubPath)
	}
	if status := GitStubStatus(); status.Eligible || status.Reason != "path is not a directory" {
		t.Fatalf("GitStubStatus() = %#v", status)
	}
}

func TestFixedPathSkipsGroupOrOtherWritableGitStubDirectory(t *testing.T) {
	for _, mode := range []os.FileMode{0o720, 0o702} {
		t.Run(mode.String(), func(t *testing.T) {
			home := t.TempDir()
			stubDir := filepath.Join(home, ".claude", "scripts", gitStubDirectoryName)
			if err := os.MkdirAll(stubDir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(stubDir, mode); err != nil {
				t.Fatal(err)
			}
			originalHomeDir := userHomeDir
			userHomeDir = func() (string, error) { return home, nil }
			t.Cleanup(func() { userHomeDir = originalHomeDir })

			if got := FixedPath(); strings.Contains(got, stubDir) {
				t.Fatalf("FixedPath() = %q contains writable stub directory %q", got, stubDir)
			}
			if status := GitStubStatus(); status.Eligible || status.Reason != "directory is writable by group or other" {
				t.Fatalf("GitStubStatus() = %#v", status)
			}
		})
	}
}

func TestGitStubStatusRejectsUnsafeDirectoryEntries(t *testing.T) {
	for _, test := range []struct {
		name        string
		createEntry func(t *testing.T, directory string)
		wantReason  string
	}{
		{
			name: "group or other writable regular file",
			createEntry: func(t *testing.T, directory string) {
				t.Helper()
				path := filepath.Join(directory, "git")
				if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(path, 0o777); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: "entry \"git\" is writable by group or other",
		},
		{
			name: "symbolic link",
			createEntry: func(t *testing.T, directory string) {
				t.Helper()
				if err := os.Symlink("/bin/sh", filepath.Join(directory, "git")); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: "entry \"git\" is not a regular file",
		},
		{
			name: "subdirectory",
			createEntry: func(t *testing.T, directory string) {
				t.Helper()
				if err := os.Mkdir(filepath.Join(directory, "nested"), 0o755); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: "entry \"nested\" is not a regular file",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			home := t.TempDir()
			stubDir := filepath.Join(home, ".claude", "scripts", gitStubDirectoryName)
			if err := os.MkdirAll(stubDir, 0o700); err != nil {
				t.Fatal(err)
			}
			test.createEntry(t, stubDir)
			setGitStubHome(t, home)

			status := GitStubStatus()
			if status.Eligible || status.Reason != test.wantReason {
				t.Fatalf("GitStubStatus() = %#v, want ineligible with reason %q", status, test.wantReason)
			}
			if got := FixedPath(); strings.Contains(got, stubDir) {
				t.Fatalf("FixedPath() = %q contains unsafe stub directory %q", got, stubDir)
			}
		})
	}
}

func TestGitStubStatusAcceptsSafeDirectoryEntries(t *testing.T) {
	home := t.TempDir()
	stubDir := filepath.Join(home, ".claude", "scripts", gitStubDirectoryName)
	if err := os.MkdirAll(stubDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"git", "git-wrapper", "helper"} {
		if err := os.WriteFile(filepath.Join(stubDir, name), []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	setGitStubHome(t, home)

	if status := GitStubStatus(); !status.Eligible {
		t.Fatalf("GitStubStatus() = %#v, want eligible", status)
	}
}

func TestGitStubStatusAcceptsEmptyDirectory(t *testing.T) {
	home := t.TempDir()
	stubDir := filepath.Join(home, ".claude", "scripts", gitStubDirectoryName)
	if err := os.MkdirAll(stubDir, 0o700); err != nil {
		t.Fatal(err)
	}
	setGitStubHome(t, home)

	if status := GitStubStatus(); !status.Eligible {
		t.Fatalf("GitStubStatus() = %#v, want eligible", status)
	}
}

func setGitStubHome(t *testing.T, home string) {
	t.Helper()
	originalHomeDir := userHomeDir
	userHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { userHomeDir = originalHomeDir })
}

func TestGitStubStatusRejectsForeignOwner(t *testing.T) {
	home := t.TempDir()
	stubDir := filepath.Join(home, ".claude", "scripts", gitStubDirectoryName)
	if err := os.MkdirAll(stubDir, 0o700); err != nil {
		t.Fatal(err)
	}
	originalHomeDir, originalOwnerUID := userHomeDir, gitStubDirectoryOwnerUID
	userHomeDir = func() (string, error) { return home, nil }
	gitStubDirectoryOwnerUID = func(os.FileInfo) (uint32, bool) { return uint32(os.Getuid()) + 1, true }
	t.Cleanup(func() {
		userHomeDir = originalHomeDir
		gitStubDirectoryOwnerUID = originalOwnerUID
	})

	if status := GitStubStatus(); status.Eligible || status.Reason != "directory is not owned by the executing user" {
		t.Fatalf("GitStubStatus() = %#v", status)
	}
}

func TestFixedPathSkipsNonexistentGitStubDirectory(t *testing.T) {
	home := t.TempDir()
	originalHomeDir := userHomeDir
	userHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { userHomeDir = originalHomeDir })

	stubDir := filepath.Join(home, ".claude", "scripts", gitStubDirectoryName)
	if got := FixedPath(); strings.Contains(got, stubDir) {
		t.Fatalf("FixedPath() = %q contains nonexistent git stub directory %q", got, stubDir)
	}
	if status := GitStubStatus(); status.Eligible || status.Reason != "directory does not exist" {
		t.Fatalf("GitStubStatus() = %#v", status)
	}
}

func TestSafeChildEnvPathMatchesFixedPathWithGitStubDirectory(t *testing.T) {
	home := t.TempDir()
	stubDir := filepath.Join(home, ".claude", "scripts", gitStubDirectoryName)
	if err := os.MkdirAll(stubDir, 0o700); err != nil {
		t.Fatal(err)
	}
	originalHomeDir := userHomeDir
	userHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { userHomeDir = originalHomeDir })

	env := SafeChildEnv()
	if len(env) == 0 || env[0] != "PATH="+FixedPath() {
		t.Fatalf("SafeChildEnv() PATH = %q, want %q", env, "PATH="+FixedPath())
	}
	if err := validateSafeEnv(env); err != nil {
		t.Fatalf("validateSafeEnv(SafeChildEnv()) error = %v", err)
	}
}

func TestFixedPathSkipsNonexistentAbsoluteHome(t *testing.T) {
	home := t.TempDir()
	if err := os.RemoveAll(home); err != nil {
		t.Fatal(err)
	}
	originalHomeDir := userHomeDir
	userHomeDir = func() (string, error) { return home, nil }
	t.Cleanup(func() { userHomeDir = originalHomeDir })

	if got, want := FixedPath(), strings.Join(fixedPathDirs, ":"); got != want {
		t.Fatalf("FixedPath() = %q, want %q", got, want)
	}
}

func TestSafeChildEnvOnlyIncludesAllowlistedKeys(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("FAKE_API_KEY", "secret")
	if got, want := envKeys(strings.Join(SafeChildEnv(), "\n")), map[string]bool{"PATH": true, "HOME": true}; !equalStringSets(got, want) {
		t.Fatalf("SafeChildEnv() keys = %v, want %v", got, want)
	}
}

func TestSafeChildEnvSkipsNonexistentHome(t *testing.T) {
	home := t.TempDir()
	if err := os.RemoveAll(home); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", home)

	if got := envKeys(strings.Join(SafeChildEnv(), "\n")); got["HOME"] {
		t.Fatalf("SafeChildEnv() keys = %v, want no HOME", got)
	}
}

func TestValidateSafeEnvFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name string
		env  []string
	}{
		{name: "empty", env: []string{}},
		{name: "missing PATH", env: []string{"HOME=/safe/home"}},
		{name: "wrong PATH", env: []string{"PATH=/unsafe/bin"}},
		{name: "unexpected key", env: []string{"PATH=" + FixedPath(), "FAKE_API_KEY=secret"}},
		{name: "duplicate PATH", env: []string{"PATH=" + FixedPath(), "PATH=" + FixedPath()}},
		{name: "relative HOME", env: []string{"PATH=" + FixedPath(), "HOME=relative-home"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateSafeEnv(test.env); err == nil {
				t.Fatal("validateSafeEnv() error = nil")
			}
		})
	}
}

func TestFindGitBinary(t *testing.T) {
	executable := filepath.Join(t.TempDir(), "git")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	originalCandidates := gitBinaryCandidates
	gitBinaryCandidates = []string{executable}
	t.Cleanup(func() { gitBinaryCandidates = originalCandidates })
	if got, err := FindGitBinary(); err != nil || got != executable {
		t.Fatalf("FindGitBinary() = %q, %v; want %q, nil", got, err, executable)
	}

	gitBinaryCandidates = []string{filepath.Join(t.TempDir(), "missing-git")}
	if _, err := FindGitBinary(); err == nil {
		t.Fatal("FindGitBinary() error = nil with no executable candidates")
	}
}

func TestFixedPathSkipsUnavailableHome(t *testing.T) {
	originalHomeDir := userHomeDir
	userHomeDir = func() (string, error) { return "", errors.New("home unavailable") }
	t.Cleanup(func() { userHomeDir = originalHomeDir })
	if got, want := FixedPath(), strings.Join(fixedPathDirs, ":"); got != want {
		t.Fatalf("FixedPath() = %q, want %q", got, want)
	}
}
