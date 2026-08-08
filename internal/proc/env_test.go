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
