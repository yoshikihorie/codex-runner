package proc

import (
	"bytes"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// These tests deliberately do not call t.Parallel because they modify process-wide
// environment variables, the default logger, and package-level test seams.

var childEnvAdditionalKeys = []string{
	"HTTP_PROXY",
	"http_proxy",
	"HTTPS_PROXY",
	"https_proxy",
	"NO_PROXY",
	"no_proxy",
	"SSL_CERT_FILE",
	"NODE_EXTRA_CA_CERTS",
}

func unsetChildEnvAdditionalKeys(t *testing.T) {
	t.Helper()
	type savedEnv struct {
		value string
		set   bool
	}
	saved := make(map[string]savedEnv, len(childEnvAdditionalKeys))
	for _, key := range childEnvAdditionalKeys {
		value, set := os.LookupEnv(key)
		saved[key] = savedEnv{value: value, set: set}
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() {
		for _, key := range childEnvAdditionalKeys {
			original := saved[key]
			if original.set {
				_ = os.Setenv(key, original.value)
			} else {
				_ = os.Unsetenv(key)
			}
		}
	})
}

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
	originalHomeDir, originalOwnerUID := userHomeDir, fileOwnerUID
	userHomeDir = func() (string, error) { return home, nil }
	fileOwnerUID = func(os.FileInfo) (uint32, bool) { return uint32(os.Getuid()) + 1, true }
	t.Cleanup(func() {
		userHomeDir = originalHomeDir
		fileOwnerUID = originalOwnerUID
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
	unsetChildEnvAdditionalKeys(t)
	t.Setenv("HOME", t.TempDir())
	t.Setenv("FAKE_API_KEY", "secret")
	if got, want := envKeys(strings.Join(SafeChildEnv(), "\n")), map[string]bool{"PATH": true, "HOME": true}; !equalStringSets(got, want) {
		t.Fatalf("SafeChildEnv() keys = %v, want %v", got, want)
	}
}

func TestSafeChildEnvUnchangedWhenAdditionalKeysAreUnset(t *testing.T) {
	unsetChildEnvAdditionalKeys(t)
	home := t.TempDir()
	t.Setenv("HOME", home)

	if got, want := SafeChildEnv(), []string{"PATH=" + FixedPath(), "HOME=" + home}; !equalStringSlices(got, want) {
		t.Fatalf("SafeChildEnv() = %q, want %q", got, want)
	}
}

func TestSafeChildEnvIncludesConfiguredAdditionalKeys(t *testing.T) {
	unsetChildEnvAdditionalKeys(t)
	certificate := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(certificate, []byte("test certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		key   string
		value string
	}{
		{"HTTP_PROXY", "http://proxy.example:8080"},
		{"http_proxy", "proxy.example:8080"},
		{"HTTPS_PROXY", "https://user:password@proxy.example"},
		{"https_proxy", ""},
		{"NO_PROXY", "example.com,127.0.0.1,::1,10.0.0.0/8,*"},
		{"no_proxy", ".internal.example"},
		{"SSL_CERT_FILE", certificate},
		{"NODE_EXTRA_CA_CERTS", certificate},
	} {
		t.Run(test.key, func(t *testing.T) {
			unsetChildEnvAdditionalKeys(t)
			t.Setenv(test.key, test.value)
			if got, ok := envValue(SafeChildEnv(), test.key); !ok || got != test.value {
				t.Fatalf("SafeChildEnv() value for %s = %q, exists = %t, want %q", test.key, got, ok, test.value)
			}
		})
	}
}

func TestSafeChildEnvPreservesProxyKeyCasingAndValues(t *testing.T) {
	unsetChildEnvAdditionalKeys(t)
	values := map[string]string{
		"HTTP_PROXY":  "http://upper.example",
		"http_proxy":  "lower.example:3128",
		"HTTPS_PROXY": "https://user:password@upper.example",
		"https_proxy": "",
		"NO_PROXY":    "example.com,127.0.0.1,::1,10.0.0.0/8,*",
		"no_proxy":    ".internal.example",
	}
	for key, value := range values {
		t.Setenv(key, value)
	}
	for key, want := range values {
		if got, ok := envValue(SafeChildEnv(), key); !ok || got != want {
			t.Fatalf("SafeChildEnv() value for %s = %q, exists = %t, want %q", key, got, ok, want)
		}
	}
}

func TestSafeChildEnvExcludesInvalidCAFilesAndWarnsWithoutValue(t *testing.T) {
	unsafeFile := filepath.Join(t.TempDir(), "unsafe-ca.pem")
	if err := os.WriteFile(unsafeFile, []byte("test certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafeFile, 0o622); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing-ca.pem")
	for _, test := range []struct {
		name   string
		value  string
		reason string
	}{
		{"relative", "relative-ca.pem", caFileReasonNotAbsolute},
		{"missing", missing, caFileReasonNotFound},
		{"directory", t.TempDir(), caFileReasonNotRegular},
		{"group writable", unsafeFile, caFileReasonGroupOrOtherWritable},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, key := range []string{"SSL_CERT_FILE", "NODE_EXTRA_CA_CERTS"} {
				t.Run(key, func(t *testing.T) {
					unsetChildEnvAdditionalKeys(t)
					var logs bytes.Buffer
					previous := slog.Default()
					slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
					t.Cleanup(func() { slog.SetDefault(previous) })
					t.Setenv(key, test.value)

					if got, ok := envValue(SafeChildEnv(), key); ok {
						t.Fatalf("SafeChildEnv() %s = %q, want excluded", key, got)
					}
					if got := logs.String(); !strings.Contains(got, key) || !strings.Contains(got, test.reason) || strings.Contains(got, test.value) || strings.Count(got, "\n") != 1 {
						t.Fatalf("warning = %q, want one line with key and reason without value", got)
					}
				})
			}
		})
	}
}

func TestSafeChildEnvExcludesCAFileWithForeignOwner(t *testing.T) {
	unsetChildEnvAdditionalKeys(t)
	certificate := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(certificate, []byte("test certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalOwnerUID := fileOwnerUID
	fileOwnerUID = func(os.FileInfo) (uint32, bool) { return uint32(os.Getuid()) + 1, true }
	t.Cleanup(func() { fileOwnerUID = originalOwnerUID })
	t.Setenv("SSL_CERT_FILE", certificate)

	if _, ok := envValue(SafeChildEnv(), "SSL_CERT_FILE"); ok {
		t.Fatal("SafeChildEnv() includes foreign-owned SSL_CERT_FILE")
	}
}

func TestSafeChildEnvAcceptsRootOwnedCAFile(t *testing.T) {
	unsetChildEnvAdditionalKeys(t)
	certificate := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(certificate, []byte("test certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	originalOwnerUID := fileOwnerUID
	fileOwnerUID = func(os.FileInfo) (uint32, bool) { return 0, true }
	t.Cleanup(func() { fileOwnerUID = originalOwnerUID })
	t.Setenv("SSL_CERT_FILE", certificate)

	if got, ok := envValue(SafeChildEnv(), "SSL_CERT_FILE"); !ok || got != certificate {
		t.Fatalf("SafeChildEnv() SSL_CERT_FILE = %q, exists = %t, want %q", got, ok, certificate)
	}
}

func TestSafeChildEnvCAFileParentDirectorySafety(t *testing.T) {
	for _, test := range []struct {
		name    string
		mode    os.FileMode
		allowed bool
	}{
		{name: "group writable without sticky bit", mode: 0o770, allowed: false},
		{name: "group writable with sticky bit", mode: os.ModeSticky | 0o777, allowed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			unsetChildEnvAdditionalKeys(t)
			directory := t.TempDir()
			certificate := filepath.Join(directory, "ca.pem")
			if err := os.WriteFile(certificate, []byte("test certificate"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(directory, test.mode); err != nil {
				t.Fatal(err)
			}
			t.Setenv("SSL_CERT_FILE", certificate)

			got, ok := envValue(SafeChildEnv(), "SSL_CERT_FILE")
			if ok != test.allowed || test.allowed && got != certificate {
				t.Fatalf("SafeChildEnv() SSL_CERT_FILE = %q, exists = %t, allowed = %t", got, ok, test.allowed)
			}
		})
	}
}

func TestSafeChildEnvWarnsForInvalidHome(t *testing.T) {
	unsetChildEnvAdditionalKeys(t)
	t.Setenv("HOME", "relative-home")
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	SafeChildEnv()

	if got := logs.String(); !strings.Contains(got, "HOME") || strings.Count(got, "\n") != 1 {
		t.Fatalf("warning = %q, want one line for HOME", got)
	}
}

func TestValidateSafeEnvAcceptsConfiguredAdditionalKeys(t *testing.T) {
	certificate := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(certificate, []byte("test certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	env := []string{"PATH=" + FixedPath(), "HTTP_PROXY=http://proxy.example", "http_proxy=proxy.example:3128", "HTTPS_PROXY=https://user:password@proxy.example", "https_proxy=", "NO_PROXY=example.com,127.0.0.1,::1,10.0.0.0/8,*", "no_proxy=.internal.example", "SSL_CERT_FILE=" + certificate, "NODE_EXTRA_CA_CERTS=" + certificate}
	if err := validateSafeEnv(env); err != nil {
		t.Fatalf("validateSafeEnv() error = %v", err)
	}
}

func TestValidateSafeEnvRejectsInvalidCAFiles(t *testing.T) {
	unsafeFile := filepath.Join(t.TempDir(), "unsafe-ca.pem")
	if err := os.WriteFile(unsafeFile, []byte("test certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(unsafeFile, 0o622); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(t.TempDir(), "missing-ca.pem")
	for _, test := range []struct {
		name   string
		value  string
		reason string
	}{
		{"relative", "relative-ca.pem", caFileReasonNotAbsolute},
		{"missing", missing, caFileReasonNotFound},
		{"directory", t.TempDir(), caFileReasonNotRegular},
		{"group writable", unsafeFile, caFileReasonGroupOrOtherWritable},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, key := range []string{"SSL_CERT_FILE", "NODE_EXTRA_CA_CERTS"} {
				t.Run(key, func(t *testing.T) {
					env := []string{"PATH=" + FixedPath(), key + "=" + test.value}
					if err := validateSafeEnv(env); err == nil || !strings.Contains(err.Error(), test.reason) {
						t.Fatalf("validateSafeEnv() error = %v, want reason %q", err, test.reason)
					}
				})
			}
		})
	}
}

func TestValidateSafeEnvRejectsUnsafeCAFileOwnershipAndParentDirectory(t *testing.T) {
	for _, test := range []struct {
		name       string
		ownerUID   uint32
		directory  os.FileMode
		wantReason string
	}{
		{name: "foreign owner", ownerUID: uint32(os.Getuid()) + 1, directory: 0o700, wantReason: caFileReasonNotOwnedByUserOrRoot},
		{name: "unsafe parent directory", ownerUID: uint32(os.Getuid()), directory: 0o770, wantReason: caFileReasonUnsafeParentDirectory},
	} {
		t.Run(test.name, func(t *testing.T) {
			directory := t.TempDir()
			certificate := filepath.Join(directory, "ca.pem")
			if err := os.WriteFile(certificate, []byte("test certificate"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(directory, test.directory); err != nil {
				t.Fatal(err)
			}
			originalOwnerUID := fileOwnerUID
			fileOwnerUID = func(os.FileInfo) (uint32, bool) { return test.ownerUID, true }
			t.Cleanup(func() { fileOwnerUID = originalOwnerUID })

			err := validateSafeEnv([]string{"PATH=" + FixedPath(), "SSL_CERT_FILE=" + certificate})
			if err == nil || !strings.Contains(err.Error(), test.wantReason) {
				t.Fatalf("validateSafeEnv() error = %v, want reason %q", err, test.wantReason)
			}
		})
	}
}

func TestValidateSafeEnvRejectsUnallowlistedMixedCaseKey(t *testing.T) {
	env := []string{"PATH=" + FixedPath(), "HTTP_Proxy=http://proxy.example", "FAKE_API_KEY=secret"}
	if err := validateSafeEnv(env); err == nil {
		t.Fatal("validateSafeEnv() error = nil")
	}
}

func envValue(env []string, wantKey string) (string, bool) {
	for _, entry := range env {
		key, value, found := strings.Cut(entry, "=")
		if found && key == wantKey {
			return value, true
		}
	}
	return "", false
}

func equalStringSlices(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
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
