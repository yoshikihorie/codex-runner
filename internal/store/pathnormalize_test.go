package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizePathResolvesSymlinkAndRejectsInvalidInput(t *testing.T) {
	t.Run("resolves an existing path and trailing separator", func(t *testing.T) {
		root := t.TempDir()
		target := filepath.Join(root, "target")
		if err := os.Mkdir(target, 0o700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(root, "link")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		got, err := NormalizePath(link+string(filepath.Separator), false)
		if err != nil {
			t.Fatal(err)
		}
		resolvedTarget, err := filepath.EvalSymlinks(target)
		if err != nil {
			t.Fatal(err)
		}
		if got.String() != resolvedTarget {
			t.Fatalf("normalized = %q, want %q", got.String(), resolvedTarget)
		}
	})

	t.Run("SCN-lock-01-13 accepts one missing final component", func(t *testing.T) {
		parent := t.TempDir()
		missing := filepath.Join(parent, "new.go")
		got, err := NormalizePath(missing, false)
		if err != nil {
			t.Fatal(err)
		}
		resolvedParent, err := filepath.EvalSymlinks(parent)
		if err != nil {
			t.Fatal(err)
		}
		if got.String() != filepath.Join(resolvedParent, "new.go") {
			t.Fatalf("normalized = %q", got.String())
		}
	})

	t.Run("SCN-lock-01-13 resolves a symlinked parent for a missing final component", func(t *testing.T) {
		root := t.TempDir()
		realParent := filepath.Join(root, "real")
		if err := os.Mkdir(realParent, 0o700); err != nil {
			t.Fatal(err)
		}
		linkedParent := filepath.Join(root, "linked")
		if err := os.Symlink(realParent, linkedParent); err != nil {
			t.Fatal(err)
		}
		got, err := NormalizePath(filepath.Join(linkedParent, "new.go"), false)
		if err != nil {
			t.Fatal(err)
		}
		resolvedParent, err := filepath.EvalSymlinks(realParent)
		if err != nil {
			t.Fatal(err)
		}
		if got.String() != filepath.Join(resolvedParent, "new.go") {
			t.Fatalf("normalized = %q", got.String())
		}
	})

	t.Run("resolves symlink before dotdot for a missing final component", func(t *testing.T) {
		root := t.TempDir()
		lexicalParent := filepath.Join(root, "a")
		physicalParent := filepath.Join(root, "physical")
		physicalChild := filepath.Join(physicalParent, "child")
		if err := os.MkdirAll(lexicalParent, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(physicalChild, 0o700); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(lexicalParent, "link")
		if err := os.Symlink(physicalChild, link); err != nil {
			t.Fatal(err)
		}
		raw := link + string(filepath.Separator) + ".." + string(filepath.Separator) + "new.go"
		got, err := NormalizePath(raw, false)
		if err != nil {
			t.Fatal(err)
		}
		resolvedParent, err := filepath.EvalSymlinks(physicalParent)
		if err != nil {
			t.Fatal(err)
		}
		want := filepath.Join(resolvedParent, "new.go")
		if got.String() != want {
			t.Fatalf("normalized = %q, want %q", got.String(), want)
		}
	})

	t.Run("accepts an existing path ending in dot", func(t *testing.T) {
		dir := t.TempDir()
		raw := dir + string(filepath.Separator) + "."
		got, err := NormalizePath(raw, false)
		if err != nil {
			t.Fatal(err)
		}
		want, err := filepath.EvalSymlinks(dir)
		if err != nil {
			t.Fatal(err)
		}
		if got.String() != want {
			t.Fatalf("normalized = %q, want %q", got.String(), want)
		}
	})

	t.Run("lowercases a missing final component on macOS", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "UPPER")
		if err := os.Mkdir(parent, 0o700); err != nil {
			t.Fatal(err)
		}
		got, err := NormalizePath(filepath.Join(parent, "New.GO"), true)
		if err != nil {
			t.Fatal(err)
		}
		resolvedParent, err := filepath.EvalSymlinks(parent)
		if err != nil {
			t.Fatal(err)
		}
		want := strings.ToLower(filepath.Join(resolvedParent, "New.GO"))
		if got.String() != want {
			t.Fatalf("normalized = %q, want %q", got.String(), want)
		}
	})

	t.Run("rejects a missing final component below an inaccessible parent", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "inaccessible")
		if err := os.Mkdir(parent, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(parent, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := os.Chmod(parent, 0o700); err != nil {
				t.Errorf("restore parent permissions: %v", err)
			}
		})
		missing := filepath.Join(parent, "new.go")
		if _, err := os.Stat(missing); !os.IsPermission(err) {
			t.Skipf("filesystem permissions are not enforced for the test user: probe error = %v", err)
		}
		if _, err := NormalizePath(missing, false); err == nil {
			t.Fatal("path below inaccessible parent accepted")
		}
	})

	t.Run("SCN-lock-01-14 rejects two missing components", func(t *testing.T) {
		missing := filepath.Join(t.TempDir(), "missing-parent", "new.go")
		if _, err := NormalizePath(missing, false); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("error = %v, want ENOENT", err)
		}
	})

	t.Run("SCN-lock-01-15 rejects a dangling final symlink", func(t *testing.T) {
		root := t.TempDir()
		dangling := filepath.Join(root, "dangling")
		if err := os.Symlink(filepath.Join(root, "missing-target"), dangling); err != nil {
			t.Fatal(err)
		}
		if _, err := NormalizePath(dangling, false); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("error = %v, want ENOENT", err)
		}
	})

	t.Run("rejects invalid missing final component names", func(t *testing.T) {
		root := t.TempDir()
		for _, raw := range []string{
			filepath.Join(root, "missing") + string(filepath.Separator),
			filepath.Join(root, "missing") + string(filepath.Separator) + ".",
			filepath.Join(root, "missing") + string(filepath.Separator) + "..",
		} {
			if _, err := NormalizePath(raw, false); err == nil {
				t.Fatalf("path %q accepted", raw)
			}
		}
	})

	t.Run("rejects a symlink loop", func(t *testing.T) {
		root := t.TempDir()
		first := filepath.Join(root, "first")
		second := filepath.Join(root, "second")
		if err := os.Symlink("second", first); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("first", second); err != nil {
			t.Fatal(err)
		}
		if _, err := NormalizePath(first, false); err == nil || errors.Is(err, os.ErrNotExist) {
			t.Fatalf("error = %v, want non-ENOENT failure", err)
		}
	})

	t.Run("rejects empty and relative paths", func(t *testing.T) {
		for _, raw := range []string{"", "relative"} {
			if _, err := NormalizePath(raw, false); err == nil {
				t.Fatalf("path %q accepted", raw)
			}
		}
	})

	t.Run("does not rescue a non-ENOENT resolution failure", func(t *testing.T) {
		root := t.TempDir()
		file := filepath.Join(root, "file")
		if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NormalizePath(filepath.Join(file, "child"), false); err == nil || errors.Is(err, os.ErrNotExist) {
			t.Fatalf("error = %v, want non-ENOENT failure", err)
		}
	})

	t.Run("preserves macOS lowercasing", func(t *testing.T) {
		root := t.TempDir()
		upper := filepath.Join(root, "UPPER")
		if err := os.Mkdir(upper, 0o700); err != nil {
			t.Fatal(err)
		}
		got, err := NormalizePath(upper+string(filepath.Separator), true)
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := filepath.EvalSymlinks(upper)
		if err != nil {
			t.Fatal(err)
		}
		if got.String() != strings.ToLower(resolved) {
			t.Fatalf("normalized = %q, want %q", got.String(), strings.ToLower(resolved))
		}
	})
}
