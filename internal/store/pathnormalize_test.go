package store

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizePathResolvesSymlinkAndRejectsInvalidInput(t *testing.T) {
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
	if _, err := NormalizePath("relative", false); err == nil {
		t.Fatal("relative path accepted")
	}
	if _, err := NormalizePath(filepath.Join(root, "missing"), false); err == nil {
		t.Fatal("missing path accepted")
	}
}
