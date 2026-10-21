package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestProjectRecordStoreValidatesAndPersistsAtomically(t *testing.T) {
	root := t.TempDir()
	s := NewProjectRecordStore(root)
	sha := strings.Repeat("a", 40)
	for _, id := range []string{"", "..", "../outside", "a/b", "a\\b", "a--b"} {
		if err := s.AppendCommit(id, sha); err == nil {
			t.Fatalf("accepted project id %q", id)
		}
	}
	if err := s.AppendCommit("project-1", "bad"); err == nil {
		t.Fatal("accepted malformed SHA")
	}
	if err := s.AppendCommit("project-1", sha); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "projects", "project-1.json")
	for _, item := range []struct {
		path string
		mode os.FileMode
	}{{filepath.Dir(path), 0700}, {path, 0600}} {
		info, err := os.Stat(item.path)
		if err != nil || info.Mode().Perm() != item.mode {
			t.Fatalf("permissions for %s: %v %v", item.path, info, err)
		}
	}
	loaded, err := NewProjectRecordStore(root).Load("project-1")
	if err != nil || len(loaded.SupervisorCommitSHAs) != 1 || loaded.SupervisorCommitSHAs[0] != sha {
		t.Fatalf("persisted record: %#v %v", loaded, err)
	}
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendCommit("project-1", strings.Repeat("b", 40)); err == nil {
		t.Fatal("overwrote corrupt record")
	}
}

func TestProjectRecordStoreHistoryLimit(t *testing.T) {
	s := NewProjectRecordStore(t.TempDir())
	for i := 0; i < supervisorCommitSHAHistoryMax+1; i++ {
		sha := fmt.Sprintf("%040x", i)
		if err := s.AppendCommit("project", sha); err != nil {
			t.Fatal(err)
		}
	}
	record, err := s.Load("project")
	if err != nil || len(record.SupervisorCommitSHAs) != supervisorCommitSHAHistoryMax {
		t.Fatalf("history length %d: %v", len(record.SupervisorCommitSHAs), err)
	}
	if record.SupervisorCommitSHAs[0] != fmt.Sprintf("%040x", 1) || record.SupervisorCommitSHAs[len(record.SupervisorCommitSHAs)-1] != fmt.Sprintf("%040x", supervisorCommitSHAHistoryMax) {
		t.Fatal("history did not preserve newest SHAs in order")
	}
}
