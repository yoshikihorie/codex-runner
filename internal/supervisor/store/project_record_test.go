package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

// SCN-sup-03-62: unknown fields do not invalidate project records.
func TestProjectRecordStoreLoadIgnoresUnknownFields(t *testing.T) {
	root := t.TempDir()
	s := NewProjectRecordStore(root)
	want := []string{strings.Repeat("a", 40), strings.Repeat("b", 40)}
	path := filepath.Join(root, "projects", "project.json")
	if err := os.Mkdir(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	body := fmt.Sprintf(`{"supervisor_commit_shas":[%q,%q],"future_field":1}`, want[0], want[1])
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	record, err := s.Load("project")
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(record.SupervisorCommitSHAs, want) {
		t.Fatalf("commit SHAs: got %v, want %v", record.SupervisorCommitSHAs, want)
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != body {
		t.Fatalf("load changed record: %q %v", data, err)
	}
}

func TestProjectRecordStoreLoadRejectsCorruptRecords(t *testing.T) {
	sha := strings.Repeat("a", 40)
	valid := fmt.Sprintf(`{"supervisor_commit_shas":[%q]}`, sha)
	for _, tc := range []struct {
		name string
		body string
	}{
		{"invalid JSON", "{"},
		{"extra JSON value", valid + " {}"},
		{"trailing garbage", valid + " garbage"},
		{"wrong field type", `{"supervisor_commit_shas":1}`},
		{"wrong element type", `{"supervisor_commit_shas":[1]}`},
		{"missing field", `{}`},
		{"null field", `{"supervisor_commit_shas":null}`},
		{"over history limit", `{"supervisor_commit_shas":[` + strings.Repeat(fmt.Sprintf("%q,", sha), supervisorCommitSHAHistoryMax) + fmt.Sprintf("%q]}", sha)},
		{"short SHA", `{"supervisor_commit_shas":["abc"]}`},
		{"uppercase SHA", fmt.Sprintf(`{"supervisor_commit_shas":[%q]}`, strings.Repeat("A", 40))},
		{"nonhex SHA", fmt.Sprintf(`{"supervisor_commit_shas":[%q]}`, strings.Repeat("g", 40))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			path := filepath.Join(root, "projects", "project.json")
			if err := os.Mkdir(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			if _, err := NewProjectRecordStore(root).Load("project"); !errors.Is(err, ErrStateFileCorrupt) {
				t.Fatalf("got %v, want ErrStateFileCorrupt", err)
			}
		})
	}
}

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
