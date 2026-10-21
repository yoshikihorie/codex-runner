package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	atom "github.com/yoshikihorie/codex-runner/internal/store"
)

// SUPERVISOR_COMMIT_SHA_HISTORY_MAX in validation-rules.md.
const supervisorCommitSHAHistoryMax = 200

type ProjectRecord struct {
	SupervisorCommitSHAs []string `json:"supervisor_commit_shas"`
}

type ProjectRecordStore struct {
	root string
	mu   sync.Mutex
}

func NewProjectRecordStore(stateDir string) *ProjectRecordStore {
	return &ProjectRecordStore{root: stateDir}
}

func (s *ProjectRecordStore) projectPath(projectID string) (string, error) {
	if !slugPattern.MatchString(projectID) {
		return "", fmt.Errorf("invalid project id")
	}
	return filepath.Join(s.root, "projects", projectID+".json"), nil
}

func (s *ProjectRecordStore) Load(projectID string) (ProjectRecord, error) {
	path, err := s.projectPath(projectID)
	if err != nil {
		return ProjectRecord{}, err
	}
	if info, e := os.Lstat(filepath.Dir(path)); e == nil && !info.IsDir() {
		return ProjectRecord{}, ErrStateFileCorrupt
	} else if e != nil && !errors.Is(e, os.ErrNotExist) {
		return ProjectRecord{}, e
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return ProjectRecord{SupervisorCommitSHAs: []string{}}, nil
	}
	if err != nil {
		return ProjectRecord{}, err
	}
	if !info.Mode().IsRegular() {
		return ProjectRecord{}, ErrStateFileCorrupt
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ProjectRecord{}, err
	}
	var record ProjectRecord
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&record); err != nil {
		return ProjectRecord{}, fmt.Errorf("%w: %v", ErrStateFileCorrupt, err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return ProjectRecord{}, ErrStateFileCorrupt
	}
	if len(record.SupervisorCommitSHAs) > supervisorCommitSHAHistoryMax || record.SupervisorCommitSHAs == nil {
		return ProjectRecord{}, ErrStateFileCorrupt
	}
	for _, sha := range record.SupervisorCommitSHAs {
		if !shaPattern.MatchString(sha) {
			return ProjectRecord{}, ErrStateFileCorrupt
		}
	}
	return record, nil
}

func (s *ProjectRecordStore) AppendCommit(projectID, sha string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path, err := s.projectPath(projectID)
	if err != nil {
		return err
	}
	if !shaPattern.MatchString(sha) {
		return fmt.Errorf("invalid commit SHA")
	}
	record, err := s.Load(projectID)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err := os.Mkdir(dir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() {
		return ErrStateFileCorrupt
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return err
	}
	record.SupervisorCommitSHAs = append(record.SupervisorCommitSHAs, sha)
	if len(record.SupervisorCommitSHAs) > supervisorCommitSHAHistoryMax {
		record.SupervisorCommitSHAs = record.SupervisorCommitSHAs[len(record.SupervisorCommitSHAs)-supervisorCommitSHAHistoryMax:]
	}
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	return atom.WriteAtomic(path, append(data, '\n'), 0600)
}
