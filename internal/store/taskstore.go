package store

import (
	"encoding/json"
	"fmt"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"io"
	"os"
	"sort"
	"sync"
	"syscall"
)

type FileTaskStore struct {
	root      string
	mu        sync.RWMutex
	index     map[string]domain.TaskSnapshot
	corrupted []domain.TaskID
}

type TaskStore interface {
	Load(domain.TaskID) (domain.TaskSnapshot, error)
	Save(domain.TaskID, domain.TaskSnapshot) error
	ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error)
	Reserve(domain.TaskID) error
	Release(domain.TaskID) error
}

var _ TaskStore = (*FileTaskStore)(nil)

func NewFileTaskStore(root string) (*FileTaskStore, error) {
	s := &FileTaskStore{root: root, index: map[string]domain.TaskSnapshot{}}
	es, e := os.ReadDir(root)
	if os.IsNotExist(e) {
		return s, nil
	}
	if e != nil {
		return nil, e
	}
	for _, x := range es {
		id, e := domain.NewTaskID(x.Name())
		if e != nil {
			continue
		}
		p, _ := newTaskPaths(root, id)
		d, e := openTaskDir(p.dir())
		if e != nil || d == nil {
			s.corrupted = append(s.corrupted, id)
			continue
		}
		d.Close()
		v, e := s.read(p.taskJSON())
		if e != nil || v.Validate() != nil || v.TaskID != id {
			s.corrupted = append(s.corrupted, id)
			continue
		}
		s.index[id.String()] = v
	}
	return s, nil
}
func (s *FileTaskStore) read(path string) (domain.TaskSnapshot, error) {
	f, e := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if e != nil {
		return domain.TaskSnapshot{}, e
	}
	defer f.Close()
	b, e := io.ReadAll(f)
	if e != nil {
		return domain.TaskSnapshot{}, e
	}
	var v domain.TaskSnapshot
	e = json.Unmarshal(b, &v)
	return v, e
}
func (s *FileTaskStore) CorruptedTaskIDs() []domain.TaskID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]domain.TaskID(nil), s.corrupted...)
}
func (s *FileTaskStore) Reserve(id domain.TaskID) error {
	p, e := newTaskPaths(s.root, id)
	if e != nil {
		return e
	}
	return os.Mkdir(p.dir(), taskDirPerm)
}
func (s *FileTaskStore) Release(id domain.TaskID) error {
	p, e := newTaskPaths(s.root, id)
	if e != nil {
		return e
	}
	if e = os.Remove(p.dir()); e != nil {
		return e
	}
	s.mu.Lock()
	delete(s.index, id.String())
	s.mu.Unlock()
	return nil
}
func (s *FileTaskStore) Save(id domain.TaskID, v domain.TaskSnapshot) error {
	if id.String() != v.TaskID.String() {
		return fmt.Errorf("taskID mismatch: %s != %s", id.String(), v.TaskID.String())
	}
	if e := v.Validate(); e != nil {
		return e
	}
	p, e := newTaskPaths(s.root, id)
	if e != nil {
		return e
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, e := openTaskDir(p.dir())
	if e != nil {
		return e
	}
	if d == nil {
		return fmt.Errorf("task directory not reserved")
	}
	d.Close()
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if e = WriteAtomic(p.taskJSON(), b, taskFilePerm); e != nil {
		return e
	}
	s.index[id.String()] = v
	return nil
}
func (s *FileTaskStore) Load(id domain.TaskID) (domain.TaskSnapshot, error) {
	p, e := newTaskPaths(s.root, id)
	if e != nil {
		return domain.TaskSnapshot{}, e
	}
	d, e := openTaskDir(p.dir())
	if e != nil {
		return domain.TaskSnapshot{}, e
	}
	if d == nil {
		return domain.TaskSnapshot{}, domain.ErrTaskNotFound
	}
	d.Close()
	v, e := s.read(p.taskJSON())
	if os.IsNotExist(e) {
		return domain.TaskSnapshot{}, domain.ErrTaskNotFound
	}
	if e != nil {
		return domain.TaskSnapshot{}, e
	}
	if e = v.Validate(); e != nil {
		return domain.TaskSnapshot{}, e
	}
	if v.TaskID != id {
		return domain.TaskSnapshot{}, fmt.Errorf("taskID mismatch: %s != %s", id.String(), v.TaskID.String())
	}
	return v, nil
}
func (s *FileTaskStore) ListByStates(states []domain.TaskState) ([]domain.TaskSnapshot, error) {
	wanted := map[domain.TaskState]struct{}{}
	for _, v := range states {
		wanted[v] = struct{}{}
	}
	out := []domain.TaskSnapshot{}
	s.mu.RLock()
	for _, v := range s.index {
		if _, ok := wanted[v.State]; ok {
			out = append(out, v)
		}
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].TaskID.String() < out[j].TaskID.String() })
	return out, nil
}
