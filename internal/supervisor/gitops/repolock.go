package gitops

import (
	"path/filepath"
	"sync"
)

type RepoLocker struct {
	mu   sync.Mutex
	held map[string]bool
}

func NewRepoLocker() *RepoLocker { return &RepoLocker{held: map[string]bool{}} }

// TryLock uses the caller-resolved real git-common-dir path as the repository key.
func (l *RepoLocker) TryLock(gitCommonDir string) (func(), bool) {
	p, e := filepath.EvalSymlinks(gitCommonDir)
	if e != nil {
		return nil, false
	}
	p, e = filepath.Abs(p)
	if e != nil {
		return nil, false
	}
	l.mu.Lock()
	if l.held == nil {
		l.held = map[string]bool{}
	}
	if l.held[p] {
		l.mu.Unlock()
		return nil, false
	}
	l.held[p] = true
	l.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { l.mu.Lock(); delete(l.held, p); l.mu.Unlock() }) }, true
}
