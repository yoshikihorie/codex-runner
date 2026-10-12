package gitops

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// PathScope stores repository-relative owned paths.
type PathScope struct {
	root  string
	paths []string
}

// NewPathScope resolves cwd-relative paths under the repository top level.
func NewPathScope(toplevel, cwd string, owned []string) (*PathScope, error) {
	root, err := filepath.EvalSymlinks(toplevel)
	if err != nil {
		return nil, err
	}
	root, err = filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	base, err := filepath.EvalSymlinks(cwd)
	if err != nil {
		return nil, err
	}
	base, err = filepath.Abs(base)
	if err != nil {
		return nil, err
	}
	if !within(root, base) {
		return nil, fmt.Errorf("cwd outside repository")
	}
	s := &PathScope{root: root}
	for _, p := range owned {
		if p == "" || filepath.IsAbs(p) {
			return nil, fmt.Errorf("invalid owned path")
		}
		for _, part := range strings.Split(filepath.ToSlash(p), "/") {
			if part == ".." {
				return nil, fmt.Errorf("path traversal")
			}
			if strings.EqualFold(part, ".git") {
				return nil, fmt.Errorf("git metadata cannot be owned")
			}
		}
		candidate := filepath.Join(base, filepath.FromSlash(p))
		resolved, e := filepath.EvalSymlinks(candidate)
		if e != nil {
			resolved, e = resolveExistingParent(candidate)
		}
		if e != nil {
			return nil, e
		}
		resolved, e = filepath.Abs(resolved)
		if e != nil {
			return nil, e
		}
		if !within(root, resolved) {
			return nil, fmt.Errorf("owned path outside repository")
		}
		rel, e := filepath.Rel(root, resolved)
		if e != nil {
			return nil, e
		}
		for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
			if strings.EqualFold(part, ".git") {
				return nil, fmt.Errorf("git metadata cannot be owned")
			}
		}
		s.paths = append(s.paths, filepath.ToSlash(rel))
	}
	return s, nil
}

func resolveExistingParent(p string) (string, error) {
	parent := p
	suffix := []string{}
	for {
		_, e := os.Lstat(parent)
		if e == nil {
			break
		}
		if !os.IsNotExist(e) {
			return "", e
		}
		next := filepath.Dir(parent)
		if next == parent {
			return "", e
		}
		suffix = append(suffix, filepath.Base(parent))
		parent = next
	}
	r, e := filepath.EvalSymlinks(parent)
	if e != nil {
		return "", e
	}
	for i := len(suffix) - 1; i >= 0; i-- {
		r = filepath.Join(r, suffix[i])
	}
	return r, nil
}
func within(root, p string) bool {
	rel, e := filepath.Rel(root, p)
	return e == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
func (s *PathScope) Paths() []string { return append([]string(nil), s.paths...) }
func (s *PathScope) Contains(path string) bool {
	p, err := s.Resolve(path)
	if err != nil {
		return false
	}
	for _, owned := range s.paths {
		if owned == "." {
			return p != ".." && !strings.HasPrefix(p, "../") && !strings.HasPrefix(p, "/")
		}
		if p == owned || strings.HasPrefix(p, strings.TrimSuffix(owned, "/")+"/") {
			return true
		}
	}
	return false
}
func (s *PathScope) Resolve(path string) (string, error) {
	if path == "" || filepath.IsAbs(path) {
		return "", fmt.Errorf("absolute path")
	}
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part == ".." {
			return "", fmt.Errorf("path traversal")
		}
		if strings.EqualFold(part, ".git") {
			return "", fmt.Errorf("git metadata path")
		}
	}
	full := filepath.Join(s.root, filepath.FromSlash(path))
	r, e := filepath.EvalSymlinks(full)
	if e != nil {
		r, e = resolveExistingParent(full)
	}
	if e != nil {
		return "", e
	}
	r, e = filepath.Abs(r)
	if e != nil {
		return "", e
	}
	if !within(s.root, r) {
		return "", fmt.Errorf("path outside repository")
	}
	rel, e := filepath.Rel(s.root, r)
	for _, part := range strings.Split(filepath.ToSlash(rel), "/") {
		if strings.EqualFold(part, ".git") {
			return "", fmt.Errorf("git metadata path")
		}
	}
	return filepath.ToSlash(rel), e
}

// OwnsLexically reports whether a repository-relative path falls under an owned name before symlink resolution.
func (s *PathScope) OwnsLexically(path string) bool {
	for _, owned := range s.paths {
		if owned == "." || path == owned || strings.HasPrefix(path, strings.TrimSuffix(owned, "/")+"/") {
			return true
		}
	}
	return false
}
