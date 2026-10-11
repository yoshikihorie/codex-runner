package gitops

import (
	"fmt"
	"strings"
)

type StatusEntry struct {
	Kind               string
	Index, Worktree    byte
	Path, OriginalPath string
}

func ParsePorcelainV1(data []byte) ([]StatusEntry, error) {
	if len(data) == 0 {
		return nil, nil
	}
	fields := strings.Split(string(data), "\x00")
	out := []StatusEntry{}
	for i := 0; i < len(fields)-1; i++ {
		v := fields[i]
		if len(v) < 3 || v[2] != ' ' {
			return nil, fmt.Errorf("invalid porcelain v1 entry")
		}
		e := StatusEntry{Index: v[0], Worktree: v[1], Path: v[3:]}
		if e.Path == "" {
			return nil, fmt.Errorf("empty porcelain path")
		}
		const allowed = " MTADRCU?!"
		if !strings.ContainsRune(allowed, rune(e.Index)) || !strings.ContainsRune(allowed, rune(e.Worktree)) {
			return nil, fmt.Errorf("invalid porcelain status")
		}
		if e.Index == ' ' && e.Worktree == ' ' || e.Index == '?' && e.Worktree != '?' || e.Worktree == '?' && e.Index != '?' || e.Index == '!' && e.Worktree != '!' || e.Worktree == '!' && e.Index != '!' {
			return nil, fmt.Errorf("invalid porcelain status pair")
		}
		if e.Index == '?' && e.Worktree == '?' {
			e.Kind = "untracked"
		} else if e.Index == '!' && e.Worktree == '!' {
			e.Kind = "ignored"
		} else {
			e.Kind = "ordinary"
			if e.Index == 'R' || e.Worktree == 'R' {
				e.Kind = "rename"
			} else if e.Index == 'C' || e.Worktree == 'C' {
				e.Kind = "copy"
			}
			if e.Kind == "rename" || e.Kind == "copy" {
				i++
				if i >= len(fields)-1 {
					return nil, fmt.Errorf("missing original path for porcelain v1 rename/copy")
				}
				e.OriginalPath = fields[i]
				if e.OriginalPath == "" {
					return nil, fmt.Errorf("empty original porcelain path")
				}
			}
		}
		out = append(out, e)
	}
	if fields[len(fields)-1] != "" {
		return nil, fmt.Errorf("unterminated porcelain v1 entry")
	}
	return out, nil
}

type StatusClassification struct {
	Entries                                                []StatusEntry
	RenamePartial, OutOfScopeUncommitted, StagedOutOfScope bool
}

func ClassifyStatus(entries []StatusEntry, scope *PathScope) StatusClassification {
	r := StatusClassification{Entries: append([]StatusEntry(nil), entries...)}
	for _, e := range entries {
		if e.Kind == "ignored" {
			continue
		}
		inside := scope.Contains(e.Path)
		if e.Kind == "rename" || e.Kind == "copy" {
			old := scope.Contains(e.OriginalPath)
			if old != inside {
				r.RenamePartial = true
				continue
			}
			inside = inside && old
		}
		if inside {
			continue
		}
		if e.Index != ' ' && e.Index != '.' && e.Index != '?' && e.Index != 0 {
			r.StagedOutOfScope = true
		}
		if e.Worktree != ' ' && e.Worktree != '.' && e.Worktree != 0 || e.Kind == "untracked" {
			r.OutOfScopeUncommitted = true
		}
	}
	return r
}
