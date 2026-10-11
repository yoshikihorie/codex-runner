package gitops

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type ConfigFingerprint struct {
	SHA256     string
	HasInclude bool
}

func configHasInclude(config []byte) bool {
	s := strings.TrimPrefix(string(config), "\xef\xbb\xbf")
	section := ""
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line == "" {
			continue
		}
		for strings.HasPrefix(line, "[") {
			end := strings.IndexByte(line, ']')
			if end < 0 {
				return true
			}
			section = strings.ToLower(strings.TrimSpace(strings.Trim(line[1:end], "\\\"")))
			line = strings.TrimSpace(line[end+1:])
			if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
				break
			}
			if !strings.HasPrefix(line, "[") {
				return true
			}
		}
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(strings.ToLower(section), "include") {
			key := line
			if i := strings.IndexAny(key, "= \t"); i >= 0 {
				key = key[:i]
			}
			if strings.EqualFold(strings.TrimSpace(key), "path") {
				return true
			}
		}
	}
	return false
}

type FingerprintEntry struct{ Scope, Status, Path, ContentHash string }

func appendSized(dst []byte, value string) []byte {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	dst = append(dst, size[:]...)
	return append(dst, value...)
}

func EntriesFingerprint(entries []FingerprintEntry) string {
	var b []byte
	for _, e := range entries {
		b = appendSized(b, e.Scope)
		b = appendSized(b, e.Status)
		b = appendSized(b, e.Path)
		b = appendSized(b, e.ContentHash)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// GitConfigFingerprintFromEntries fingerprints the named config and hook entries produced by BuildGitConfigFingerprintEntries.
func GitConfigFingerprintFromEntries(entries []FingerprintEntry, configs ...[]byte) ConfigFingerprint {
	ordered := append([]FingerprintEntry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Scope != ordered[j].Scope {
			return ordered[i].Scope < ordered[j].Scope
		}
		return ordered[i].Path < ordered[j].Path
	})
	var framed []byte
	for _, e := range ordered {
		framed = appendSized(framed, e.Scope)
		framed = appendSized(framed, e.Status)
		framed = appendSized(framed, e.Path)
		framed = appendSized(framed, e.ContentHash)
	}
	hasInclude := false
	for _, item := range configs {
		framed = appendSized(framed, string(item))
		hasInclude = hasInclude || configHasInclude(item)
	}
	sum := sha256.Sum256(framed)
	return ConfigFingerprint{SHA256: hex.EncodeToString(sum[:]), HasInclude: hasInclude}
}

// BuildGitConfigFingerprintEntries captures config, worktree config, and hooks sorted by name.
func BuildGitConfigFingerprintEntries(commonDir string, gitDir string) ([]FingerprintEntry, bool, error) {
	entries := []FingerprintEntry{}
	hasInclude := false
	configs := []string{filepath.Join(commonDir, "config"), filepath.Join(gitDir, "config.worktree")}
	for _, p := range configs {
		data, e := os.ReadFile(p)
		if e != nil && !os.IsNotExist(e) {
			hasInclude = true
			continue
		}
		if e == nil {
			h := sha256.Sum256(data)
			hasInclude = hasInclude || configHasInclude(data)
			entries = append(entries, FingerprintEntry{Scope: "config", Path: filepath.Base(p), ContentHash: hex.EncodeToString(h[:])})
		}
	}
	hooks := filepath.Join(commonDir, "hooks")
	names, e := os.ReadDir(hooks)
	if e != nil && !os.IsNotExist(e) {
		return nil, true, e
	}
	for _, n := range names {
		info, e := os.Lstat(filepath.Join(hooks, n.Name()))
		if e != nil {
			continue
		}
		status := info.Mode().String()
		data := []byte{}
		if info.Mode()&os.ModeSymlink != 0 {
			target, err := os.Readlink(filepath.Join(hooks, n.Name()))
			if err == nil {
				data = []byte(target)
			} else {
				status += ":unreadable"
			}
		} else if info.IsDir() {
			status += ":directory"
		} else {
			data, e = os.ReadFile(filepath.Join(hooks, n.Name()))
			if e != nil {
				status += ":unreadable"
				data = []byte(e.Error())
			}
		}
		h := sha256.Sum256(data)
		entries = append(entries, FingerprintEntry{Scope: "hook", Status: status, Path: n.Name(), ContentHash: hex.EncodeToString(h[:])})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Scope != entries[j].Scope {
			return entries[i].Scope < entries[j].Scope
		}
		return entries[i].Path < entries[j].Path
	})
	return entries, hasInclude, nil
}
