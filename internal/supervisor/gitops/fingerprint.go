package gitops

import (
	"sort"
)

// TreeFingerprint returns a stable SHA-256 over a tree id and path set.
func TreeFingerprint(headSHA string, entries []FingerprintEntry) string {
	ordered := append([]FingerprintEntry(nil), entries...)
	sort.Slice(ordered, func(i, j int) bool {
		a, b := ordered[i], ordered[j]
		if a.Scope != b.Scope {
			return a.Scope < b.Scope
		}
		if a.Status != b.Status {
			return a.Status < b.Status
		}
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		return a.ContentHash < b.ContentHash
	})
	ordered = append([]FingerprintEntry{{Scope: "head", ContentHash: headSHA}}, ordered...)
	return EntriesFingerprint(ordered)
}
