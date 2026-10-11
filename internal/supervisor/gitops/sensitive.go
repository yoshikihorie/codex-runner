package gitops

import (
	"path"
	"strings"
)

type SensitivePathMatcher struct{ patterns []string }

func NewSensitivePathMatcher() *SensitivePathMatcher {
	return &SensitivePathMatcher{[]string{".env*", "*.pem", "*.key", "id_rsa*", "id_ed25519*", "id_ecdsa*", "id_dsa*", "credentials*", "*.p12"}}
}
func (m *SensitivePathMatcher) Matches(p string) bool {
	p = strings.TrimPrefix(path.Clean("/"+p), "/")
	base := strings.ToLower(path.Base(p))
	for _, pattern := range m.patterns {
		if ok, _ := path.Match(pattern, base); ok {
			return true
		}
	}
	return false
}
