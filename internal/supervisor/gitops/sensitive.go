package gitops

import (
	"path"
	"strings"
)

var sensitivePathPatterns = []string{".env*", "*.pem", "*.key", "id_rsa*", "id_ed25519*", "id_ecdsa*", "id_dsa*", "credentials*", "*.p12"}

type SensitivePathMatcher struct{ patterns []string }

func ValidSensitivePattern(value string) bool {
	for _, pattern := range sensitivePathPatterns {
		if value == pattern {
			return true
		}
	}
	return false
}

func NewSensitivePathMatcher() *SensitivePathMatcher {
	return &SensitivePathMatcher{append([]string(nil), sensitivePathPatterns...)}
}
func (m *SensitivePathMatcher) Matches(p string) bool { return m.MatchPattern(p) != "" }

func (m *SensitivePathMatcher) MatchPattern(p string) string {
	p = strings.TrimPrefix(path.Clean("/"+p), "/")
	base := strings.ToLower(path.Base(p))
	for _, pattern := range m.patterns {
		if ok, _ := path.Match(pattern, base); ok {
			return pattern
		}
	}
	return ""
}
