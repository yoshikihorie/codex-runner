package detect

import "strings"

// ScreenPatternDetector extracts screen evidence and footer permission mode.
type ScreenPatternDetector struct{ Patterns DetectorPatternSet }

func NewScreenPatternDetector(p DetectorPatternSet) *ScreenPatternDetector {
	return &ScreenPatternDetector{Patterns: p}
}
func (d *ScreenPatternDetector) Detect(in DetectInput) DetectResult {
	p := d.Patterns
	if in.Patterns != nil {
		p = *in.Patterns
	}
	mode := PermissionDefaultOrUnknown
	footerStart := len(in.ScreenRows)
	for i := 1; i < len(in.ScreenRows)-1; i++ {
		if strings.HasPrefix(in.ScreenRows[i], p.Prompt) && allBorder(in.ScreenRows[i-1], p.IdleTop) && allBorder(in.ScreenRows[i+1], p.IdleBottom) {
			footerStart = i + 2
			break
		}
	}
	for _, row := range in.ScreenRows[footerStart:] {
		for _, g := range p.UnsafeFooter {
			if len(g) > 0 && strings.Contains(row, g[0]) {
				mode = PermissionUnsafe
			}
		}
	}
	return DetectResult{PermissionMode: mode}
}
