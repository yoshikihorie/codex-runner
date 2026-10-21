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
	if input := lastInputRow(in.ScreenRows, p); input >= 0 {
		footerStart = input + 2
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
