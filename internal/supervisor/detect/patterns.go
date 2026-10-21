package detect

import (
	supervisor "github.com/yoshikihorie/codex-runner/internal/supervisor"
	"regexp"
	"strings"
	"unicode"
)

// DetectorPatternSet holds CLI-specific patterns. Each inner slice is an AND group; groups are ORed.
type DetectorPatternSet struct {
	IdleTop, IdleBottom, Prompt             string
	Working, Recommendation, UnsafeFooter   [][]string
	Permission                              [][]LinePattern
	ObservedWorking, ObservedRecommendation bool
}

// LineMatchMode defines how a pattern is matched against a trimmed screen row.
type LineMatchMode uint8

const (
	LinePrefix LineMatchMode = iota + 1
	LineExact
	LineRegex
)

// LinePattern makes the distinction between a row prefix and a complete row explicit.
type LinePattern struct {
	Text string
	Mode LineMatchMode
}

// DefaultPatternSet returns the supported Claude screen patterns.
func DefaultPatternSet() DetectorPatternSet {
	return DetectorPatternSet{IdleTop: "─", IdleBottom: "─", Prompt: "❯\u00a0", ObservedWorking: true, ObservedRecommendation: true, Permission: [][]LinePattern{{{Text: "Quick safety check: Is this a project you created or one you trust?", Mode: LinePrefix}, {Text: "Enter to confirm · Esc to cancel", Mode: LineExact}}, {{Text: "Do you want to proceed?", Mode: LineExact}, {Text: `^❯?\s*1\. Yes(\s|$)`, Mode: LineRegex}}}, UnsafeFooter: [][]string{{"auto mode on"}, {"bypass permissions on"}}}
}
func containsAny(rows []string, groups [][]string) bool {
	for _, g := range groups {
		if groupFound(rows, g) {
			return true
		}
	}
	return false
}
func groupFound(rows, g []string) bool {
	if len(g) == 0 {
		return false
	}
	previous := -1
	for _, p := range g {
		found := -1
		for i := previous + 1; i < len(rows); i++ {
			if p != "" && strings.Contains(rows[i], p) {
				found = i
				break
			}
		}
		if found < 0 {
			return false
		}
		previous = found
	}
	return true
}
func orderedLinePatternFound(rows []string, group []LinePattern) bool {
	if len(group) == 0 {
		return false
	}
	previous := -1
	for i, row := range rows {
		trimmed := strings.TrimSpace(row)
		if linePatternMatches(trimmed, group[0]) {
			previous = i
			group = group[1:]
			if len(group) == 0 {
				return true
			}
			break
		}
	}
	for _, pattern := range group {
		found := false
		for i := previous + 1; i < len(rows); i++ {
			if linePatternMatches(strings.TrimSpace(rows[i]), pattern) {
				previous = i
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func linePatternMatches(row string, pattern LinePattern) bool {
	if pattern.Text == "" {
		return false
	}
	switch pattern.Mode {
	case LinePrefix:
		return strings.HasPrefix(row, pattern.Text)
	case LineExact:
		return row == pattern.Text
	case LineRegex:
		matched, err := regexp.MatchString(pattern.Text, row)
		return err == nil && matched
	default:
		return false
	}
}

var observedWorkingLine = regexp.MustCompile(`^[^\pL\pN\s] [A-Za-z]+…(\s|$)`)

// hasObservedWorking inspects the status row above the lowest input's top border.
// FD-sup-01 §5.3a-2 places working and completion lines at the same position.
// Skip blank and auxiliary rows, then match only the first remaining row.
func hasObservedWorking(rows []string, p DetectorPatternSet) bool {
	input := lastInputRow(rows, p)
	for i := input - 2; i >= 0; i-- {
		if strings.TrimSpace(rows[i]) == "" {
			continue
		}
		if start := auxiliaryRowStart(rows, i); start >= 0 {
			i = start
			continue
		}
		return observedWorkingLine.MatchString(rows[i])
	}
	return false
}

// auxiliaryRowStart finds the ⎿ row that starts an auxiliary continuation chain.
// FD-sup-01 §5.3a-2 requires each continuation to be indented and immediately
// follow an auxiliary row or continuation; unrelated indented body rows do not qualify.
func auxiliaryRowStart(rows []string, end int) int {
	for i := end; i >= 0; i-- {
		if strings.HasPrefix(strings.TrimSpace(rows[i]), "⎿") {
			return i
		}
		if strings.TrimLeftFunc(rows[i], unicode.IsSpace) == rows[i] {
			break
		}
	}
	return -1
}

// lastInputRow identifies the active input rather than an older bordered prompt.
func lastInputRow(rows []string, p DetectorPatternSet) int {
	if p.Prompt == "" || p.IdleTop == "" || p.IdleBottom == "" {
		return -1
	}
	for i := len(rows) - 2; i >= 1; i-- {
		if strings.HasPrefix(rows[i], p.Prompt) && allBorder(rows[i-1], p.IdleTop) && allBorder(rows[i+1], p.IdleBottom) {
			return i
		}
	}
	return -1
}
func hasIdle(rows []string, p DetectorPatternSet) bool {
	return lastInputRow(rows, p) >= 0
}
func allBorder(row, border string) bool {
	if row == "" || border == "" {
		return false
	}
	for _, r := range row {
		if !strings.ContainsRune(border, r) {
			return false
		}
	}
	return true
}

// IsInputBlank locates the prompt row between borders, then checks its display cells and dim attributes.
func IsInputBlank(patterns DetectorPatternSet, rows []string, cells [][]supervisor.Cell) bool {
	if cells == nil || len(cells) != len(rows) {
		return false
	}
	row := lastInputRow(rows, patterns)
	if row < 0 {
		return false
	}
	markerColumns := len([]rune(patterns.Prompt))
	if len(cells[row]) < markerColumns {
		return false
	}
	for col := markerColumns; col < len(cells[row]); col++ {
		cell := cells[row][col]
		if cell.Text != "" && cell.Text != " " && cell.Text != "\u00a0" && !cell.Faint {
			return false
		}
	}
	return true
}
