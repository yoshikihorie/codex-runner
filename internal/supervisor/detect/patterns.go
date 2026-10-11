package detect

import (
	supervisor "github.com/yoshikihorie/codex-runner/internal/supervisor"
	"strings"
)

// DetectorPatternSet holds CLI-specific patterns. Each inner slice is an AND group; groups are ORed.
type DetectorPatternSet struct {
	IdleTop, IdleBottom, Prompt           string
	Working, Recommendation, UnsafeFooter [][]string
	Permission                            [][]LinePattern
}

// LineMatchMode defines how a pattern is matched against a trimmed screen row.
type LineMatchMode uint8

const (
	LinePrefix LineMatchMode = iota + 1
	LineExact
)

// LinePattern makes the distinction between a row prefix and a complete row explicit.
type LinePattern struct {
	Text string
	Mode LineMatchMode
}

// DefaultPatternSet returns the supported Claude screen patterns.
func DefaultPatternSet() DetectorPatternSet {
	return DetectorPatternSet{IdleTop: "─", IdleBottom: "─", Prompt: "❯\u00a0", Permission: [][]LinePattern{{{Text: "Quick safety check: Is this a project you created or one you trust?", Mode: LinePrefix}, {Text: "Enter to confirm · Esc to cancel", Mode: LineExact}}}, UnsafeFooter: [][]string{{"auto mode on"}, {"bypass permissions on"}}}
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
	default:
		return false
	}
}
func hasIdle(rows []string, p DetectorPatternSet) bool {
	if p.Prompt == "" || p.IdleTop == "" || p.IdleBottom == "" {
		return false
	}
	for i := 1; i < len(rows)-1; i++ {
		if strings.HasPrefix(rows[i], p.Prompt) && allBorder(rows[i-1], p.IdleTop) && allBorder(rows[i+1], p.IdleBottom) {
			return true
		}
	}
	return false
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
	row := -1
	for i := 1; i < len(rows)-1; i++ {
		if strings.HasPrefix(rows[i], patterns.Prompt) && allBorder(rows[i-1], patterns.IdleTop) && allBorder(rows[i+1], patterns.IdleBottom) {
			row = i
			break
		}
	}
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
