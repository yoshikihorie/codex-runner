package usecase

import "strings"

// CompletionSelection reads the selected slash-command candidate above a framed input row.
// It receives virtual-terminal text rows, without continuation cells.
func CompletionSelection(rows []string) (bool, string) {
	if hasPermissionDialog(rows) {
		return false, ""
	}
	for input := len(rows) - 2; input > 0; input-- {
		if !strings.HasPrefix(rows[input], "❯\u00a0") || !isInputRule(rows[input-1]) || !isInputRule(rows[input+1]) {
			continue
		}
		for i := input - 2; i >= 0; i-- {
			row := rows[i]
			if strings.TrimSpace(row) == "" || isInputRule(row) {
				break
			}
			if !strings.HasPrefix(row, "  ❯ ") {
				continue
			}
			selected := strings.TrimSpace(strings.TrimPrefix(row, "  ❯ "))
			if strings.HasPrefix(selected, "/") {
				return true, selected
			}
			return false, ""
		}
	}
	return false, ""
}

func isInputRule(row string) bool {
	trimmed := strings.TrimSpace(row)
	return trimmed != "" && strings.Trim(trimmed, "─") == ""
}

// completionCandidateMatches checks only the command name and optional parenthesized alias.
func completionCandidateMatches(candidate, command string) bool {
	words := strings.Fields(candidate)
	if len(words) == 0 || len(words[0]) < 2 || !strings.HasPrefix(words[0], "/") {
		return false
	}
	name := words[0]
	alias := ""
	if len(words) > 1 && strings.HasPrefix(words[1], "(") {
		word := words[1]
		if len(word) < 3 || !strings.HasSuffix(word, ")") || strings.ContainsAny(word[1:len(word)-1], "()") {
			return false
		}
		alias = "/" + word[1:len(word)-1]
	}
	return command == name || alias != "" && command == alias
}
