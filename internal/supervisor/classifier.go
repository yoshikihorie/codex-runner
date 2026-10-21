package supervisor

import "strings"

// RecommendationExtractor requires the previous input, recommendation start,
// and input top border to be visible. Empty patterns (including the default
// pattern set before PT-11) never authorize extraction.
type RecommendationExtractor struct {
	PreviousInput       string
	RecommendationStart string
	InputTop            string
	ObservedScreen      bool
}

func (e RecommendationExtractor) Extract(rows []string) (string, bool) {
	if e.ObservedScreen {
		start, end, ok := ObservedRecommendationBounds(rows)
		if !ok {
			return "", false
		}
		body := strings.TrimSpace(strings.Join(rows[start:end], "\n"))
		return body, body != ""
	}
	if e.PreviousInput == "" || e.RecommendationStart == "" || e.InputTop == "" {
		return "", false
	}
	previous := -1
	for i, row := range rows {
		if strings.TrimSpace(row) == e.PreviousInput {
			previous = i
		}
	}
	if previous < 0 {
		return "", false
	}
	start := -1
	for i := previous + 1; i < len(rows); i++ {
		if strings.TrimSpace(rows[i]) == e.RecommendationStart {
			start = i
			break
		}
	}
	if start < 0 {
		return "", false
	}
	for end := start + 1; end < len(rows); end++ {
		if strings.TrimSpace(rows[end]) == e.InputTop {
			body := strings.TrimSpace(strings.Join(rows[start:end], "\n"))
			return body, strings.TrimSpace(strings.Join(rows[start+1:end], "")) != ""
		}
	}
	return "", false
}

// ObservedRecommendationBounds returns the response between the last echoed
// input above the input box and its completion row. See FD-sup-01 §5.3a-2.
func ObservedRecommendationBounds(rows []string) (int, int, bool) {
	inputBox := -1
	for i := 1; i < len(rows)-1; i++ {
		if strings.HasPrefix(rows[i], "❯\u00a0") && isBorderRow(rows[i-1]) && isBorderRow(rows[i+1]) {
			inputBox = i
		}
	}
	if inputBox < 0 {
		return 0, 0, false
	}
	echo := -1
	for i := 0; i < inputBox; i++ {
		if strings.HasPrefix(rows[i], "❯ ") {
			echo = i
		}
	}
	if echo < 0 {
		return 0, 0, false
	}
	for end := inputBox - 1; end > echo; end-- {
		if strings.HasPrefix(rows[end], "✻ ") && strings.Contains(rows[end], " · done ") {
			for _, row := range rows[echo+1 : end] {
				if strings.TrimSpace(row) != "" {
					return echo + 1, end, true
				}
			}
			return 0, 0, false
		}
	}
	return 0, 0, false
}

func isBorderRow(row string) bool {
	return row != "" && strings.Trim(row, "─") == ""
}
