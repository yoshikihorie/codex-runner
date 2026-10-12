package supervisor

import "strings"

// RecommendationExtractor requires the previous input, recommendation start,
// and input top border to be visible. Empty patterns (including the default
// pattern set before PT-11) never authorize extraction.
type RecommendationExtractor struct {
	PreviousInput       string
	RecommendationStart string
	InputTop            string
}

func (e RecommendationExtractor) Extract(rows []string) (string, bool) {
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
