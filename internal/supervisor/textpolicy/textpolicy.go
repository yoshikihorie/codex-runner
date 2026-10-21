// Package textpolicy contains supervisor text classification and display width.
// Normalization removes Unicode format characters, applies NFKC, and lowercases.
package textpolicy

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

// Category identifies a forbidden-operation category from FD-sup-02 §5.3a.
type Category string

const (
	CategoryForcePush   Category = "force_push"
	CategoryPullRequest Category = "pull_request"
	CategoryDeploy      Category = "deploy"
	CategoryDeletion    Category = "deletion"
	CategoryProduction  Category = "production"
	CategoryBilling     Category = "billing"
	CategoryGitHistory  Category = "git_history"
	CategoryMigration   Category = "migration"
	CategoryPrivilege   Category = "privilege"
)

type vocabularyEntry struct {
	category Category
	word     string
}

// Vocabulary is transcribed from FD-sup-02 §5.3a with the explicitly requested
// additional billing term “プラン変更”.
var vocabulary = []vocabularyEntry{
	{CategoryForcePush, "force push"}, {CategoryForcePush, "--force"}, {CategoryForcePush, "-f"}, {CategoryForcePush, "+refspec"}, {CategoryForcePush, "強制プッシュ"}, {CategoryForcePush, "force-with-lease"},
	{CategoryPullRequest, "PR"}, {CategoryPullRequest, "pull request"}, {CategoryPullRequest, "プルリクエスト"}, {CategoryPullRequest, "gh pr"},
	{CategoryDeploy, "deploy"}, {CategoryDeploy, "デプロイ"}, {CategoryDeploy, "リリース"}, {CategoryDeploy, "本番反映"}, {CategoryDeploy, "publish"},
	{CategoryDeletion, "delete"}, {CategoryDeletion, "rm -rf"}, {CategoryDeletion, "rm -fr"}, {CategoryDeletion, "削除"}, {CategoryDeletion, "drop"}, {CategoryDeletion, "truncate"}, {CategoryDeletion, "reset --hard"}, {CategoryDeletion, "clean -f"}, {CategoryDeletion, "消去"},
	{CategoryProduction, "production"}, {CategoryProduction, "本番"}, {CategoryProduction, "prod 環境"},
	{CategoryBilling, "課金"}, {CategoryBilling, "購入"}, {CategoryBilling, "支払"}, {CategoryBilling, "有料"}, {CategoryBilling, "billing"}, {CategoryBilling, "API キー"}, {CategoryBilling, "プラン変更"},
	{CategoryGitHistory, "merge"}, {CategoryGitHistory, "rebase"}, {CategoryGitHistory, "マージ"}, {CategoryGitHistory, "リベース"},
	{CategoryMigration, "migrate"}, {CategoryMigration, "マイグレーション"}, {CategoryPrivilege, "sudo"},
}

// Classify returns every matching forbidden category. undetermined is true for
// empty text, text made only of control/format characters, or invalid UTF-8.
func Classify(text string) (categories []Category, undetermined bool) {
	if !utf8.ValidString(text) || strings.ContainsRune(text, utf8.RuneError) {
		return nil, true
	}
	normalized := normalize(text)
	meaningful := false
	for _, r := range normalized {
		if !unicode.IsControl(r) && !unicode.Is(unicode.Cf, r) && !unicode.IsSpace(r) {
			meaningful = true
			break
		}
	}
	if !meaningful {
		return nil, true
	}
	seen := make(map[Category]bool)
	for _, entry := range vocabulary {
		if matches(normalized, normalize(entry.word)) && !seen[entry.category] {
			categories = append(categories, entry.category)
			seen[entry.category] = true
		}
	}
	return categories, false
}

// IsForbidden is a compatibility boolean wrapper around Classify.
func IsForbidden(text string) bool {
	categories, unknown := Classify(text)
	return unknown || len(categories) != 0
}

// ValidateParallelInstruction retains the established configuration API.
func ValidateParallelInstruction(value string, maxRunes, maxWidth int) bool {
	if !utf8.ValidString(value) || value == "" || len([]rune(value)) > maxRunes || DisplayWidth(value) > maxWidth || strings.ContainsFunc(value, func(r rune) bool {
		return unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == '\u2028' || r == '\u2029'
	}) {
		return false
	}
	runes := []rune(value)
	if runes[0] == '!' || runes[0] == '/' || runes[0] == '#' {
		return false
	}
	return !IsForbidden(value)
}

func matches(text, word string) bool {
	if hasJapanese(word) && !strings.Contains(word, " ") {
		return strings.Contains(text, word)
	}
	parts := strings.Fields(word)
	runes := []rune(text)
	for start := range runes {
		at := start
		matched := true
		for index, part := range parts {
			if index > 0 {
				before := at
				for at < len(runes) && unicode.IsSpace(runes[at]) {
					at++
				}
				if at == before && !hasJapanese(part) {
					matched = false
					break
				}
			}
			end, ok := phraseTokenMatches(runes, at, part)
			if !ok {
				matched = false
				break
			}
			at = end
		}
		if matched {
			return true
		}
	}
	return false
}

func phraseTokenMatches(text []rune, at int, word string) (int, bool) {
	if at >= len(text) {
		return at, false
	}
	start := at
	if word == "+refspec" {
		if text[at] != '+' || at+1 >= len(text) || !isAlphaNumeric(text[at+1]) {
			return at, false
		}
		at += 2
		for at < len(text) && (isWordRune(text[at]) || text[at] == '-') {
			at++
		}
	} else {
		want := []rune(word)
		if len(text)-at < len(want) || string(text[at:at+len(want)]) != word {
			return at, false
		}
		at += len(want)
	}
	if hasJapanese(word) {
		return at, true
	}
	symbol := word[0] == '-' || word[0] == '+'
	boundary := isWordRune
	if symbol {
		boundary = isSymbolBodyRune
	}
	if start > 0 && boundary(text[start-1]) || at < len(text) && boundary(text[at]) {
		return at, false
	}
	return at, true
}

func isWordRune(r rune) bool       { return r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' }
func isAlphaNumeric(r rune) bool   { return r >= 'a' && r <= 'z' || r >= '0' && r <= '9' }
func isSymbolBodyRune(r rune) bool { return isWordRune(r) || r == '-' }
func hasJapanese(s string) bool {
	for _, r := range s {
		if r > 127 {
			return true
		}
	}
	return false
}

func normalize(s string) string {
	var b strings.Builder
	for _, r := range s {
		if !unicode.Is(unicode.Cf, r) {
			b.WriteRune(r)
		}
	}
	return strings.ToLower(norm.NFKC.String(b.String()))
}

// NormalizeRecommendationForHash returns the canonical recommendation body
// used to compute the in-memory duplicate-detection hash (FD-sup-02 §5.3).
func NormalizeRecommendationForHash(s string) string {
	return strings.Join(strings.Fields(normalize(s)), " ")
}
