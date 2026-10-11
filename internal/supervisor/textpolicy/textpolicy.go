// Package textpolicy contains supervisor text classification and display width.
// Normalization uses only the standard library: fullwidth ASCII and ideographic
// space, halfwidth katakana, mathematical alphanumeric symbols U+1D400–U+1D7FF,
// Unicode Cf removal, and lowercase conversion. It is not full Unicode NFKC.
package textpolicy

import (
	"strings"
	"unicode"
	"unicode/utf8"
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
	if value == "" || len([]rune(value)) > maxRunes || DisplayWidth(value) > maxWidth || strings.ContainsFunc(value, unicode.IsControl) {
		return false
	}
	runes := []rune(value)
	if runes[0] == '!' || runes[0] == '/' || runes[0] == '#' {
		return false
	}
	return !IsForbidden(value)
}

func matches(text, word string) bool {
	if hasJapanese(word) {
		return strings.Contains(text, word)
	}
	if strings.HasPrefix(word, "+") || strings.HasPrefix(word, "-") && len(word) <= 3 && !strings.Contains(word, " ") {
		for _, token := range strings.Fields(text) {
			token = trimPunctuation(token)
			if token == word || strings.HasPrefix(word, "+") && strings.HasPrefix(token, "+") && len(token) > 1 {
				return true
			}
		}
		return false
	}
	if strings.Contains(word, " ") {
		want := strings.Fields(word)
		tokens := strings.Fields(text)
		for i := range tokens {
			if i+len(want) > len(tokens) {
				break
			}
			ok := true
			for j := range want {
				if trimPunctuation(tokens[i+j]) != want[j] {
					ok = false
					break
				}
			}
			if ok {
				return true
			}
		}
		return false
	}
	for start := 0; start < len(text); {
		i := strings.Index(text[start:], word)
		if i < 0 {
			return false
		}
		i += start
		end := i + len(word)
		leftOK := i == 0 || !isASCIIWord(text[i-1])
		rightOK := end == len(text) || !isASCIIWord(text[end])
		if leftOK && rightOK {
			return true
		}
		start = i + 1
	}
	return false
}

func trimPunctuation(s string) string {
	start := 0
	for start < len(s) {
		r, size := utf8.DecodeRuneInString(s[start:])
		if unicode.IsPunct(r) && r != '+' && r != '-' {
			start += size
			continue
		}
		break
	}
	return strings.TrimRightFunc(s[start:], unicode.IsPunct)
}
func isASCIIWord(b byte) bool { return b >= 'a' && b <= 'z' || b >= '0' && b <= '9' || b == '_' }
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
		if unicode.Is(unicode.Cf, r) || r == 0x200b || r == 0xfeff {
			continue
		}
		if r == 0x3000 {
			r = ' '
		} else if r >= 0xff01 && r <= 0xff5e {
			r -= 0xfee0
		} else if r >= 0xff61 && r <= 0xff9f {
			r = halfwidthKatakana(r)
		} else if r >= 0x1d400 && r <= 0x1d7ff {
			r = mathAlphanumericASCII(r)
		}
		if (r == 0x3099 || r == 0x309a) && b.Len() > 0 {
			previous := []rune(b.String())
			if composed, ok := composeKatakana(previous[len(previous)-1], r); ok {
				b.Reset()
				b.WriteString(string(previous[:len(previous)-1]))
				b.WriteRune(composed)
				continue
			}
		}
		b.WriteRune(r)
	}
	return strings.ToLower(b.String())
}

func halfwidthKatakana(r rune) rune {
	const chars = "。「」、・ヲァィゥェォャュョッーアイウエオカキクケコサシスセソタチツテトナニヌネノハヒフヘホマミムメモヤユヨラリルレロワン\u3099\u309a"
	return []rune(chars)[r-0xff61]
}

func composeKatakana(base, mark rune) (rune, bool) {
	if mark == 0x3099 {
		if strings.ContainsRune("カキクケコサシスセソタチツテトハヒフヘホ", base) {
			return base + 1, true
		}
		if base == 'ウ' {
			return 'ヴ', true
		}
	} else if mark == 0x309a && strings.ContainsRune("ハヒフヘホ", base) {
		return base + 2, true
	}
	return 0, false
}

func mathAlphanumericASCII(r rune) rune {
	// Regular mathematical alphabets, including the common legacy alphabet gaps.
	for _, block := range []struct{ start, end, ascii rune }{
		{0x1d400, 0x1d419, 'A'}, {0x1d41a, 0x1d433, 'a'}, {0x1d434, 0x1d44d, 'A'}, {0x1d44e, 0x1d467, 'a'},
		{0x1d468, 0x1d481, 'A'}, {0x1d482, 0x1d49b, 'a'}, {0x1d4b6, 0x1d4cf, 'a'}, {0x1d4d0, 0x1d4e9, 'A'}, {0x1d4ea, 0x1d503, 'a'},
		{0x1d504, 0x1d51d, 'A'}, {0x1d51e, 0x1d537, 'a'}, {0x1d538, 0x1d551, 'A'}, {0x1d552, 0x1d56b, 'a'},
		{0x1d56c, 0x1d585, 'A'}, {0x1d586, 0x1d59f, 'a'}, {0x1d5a0, 0x1d5b9, 'A'}, {0x1d5ba, 0x1d5d3, 'a'},
		{0x1d5d4, 0x1d5ed, 'A'}, {0x1d5ee, 0x1d607, 'a'}, {0x1d608, 0x1d621, 'A'}, {0x1d622, 0x1d63b, 'a'},
		{0x1d63c, 0x1d655, 'A'}, {0x1d656, 0x1d66f, 'a'}, {0x1d670, 0x1d689, 'A'}, {0x1d68a, 0x1d6a3, 'a'},
		{0x1d7ce, 0x1d7d7, '0'}, {0x1d7d8, 0x1d7e1, '0'}, {0x1d7e2, 0x1d7eb, '0'}, {0x1d7ec, 0x1d7f5, '0'}, {0x1d7f6, 0x1d7ff, '0'},
	} {
		if r >= block.start && r <= block.end {
			return block.ascii + r - block.start
		}
	}
	return r
}
