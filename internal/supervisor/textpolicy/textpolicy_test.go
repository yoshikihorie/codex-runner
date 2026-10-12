package textpolicy

import "testing"

// canonicalVocabulary is copied from FD-sup-02 §5.3a so vocabulary drift is visible.
var canonicalVocabulary = map[Category][]string{
	"force_push":   {"force push", "--force", "-f", "+main", "強制プッシュ", "force-with-lease"},
	"pull_request": {"PR", "pull request", "プルリクエスト", "gh pr"},
	"deploy":       {"deploy", "デプロイ", "リリース", "本番反映", "publish"},
	"deletion":     {"delete", "rm -rf", "削除", "drop", "truncate", "reset --hard", "clean -f", "消去"},
	"production":   {"production", "本番", "prod 環境"},
	"billing":      {"課金", "購入", "支払", "有料", "billing", "API キー", "プラン変更"},
	"git_history":  {"merge", "rebase", "マージ", "リベース"},
	"migration":    {"migrate", "マイグレーション"},
	"privilege":    {"sudo"},
}

func TestClassifyCanonicalVocabulary(t *testing.T) {
	seen := map[string]bool{}
	for category, words := range canonicalVocabulary {
		for _, word := range words {
			seen[word] = true
			categories, undetermined := Classify("実行: (" + word + ").")
			if undetermined || !containsCategory(categories, category) {
				t.Errorf("Classify(%q) = %v, undetermined=%v; want %s", word, categories, undetermined, category)
			}
		}
	}
	for category, words := range canonicalVocabulary {
		for _, word := range words {
			if !seen[word] {
				t.Errorf("canonical vocabulary missing from tests: %s (%s)", word, category)
			}
		}
	}
}

func TestClassifyBoundaries(t *testing.T) {
	for _, value := range []string{"self-fix", "dropdown", "remerge", "process", "prompt", "-file", "perforce", "reset-password"} {
		if categories, unknown := Classify(value); unknown || len(categories) != 0 {
			t.Errorf("Classify(%q) = %v, %v; want safe", value, categories, unknown)
		}
	}
	for _, value := range []string{"git push --force-with-lease", "reset --hard.", "(--force)", "rm -fr"} {
		if categories, _ := Classify(value); len(categories) == 0 {
			t.Errorf("Classify(%q) missed forbidden phrase", value)
		}
	}
}

func TestClassifyNormalization(t *testing.T) {
	for _, value := range []string{"ﾃﾞﾌﾟﾛｲ", "ﾌﾟﾙﾘｸｴｽﾄ", "𝐝𝐞𝐩𝐥𝐨𝐲", "de\u200bploy", "ｄｅｐｌｏｙ"} {
		if categories, _ := Classify(value); len(categories) == 0 {
			t.Errorf("Classify(%q) missed normalized term", value)
		}
	}
}

func TestClassifyUndetermined(t *testing.T) {
	for _, value := range []string{"", "\x00\n\t", "\u200b\u200d", "\ufffd"} {
		if categories, unknown := Classify(value); len(categories) != 0 || !unknown {
			t.Errorf("Classify(%q) = %v, %v; want undetermined", value, categories, unknown)
		}
	}
}

func TestValidateParallelInstructionUsesClassifier(t *testing.T) {
	for _, value := range []string{"self-fix", "dropdown", "remerge", "process", "prompt", "-file", "perforce", "reset-password"} {
		if !ValidateParallelInstruction(value, 200, 190) {
			t.Errorf("rejected safe instruction %q", value)
		}
	}
}

func TestRuneWidthAndDisplayWidth(t *testing.T) {
	for _, tc := range []struct {
		value string
		width int
	}{
		{"。", 2}, {"、", 2}, {"ア", 2}, {"한", 2}, {"✅", 2}, {"⭐", 2}, {"⚡", 2}, {"Ａ", 2},
		{"❯", 1}, {"⏵", 1}, {"✳", 1}, {"─", 1}, {"a", 1}, {"\u0301", 0}, {"🏻", 0},
		{"\u0378", 1}, {"\u061d", 1}, {"\u0870", 1},
	} {
		runes := []rune(tc.value)
		if got := RuneWidth(runes[0]); got != tc.width {
			t.Errorf("RuneWidth(%U) = %d, want %d", runes[0], got, tc.width)
		}
		if got := DisplayWidth(tc.value); got != tc.width {
			t.Errorf("DisplayWidth(%q) = %d, want %d", tc.value, got, tc.width)
		}
	}
}

func containsCategory(categories []Category, want Category) bool {
	for _, category := range categories {
		if category == want {
			return true
		}
	}
	return false
}

func TestValidateParallelInstructionRejectsFormatAndSeparators(t *testing.T) {
	for _, value := range []string{"run\u2028now", "run\u2029now", "run\u202enow", "run\u200bnow"} {
		if ValidateParallelInstruction(value, 100, 100) {
			t.Fatalf("accepted %q", value)
		}
	}
}

func TestFix3ParallelInstructionRejectsInvalidUTF8(t *testing.T) {
	if ValidateParallelInstruction(string([]byte{'a', 0xff, 'b'}), 100, 100) {
		t.Fatal("accepted invalid UTF-8")
	}
}
