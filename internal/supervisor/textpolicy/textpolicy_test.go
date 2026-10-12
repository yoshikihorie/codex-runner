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
	for _, value := range []string{"git push --force-with-lease", "reset --hard.", "(--force)", "rm -fr", "force push,then continue"} {
		if categories, _ := Classify(value); len(categories) == 0 {
			t.Errorf("Classify(%q) missed forbidden phrase", value)
		}
	}
}

func TestClassify_SCN28_SymbolBoundaries(t *testing.T) {
	for _, tc := range []struct {
		text string
		want Category
	}{
		{"`git push --force` を実行", CategoryForcePush},
		{"git push --force=true", CategoryForcePush},
		{"「--force」で push", CategoryForcePush},
		{"main:--force", CategoryForcePush},
		{"--forceで push", CategoryForcePush},
		{"`git push -f`", CategoryForcePush},
		{"`git reset --hard`", CategoryDeletion},
		{"`rm -rf build`", CategoryDeletion},
		{"ＦＯＲＣＥ　ＰＵＳＨ して", CategoryForcePush},
		{"git push -f origin x", CategoryForcePush},
		{"git push +main:main", CategoryForcePush},
		{"main を merge する", CategoryGitHistory},
		{"rebase してから進める", CategoryGitHistory},
		{"migrate を実行する", CategoryMigration},
		{"sudo で実行する", CategoryPrivilege},
		{"ファイルを rm -rf で消す", CategoryDeletion},
		{"強制プッシュは避ける", CategoryForcePush},
	} {
		got, unknown := Classify(tc.text)
		if unknown || !containsCategory(got, tc.want) {
			t.Errorf("Classify(%q) = %v, %v; want %s", tc.text, got, unknown, tc.want)
		}
	}
	if got, unknown := Classify("the profile page を直す"); unknown || len(got) != 0 {
		t.Errorf("false positive: %v, %v", got, unknown)
	}
	if got, unknown := Classify("--forcefulで進める"); unknown || len(got) != 0 {
		t.Errorf("symbol prefix false positive: %v, %v", got, unknown)
	}
}

func TestClassifyNormalization(t *testing.T) {
	for _, value := range []string{"ﾃﾞﾌﾟﾛｲ", "ﾌﾟﾙﾘｸｴｽﾄ", "𝐝𝐞𝐩𝐥𝐨𝐲", "de\u200bploy", "ｄｅｐｌｏｙ"} {
		if categories, _ := Classify(value); len(categories) == 0 {
			t.Errorf("Classify(%q) missed normalized term", value)
		}
	}
}

func TestNFKCNormalization(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"ｇｉｔ　ｐｕｓｈ", "git push"},
		{"ﾃﾞﾌﾟﾛｲ", "デプロイ"},
		{"ﬁ", "fi"},
		{"①", "1"},
		{"海", "海"},
		{"for\u200bce", "force"},
	} {
		if got := normalize(tc.input); got != tc.want {
			t.Errorf("normalize(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
	for _, value := range []string{"ＦＯＲＣＥ　ＰＵＳＨ", "for\u200bce push", "git\u200b push --force", "ﾃﾞﾌﾟﾛｲ"} {
		if categories, unknown := Classify(value); unknown || len(categories) == 0 {
			t.Errorf("Classify(%q) = %v, %v; want forbidden", value, categories, unknown)
		}
	}
}

func TestMixedVocabularyBoundaries(t *testing.T) {
	for _, tc := range []struct {
		word, match, miss string
		category          Category
	}{
		{"rm -rf", "rm -rf files", "term -rf files", CategoryDeletion},
		{"reset --hard", "reset --hard now", "reset --hardening now", CategoryDeletion},
		{"gh pr", "gh pr list", "gh prefix list", CategoryPullRequest},
		{"prod 環境", "prod 環境へ", "product 環境へ", CategoryProduction},
		{"API キー", "API キーを確認", "XAPI キーを確認", CategoryBilling},
	} {
		t.Run(tc.word, func(t *testing.T) {
			if categories, _ := Classify(tc.match); !containsCategory(categories, tc.category) {
				t.Errorf("Classify(%q) = %v; want %s", tc.match, categories, tc.category)
			}
			if matches(normalize(tc.miss), normalize(tc.word)) {
				t.Errorf("%q unexpectedly matched %q", tc.miss, tc.word)
			}
		})
	}
	for _, value := range []string{"--forceful", "--force-extra", "-file", "-f-extra", "+", "x+main", "merge_request", "predeploy"} {
		if categories, unknown := Classify(value); unknown || len(categories) != 0 {
			t.Errorf("Classify(%q) = %v, %v; want safe", value, categories, unknown)
		}
	}
}

func TestG20SymbolAndMixedJapaneseBoundaries(t *testing.T) {
	for _, tc := range []struct {
		text     string
		category Category
	}{
		{"--force:main", CategoryForcePush}, {"必ず--forceでpush", CategoryForcePush},
		{"強制的に-fで", CategoryForcePush}, {"FLAGS=--force", CategoryForcePush},
		{"で+mainを", CategoryForcePush}, {"APIキーを設定", CategoryBilling},
		{"prod環境へ", CategoryProduction},
	} {
		got, unknown := Classify(tc.text)
		if unknown || !containsCategory(got, tc.category) {
			t.Errorf("Classify(%q) = %v, %v; want %s", tc.text, got, unknown, tc.category)
		}
	}
	for _, text := range []string{"-file", "self-fix", "x+main", "the profile page を直す", "XAPIキー"} {
		if got, unknown := Classify(text); unknown || len(got) != 0 {
			t.Errorf("Classify(%q) = %v, %v; want safe", text, got, unknown)
		}
	}
}

func TestNormalizeRecommendationForHash(t *testing.T) {
	first := NormalizeRecommendationForHash("  Ｇｉｔ　ｐｕｓｈ  する  ")
	second := NormalizeRecommendationForHash("git push\tする")
	if first != second {
		t.Fatalf("whitespace variants differ: %q != %q", first, second)
	}
	if first == NormalizeRecommendationForHash("git pull する") {
		t.Fatal("different words normalized to the same value")
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
