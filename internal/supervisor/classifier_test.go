package supervisor

import "testing"

func TestRecommendationExtractor_SCN09_CompleteBoundedBody(t *testing.T) {
	e := RecommendationExtractor{PreviousInput: "❯ 前の入力", InputTop: "────", RecommendationStart: "推奨:"}
	body, ok := e.Extract([]string{"以前の内容", "❯ 前の入力", "処理しました", "推奨:", "テストを追加する", "────", "❯\u00a0"})
	if !ok || body != "推奨:\nテストを追加する" {
		t.Fatalf("body=%q ok=%v", body, ok)
	}
}

func TestRecommendationExtractor_SCN12_FailClosed(t *testing.T) {
	rows := []string{"❯ 前の入力", "推奨:", "テストを追加する", "────", "❯\u00a0"}
	cases := []RecommendationExtractor{
		{},
		{PreviousInput: "❯ 前の入力", InputTop: "────"},
		{PreviousInput: "❯ 前の入力", InputTop: "────", RecommendationStart: "別の印"},
		{PreviousInput: "見えない入力", InputTop: "────", RecommendationStart: "推奨:"},
	}
	for i, e := range cases {
		if body, ok := e.Extract(rows); ok {
			t.Fatalf("case %d: extracted %q", i, body)
		}
	}
}

func TestRecommendationExtractor_SCN12_WhitespaceOnlyBody(t *testing.T) {
	e := RecommendationExtractor{PreviousInput: "previous", RecommendationStart: "start", InputTop: "top"}
	if body, ok := e.Extract([]string{"previous", "start", " \t ", "top"}); ok {
		t.Fatalf("extracted whitespace-only body %q", body)
	}
}

func TestRecommendationExtractorObservedBoundaries(t *testing.T) {
	e := RecommendationExtractor{ObservedScreen: true}
	rows := []string{"❯ older input", "⏺ old", "✻ Cooked for 1s · done 09:00", "❯ latest input", "⏺ 本番にデプロイ", "  force push", "✻ Cooked for 2s · done 10:00", "────────", "❯\u00a0Try a prompt", "────────"}
	body, ok := e.Extract(rows)
	if !ok || body != "⏺ 本番にデプロイ\n  force push" {
		t.Fatalf("body=%q ok=%v", body, ok)
	}
	for _, missing := range []int{6} {
		copyRows := append([]string{}, rows...)
		copyRows[missing] = "other"
		if got, ok := e.Extract(copyRows); ok {
			t.Fatalf("missing marker %d: %q", missing, got)
		}
	}
	copyRows := append([]string{}, rows...)
	copyRows[0], copyRows[3] = "other", "other"
	if got, ok := e.Extract(copyRows); ok {
		t.Fatalf("missing echoed input: %q", got)
	}
}

func TestObservedRecommendationBoundsUsesLastCompletion(t *testing.T) {
	rows := []string{
		"❯ latest input",
		"⏺ 次の作業を提案します",
		"✻ x · done 10:00",
		"  force push する",
		"✻ Cooked for 2s · done 10:01",
		"────────",
		"❯\u00a0Try a prompt",
		"────────",
	}
	start, end, ok := ObservedRecommendationBounds(rows)
	if !ok || start != 1 || end != 4 {
		t.Fatalf("bounds=(%d, %d, %v), want (1, 4, true)", start, end, ok)
	}
	if body, extracted := (RecommendationExtractor{ObservedScreen: true}).Extract(rows); !extracted || body != "⏺ 次の作業を提案します\n✻ x · done 10:00\n  force push する" {
		t.Fatalf("body=%q extracted=%v", body, extracted)
	}
}

func TestObservedRecommendationBoundsRejectsEmptyBody(t *testing.T) {
	rows := []string{"❯ latest input", " \t ", "✻ Cooked for 2s · done 10:00", "────────", "❯\u00a0Try a prompt", "────────"}
	if start, end, ok := ObservedRecommendationBounds(rows); ok {
		t.Fatalf("empty body bounds=(%d, %d, %v)", start, end, ok)
	}
}
