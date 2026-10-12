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
