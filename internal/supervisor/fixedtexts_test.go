package supervisor

import "testing"

func TestSCNsup0219FixedTexts(t *testing.T) {
	cases := []struct {
		kind SupervisorSendKind
		want string
	}{{SendKindRecommendationContinue, "推奨どおり進めて"}, {SendKindHandoffEnd, "#end"}, {SendKindHandoffExit, "/exit"}, {SendKindSessionStart, "#start"}, {SendKindRemoteControl, "/rc"}, {SendKindParallelInstruction, "parallel instruction"}}
	for _, tc := range cases {
		got, ok := FixedText(tc.kind, "parallel instruction")
		if !ok || got != tc.want {
			t.Errorf("%s: %q %v", tc.kind, got, ok)
		}
	}
	for _, value := range []string{"", "bad\ntext", "! forbidden"} {
		if _, ok := FixedText(SendKindParallelInstruction, value); ok {
			t.Errorf("accepted %q", value)
		}
	}
	if _, ok := FixedText("arbitrary", "parallel instruction"); ok {
		t.Fatal("arbitrary kind accepted")
	}
}
