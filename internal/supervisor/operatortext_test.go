package supervisor

import (
	"fmt"
	"strings"
	"testing"
)

func TestOperatorText_SCNSup0569_ValidationAndRedaction(t *testing.T) {
	for _, value := range []string{"", "a\nb", "a\rb", "a\u2028b", "a\u2029b", "a\tb", "a\x1bb", "a\x7fb", "a\u0080b", "!a", "/a", "#a", "\ufffd", string([]byte{0xff}), strings.Repeat("a", SupervisorOperatorInputMaxLength+1)} {
		if _, err := NewOperatorText(value); err == nil {
			t.Fatalf("accepted invalid operator text: %q", value)
		}
	}
	for _, value := range []string{strings.Repeat("a", SupervisorOperatorInputMaxLength), "-a!#", "@a", "日本語"} {
		got, err := NewOperatorText(value)
		if err != nil || got.Len() != len([]rune(value)) || got.PTYText() != value {
			t.Fatalf("valid text rejected or changed: len=%d err=%v", got.Len(), err)
		}
	}
	private := "固有の日本語😀"
	value, err := NewOperatorText(private)
	if err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"%v", "%+v", "%#v"} {
		if strings.Contains(fmt.Sprintf(format, value), private) {
			t.Fatalf("fmt leaked operator text with %s", format)
		}
	}
}
