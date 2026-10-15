package supervisor

import (
	"errors"
	"fmt"
	"unicode"
	"unicode/utf8"
)

// Canonical value: specifications/10-shared/validation-rules.md.
const SupervisorOperatorInputMaxLength = 2000

// Canonical values: specifications/10-shared/published-language/error-codes.md
// and specifications/10-shared/message-catalog.md.
const ErrorCodeOperatorInputForbidden = "SUP_OPERATOR_INPUT_FORBIDDEN"
const MessageKeyOperatorInputForbidden = "error.supervisor.operatorInputForbidden"

// OperatorText carries validated input without exposing its body to formatting
// or JSON encoding. The body is available only for the PTY write.
type OperatorText struct{ payload *operatorTextPayload }

type operatorTextPayload struct{ text string }

func (*operatorTextPayload) Format(f fmt.State, verb rune) {
	_, _ = f.Write([]byte("[redacted]"))
}

func NewOperatorText(value string) (OperatorText, error) {
	if !utf8.ValidString(value) {
		return OperatorText{}, errors.New("SUP_REQUEST_MALFORMED")
	}
	count := utf8.RuneCountInString(value)
	if count < 1 || count > SupervisorOperatorInputMaxLength {
		return OperatorText{}, errors.New("SUP_REQUEST_MALFORMED")
	}
	for index, r := range value {
		if index == 0 && (r == '!' || r == '/' || r == '#') || r == utf8.RuneError || unicode.IsControl(r) || r == '\u2028' || r == '\u2029' || unicode.Is(unicode.Zl, r) || unicode.Is(unicode.Zp, r) {
			return OperatorText{}, errors.New("SUP_REQUEST_MALFORMED")
		}
	}
	return OperatorText{payload: &operatorTextPayload{text: value}}, nil
}

func (o OperatorText) Len() int {
	if o.payload == nil {
		return 0
	}
	return utf8.RuneCountInString(o.payload.text)
}

func (o OperatorText) PTYText() string {
	if o.payload == nil {
		return ""
	}
	return o.payload.text
}
