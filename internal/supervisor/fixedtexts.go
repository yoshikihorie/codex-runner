package supervisor

import "github.com/yoshikihorie/codex-runner/internal/supervisor/textpolicy"

// Canonical value: specifications/10-shared/validation-rules.md SUPERVISOR_RECOMMEND_CONTINUE_TEXT.
const RecommendationContinueText = "推奨どおり進めて"

// Canonical limits: specifications/10-shared/validation-rules.md
// SUPERVISOR_INSTRUCTION_MAX_LENGTH and SUPERVISOR_PTY_COLUMNS_DEFAULT;
// FD-sup-02 §5.2 reserves ten columns to avoid wrapping.
const parallelInstructionMaxLength = 200
const parallelInstructionMaxWidth = 190

// FixedText maps a send kind and validated project setting to its sole permitted text.
func FixedText(kind SupervisorSendKind, parallelInstruction string) (string, bool) {
	switch kind {
	case SendKindRecommendationContinue:
		return RecommendationContinueText, true
	case SendKindHandoffEnd:
		return "#end", true
	case SendKindHandoffExit:
		return "/exit", true
	case SendKindSessionStart:
		return "#start", true
	case SendKindRemoteControl:
		return "/rc", true
	case SendKindParallelInstruction:
		if textpolicy.ValidateParallelInstruction(parallelInstruction, parallelInstructionMaxLength, parallelInstructionMaxWidth) {
			return parallelInstruction, true
		}
	}
	return "", false
}
