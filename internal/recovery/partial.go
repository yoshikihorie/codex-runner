package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const (
	partialOutputTailBytes = 200000
	partialOutputTailLines = 400
)

// 既知の制約: このパターンは正典 ANSI_ESCAPE_PATTERN（CSI エスケープシーケンス）どおりだが、
// OSC（ESC ] ... BEL）・DCS（ESC P ...）・末尾で切れたエスケープシーケンスは除去しない。
// ターミナル表示時にハイパーリンク偽装・タイトル変更等が残り得るが、正典のパターン定義自体の
// 不足であり、T1-19 単独では解消できない。正典是正は 2026-08-08 に既知制約として受容済み。
var ansiEscapePattern = regexp.MustCompile(`\x1b\[[0-?]*[ -/]*[@-~]`)

const partialOutputHeader = "## 途中経過（未完了）\n\n" +
	"タイムアウトで最終回答が生成されなかったため、stderr.log の末尾を途中経過として保存しました。" +
	"以下は完全な成果物ではありません。参考情報として扱い、レビュー実績には数えないでください。" +
	"\n\n---\n\n"

type SavePartialOutputInput struct {
	TaskID     domain.TaskID
	OccurredAt time.Time
}

type SavePartialOutputOutput struct {
	Saved        bool
	BytesWritten int
}

// ContractReader contains only the recovery dependencies needed to save partial output.
type ContractReader interface {
	ReadLastMessage(taskID domain.TaskID) (present bool, err error)
	ReadStderrLog(taskID domain.TaskID) ([]byte, error)
}

// ContractWriter contains only the recovery dependency needed to save partial output.
type ContractWriter interface {
	WritePartialOutput(taskID domain.TaskID, content string) error
}

// SavePartialOutputUseCase saves sanitized tail output when timeout recovery cannot finish.
type SavePartialOutputUseCase struct {
	reader   ContractReader
	contract ContractWriter
	logger   *slog.Logger
}

// NewSavePartialOutputUseCase constructs the use case. logger is optional and defaults to slog.Default.
func NewSavePartialOutputUseCase(reader ContractReader, contract ContractWriter, loggers ...*slog.Logger) *SavePartialOutputUseCase {
	if isNilAdoptionDependency(reader) || isNilAdoptionDependency(contract) {
		panic("save partial output use case requires non-nil dependencies")
	}
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &SavePartialOutputUseCase{reader: reader, contract: contract, logger: logger}
}

func (uc *SavePartialOutputUseCase) Execute(ctx context.Context, in SavePartialOutputInput) (SavePartialOutputOutput, error) {
	if err := ctx.Err(); err != nil {
		return SavePartialOutputOutput{}, err
	}
	if in.TaskID.String() == "" {
		return SavePartialOutputOutput{}, fmt.Errorf("recovery: task id is required")
	}

	present, err := uc.reader.ReadLastMessage(in.TaskID)
	if err != nil {
		uc.logError("failed to read last-message.md", in, err)
		return SavePartialOutputOutput{}, nil
	}
	if present {
		return SavePartialOutputOutput{}, nil
	}

	// 既知の制約: ContractReader.ReadStderrLog は stderr.log 全体をメモリへ読み込んでから
	// 本メソッドが末尾 partialOutputTailBytes バイトへ切り詰める。巨大な stderr.log では
	// メモリ消費が問題になり得るが、この制約は T1-05 が確定した ReadStderrLog([]byte, error)
	// という共有 I/F 自体に起因し、T1-19 単独では解消できない。末尾限定読み込み I/F への変更は
	// 正典・T1-05 との調整事項として 2026-08-08 に受容済み。
	raw, err := uc.reader.ReadStderrLog(in.TaskID)
	if err != nil {
		uc.logError("failed to read stderr.log", in, err)
		return SavePartialOutputOutput{}, nil
	}
	if len(raw) == 0 {
		return SavePartialOutputOutput{}, nil
	}

	if len(raw) > partialOutputTailBytes {
		raw = raw[len(raw)-partialOutputTailBytes:]
	}
	text := strings.ToValidUTF8(string(raw), "")
	text = ansiEscapePattern.ReplaceAllString(text, "")
	text = strings.ReplaceAll(text, "\r\n", "\n")
	content := partialOutputHeader + tailLogicalLines(text, partialOutputTailLines)

	if err := uc.contract.WritePartialOutput(in.TaskID, content); err != nil {
		uc.logError("failed to write partial-output.md", in, err)
		return SavePartialOutputOutput{}, nil
	}
	return SavePartialOutputOutput{Saved: true, BytesWritten: len(content)}, nil
}

func (uc *SavePartialOutputUseCase) logError(message string, in SavePartialOutputInput, err error) {
	uc.logger.Error(message,
		"task_id", in.TaskID.String(),
		"op", "save_partial_output",
		"occurred_at", in.OccurredAt,
		"error", err,
	)
}

// tailLogicalLines returns the last n LF-delimited logical lines. A trailing LF
// terminates the preceding line and does not introduce another logical line.
func tailLogicalLines(s string, n int) string {
	trailingNewline := strings.HasSuffix(s, "\n")
	trimmed := strings.TrimSuffix(s, "\n")
	if trimmed == "" {
		return s
	}
	lines := strings.Split(trimmed, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	result := strings.Join(lines, "\n")
	if trailingNewline {
		result += "\n"
	}
	return result
}
