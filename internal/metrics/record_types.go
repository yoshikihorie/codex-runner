package metrics

import (
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// RecordTaskMetricsInput はタスクの終端状態に関する実行ログ統計の記録入力を表す。
type RecordTaskMetricsInput struct {
	TaskID         domain.TaskID
	FinalState     domain.TaskState
	Estimated      bool
	OccurredAt     time.Time
	StalledTotalMs int
}

// RecordTaskMetricsOutput は実行ログ統計の記録結果を表す。
type RecordTaskMetricsOutput struct {
	Recorded bool
}
