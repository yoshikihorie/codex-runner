package recovery

import (
	"context"

	"github.com/yoshikihorie/codex-runner/internal/metrics"
)

// MetricsRecorder は実行ログ統計を記録する境界を表す。
type MetricsRecorder interface {
	Execute(
		ctx context.Context,
		in metrics.RecordTaskMetricsInput,
	) metrics.RecordTaskMetricsOutput
}
