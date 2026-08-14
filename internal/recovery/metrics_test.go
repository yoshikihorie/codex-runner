package recovery

import (
	"context"

	"github.com/yoshikihorie/codex-runner/internal/metrics"
)

type metricsRecorderFake struct{}

func (metricsRecorderFake) Execute(
	ctx context.Context,
	in metrics.RecordTaskMetricsInput,
) metrics.RecordTaskMetricsOutput {
	return metrics.RecordTaskMetricsOutput{}
}

var _ MetricsRecorder = metricsRecorderFake{}
