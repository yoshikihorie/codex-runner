package metrics

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"sort"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

const (
	MessageKeyStatsInvalidDateRange  = "error.stats.invalidDateRange"
	MessageKeyStatsInvalidSubcommand = "error.stats.invalidSubcommand"
	MessageKeyStatsSkippedLines      = "info.stats.skippedLines"
	machineCodeMetricsFileCorrupted  = "METRICS_FILE_CORRUPTED"
)

type StatsQuery struct {
	Since            *string
	Until            *string
	SubcommandFilter *domain.Subcommand
	JSON             bool
}

type StatsReport struct {
	MatchedFiles                          int                               `json:"matched_files"`
	TotalRecords                          int                               `json:"total_records"`
	SkippedLines                          int                               `json:"skipped_lines"`
	SuccessRateBySubcommand               map[domain.Subcommand]SuccessStat `json:"success_rate_by_subcommand"`
	SuccessRateByModel                    map[string]SuccessStat            `json:"success_rate_by_model"`
	QueueWaitMedian                       *int                              `json:"queue_wait_median"`
	QueueWaitP95                          *int                              `json:"queue_wait_p95"`
	StartupMedian                         *int                              `json:"startup_median"`
	StartupP95                            *int                              `json:"startup_p95"`
	ExecutionMedian                       *int                              `json:"execution_median"`
	ExecutionP95                          *int                              `json:"execution_p95"`
	PromptLengthToOutputLengthCorrelation *float64                          `json:"prompt_length_to_output_length_correlation"`
	PromptLengthToOutputTokensCorrelation *float64                          `json:"prompt_length_to_output_tokens_correlation"`
	MaxEventGapMedian                     *float64                          `json:"max_event_gap_median"`
	MaxEventGapP95                        *float64                          `json:"max_event_gap_p95"`
	MaxEventGapMax                        *float64                          `json:"max_event_gap_max"`
	TimeoutCount                          int                               `json:"timeout_count"`
	RecoveryAttemptedCount                int                               `json:"recovery_attempted_count"`
	RecoverySucceededCount                int                               `json:"recovery_succeeded_count"`
	RecoverySuccessRate                   *float64                          `json:"recovery_success_rate"`
}

type SuccessStat struct {
	Total   int      `json:"total"`
	Success int      `json:"success"`
	Rate    *float64 `json:"rate"`
}

type ComputeTaskStatsUseCase struct {
	reader  store.MetricsReader
	logsDir string
	logger  *slog.Logger
}

func NewComputeTaskStatsUseCase(reader store.MetricsReader, logsDir string, loggers ...*slog.Logger) *ComputeTaskStatsUseCase {
	if reader == nil {
		panic("compute task stats use case requires a non-nil MetricsReader")
	}
	if len(loggers) > 1 {
		panic("compute task stats use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &ComputeTaskStatsUseCase{reader: reader, logsDir: logsDir, logger: logger}
}

func parseTaskMetricsRecord(line []byte) (taskMetricsRecord, error) {
	var record taskMetricsRecord
	err := json.Unmarshal(line, &record)
	return record, err
}

func (u *ComputeTaskStatsUseCase) Execute(q StatsQuery) (StatsReport, error) {
	files, err := u.reader.ListMonthlyFiles(u.logsDir, q.Since, q.Until)
	if err != nil {
		return StatsReport{}, err
	}

	records := make([]taskMetricsRecord, 0)
	skippedLines := 0
	for _, path := range files {
		func() {
			stream, openErr := u.reader.OpenMonthlyFile(path)
			if openErr != nil {
				u.logger.Warn("metrics file could not be opened", "path", path, "error", openErr)
				return
			}
			defer func() {
				if closeErr := stream.Close(); closeErr != nil {
					u.logger.Warn("metrics file could not be closed", "path", path, "error", closeErr)
				}
			}()

			reader := bufio.NewReader(stream)
			for {
				line, readErr := reader.ReadBytes('\n')
				if len(line) > 0 {
					if readErr == io.EOF {
						skippedLines++
						u.warnCorrupted(path, "unterminated-line", readErr)
					} else {
						record, parseErr := parseTaskMetricsRecord(bytes.TrimSuffix(line, []byte{'\n'}))
						if parseErr != nil {
							skippedLines++
							u.warnCorrupted(path, "parse-line", parseErr)
						} else if q.SubcommandFilter == nil || record.Subcommand == *q.SubcommandFilter {
							records = append(records, record)
						}
					}
				}
				if readErr == io.EOF {
					return
				}
				if readErr != nil {
					u.warnCorrupted(path, "read-line", readErr)
					return
				}
			}
		}()
	}

	report := buildStatsReport(records)
	report.MatchedFiles = len(files)
	report.SkippedLines = skippedLines
	return report, nil
}

func (u *ComputeTaskStatsUseCase) warnCorrupted(path, stage string, err error) {
	u.logger.Warn("metrics file is corrupted", "code", machineCodeMetricsFileCorrupted, "path", path, "stage", stage, "error", err)
}

func buildStatsReport(records []taskMetricsRecord) StatsReport {
	report := StatsReport{
		TotalRecords:            len(records),
		SuccessRateBySubcommand: make(map[domain.Subcommand]SuccessStat),
		SuccessRateByModel:      make(map[string]SuccessStat),
	}
	queued, startup, run, gaps := make([]int, 0, len(records)), []int{}, []int{}, []int{}
	promptOutputX, promptOutputY := []float64{}, []float64{}
	promptTokensX, promptTokensY := []float64{}, []float64{}
	for _, record := range records {
		queued = append(queued, record.QueuedMs)
		if record.StartupMs != nil {
			startup = append(startup, *record.StartupMs)
		}
		if record.RunMs != nil {
			run = append(run, *record.RunMs)
		}
		if record.MaxEventGapMs != nil {
			gaps = append(gaps, *record.MaxEventGapMs)
		}
		if record.TimedOut {
			report.TimeoutCount++
		}
		if record.RecoveryOrigin != nil {
			report.RecoveryAttemptedCount++
		}
		if record.Recovered {
			report.RecoverySucceededCount++
		}
		report.SuccessRateBySubcommand[record.Subcommand] = addSuccess(report.SuccessRateBySubcommand[record.Subcommand], record.FinalState)
		report.SuccessRateByModel[record.Model] = addSuccess(report.SuccessRateByModel[record.Model], record.FinalState)
		promptOutputX, promptOutputY = append(promptOutputX, float64(record.PromptBytes)), append(promptOutputY, float64(record.LastMessageBytes))
		if record.OutputTokens != nil {
			promptTokensX, promptTokensY = append(promptTokensX, float64(record.PromptBytes)), append(promptTokensY, float64(*record.OutputTokens))
		}
	}
	for key, stat := range report.SuccessRateBySubcommand {
		report.SuccessRateBySubcommand[key] = finishSuccess(stat)
	}
	for key, stat := range report.SuccessRateByModel {
		report.SuccessRateByModel[key] = finishSuccess(stat)
	}
	report.QueueWaitMedian, report.QueueWaitP95 = percentilePtr(queued, .5), percentilePtr(queued, .95)
	report.StartupMedian, report.StartupP95 = percentilePtr(startup, .5), percentilePtr(startup, .95)
	report.ExecutionMedian, report.ExecutionP95 = percentilePtr(run, .5), percentilePtr(run, .95)
	report.PromptLengthToOutputLengthCorrelation = pearson(promptOutputX, promptOutputY)
	report.PromptLengthToOutputTokensCorrelation = pearson(promptTokensX, promptTokensY)
	if len(gaps) > 0 {
		report.MaxEventGapMedian = secondsRounded(percentileNearestRank(gaps, .5))
		report.MaxEventGapP95 = secondsRounded(percentileNearestRank(gaps, .95))
		report.MaxEventGapMax = secondsRounded(percentileNearestRank(gaps, 1))
	}
	if report.RecoveryAttemptedCount > 0 {
		rate := float64(report.RecoverySucceededCount) / float64(report.RecoveryAttemptedCount)
		report.RecoverySuccessRate = &rate
	}
	return report
}

func addSuccess(stat SuccessStat, finalState domain.TaskState) SuccessStat {
	stat.Total++
	if finalState == domain.StateCompleted || finalState == domain.StateRecovered {
		stat.Success++
	}
	return stat
}

func finishSuccess(stat SuccessStat) SuccessStat {
	if stat.Total > 0 {
		rate := float64(stat.Success) / float64(stat.Total)
		stat.Rate = &rate
	}
	return stat
}

func percentilePtr(values []int, p float64) *int {
	if len(values) == 0 {
		return nil
	}
	value := percentileNearestRank(values, p)
	return &value
}

func percentileNearestRank(values []int, p float64) int {
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	return sorted[int(math.Ceil(p*float64(len(sorted))))-1]
}

func pearson(xs, ys []float64) *float64 {
	if len(xs) < 2 || len(xs) != len(ys) {
		return nil
	}
	var sumX, sumY float64
	for i := range xs {
		sumX += xs[i]
		sumY += ys[i]
	}
	meanX, meanY := sumX/float64(len(xs)), sumY/float64(len(ys))
	var covariance, varianceX, varianceY float64
	for i := range xs {
		dx, dy := xs[i]-meanX, ys[i]-meanY
		covariance += dx * dy
		varianceX += dx * dx
		varianceY += dy * dy
	}
	if varianceX == 0 || varianceY == 0 {
		return nil
	}
	result := covariance / math.Sqrt(varianceX*varianceY)
	return &result
}

func secondsRounded(milliseconds int) *float64 {
	seconds := math.Round(float64(milliseconds)/100) / 10
	return &seconds
}
