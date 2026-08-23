package metrics

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type fakeMetricsReader struct {
	files    []string
	listErr  error
	contents map[string]string
	openErr  map[string]error
	gotSince *string
	gotUntil *string
}

func (f *fakeMetricsReader) ListMonthlyFiles(_ string, since, until *string) ([]string, error) {
	f.gotSince, f.gotUntil = since, until
	return f.files, f.listErr
}

func (f *fakeMetricsReader) OpenMonthlyFile(path string) (io.ReadCloser, error) {
	if err := f.openErr[path]; err != nil {
		return nil, err
	}
	return io.NopCloser(strings.NewReader(f.contents[path])), nil
}

func statsLine(t *testing.T, mutate func(*taskMetricsRecord)) string {
	t.Helper()
	startup, run, gap, tokens := 20, 30, 1234, 100
	r := taskMetricsRecord{Subcommand: domain.SubcommandImpl, Model: "model-a", QueuedMs: 10, StartupMs: &startup, RunMs: &run, FinalState: domain.StateCompleted, PromptBytes: 10, LastMessageBytes: 20, MaxEventGapMs: &gap, OutputTokens: &tokens}
	if mutate != nil {
		mutate(&r)
	}
	b, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b) + "\n"
}

func newStatsUseCase(reader *fakeMetricsReader, logger *slog.Logger) *ComputeTaskStatsUseCase {
	return NewComputeTaskStatsUseCase(reader, "/tmp/logs", logger)
}

func TestParseTaskMetricsRecord_AllFields(t *testing.T) { // T-A1, SCN-01
	text, hash := "body", "hash"
	started := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	finished := time.Date(2026, 1, 2, 4, 5, 6, 0, time.UTC)
	intValue, tokenValue := 7, 8
	want := taskMetricsRecord{
		SchemaVersion: 1, TaskID: "id", Subcommand: domain.SubcommandImpl, Model: "model", ReasoningEffort: ptr("high"), Route: domain.ExecutionRouteDaemon,
		RequestedAt: started, StartedAt: &started, FinishedAt: finished, QueuedMs: 1, StartupMs: &intValue, RunMs: &intValue, TotalMs: 2,
		FinalState: domain.StateRecovered, ExitCode: &intValue, ExitCodeClass: exitCodeClassPtr(domain.ExitCodeClassSuccess), Estimated: true,
		PromptBytes: 3, PromptLines: 4, PromptSHA256: hash, PromptBody: &text, LastMessageBytes: 5, LastMessageLines: 6, LastMessageSHA256: &hash, LastMessageBody: &text,
		EventCount: 9, MaxEventGapMs: &intValue, StalledTotalMs: 10, Recovered: true, RecoveryOrigin: recoveryPtr(domain.RecoveryOriginTimeout), PartialOutputBytes: &intValue,
		TimeoutRequestedSeconds: &intValue, TimeoutResolvedSeconds: 11, TimedOut: true, CancelRequested: true, InputTokens: &tokenValue, CachedInputTokens: &tokenValue,
		OutputTokens: &tokenValue, ReasoningOutputTokens: &tokenValue, DaemonVersion: "daemon", CodexCLIVersion: &text,
	}
	line, marshalErr := json.Marshal(want)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	got, err := parseTaskMetricsRecord(line)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("parse = %#v, %v", got, err)
	}
}

func TestParseTaskMetricsRecord_Invalid(t *testing.T) { // T-A2, SCN-06
	if _, err := parseTaskMetricsRecord([]byte("{")); err == nil {
		t.Fatal("expected error")
	}
}

func TestComputeTaskStats_SuccessGrouping(t *testing.T) { // T-A3, T-A4, SCN-01/02
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, nil) + statsLine(t, func(r *taskMetricsRecord) { r.FinalState = domain.StateFailed })}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{})
	if err != nil || r.SuccessRateBySubcommand[domain.SubcommandImpl].Success != 1 || r.SuccessRateByModel["model-a"].Total != 2 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestPercentileNearestRank(t *testing.T) { // T-A5, SCN-03
	for _, tc := range []struct {
		values []int
		p      float64
		want   int
	}{{[]int{7}, .95, 7}, {[]int{1, 9}, .5, 1}, {sequence(20), .95, 19}} {
		if got := percentileNearestRank(tc.values, tc.p); got != tc.want {
			t.Fatalf("percentile = %d, want %d", got, tc.want)
		}
	}
}

func TestComputeTaskStats_Durations(t *testing.T) { // T-A6, SCN-03
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, nil) + statsLine(t, func(r *taskMetricsRecord) { r.QueuedMs = 50; r.StartupMs = nil; r.RunMs = nil })}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{})
	if err != nil || r.QueueWaitMedian == nil || *r.QueueWaitMedian != 10 || r.StartupP95 == nil || *r.StartupP95 != 20 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_Filter(t *testing.T) { // T-A7, SCN-04
	f := domain.SubcommandReview
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, nil)}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{SubcommandFilter: &f})
	if err != nil || r.TotalRecords != 0 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_NonRecordingFilter(t *testing.T) { // T-A8, SCN-13
	f := domain.SubcommandStats
	r, err := newStatsUseCase(&fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, nil)}}, nil).Execute(StatsQuery{SubcommandFilter: &f})
	if err != nil || r.TotalRecords != 0 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_SkipsCorruptedLines(t *testing.T) { // T-A9, SCN-06
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": "bad\n" + statsLine(t, nil)}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{})
	if err != nil || r.SkippedLines != 1 || r.TotalRecords != 1 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_SkipsUnreadableFile(t *testing.T) { // T-A10, SCN-18
	reader := &fakeMetricsReader{files: []string{"a", "b", "c"}, contents: map[string]string{"b": statsLine(t, nil), "c": statsLine(t, nil)}, openErr: map[string]error{"a": errors.New("denied")}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{})
	if err != nil || r.MatchedFiles != 3 || r.TotalRecords != 2 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_Empty(t *testing.T) { // T-A11, SCN-07
	r, err := newStatsUseCase(&fakeMetricsReader{}, nil).Execute(StatsQuery{})
	if err != nil || r.MatchedFiles != 0 || r.QueueWaitMedian != nil || r.RecoverySuccessRate != nil || r.SuccessRateByModel == nil || len(r.SuccessRateByModel) != 0 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_Correlation(t *testing.T) { // T-A12, SCN-14
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, nil) + statsLine(t, func(r *taskMetricsRecord) { r.PromptBytes = 20; r.LastMessageBytes = 40; v := 200; r.OutputTokens = &v })}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{})
	if err != nil || r.PromptLengthToOutputLengthCorrelation == nil || *r.PromptLengthToOutputLengthCorrelation != 1 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_CorrelationBoundaries(t *testing.T) { // T-A13, SCN-14b
	for _, tc := range []struct {
		name, content string
		wantNil       bool
	}{
		{"one", statsLine(t, nil), true},
		{"two", statsLine(t, nil) + statsLine(t, func(r *taskMetricsRecord) { r.PromptBytes = 20; r.LastMessageBytes = 30 }), false},
		{"zero_variance", statsLine(t, nil) + statsLine(t, func(r *taskMetricsRecord) { r.LastMessageBytes = 30 }), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := newStatsUseCase(&fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": tc.content}}, nil).Execute(StatsQuery{})
			if err != nil || (r.PromptLengthToOutputLengthCorrelation == nil) != tc.wantNil {
				t.Fatalf("report = %#v, %v", r, err)
			}
			if _, err := json.Marshal(r); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestComputeTaskStats_RecoveryAndTimeout(t *testing.T) { // T-A14, SCN-15
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, func(r *taskMetricsRecord) {
		r.TimedOut = true
		r.Recovered = true
		r.RecoveryOrigin = recoveryPtr(domain.RecoveryOriginTimeout)
	})}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{})
	if err != nil || r.TimeoutCount != 1 || r.RecoveryAttemptedCount != 1 || r.RecoverySucceededCount != 1 || r.RecoverySuccessRate == nil || *r.RecoverySuccessRate != 1 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_MaxEventGap(t *testing.T) { // T-A15, SCN-16
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, nil) + statsLine(t, func(r *taskMetricsRecord) { v := 1999; r.MaxEventGapMs = &v })}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{})
	if err != nil || r.MaxEventGapMedian == nil || *r.MaxEventGapMedian != 1.2 || r.MaxEventGapP95 == nil || *r.MaxEventGapP95 != 2 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_Idempotent(t *testing.T) { // T-A16, SCN-17
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, nil)}}
	u := newStatsUseCase(reader, nil)
	first, err := u.Execute(StatsQuery{})
	second, err2 := u.Execute(StatsQuery{})
	if err != nil || err2 != nil || !reportsEqual(first, second) {
		t.Fatalf("reports = %#v %#v, errors %v %v", first, second, err, err2)
	}
}

func TestComputeTaskStats_CorruptionLog(t *testing.T) { // T-A17, SCN-06
	var buf bytes.Buffer
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": "bad\n"}}
	_, _ = newStatsUseCase(reader, slog.New(slog.NewJSONHandler(&buf, nil))).Execute(StatsQuery{})
	if !strings.Contains(buf.String(), "METRICS_FILE_CORRUPTED") {
		t.Fatal("missing corruption code")
	}
}

func TestStatsMessageKeys(t *testing.T) { // T-A18
	if MessageKeyStatsInvalidDateRange != "error.stats.invalidDateRange" || MessageKeyStatsInvalidSubcommand != "error.stats.invalidSubcommand" || MessageKeyStatsSkippedLines != "info.stats.skippedLines" {
		t.Fatal("unexpected message key")
	}
}

func TestComputeTaskStats_PassesDateBounds(t *testing.T) { // T-A19, SCN-05
	since, until := "2026-01", "2026-02"
	reader := &fakeMetricsReader{}
	_, _ = newStatsUseCase(reader, nil).Execute(StatsQuery{Since: &since, Until: &until})
	if reader.gotSince != &since || reader.gotUntil != &until {
		t.Fatal("date bounds not passed through")
	}
}

func TestComputeTaskStats_UnterminatedLineIsSkipped(t *testing.T) { // T-A20, SCN-06
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": strings.TrimSuffix(statsLine(t, nil), "\n")}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{})
	if err != nil || r.SkippedLines != 1 || r.TotalRecords != 0 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_MultipleSuccessGroups(t *testing.T) { // T-A21, SCN-01/02
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, nil) +
		statsLine(t, func(r *taskMetricsRecord) {
			r.Subcommand, r.Model, r.FinalState = domain.SubcommandReview, "model-a", domain.StateFailed
		}) +
		statsLine(t, func(r *taskMetricsRecord) {
			r.Model, r.FinalState = "model-b", domain.StateRecovered
		}),
	}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.SuccessRateBySubcommand[domain.SubcommandImpl]; got.Total != 2 || got.Success != 2 || got.Rate == nil || *got.Rate != 1 {
		t.Fatalf("impl success stat = %#v", got)
	}
	if got := r.SuccessRateBySubcommand[domain.SubcommandReview]; got.Total != 1 || got.Success != 0 || got.Rate == nil || *got.Rate != 0 {
		t.Fatalf("review success stat = %#v", got)
	}
	if got := r.SuccessRateByModel["model-a"]; got.Total != 2 || got.Success != 1 || got.Rate == nil || *got.Rate != .5 {
		t.Fatalf("model-a success stat = %#v", got)
	}
	if got := r.SuccessRateByModel["model-b"]; got.Total != 1 || got.Success != 1 || got.Rate == nil || *got.Rate != 1 {
		t.Fatalf("model-b success stat = %#v", got)
	}
}

func TestComputeTaskStats_AllDurationPercentiles(t *testing.T) { // T-A22, SCN-03
	var content strings.Builder
	for i := 1; i <= 20; i++ {
		i := i
		content.WriteString(statsLine(t, func(r *taskMetricsRecord) {
			startup, run := 100+i, 200+i
			r.QueuedMs, r.StartupMs, r.RunMs = i, &startup, &run
		}))
	}
	r, err := newStatsUseCase(&fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": content.String()}}, nil).Execute(StatsQuery{})
	if err != nil || r.QueueWaitP95 == nil || *r.QueueWaitP95 != 19 || r.StartupMedian == nil || *r.StartupMedian != 110 || r.ExecutionMedian == nil || *r.ExecutionMedian != 210 || r.ExecutionP95 == nil || *r.ExecutionP95 != 219 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_FilterIncludesOnlyRequestedSubcommand(t *testing.T) { // T-A23, SCN-04
	filter := domain.SubcommandReview
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, nil) + statsLine(t, func(r *taskMetricsRecord) {
		r.Subcommand = domain.SubcommandReview
	})}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{SubcommandFilter: &filter})
	if err != nil || r.TotalRecords != 1 || len(r.SuccessRateBySubcommand) != 1 {
		t.Fatalf("report = %#v, %v", r, err)
	}
	if _, ok := r.SuccessRateBySubcommand[domain.SubcommandReview]; !ok {
		t.Fatalf("filter result = %#v", r.SuccessRateBySubcommand)
	}
}

func TestComputeTaskStats_EmptyReportHasNilDistributionsAndEmptyMaps(t *testing.T) { // T-A24, SCN-07
	r, err := newStatsUseCase(&fakeMetricsReader{}, nil).Execute(StatsQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if r.QueueWaitMedian != nil || r.QueueWaitP95 != nil || r.StartupMedian != nil || r.StartupP95 != nil || r.ExecutionMedian != nil || r.ExecutionP95 != nil || r.PromptLengthToOutputLengthCorrelation != nil || r.PromptLengthToOutputTokensCorrelation != nil || r.MaxEventGapMedian != nil || r.MaxEventGapP95 != nil || r.MaxEventGapMax != nil || r.RecoverySuccessRate != nil {
		t.Fatalf("expected nil distributions, got %#v", r)
	}
	if r.SuccessRateBySubcommand == nil || len(r.SuccessRateBySubcommand) != 0 || r.SuccessRateByModel == nil || len(r.SuccessRateByModel) != 0 {
		t.Fatalf("expected empty initialized maps, got %#v", r)
	}
}

func TestComputeTaskStats_OutputTokensCorrelation(t *testing.T) { // T-A25, SCN-14/14b
	withTokens := statsLine(t, nil) + statsLine(t, func(r *taskMetricsRecord) {
		r.PromptBytes = 20
		tokens := 200
		r.OutputTokens = &tokens
	})
	r, err := newStatsUseCase(&fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": withTokens}}, nil).Execute(StatsQuery{})
	if err != nil || r.PromptLengthToOutputTokensCorrelation == nil || *r.PromptLengthToOutputTokensCorrelation != 1 {
		t.Fatalf("report = %#v, %v", r, err)
	}
	withoutTokens := statsLine(t, func(r *taskMetricsRecord) { r.OutputTokens = nil }) + statsLine(t, func(r *taskMetricsRecord) { r.PromptBytes, r.OutputTokens = 20, nil })
	r, err = newStatsUseCase(&fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": withoutTokens}}, nil).Execute(StatsQuery{})
	if err != nil || r.PromptLengthToOutputTokensCorrelation != nil {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_MultipleRecoveryAndTimeoutCounts(t *testing.T) { // T-A26, SCN-15
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, func(r *taskMetricsRecord) {
		r.TimedOut, r.Recovered, r.RecoveryOrigin = true, true, recoveryPtr(domain.RecoveryOriginTimeout)
	}) +
		statsLine(t, func(r *taskMetricsRecord) {
			r.TimedOut, r.RecoveryOrigin = true, recoveryPtr(domain.RecoveryOriginTimeout)
		}) +
		statsLine(t, func(r *taskMetricsRecord) { r.TimedOut = true }),
	}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{})
	if err != nil || r.TimeoutCount != 3 || r.RecoveryAttemptedCount != 2 || r.RecoverySucceededCount != 1 || r.RecoverySuccessRate == nil || *r.RecoverySuccessRate != .5 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_MaxEventGapMaximum(t *testing.T) { // T-A27, SCN-16
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": statsLine(t, nil) + statsLine(t, func(r *taskMetricsRecord) {
		gap := 1999
		r.MaxEventGapMs = &gap
	})}}
	r, err := newStatsUseCase(reader, nil).Execute(StatsQuery{})
	if err != nil || r.MaxEventGapMax == nil || *r.MaxEventGapMax != 2 {
		t.Fatalf("report = %#v, %v", r, err)
	}
}

func TestComputeTaskStats_ThreeExecutionsDoNotMutateInput(t *testing.T) { // T-A28, SCN-17
	content := statsLine(t, nil)
	reader := &fakeMetricsReader{files: []string{"a"}, contents: map[string]string{"a": content}}
	u := newStatsUseCase(reader, nil)
	first, err1 := u.Execute(StatsQuery{})
	second, err2 := u.Execute(StatsQuery{})
	third, err3 := u.Execute(StatsQuery{})
	if err1 != nil || err2 != nil || err3 != nil || !reportsEqual(first, second) || !reportsEqual(second, third) || reader.contents["a"] != content {
		t.Fatalf("reports = %#v %#v %#v, errors = %v %v %v, content = %q", first, second, third, err1, err2, err3, reader.contents["a"])
	}
}

func ptr(v string) *string                                          { return &v }
func recoveryPtr(v domain.RecoveryOrigin) *domain.RecoveryOrigin    { return &v }
func exitCodeClassPtr(v domain.ExitCodeClass) *domain.ExitCodeClass { return &v }
func sequence(n int) []int {
	out := make([]int, n)
	for i := range out {
		out[i] = i + 1
	}
	return out
}
func reportsEqual(a, b StatsReport) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(ab, bb)
}
