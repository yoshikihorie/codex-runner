package metrics

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

const (
	metricsRecordSchemaVersion = 1
	promptSHAPrefixLength      = 16
	machineCodeAppendFailed    = "METRICS_APPEND_FAILED"
)

// RecordTaskMetricsUseCase writes an analysis-only metrics record for a terminal task.
type RecordTaskMetricsUseCase struct {
	tasks                   store.TaskStore
	events                  store.EventReader
	contract                store.ContractReader
	writer                  MetricsWriter
	contentRecordingEnabled bool
	clock                   domain.Clock
	daemonVersion           string
	codexCLIVersion         *string
	logger                  *slog.Logger
	marshal                 func(any) ([]byte, error)
}

// NewRecordTaskMetricsUseCase constructs the metrics recorder.
func NewRecordTaskMetricsUseCase(tasks store.TaskStore, events store.EventReader, contract store.ContractReader, writer MetricsWriter, contentRecordingEnabled bool, clock domain.Clock, daemonVersion string, codexCLIVersion *string, loggers ...*slog.Logger) *RecordTaskMetricsUseCase {
	if tasks == nil || events == nil || contract == nil || writer == nil || clock == nil {
		panic("record task metrics use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("record task metrics use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	var cliVersionCopy *string
	if codexCLIVersion != nil {
		value := *codexCLIVersion
		cliVersionCopy = &value
	}
	return &RecordTaskMetricsUseCase{
		tasks: tasks, events: events, contract: contract, writer: writer,
		contentRecordingEnabled: contentRecordingEnabled, clock: clock,
		daemonVersion: daemonVersion, codexCLIVersion: cliVersionCopy,
		logger: logger, marshal: json.Marshal,
	}
}

type taskMetricsRecord struct {
	SchemaVersion           int                    `json:"schema_version"`
	TaskID                  string                 `json:"task_id"`
	Subcommand              domain.Subcommand      `json:"subcommand"`
	Model                   string                 `json:"model"`
	ReasoningEffort         *string                `json:"reasoning_effort"`
	Route                   domain.ExecutionRoute  `json:"route"`
	RequestedAt             time.Time              `json:"requested_at"`
	StartedAt               *time.Time             `json:"started_at"`
	FinishedAt              time.Time              `json:"finished_at"`
	QueuedMs                int                    `json:"queued_ms"`
	StartupMs               *int                   `json:"startup_ms"`
	RunMs                   *int                   `json:"run_ms"`
	TotalMs                 int                    `json:"total_ms"`
	FinalState              domain.TaskState       `json:"final_state"`
	ExitCode                *int                   `json:"exit_code"`
	ExitCodeClass           *domain.ExitCodeClass  `json:"exit_code_class"`
	Estimated               bool                   `json:"estimated"`
	PromptBytes             int                    `json:"prompt_bytes"`
	PromptLines             int                    `json:"prompt_lines"`
	PromptSHA256            string                 `json:"prompt_sha256"`
	PromptBody              *string                `json:"prompt_body"`
	LastMessageBytes        int                    `json:"last_message_bytes"`
	LastMessageLines        int                    `json:"last_message_lines"`
	LastMessageSHA256       *string                `json:"last_message_sha256"`
	LastMessageBody         *string                `json:"last_message_body"`
	EventCount              int                    `json:"event_count"`
	MaxEventGapMs           *int                   `json:"max_event_gap_ms"`
	StalledTotalMs          int                    `json:"stalled_total_ms"`
	Recovered               bool                   `json:"recovered"`
	RecoveryOrigin          *domain.RecoveryOrigin `json:"recovery_origin"`
	PartialOutputBytes      *int                   `json:"partial_output_bytes"`
	TimeoutRequestedSeconds *int                   `json:"timeout_requested_seconds"`
	TimeoutResolvedSeconds  int                    `json:"timeout_resolved_seconds"`
	TimedOut                bool                   `json:"timed_out"`
	CancelRequested         bool                   `json:"cancel_requested"`
	InputTokens             *int                   `json:"input_tokens"`
	CachedInputTokens       *int                   `json:"cached_input_tokens"`
	OutputTokens            *int                   `json:"output_tokens"`
	ReasoningOutputTokens   *int                   `json:"reasoning_output_tokens"`
	DaemonVersion           string                 `json:"daemon_version"`
	CodexCLIVersion         *string                `json:"codex_cli_version"`
}

// Execute records one line when every read and derivation succeeds. Metrics failures are fail-soft.
func (u *RecordTaskMetricsUseCase) Execute(_ context.Context, in RecordTaskMetricsInput) RecordTaskMetricsOutput {
	if !in.FinalState.IsTerminal() || in.StalledTotalMs < 0 || in.OccurredAt.IsZero() {
		return u.fail(in.TaskID, "validate-input", errors.New("invalid metrics record input"), false)
	}

	snapshot, err := u.tasks.Load(in.TaskID)
	if err != nil {
		return u.fail(in.TaskID, "load-snapshot", err, false)
	}
	if !snapshot.State.IsTerminal() {
		return u.fail(in.TaskID, "snapshot-not-terminal", errors.New("snapshot state is not terminal"), false)
	}
	events, err := u.events.ReadFrom(in.TaskID, 0)
	if err != nil {
		return u.fail(in.TaskID, "read-events", err, false)
	}
	prompt, err := u.contract.ReadPromptContent(in.TaskID)
	if err != nil {
		return u.fail(in.TaskID, "read-prompt", err, false)
	}
	lastMessage, err := u.contract.ReadLastMessageContent(in.TaskID)
	if err != nil {
		return u.fail(in.TaskID, "read-last-message", err, false)
	}
	partialOutput, err := u.contract.ReadPartialOutputContent(in.TaskID)
	if err != nil {
		return u.fail(in.TaskID, "read-partial-output", err, false)
	}

	record, err := u.derive(snapshot, events, prompt, lastMessage, partialOutput, in)
	if err != nil {
		return u.fail(in.TaskID, "derive-record", err, false)
	}
	line, err := u.marshal(record)
	if err != nil {
		return u.fail(in.TaskID, "marshal-json", err, false)
	}
	if err := u.writer.Append(in.TaskID, in.OccurredAt.Format("2006-01"), line); err != nil {
		return u.fail(in.TaskID, "append-metrics", err, true)
	}
	return RecordTaskMetricsOutput{Recorded: true}
}

func (u *RecordTaskMetricsUseCase) fail(taskID domain.TaskID, stage string, err error, appendFailure bool) RecordTaskMetricsOutput {
	// Dependency errors may embed task content. Keep the diagnostic error useful
	// without allowing arbitrary reader or writer error text into the log.
	args := []any{"task_id", taskID.String(), "stage", stage, "error", errors.New("metrics operation failed"), "diagnosed_at", u.clock.Now()}
	if appendFailure {
		args = append(args, "code", machineCodeAppendFailed)
	}
	u.logger.Warn("record task metrics failed", args...)
	return RecordTaskMetricsOutput{}
}

func (u *RecordTaskMetricsUseCase) derive(snapshot domain.TaskSnapshot, inputEvents []store.EventRecord, prompt, lastMessage, partialOutput []byte, in RecordTaskMetricsInput) (taskMetricsRecord, error) {
	events := append([]store.EventRecord(nil), inputEvents...)
	sort.SliceStable(events, func(i, j int) bool { return events[i].Seq < events[j].Seq })

	queued, startup, run, gap, cancelRequested, tokens, err := deriveEventMetrics(events, snapshot)
	if err != nil {
		return taskMetricsRecord{}, err
	}
	total := snapshot.StateUpdatedAt.Sub(snapshot.RequestedAt).Milliseconds()
	if total < 0 || queued < 0 || (startup != nil && *startup < 0) || (run != nil && *run < 0) {
		return taskMetricsRecord{}, errors.New("negative metrics duration")
	}

	promptHash := contentHash(prompt)
	lastHash := (*string)(nil)
	if len(lastMessage) > 0 {
		value := contentHash(lastMessage)
		lastHash = &value
	}
	var promptBody, lastMessageBody *string
	if u.contentRecordingEnabled {
		promptValue, lastValue := string(prompt), string(lastMessage)
		promptBody, lastMessageBody = &promptValue, &lastValue
	}
	var exitCode *int
	var exitCodeClass *domain.ExitCodeClass
	if snapshot.ExitCode != nil {
		raw, class := snapshot.ExitCode.Raw(), snapshot.ExitCode.Class()
		exitCode, exitCodeClass = &raw, &class
	}
	var partialBytes *int
	if partialOutput != nil {
		value := len(partialOutput)
		partialBytes = &value
	}
	timedOut := snapshot.State == domain.StateTimeoutLost || (snapshot.RecoveryOrigin != nil && *snapshot.RecoveryOrigin == domain.RecoveryOriginTimeout)

	return taskMetricsRecord{
		SchemaVersion: metricsRecordSchemaVersion, TaskID: snapshot.TaskID.String(),
		Subcommand: snapshot.Subcommand, Model: snapshot.Model, ReasoningEffort: snapshot.ReasoningEffort,
		Route: snapshot.Route, RequestedAt: snapshot.RequestedAt, StartedAt: snapshot.ProcessStartedAt,
		FinishedAt: snapshot.StateUpdatedAt, QueuedMs: int(queued), StartupMs: startup, RunMs: run,
		TotalMs: int(total), FinalState: snapshot.State, ExitCode: exitCode, ExitCodeClass: exitCodeClass,
		Estimated: in.Estimated, PromptBytes: len(prompt), PromptLines: logicalLines(prompt),
		PromptSHA256: promptHash, PromptBody: promptBody, LastMessageBytes: len(lastMessage),
		LastMessageLines: logicalLines(lastMessage), LastMessageSHA256: lastHash, LastMessageBody: lastMessageBody,
		EventCount: len(inputEvents), MaxEventGapMs: gap, StalledTotalMs: in.StalledTotalMs,
		Recovered: snapshot.Recovered, RecoveryOrigin: snapshot.RecoveryOrigin, PartialOutputBytes: partialBytes,
		TimeoutRequestedSeconds: snapshot.RequestedTimeoutSeconds, TimeoutResolvedSeconds: snapshot.ResolvedTimeoutSeconds,
		TimedOut: timedOut, CancelRequested: cancelRequested, InputTokens: tokens.input,
		CachedInputTokens: tokens.cachedInput, OutputTokens: tokens.output, ReasoningOutputTokens: tokens.reasoningOutput,
		DaemonVersion: u.daemonVersion, CodexCLIVersion: u.codexCLIVersion,
	}, nil
}

type tokenMetrics struct{ input, cachedInput, output, reasoningOutput *int }

func deriveEventMetrics(events []store.EventRecord, snapshot domain.TaskSnapshot) (int64, *int, *int, *int, bool, tokenMetrics, error) {
	var startedAt, exitedAt *time.Time
	var maxGap *int
	cancelRequested := false
	var latestCompleted *store.EventRecord
	for i := range events {
		event := &events[i]
		if i > 0 {
			gap := event.RecordedAt.Sub(events[i-1].RecordedAt).Milliseconds()
			if gap < 0 {
				return 0, nil, nil, nil, false, tokenMetrics{}, errors.New("negative event gap")
			}
			gapValue := int(gap)
			if maxGap == nil || gapValue > *maxGap {
				maxGap = &gapValue
			}
		}
		switch event.EventType {
		case "TaskStarted":
			if startedAt == nil {
				value, err := eventOccurredAt(event.Raw)
				if err != nil {
					return 0, nil, nil, nil, false, tokenMetrics{}, err
				}
				startedAt = &value
			}
		case "TaskExited":
			if exitedAt == nil {
				value, err := eventOccurredAt(event.Raw)
				if err != nil {
					return 0, nil, nil, nil, false, tokenMetrics{}, err
				}
				exitedAt = &value
			}
		case "TaskCancelRequested":
			cancelRequested = true
		case "turn.completed":
			latestCompleted = event
		}
	}

	queued := snapshot.StateUpdatedAt.Sub(snapshot.RequestedAt).Milliseconds()
	var startup, run *int
	if startedAt != nil {
		queued = startedAt.Sub(snapshot.RequestedAt).Milliseconds()
		if snapshot.ProcessStartedAt != nil {
			value := int(snapshot.ProcessStartedAt.Sub(*startedAt).Milliseconds())
			startup = &value
		}
	}
	if exitedAt != nil && snapshot.ProcessStartedAt != nil {
		value := int(exitedAt.Sub(*snapshot.ProcessStartedAt).Milliseconds())
		run = &value
	}
	var tokens tokenMetrics
	if latestCompleted != nil {
		tokens = usageMetrics(latestCompleted.Raw)
	}
	return queued, startup, run, maxGap, cancelRequested, tokens, nil
}

func eventOccurredAt(raw any) (time.Time, error) {
	fields, ok := raw.(map[string]any)
	if !ok {
		return time.Time{}, errors.New("event raw is not an object")
	}
	value, ok := fields["occurred_at"].(string)
	if !ok {
		return time.Time{}, errors.New("event occurred_at is invalid")
	}
	occurredAt, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse event occurred_at: %w", err)
	}
	return occurredAt, nil
}

func usageMetrics(raw any) tokenMetrics {
	fields, ok := raw.(map[string]any)
	if !ok {
		return tokenMetrics{}
	}
	usage, ok := fields["usage"].(map[string]any)
	if !ok {
		return tokenMetrics{}
	}
	return tokenMetrics{
		input: intValue(usage["input_tokens"]), cachedInput: intValue(usage["cached_input_tokens"]),
		output: intValue(usage["output_tokens"]), reasoningOutput: intValue(usage["reasoning_output_tokens"]),
	}
}

func intValue(value any) *int {
	number, ok := value.(float64)
	if !ok || math.IsNaN(number) || math.IsInf(number, 0) || math.Trunc(number) != number || number < 0 || !fitsInt(number) {
		return nil
	}
	converted := int(number)
	return &converted
}

func fitsInt(value float64) bool {
	if strconvIntSize() == 32 {
		return value <= math.MaxInt32
	}
	return value < math.Exp2(63)
}

func strconvIntSize() int {
	return 32 << (^uint(0) >> 63)
}

func contentHash(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])[:promptSHAPrefixLength]
}

func logicalLines(content []byte) int {
	if len(content) == 0 {
		return 0
	}
	lines := 0
	for _, value := range content {
		if value == '\n' {
			lines++
		}
	}
	if content[len(content)-1] != '\n' {
		lines++
	}
	return lines
}
