package execution_test

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	executionusecase "github.com/yoshikihorie/codex-runner/internal/execution/usecase"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
	transportusecase "github.com/yoshikihorie/codex-runner/internal/transport/usecase"
)

type queueIntegrationStarter struct{ payloads []execution.TaskLaunchPayload }

func (s *queueIntegrationStarter) Start(payload execution.TaskLaunchPayload) bool {
	s.payloads = append(s.payloads, cloneLaunchPayload(payload))
	return true
}

func (*queueIntegrationStarter) Shutdown(context.Context) {}

type queueIntegrationOptions struct {
	model  string
	effort *string
}

func (o queueIntegrationOptions) ResolveModel(domain.Subcommand, *string) (string, bool) {
	return o.model, true
}
func (o queueIntegrationOptions) ResolveReasoningEffort(domain.Subcommand, *string) (*string, bool) {
	return cloneString(o.effort), true
}

type recordedAdmission struct {
	input  execution.TaskAdmissionInput
	result execution.TaskAdmissionResult
}

type recordingAdmitter struct {
	inner   *executionusecase.AdmitTaskUseCase
	records []recordedAdmission
}

func (a *recordingAdmitter) Admit(input execution.TaskAdmissionInput) (execution.TaskAdmissionResult, error) {
	result, err := a.inner.Admit(input)
	a.records = append(a.records, recordedAdmission{input: cloneAdmissionInput(input), result: cloneAdmissionResult(result)})
	return result, err
}

func (a *recordingAdmitter) CompensateRejectedStart(taskID domain.TaskID) error {
	return a.inner.CompensateRejectedStart(taskID)
}

type queueIntegrationFixture struct {
	tasksRoot string
	queue     execution.TaskQueue
	registry  execution.ActiveTaskRegistry
	launching execution.LaunchingTaskRegistry
	admitter  *recordingAdmitter
	starter   *queueIntegrationStarter
	submit    *transportusecase.SubmitTaskUseCase
}

func newQueueIntegrationFixture(t *testing.T, maxConcurrent int, maxConcurrentImpl int, options queueIntegrationOptions) queueIntegrationFixture {
	t.Helper()
	tasksRoot := t.TempDir()
	tasks, err := store.NewFileTaskStore(tasksRoot)
	if err != nil {
		t.Fatal(err)
	}
	queue, registry := execution.NewTaskQueue(), execution.NewActiveTaskRegistry()
	const queueMaxDepth = 10
	launching := execution.NewLaunchingTaskRegistry()
	promotions := execution.NewPromotionRegistry()
	admit := executionusecase.NewAdmitTaskUseCase(queue, registry, launching, promotions, &sync.Mutex{}, maxConcurrent, maxConcurrentImpl, queueMaxDepth)
	recording := &recordingAdmitter{inner: admit}
	starter := &queueIntegrationStarter{}
	now := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	submit := transportusecase.NewSubmitTaskUseCase(
		tasks, nil, nil, recording, queueMaxDepth, starter, options,
		domain.ClockFunc(func() time.Time { return now }), slog.New(slog.NewTextHandler(os.Stderr, nil)),
	)
	return queueIntegrationFixture{tasksRoot: tasksRoot, queue: queue, registry: registry, launching: launching, admitter: recording, starter: starter, submit: submit}
}

func TestQueuePromotionReservationPreventsAdmissionOverflowUntilCompensated(t *testing.T) {
	queue := execution.NewTaskQueue()
	registry := execution.NewActiveTaskRegistry()
	launching := execution.NewLaunchingTaskRegistry()
	promotions := execution.NewPromotionRegistry()
	queueMu := &sync.Mutex{}
	admit := executionusecase.NewAdmitTaskUseCase(queue, registry, launching, promotions, queueMu, 1, 1, 1)
	advance := executionusecase.NewAdvanceQueueUseCase(queue, registry, launching, promotions, queueMu, 1, 1)

	activeInput := promotionAdmissionInput(t, "active")
	active, err := admit.Execute(context.Background(), activeInput)
	if err != nil || active.LaunchPayload == nil {
		t.Fatalf("active=%#v err=%v", active, err)
	}
	waitingInput := promotionAdmissionInput(t, "waiting")
	waiting, err := admit.Execute(context.Background(), waitingInput)
	if err != nil || waiting.QueuePosition == nil || queue.Len() != 1 {
		t.Fatalf("waiting=%#v err=%v queue=%d", waiting, err, queue.Len())
	}

	payload, found, err := advance.Execute(context.Background(), activeInput.TaskID, time.Now())
	if err != nil || !found || payload.Task.ID() != waitingInput.TaskID || queue.Len() != 0 || promotions.Len() != 1 {
		t.Fatalf("payload=%#v found=%t err=%v queue=%d promotions=%d", payload, found, err, queue.Len(), promotions.Len())
	}
	result, err := admit.Execute(context.Background(), promotionAdmissionInput(t, "overflow"))
	if !errors.Is(err, domain.ErrQueueFull) || result.State != "" || queue.Len() != 0 || promotions.Len() != 1 {
		t.Fatalf("result=%#v err=%v queue=%d promotions=%d", result, err, queue.Len(), promotions.Len())
	}

	advance.CompensateRejectedStart(payload, time.Now())
	if queue.Len() != 1 || promotions.Len() != 0 {
		t.Fatalf("queue=%d promotions=%d", queue.Len(), promotions.Len())
	}
}

func promotionAdmissionInput(t *testing.T, suffix string) execution.TaskAdmissionInput {
	t.Helper()
	taskID, err := domain.NewTaskID("review-20260825-120000-a1b2-promotion-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	slug, err := domain.NewSlug("promotion-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	timeout, err := domain.NewTimeout(nil, 1800)
	if err != nil {
		t.Fatal(err)
	}
	return execution.TaskAdmissionInput{
		TaskID: taskID, Subcommand: domain.SubcommandReview, Slug: slug, RequestedAt: time.Now(),
		PromptText: "prompt", ResolvedTimeout: timeout, Model: "model", SandboxMode: "read-only", SourceWorkingDir: "/private/tmp/source",
	}
}

func queueIntegrationInput(t *testing.T, slug string) transportusecase.SubmitTaskInput {
	t.Helper()
	return transportusecase.SubmitTaskInput{
		Subcommand: "review", RawSlug: slug, Prompt: "integration prompt", RawWorkingDir: t.TempDir(),
		RequestedAt: time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC),
	}
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func clonePaths(paths []domain.NormalizedPath) []domain.NormalizedPath {
	return append([]domain.NormalizedPath(nil), paths...)
}

func cloneLaunchPayload(payload execution.TaskLaunchPayload) execution.TaskLaunchPayload {
	payload.ReasoningEffort = cloneString(payload.ReasoningEffort)
	payload.NormalizedPaths = clonePaths(payload.NormalizedPaths)
	if payload.WorkingDir != nil {
		workingDir := *payload.WorkingDir
		payload.WorkingDir = &workingDir
	}
	return payload
}

func cloneAdmissionInput(input execution.TaskAdmissionInput) execution.TaskAdmissionInput {
	input.ReasoningEffort = cloneString(input.ReasoningEffort)
	input.NormalizedPaths = clonePaths(input.NormalizedPaths)
	return input
}

func cloneAdmissionResult(result execution.TaskAdmissionResult) execution.TaskAdmissionResult {
	if result.QueuePosition != nil {
		position := *result.QueuePosition
		result.QueuePosition = &position
	}
	result.Events = append([]domain.Event(nil), result.Events...)
	if result.LaunchPayload != nil {
		payload := cloneLaunchPayload(*result.LaunchPayload)
		result.LaunchPayload = &payload
	}
	return result
}

func requireReservedTaskDir(t *testing.T, root string, taskID domain.TaskID) {
	t.Helper()
	info, err := os.Stat(filepath.Join(root, taskID.String()))
	if err != nil || !info.IsDir() {
		t.Fatal("expected reserved task directory")
	}
}

func requireQueuedOnly(t *testing.T, events []domain.Event) {
	t.Helper()
	if len(events) != 1 {
		t.Fatal("expected exactly one event")
	}
	if _, ok := events[0].(domain.TaskQueued); !ok {
		t.Fatal("expected TaskQueued event")
	}
}

// SCN-proto-01-16.
func TestSubmitQueueIntegrationStartsQueuedTaskImmediatelyWhenSlotIsAvailable(t *testing.T) {
	fixture := newQueueIntegrationFixture(t, 1, 1, queueIntegrationOptions{model: "gpt-5.6-terra"})
	out, err := fixture.submit.Execute(context.Background(), queueIntegrationInput(t, "immediate"))
	if err != nil || out.State != domain.StateQueued || out.QueuePosition != nil {
		t.Fatal("unexpected immediate submit result")
	}
	if len(fixture.admitter.records) != 1 || fixture.admitter.records[0].result.LaunchPayload == nil || len(fixture.starter.payloads) != 1 {
		t.Fatal("expected one immediate admission and starter payload")
	}
	payload := fixture.starter.payloads[0]
	if payload.Task.ID() != out.TaskID || payload.Task.State() != domain.StateQueued || fixture.queue.Len() != 0 || fixture.registry.Size() != 1 {
		t.Fatal("immediate admission did not preserve queued task state")
	}
	if snapshot, found := fixture.launching.Lookup(out.TaskID); !found || snapshot.TaskID != out.TaskID || snapshot.State != domain.StateQueued {
		t.Fatalf("launching snapshot=%#v found=%t", snapshot, found)
	}
	requireReservedTaskDir(t, fixture.tasksRoot, out.TaskID)
	requireQueuedOnly(t, out.Events)
	record := fixture.admitter.records[0]
	if record.input.TaskID != out.TaskID || record.result.LaunchPayload.Task.ID() != out.TaskID || !reflect.DeepEqual(record.result.LaunchPayload, &payload) {
		t.Fatal("recorded admission did not match starter payload")
	}
}

// SCN-proto-01-17.
func TestSubmitQueueIntegrationEnqueuesWhenSlotsAreFull(t *testing.T) {
	fixture := newQueueIntegrationFixture(t, 1, 1, queueIntegrationOptions{model: "gpt-5.6-terra"})
	active, err := domain.NewTaskID("review-20260809-120000-a1b2-active")
	if err != nil {
		t.Fatal(err)
	}
	fixture.registry.Add(active, domain.SubcommandImpl)
	out, err := fixture.submit.Execute(context.Background(), queueIntegrationInput(t, "queued"))
	if err != nil || out.QueuePosition == nil || *out.QueuePosition < 1 || len(fixture.starter.payloads) != 0 || fixture.queue.Len() != 1 {
		t.Fatal("expected queued admission")
	}
	record := fixture.admitter.records[0]
	if record.result.LaunchPayload != nil || record.result.QueuePosition == nil || *record.result.QueuePosition != *out.QueuePosition {
		t.Fatal("recorded queued admission result mismatch")
	}
	payload, found := fixture.queue.Dequeue()
	if !found || payload.Task.ID() != out.TaskID || payload.Model != record.input.Model || !reflect.DeepEqual(payload.ReasoningEffort, record.input.ReasoningEffort) || payload.PromptText != record.input.PromptText || payload.ResolvedTimeout.ResolvedSeconds() != record.input.ResolvedTimeout.ResolvedSeconds() || payload.SandboxMode != record.input.SandboxMode || payload.SourceWorkingDir != record.input.SourceWorkingDir {
		t.Fatal("dequeued payload did not match recorded admission input")
	}
	if fixture.registry.Size() != 1 {
		t.Fatal("queued task was incorrectly registered as active")
	}
	if _, found := fixture.launching.Lookup(out.TaskID); found {
		t.Fatal("queued task was incorrectly registered as launching")
	}
}

// SCN-proto-01-19.
func TestSubmitQueueIntegrationKeepsRepeatedSubmissionsDistinct(t *testing.T) {
	fixture := newQueueIntegrationFixture(t, 2, 2, queueIntegrationOptions{model: "gpt-5.6-terra"})
	first, err := fixture.submit.Execute(context.Background(), queueIntegrationInput(t, "repeat"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := fixture.submit.Execute(context.Background(), queueIntegrationInput(t, "repeat"))
	if err != nil || first.TaskID == second.TaskID || len(fixture.admitter.records) != 2 || len(fixture.starter.payloads) != 2 || fixture.registry.Size() != 2 || fixture.queue.Len() != 0 {
		t.Fatal("repeated submissions were not independently admitted")
	}
	requireReservedTaskDir(t, fixture.tasksRoot, first.TaskID)
	requireReservedTaskDir(t, fixture.tasksRoot, second.TaskID)
	for index, id := range []domain.TaskID{first.TaskID, second.TaskID} {
		if fixture.admitter.records[index].input.TaskID != id || fixture.admitter.records[index].result.LaunchPayload == nil || fixture.admitter.records[index].result.LaunchPayload.Task.ID() != fixture.starter.payloads[index].Task.ID() {
			t.Fatal("distinct admission payload mismatch")
		}
	}
}

// SCN-proto-01-22.
func TestSubmitQueueIntegrationCarriesResolvedModelToStarter(t *testing.T) {
	fixture := newQueueIntegrationFixture(t, 1, 1, queueIntegrationOptions{model: "gpt-5.6-sol"})
	out, err := fixture.submit.Execute(context.Background(), queueIntegrationInput(t, "model"))
	if err != nil || len(fixture.admitter.records) != 1 || len(fixture.starter.payloads) != 1 {
		t.Fatal("model integration submit failed")
	}
	record := fixture.admitter.records[0]
	if record.input.Model != "gpt-5.6-sol" || record.result.LaunchPayload == nil || record.result.LaunchPayload.Model != "gpt-5.6-sol" || fixture.starter.payloads[0].Model != "gpt-5.6-sol" || fixture.starter.payloads[0].Task.ID() != out.TaskID || fixture.starter.payloads[0].Task.State() != domain.StateQueued {
		t.Fatal("resolved model did not reach starter")
	}
}

// SCN-proto-01-23.
func TestSubmitQueueIntegrationCarriesResolvedReasoningEffortToStarter(t *testing.T) {
	t.Run("resolved", func(t *testing.T) {
		effort := "high"
		fixture := newQueueIntegrationFixture(t, 1, 1, queueIntegrationOptions{model: "gpt-5.6-terra", effort: &effort})
		_, err := fixture.submit.Execute(context.Background(), queueIntegrationInput(t, "effort"))
		if err != nil || len(fixture.admitter.records) != 1 || len(fixture.starter.payloads) != 1 {
			t.Fatal("reasoning effort integration submit failed")
		}
		record := fixture.admitter.records[0]
		if record.input.ReasoningEffort == nil || record.result.LaunchPayload == nil || record.result.LaunchPayload.ReasoningEffort == nil || fixture.starter.payloads[0].ReasoningEffort == nil || *record.input.ReasoningEffort != effort || *record.result.LaunchPayload.ReasoningEffort != effort || *fixture.starter.payloads[0].ReasoningEffort != effort {
			t.Fatal("resolved reasoning effort did not reach every stage")
		}
	})
	t.Run("unspecified", func(t *testing.T) {
		fixture := newQueueIntegrationFixture(t, 1, 1, queueIntegrationOptions{model: "gpt-5.6-terra"})
		_, err := fixture.submit.Execute(context.Background(), queueIntegrationInput(t, "no-effort"))
		if err != nil || fixture.admitter.records[0].input.ReasoningEffort != nil || fixture.admitter.records[0].result.LaunchPayload.ReasoningEffort != nil || fixture.starter.payloads[0].ReasoningEffort != nil {
			t.Fatal("unspecified reasoning effort was not preserved as nil")
		}
	})
}

func TestSubmitQueueIntegrationQueueFullIncludesConfiguredDepth(t *testing.T) {
	fixture := newQueueIntegrationFixture(t, 1, 1, queueIntegrationOptions{model: "gpt-5.6-terra"})
	active, err := domain.NewTaskID("review-20260809-120000-a1b2-active")
	if err != nil {
		t.Fatal(err)
	}
	fixture.registry.Add(active, domain.SubcommandImpl)
	const queueMaxDepth = 10
	for index := 0; index < queueMaxDepth; index++ {
		if _, err := fixture.submit.Execute(context.Background(), queueIntegrationInput(t, "full")); err != nil {
			t.Fatal(err)
		}
	}
	params := []byte(`{"subcommand":"review","slug":"overflow","prompt":"integration prompt","working_dir":"` + t.TempDir() + `"}`)
	response := fixture.submit.Handle(transport.Request{RequestID: "queue-full", Params: params})
	if response.OK || response.Error == nil || response.Error.Code != "QUEUE_FULL" || response.Error.MessageKey != "error.queue.full" || !reflect.DeepEqual(response.Error.Detail, map[string]any{"queue_max_depth": queueMaxDepth}) {
		t.Fatalf("response=%#v", response)
	}
	if len(fixture.starter.payloads) != 0 || fixture.queue.Len() != queueMaxDepth {
		t.Fatal("queue full submission started or changed the queue")
	}
}
