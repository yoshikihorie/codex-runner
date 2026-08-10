package usecase

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type recordStartingTaskStoreFake struct {
	store.TaskStore
	snapshot domain.TaskSnapshot
	saveErr  error
	saves    int
	trace    *[]string
}

func (f *recordStartingTaskStoreFake) Save(_ domain.TaskID, snapshot domain.TaskSnapshot) error {
	f.saves++
	f.snapshot = snapshot
	*f.trace = append(*f.trace, "save")
	return f.saveErr
}

type recordStartingContractWriterFake struct {
	contract.ContractWriter
	writePromptErr error
	prompts        int
	appendEvents   int
	trace          *[]string
}

func (f *recordStartingContractWriterFake) WritePrompt(_ domain.TaskID, _ []byte) error {
	f.prompts++
	*f.trace = append(*f.trace, "prompt")
	return f.writePromptErr
}

func (f *recordStartingContractWriterFake) AppendEvent(_ domain.TaskID, _ domain.Event) error {
	f.appendEvents++
	return nil
}

func recordStartingTask(t *testing.T) *domain.Task {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260810-120000-a1b2-record-starting")
	if err != nil {
		t.Fatal(err)
	}
	slug, err := domain.NewSlug("record-starting")
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := domain.NewTask(id, domain.SubcommandImpl, slug, nil, time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC), 1)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func recordStartingTimeout(t *testing.T, requested *int) domain.Timeout {
	t.Helper()
	timeout, err := domain.NewTimeout(requested, 1800)
	if err != nil {
		t.Fatal(err)
	}
	return timeout
}

func TestRecordTaskStartingUseCase_WritesPromptThenTaskJSONWithNilPID(t *testing.T) {
	trace := []string{}
	tasks := &recordStartingTaskStoreFake{trace: &trace}
	writer := &recordStartingContractWriterFake{trace: &trace}
	err := NewRecordTaskStartingUseCase(tasks, writer).Execute(context.Background(), recordStartingTask(t), recordStartingTimeout(t, nil), "gpt-5", nil, domain.ExecutionRouteDaemon, "prompt", time.Date(2026, time.August, 10, 12, 1, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(trace) != "[prompt save]" || tasks.snapshot.PID != nil || tasks.snapshot.ProcessStartedAt != nil || tasks.snapshot.State != domain.StateStarting || tasks.snapshot.Route != domain.ExecutionRouteDaemon || writer.appendEvents != 0 {
		t.Fatalf("trace=%v snapshot=%#v", trace, tasks.snapshot)
	}
}

func TestRecordTaskStartingUseCase_TaskJSONWriteFailureReturnsContractWriteFailed(t *testing.T) {
	trace := []string{}
	tasks := &recordStartingTaskStoreFake{trace: &trace, saveErr: errors.New("save")}
	writer := &recordStartingContractWriterFake{trace: &trace}
	task := recordStartingTask(t)
	err := NewRecordTaskStartingUseCase(tasks, writer).Execute(context.Background(), task, recordStartingTimeout(t, nil), "gpt-5", nil, domain.ExecutionRouteDaemon, "prompt", time.Now())
	if !errors.Is(err, domain.ErrContractWriteFailed) || task.State() != domain.StateStarting {
		t.Fatalf("err=%v state=%s", err, task.State())
	}
}

func TestRecordTaskStartingUseCase_NonQueuedTaskRejected(t *testing.T) {
	trace := []string{}
	tasks := &recordStartingTaskStoreFake{trace: &trace}
	writer := &recordStartingContractWriterFake{trace: &trace}
	task := recordStartingTask(t)
	_, _ = task.Start(recordStartingTimeout(t, nil), "gpt-5", time.Now())
	err := NewRecordTaskStartingUseCase(tasks, writer).Execute(context.Background(), task, recordStartingTimeout(t, nil), "gpt-5", nil, domain.ExecutionRouteDaemon, "prompt", time.Now())
	if !errors.Is(err, domain.ErrInvalidStateTransition) || writer.prompts != 0 || tasks.saves != 0 {
		t.Fatalf("err=%v", err)
	}
}

func TestRecordTaskStartingUseCase_OmitsRequestedTimeoutWhenNil(t *testing.T) {
	trace := []string{}
	tasks := &recordStartingTaskStoreFake{trace: &trace}
	writer := &recordStartingContractWriterFake{trace: &trace}
	if err := NewRecordTaskStartingUseCase(tasks, writer).Execute(context.Background(), recordStartingTask(t), recordStartingTimeout(t, nil), "gpt-5", nil, domain.ExecutionRouteDaemon, "prompt", time.Now()); err != nil || tasks.snapshot.RequestedTimeoutSeconds != nil {
		t.Fatalf("err=%v snapshot=%#v", err, tasks.snapshot)
	}
}

func TestRecordTaskStartingUseCase_SecondCallRejectedWithoutOverwrite(t *testing.T) {
	trace := []string{}
	tasks := &recordStartingTaskStoreFake{trace: &trace}
	writer := &recordStartingContractWriterFake{trace: &trace}
	task := recordStartingTask(t)
	uc := NewRecordTaskStartingUseCase(tasks, writer)
	if err := uc.Execute(context.Background(), task, recordStartingTimeout(t, nil), "gpt-5", nil, domain.ExecutionRouteDaemon, "prompt", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := uc.Execute(context.Background(), task, recordStartingTimeout(t, nil), "gpt-5", nil, domain.ExecutionRouteDaemon, "prompt", time.Now()); !errors.Is(err, domain.ErrInvalidStateTransition) || tasks.saves != 1 {
		t.Fatalf("err=%v saves=%d", err, tasks.saves)
	}
}

func TestRecordTaskStartingUseCase_PromptWriteFailureSkipsTaskJSON(t *testing.T) {
	trace := []string{}
	tasks := &recordStartingTaskStoreFake{trace: &trace}
	writer := &recordStartingContractWriterFake{trace: &trace, writePromptErr: fmt.Errorf("%w: prompt", domain.ErrContractWriteFailed)}
	err := NewRecordTaskStartingUseCase(tasks, writer).Execute(context.Background(), recordStartingTask(t), recordStartingTimeout(t, nil), "gpt-5", nil, domain.ExecutionRouteDaemon, "prompt", time.Now())
	if !errors.Is(err, domain.ErrContractWriteFailed) || tasks.saves != 0 {
		t.Fatalf("err=%v saves=%d", err, tasks.saves)
	}
}

func TestRecordTaskStartingUseCase_ReasoningEffortPersistedAsGiven(t *testing.T) {
	for _, reasoning := range []*string{func() *string { value := "high"; return &value }(), nil} {
		trace := []string{}
		tasks := &recordStartingTaskStoreFake{trace: &trace}
		writer := &recordStartingContractWriterFake{trace: &trace}
		if err := NewRecordTaskStartingUseCase(tasks, writer).Execute(context.Background(), recordStartingTask(t), recordStartingTimeout(t, nil), "gpt-5", reasoning, domain.ExecutionRouteDaemon, "prompt", time.Now()); err != nil || !sameStringPtr(tasks.snapshot.ReasoningEffort, reasoning) {
			t.Fatalf("err=%v got=%v want=%v", err, tasks.snapshot.ReasoningEffort, reasoning)
		}
	}
}

func TestRecordTaskStartingUseCase_RejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name          string
		task          *domain.Task
		model, prompt string
		route         domain.ExecutionRoute
		now           time.Time
	}{{"nil task", nil, "gpt-5", "prompt", domain.ExecutionRouteDaemon, time.Now()}, {"empty model", recordStartingTask(t), "", "prompt", domain.ExecutionRouteDaemon, time.Now()}, {"empty prompt", recordStartingTask(t), "gpt-5", "", domain.ExecutionRouteDaemon, time.Now()}, {"non daemon", recordStartingTask(t), "gpt-5", "prompt", domain.ExecutionRouteLegacy, time.Now()}, {"zero now", recordStartingTask(t), "gpt-5", "prompt", domain.ExecutionRouteDaemon, time.Time{}}} {
		t.Run(tc.name, func(t *testing.T) {
			trace := []string{}
			tasks := &recordStartingTaskStoreFake{trace: &trace}
			writer := &recordStartingContractWriterFake{trace: &trace}
			if NewRecordTaskStartingUseCase(tasks, writer).Execute(context.Background(), tc.task, recordStartingTimeout(t, nil), tc.model, nil, tc.route, tc.prompt, tc.now) == nil || writer.prompts != 0 || tasks.saves != 0 {
				t.Fatal("invalid input was accepted")
			}
		})
	}
}

func TestRecordTaskStartingUseCase_RejectsNilTask(t *testing.T) {
	assertRecordStartingInvalidInput(t, nil, "gpt-5", "prompt", domain.ExecutionRouteDaemon, time.Now())
}

func TestRecordTaskStartingUseCase_RejectsEmptyModel(t *testing.T) {
	assertRecordStartingInvalidInput(t, recordStartingTask(t), "", "prompt", domain.ExecutionRouteDaemon, time.Now())
}

func TestRecordTaskStartingUseCase_RejectsEmptyPromptText(t *testing.T) {
	assertRecordStartingInvalidInput(t, recordStartingTask(t), "gpt-5", "", domain.ExecutionRouteDaemon, time.Now())
}

func TestRecordTaskStartingUseCase_RejectsNonDaemonRoute(t *testing.T) {
	assertRecordStartingInvalidInput(t, recordStartingTask(t), "gpt-5", "prompt", domain.ExecutionRouteLegacy, time.Now())
}

func TestRecordTaskStartingUseCase_RejectsZeroNow(t *testing.T) {
	assertRecordStartingInvalidInput(t, recordStartingTask(t), "gpt-5", "prompt", domain.ExecutionRouteDaemon, time.Time{})
}

func TestRecordTaskStartingUseCase_RejectsZeroTimeout(t *testing.T) {
	trace := []string{}
	tasks := &recordStartingTaskStoreFake{trace: &trace}
	writer := &recordStartingContractWriterFake{trace: &trace}
	err := NewRecordTaskStartingUseCase(tasks, writer).Execute(context.Background(), recordStartingTask(t), domain.Timeout{}, "gpt-5", nil, domain.ExecutionRouteDaemon, "prompt", time.Now())
	if err == nil || writer.prompts != 0 || tasks.saves != 0 {
		t.Fatalf("err=%v prompts=%d saves=%d", err, writer.prompts, tasks.saves)
	}
}

func assertRecordStartingInvalidInput(t *testing.T, task *domain.Task, model, prompt string, route domain.ExecutionRoute, now time.Time) {
	t.Helper()
	trace := []string{}
	tasks := &recordStartingTaskStoreFake{trace: &trace}
	writer := &recordStartingContractWriterFake{trace: &trace}
	if NewRecordTaskStartingUseCase(tasks, writer).Execute(context.Background(), task, recordStartingTimeout(t, nil), model, nil, route, prompt, now) == nil || writer.prompts != 0 || tasks.saves != 0 {
		t.Fatal("invalid input was accepted")
	}
}

func sameStringPtr(got, want *string) bool {
	return got == nil && want == nil || got != nil && want != nil && *got == *want
}
