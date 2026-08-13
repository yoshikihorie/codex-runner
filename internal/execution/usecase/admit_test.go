package usecase

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

func testAdmissionInput(t *testing.T, subcommand domain.Subcommand, suffix string) execution.TaskAdmissionInput {
	t.Helper()
	id, err := domain.NewTaskID(string(subcommand) + "-20260809-120000-a1b2-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	slug, err := domain.NewSlug(suffix)
	if err != nil {
		t.Fatal(err)
	}
	timeout, err := domain.NewTimeout(nil, 1800)
	if err != nil {
		t.Fatal(err)
	}
	return execution.TaskAdmissionInput{
		TaskID: id, Subcommand: subcommand, Slug: slug, RequestedAt: time.Now(), PromptText: "prompt", ResolvedTimeout: timeout,
		Model: "model", SandboxMode: "workspace-write", SourceWorkingDir: "/private/tmp/source",
	}
}

func TestAdmitTaskUseCaseImmediateAndQueuedResults(t *testing.T) {
	queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
	launching := execution.NewLaunchingTaskRegistry()
	useCase := NewAdmitTaskUseCase(queue, registry, launching, mutex, 1, 2)
	immediate, err := useCase.Execute(context.Background(), testAdmissionInput(t, domain.SubcommandReview, "immediate"))
	if err != nil || immediate.QueuePosition != nil || immediate.LaunchPayload == nil || immediate.LaunchPayload.WorkingDir == nil {
		t.Fatalf("immediate=%#v err=%v", immediate, err)
	}
	if snapshot, found := launching.Lookup(immediate.LaunchPayload.Task.ID()); !found || snapshot.State != domain.StateQueued || !snapshot.RequestedAt.Equal(immediate.LaunchPayload.Task.RequestedAt()) || !snapshot.StateUpdatedAt.Equal(immediate.LaunchPayload.Task.RequestedAt()) {
		t.Fatalf("snapshot=%#v found=%t", snapshot, found)
	}
	queued, err := useCase.Admit(testAdmissionInput(t, domain.SubcommandImpl, "queued"))
	if err != nil || queued.QueuePosition == nil || queued.LaunchPayload != nil || *queued.QueuePosition != 1 {
		t.Fatalf("queued=%#v err=%v", queued, err)
	}
}

func TestAdmitTaskUseCaseRejectsFullQueueBeforeTaskCreation(t *testing.T) {
	queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
	registry.Add(testAdmissionInput(t, domain.SubcommandImpl, "active").TaskID)
	queuedTask := testAdmissionInput(t, domain.SubcommandImpl, "waiting")
	first := NewAdmitTaskUseCase(queue, registry, execution.NewLaunchingTaskRegistry(), mutex, 1, 1)
	if _, err := first.Execute(context.Background(), queuedTask); err != nil {
		t.Fatal(err)
	}
	result, err := first.Execute(context.Background(), testAdmissionInput(t, domain.SubcommandImpl, "full"))
	if !errors.Is(err, domain.ErrQueueFull) || result.State != "" || result.QueuePosition != nil || len(result.Events) != 0 || result.LaunchPayload != nil {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestAdmitTaskUseCaseValidatesRequiredFields(t *testing.T) {
	input := testAdmissionInput(t, domain.SubcommandImpl, "invalid")
	input.Model = ""
	result, err := NewAdmitTaskUseCase(execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1, 1).Execute(context.Background(), input)
	if err == nil || result.State != "" || result.QueuePosition != nil || len(result.Events) != 0 || result.LaunchPayload != nil {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestAdmitTaskUseCaseValidatesEveryRequiredFieldWithoutMutation(t *testing.T) {
	for _, field := range []struct {
		name  string
		clear func(*execution.TaskAdmissionInput)
	}{
		{"task_id", func(in *execution.TaskAdmissionInput) { in.TaskID = domain.TaskID{} }},
		{"subcommand", func(in *execution.TaskAdmissionInput) { in.Subcommand = "" }},
		{"slug", func(in *execution.TaskAdmissionInput) { in.Slug = domain.Slug{} }},
		{"requested_at", func(in *execution.TaskAdmissionInput) { in.RequestedAt = time.Time{} }},
		{"model", func(in *execution.TaskAdmissionInput) { in.Model = "" }},
		{"prompt", func(in *execution.TaskAdmissionInput) { in.PromptText = "" }},
		{"timeout", func(in *execution.TaskAdmissionInput) { in.ResolvedTimeout = domain.Timeout{} }},
		{"sandbox", func(in *execution.TaskAdmissionInput) { in.SandboxMode = "" }},
		{"working_dir", func(in *execution.TaskAdmissionInput) { in.SourceWorkingDir = "" }},
	} {
		t.Run(field.name, func(t *testing.T) {
			queue, registry := execution.NewTaskQueue(), execution.NewActiveTaskRegistry()
			input := testAdmissionInput(t, domain.SubcommandImpl, "required-"+strings.ReplaceAll(field.name, "_", "-"))
			field.clear(&input)
			result, err := NewAdmitTaskUseCase(queue, registry, execution.NewLaunchingTaskRegistry(), &sync.Mutex{}, 1, 1).Execute(context.Background(), input)
			if err == nil || result.State != "" || result.QueuePosition != nil || len(result.Events) != 0 || result.LaunchPayload != nil || queue.Len() != 0 || registry.Size() != 0 {
				t.Fatalf("result=%#v err=%v queue=%d registry=%d", result, err, queue.Len(), registry.Size())
			}
		})
	}
}

func TestAdmitTaskUseCaseCopiesQueuedPayloadReferences(t *testing.T) {
	queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
	registry.Add(testAdmissionInput(t, domain.SubcommandImpl, "copy-active").TaskID)
	value := "high"
	paths := []domain.NormalizedPath{mustNormalizedPath(t, "/private/tmp/a"), mustNormalizedPath(t, "/private/tmp/b")}
	input := testAdmissionInput(t, domain.SubcommandReview, "copy-waiting")
	input.ReasoningEffort, input.NormalizedPaths = &value, paths
	result, err := NewAdmitTaskUseCase(queue, registry, execution.NewLaunchingTaskRegistry(), mutex, 1, 2).Execute(context.Background(), input)
	if err != nil || result.QueuePosition == nil {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	value = "low"
	paths[0] = mustNormalizedPath(t, "/private/tmp/changed")
	payload, found := queue.Dequeue()
	if !found || payload.ReasoningEffort == input.ReasoningEffort || *payload.ReasoningEffort != "high" || len(payload.NormalizedPaths) != 2 || payload.NormalizedPaths[0] == paths[0] || &payload.NormalizedPaths[0] == &paths[0] {
		t.Fatalf("payload=%#v input=%#v", payload, input)
	}
}

func TestAdmitTaskUseCaseEmitsOneQueuedEventAndHonorsCapacityBoundary(t *testing.T) {
	for _, tc := range []struct {
		name            string
		active, maximum int
		queued          bool
	}{
		{"at_limit", 1, 1, true},
		{"below_limit", 0, 1, false},
		{"overridden_limit", 6, 8, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
			suffix := strings.ReplaceAll(tc.name, "_", "-")
			for index := 0; index < tc.active; index++ {
				registry.Add(testAdmissionInput(t, domain.SubcommandImpl, suffix+string(rune('a'+index))).TaskID)
			}
			input := testAdmissionInput(t, domain.SubcommandReview, "capacity-"+suffix)
			result, err := NewAdmitTaskUseCase(queue, registry, execution.NewLaunchingTaskRegistry(), mutex, tc.maximum, 10).Execute(context.Background(), input)
			if err != nil || len(result.Events) != 1 {
				t.Fatalf("result=%#v err=%v", result, err)
			}
			event, ok := result.Events[0].(domain.TaskQueued)
			if !ok || event.TaskID != input.TaskID || event.Subcommand != input.Subcommand || event.Slug != input.Slug || !event.OccurredAt.Equal(input.RequestedAt) {
				t.Fatalf("event=%#v", result.Events)
			}
			if tc.queued != (result.QueuePosition != nil) || tc.queued != (result.LaunchPayload == nil) {
				t.Fatalf("result=%#v", result)
			}
		})
	}
}

func mustNormalizedPath(t *testing.T, value string) domain.NormalizedPath {
	t.Helper()
	path, err := domain.NewNormalizedPath(value)
	if err != nil {
		t.Fatal(err)
	}
	return path
}
