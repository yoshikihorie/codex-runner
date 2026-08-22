package usecase

import (
	"context"
	"fmt"
	"sync"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

// AdmitTaskUseCase decides whether a task reserves a slot immediately or waits in FIFO order.
type AdmitTaskUseCase struct {
	queue                  execution.TaskQueue
	registry               execution.ActiveTaskRegistry
	launching              execution.LaunchingTaskRegistry
	queueMu                *sync.Mutex
	maxConcurrentTasks     int
	maxConcurrentImplTasks int
	queueMaxDepth          int
}

func NewAdmitTaskUseCase(queue execution.TaskQueue, registry execution.ActiveTaskRegistry, launching execution.LaunchingTaskRegistry, queueMu *sync.Mutex, maxConcurrentTasks int, maxConcurrentImplTasks int, queueMaxDepth int) *AdmitTaskUseCase {
	if queue == nil || registry == nil || launching == nil || queueMu == nil {
		panic("admit task use case requires non-nil dependencies")
	}
	return &AdmitTaskUseCase{queue: queue, registry: registry, launching: launching, queueMu: queueMu, maxConcurrentTasks: maxConcurrentTasks, maxConcurrentImplTasks: maxConcurrentImplTasks, queueMaxDepth: queueMaxDepth}
}

func (u *AdmitTaskUseCase) Execute(_ context.Context, input execution.TaskAdmissionInput) (execution.TaskAdmissionResult, error) {
	if err := validateAdmissionInput(input); err != nil {
		return execution.TaskAdmissionResult{}, err
	}
	u.queueMu.Lock()
	defer u.queueMu.Unlock()
	globalAvailable := u.registry.Size() < u.maxConcurrentTasks
	implAvailable := input.Subcommand != domain.SubcommandImpl || u.registry.ImplSize() < u.maxConcurrentImplTasks
	fifoAvailable := u.queue.Len() == 0
	immediate := globalAvailable && implAvailable && fifoAvailable
	if !immediate && u.queue.Len() >= u.queueMaxDepth {
		return execution.TaskAdmissionResult{}, domain.ErrQueueFull
	}
	initialQueuePosition := u.queue.Len() + 1
	task, events, err := domain.NewTask(input.TaskID, input.Subcommand, input.Slug, input.RequestedTimeout, input.RequestedAt, initialQueuePosition)
	if err != nil {
		return execution.TaskAdmissionResult{}, err
	}
	payload := execution.TaskLaunchPayload{
		Task: task, Model: input.Model, ReasoningEffort: copyReasoningEffort(input.ReasoningEffort), PromptText: input.PromptText,
		NormalizedPaths: copyNormalizedPaths(input.NormalizedPaths), ResolvedTimeout: input.ResolvedTimeout,
		SandboxMode: input.SandboxMode, SourceWorkingDir: input.SourceWorkingDir,
	}
	if input.Subcommand != domain.SubcommandImpl {
		workingDir := input.SourceWorkingDir
		payload.WorkingDir = &workingDir
	}
	if immediate {
		snapshot, snapshotErr := domain.NewTaskSnapshotFromAdmission(task, input.ResolvedTimeout, input.Model, input.ReasoningEffort, domain.ExecutionRouteDaemon, task.RequestedAt())
		if snapshotErr != nil {
			return execution.TaskAdmissionResult{}, snapshotErr
		}
		u.registry.Add(task.ID(), input.Subcommand)
		u.launching.Register(task.ID(), snapshot)
		return execution.TaskAdmissionResult{State: domain.StateQueued, Events: events, LaunchPayload: &payload}, nil
	}
	position := u.queue.Enqueue(payload)
	return execution.TaskAdmissionResult{State: domain.StateQueued, QueuePosition: &position, Events: events}, nil
}

func copyReasoningEffort(value *string) *string {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func copyNormalizedPaths(paths []domain.NormalizedPath) []domain.NormalizedPath {
	if paths == nil {
		return nil
	}
	cloned := make([]domain.NormalizedPath, len(paths))
	copy(cloned, paths)
	return cloned
}

// Admit adapts the use case to the context-free transport boundary.
func (u *AdmitTaskUseCase) Admit(input execution.TaskAdmissionInput) (execution.TaskAdmissionResult, error) {
	return u.Execute(context.Background(), input)
}

// CompensateRejectedStart releases the immediate-admission reservation after
// the lifecycle shutdown gate has rejected its launch.
func (u *AdmitTaskUseCase) CompensateRejectedStart(taskID domain.TaskID) error {
	u.queueMu.Lock()
	defer u.queueMu.Unlock()
	u.launching.Unregister(taskID)
	u.registry.Remove(taskID)
	return nil
}

func validateAdmissionInput(input execution.TaskAdmissionInput) error {
	switch {
	case input.TaskID.String() == "":
		return fmt.Errorf("task admission input task ID is required")
	case input.Subcommand == "":
		return fmt.Errorf("task admission input subcommand is required")
	case input.Slug.String() == "":
		return fmt.Errorf("task admission input slug is required")
	case input.RequestedAt.IsZero():
		return fmt.Errorf("task admission input requested at is required")
	case input.Model == "":
		return fmt.Errorf("task admission input model is required")
	case input.PromptText == "":
		return fmt.Errorf("task admission input prompt text is required")
	case input.ResolvedTimeout.ResolvedSeconds() == 0:
		return fmt.Errorf("task admission input resolved timeout is required")
	case input.SandboxMode == "":
		return fmt.Errorf("task admission input sandbox mode is required")
	case input.SourceWorkingDir == "":
		return fmt.Errorf("task admission input source working directory is required")
	default:
		return nil
	}
}
