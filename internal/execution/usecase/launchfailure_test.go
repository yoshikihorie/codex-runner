package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

func TestFailTaskLaunchUseCaseContract(t *testing.T) {
	var input FailTaskLaunchInput
	if input.Task != nil {
		t.Fatal("zero input unexpectedly has a task")
	}
}
func TestFailTaskLaunchUseCaseRejectsNilTaskAndZeroOccurredAtBeforeSideEffects(t *testing.T) {
	uc := &FailTaskLaunchUseCase{}
	if err := uc.Execute(context.Background(), FailTaskLaunchInput{OccurredAt: time.Now()}); err == nil {
		t.Fatal("nil task was accepted")
	}
	if err := uc.Execute(context.Background(), FailTaskLaunchInput{}); err == nil {
		t.Fatal("zero occurredAt was accepted")
	}
}

func TestFailTaskLaunchUseCaseRejectsInvalidTimeoutAndModelBeforeSideEffects(t *testing.T) {
	task := lifecycleTask(t, domain.SubcommandReview)
	uc := &FailTaskLaunchUseCase{}
	if err := uc.Execute(context.Background(), FailTaskLaunchInput{Task: task, Model: "gpt-5", OccurredAt: testLifecycleTime}); err == nil {
		t.Fatal("zero timeout was accepted")
	}
	if err := uc.Execute(context.Background(), FailTaskLaunchInput{Task: task, ResolvedTimeout: lifecycleTimeout(t), OccurredAt: testLifecycleTime}); err == nil {
		t.Fatal("empty model was accepted")
	}
}
func TestFailTaskLaunchInputPreservesOptionalReasoningEffort(t *testing.T) {
	input := FailTaskLaunchInput{ReasoningEffort: nil}
	if input.ReasoningEffort != nil {
		t.Fatal("nil reasoning effort changed")
	}
}

func TestFailTaskLaunchUseCaseTransitionsAndReleases(t *testing.T) {
	cases := []struct {
		name       string
		subcommand domain.Subcommand
		starting   bool
	}{{"queued-impl", domain.SubcommandImpl, false}, {"starting-impl", domain.SubcommandImpl, true}, {"queued-review", domain.SubcommandReview, false}, {"starting-review", domain.SubcommandReview, true}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trace := []string{}
			inputTask := lifecycleTask(t, tc.subcommand)
			storeFake := &lifecycleRecordingTaskStore{trace: &trace, loadName: "load", saveName: "save"}
			if tc.starting {
				stored := lifecycleTask(t, tc.subcommand)
				storeFake.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, stored, domain.StateStarting)}}
			} else {
				storeFake.loads = []lifecycleLoadResult{{err: domain.ErrTaskNotFound}}
			}
			locker := &lifecycleRecordingTaskLocker{trace: &trace}
			writer := &lifecycleRecordingContractWriter{trace: &trace}
			slots := &lifecycleRecordingSlotReleaser{trace: &trace}
			paths := &lifecycleRecordingPathLockReleaser{trace: &trace}
			clock := &lifecycleRecordingClock{now: testLifecycleTime, trace: &trace}
			uc := NewFailTaskLaunchUseCase(storeFake, locker, writer, &lifecycleRecordingContractReader{}, slots, paths, clock)
			if err := uc.Execute(context.Background(), FailTaskLaunchInput{Task: inputTask, ResolvedTimeout: lifecycleTimeout(t), Model: "gpt-5", OccurredAt: testLifecycleTime}); err != nil {
				t.Fatal(err)
			}
			if storeFake.saveCalls != 1 || storeFake.saved[0].State != domain.StateFailed || writer.appendCalls != 2 || locker.lockCalls != 1 || locker.unlockCalls != 1 || slots.calls != 1 || slots.nows[0] != testLifecycleTime {
				t.Fatalf("unexpected terminal side effects: trace=%v", trace)
			}
			if writer.events[0].Type() != "TaskExited" || writer.events[1].Type() != "TaskFailed" {
				t.Fatalf("events=%v", writer.events)
			}
			if tc.subcommand == domain.SubcommandImpl {
				if paths.calls != 1 {
					t.Fatal("impl did not release path lock")
				}
				if !lifecycleTraceSubsequence(trace, "task-unlock", "release-path-lock", "release-slot") {
					t.Fatalf("release order=%v", trace)
				}
			} else {
				if paths.calls != 0 {
					t.Fatal("non-impl released path lock")
				}
				if !lifecycleTraceSubsequence(trace, "task-unlock", "release-slot") {
					t.Fatalf("release order=%v", trace)
				}
			}
		})
	}
}

func TestFailTaskLaunchUseCaseExecuteLockedDoesNotManageResources(t *testing.T) {
	trace := []string{}
	task := lifecycleTask(t, domain.SubcommandImpl)
	storeFake := &lifecycleRecordingTaskStore{loads: []lifecycleLoadResult{{err: domain.ErrTaskNotFound}}, trace: &trace, loadName: "load", saveName: "save"}
	locker := &lifecycleRecordingTaskLocker{trace: &trace}
	writer := &lifecycleRecordingContractWriter{trace: &trace}
	slots := &lifecycleRecordingSlotReleaser{trace: &trace}
	paths := &lifecycleRecordingPathLockReleaser{trace: &trace}
	uc := NewFailTaskLaunchUseCase(storeFake, locker, writer, &lifecycleRecordingContractReader{}, slots, paths, &lifecycleRecordingClock{now: testLifecycleTime, trace: &trace})

	result, err := uc.ExecuteLocked(context.Background(), FailTaskLaunchInput{Task: task, ResolvedTimeout: lifecycleTimeout(t), Model: "gpt-5", OccurredAt: testLifecycleTime})
	if err != nil || !result.Terminal || !result.Impl {
		t.Fatalf("ExecuteLocked() = %+v, %v", result, err)
	}
	if locker.lockCalls != 0 || locker.unlockCalls != 0 || paths.calls != 0 || slots.calls != 0 || storeFake.saveCalls != 1 {
		t.Fatalf("ExecuteLocked managed resources: trace=%v", trace)
	}
	uc.ReleaseAfterFailure(context.Background(), task.ID(), result.Impl)
	if paths.calls != 1 || slots.calls != 1 || !lifecycleTraceSubsequence(trace, "release-path-lock", "release-slot") {
		t.Fatalf("ReleaseAfterFailure() side effects: trace=%v", trace)
	}
}

func TestFailTaskLaunchUseCaseExecuteLockedRetainsTerminalResultOnSaveFailure(t *testing.T) {
	task := lifecycleTask(t, domain.SubcommandReview)
	storeFake := &lifecycleRecordingTaskStore{loads: []lifecycleLoadResult{{err: domain.ErrTaskNotFound}}, saveErr: errors.New("save")}
	locker := &lifecycleRecordingTaskLocker{}
	slots := &lifecycleRecordingSlotReleaser{}
	paths := &lifecycleRecordingPathLockReleaser{}
	uc := NewFailTaskLaunchUseCase(storeFake, locker, &lifecycleRecordingContractWriter{}, &lifecycleRecordingContractReader{}, slots, paths, &lifecycleRecordingClock{now: testLifecycleTime})
	result, err := uc.ExecuteLocked(context.Background(), FailTaskLaunchInput{Task: task, ResolvedTimeout: lifecycleTimeout(t), Model: "gpt-5", OccurredAt: testLifecycleTime})
	if err == nil || !result.Terminal || result.Impl {
		t.Fatalf("ExecuteLocked() = %+v, %v", result, err)
	}
	if slots.calls != 0 || paths.calls != 0 {
		t.Fatal("ExecuteLocked released resources after a persistence failure")
	}
}

func TestFailTaskLaunchUseCaseExecuteLockedCompletesUnderCallerHeldRealTaskMutex(t *testing.T) {
	trace := []string{}
	task := lifecycleTask(t, domain.SubcommandImpl)
	storeFake := &lifecycleRecordingTaskStore{loads: []lifecycleLoadResult{{err: domain.ErrTaskNotFound}}, trace: &trace, loadName: "load", saveName: "save"}
	shared := store.NewTaskMutex()
	writer := &lifecycleRecordingContractWriter{trace: &trace}
	slots := &lifecycleRecordingSlotReleaser{trace: &trace}
	paths := &lifecycleRecordingPathLockReleaser{trace: &trace}
	uc := NewFailTaskLaunchUseCase(storeFake, shared, writer, &lifecycleRecordingContractReader{}, slots, paths, &lifecycleRecordingClock{now: testLifecycleTime, trace: &trace})

	shared.Lock(task.ID())
	contenderAcquired := make(chan struct{})
	go func() {
		shared.Lock(task.ID())
		close(contenderAcquired)
		shared.Unlock(task.ID())
	}()
	result, err := uc.ExecuteLocked(context.Background(), FailTaskLaunchInput{Task: task, ResolvedTimeout: lifecycleTimeout(t), Model: "gpt-5", OccurredAt: testLifecycleTime})
	if err != nil || !result.Terminal || !result.Impl {
		shared.Unlock(task.ID())
		t.Fatalf("ExecuteLocked() = %+v, %v", result, err)
	}
	if paths.calls != 0 || slots.calls != 0 {
		shared.Unlock(task.ID())
		t.Fatal("ExecuteLocked released resources while caller held task mutex")
	}
	shared.Unlock(task.ID())
	select {
	case <-contenderAcquired:
	case <-time.After(3 * time.Second):
		t.Fatal("competing task mutex holder did not complete")
	}
	uc.ReleaseAfterFailure(context.Background(), task.ID(), result.Impl)
	if paths.calls != 1 || slots.calls != 1 || !lifecycleTraceSubsequence(trace, "save", "release-path-lock", "release-slot") {
		t.Fatalf("release side effects=%v", trace)
	}
}

func lifecycleTraceSubsequence(trace []string, names ...string) bool {
	index := 0
	for _, entry := range trace {
		if index < len(names) && entry == names[index] {
			index++
		}
	}
	return index == len(names)
}
