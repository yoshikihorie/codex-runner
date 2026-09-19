package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type launchFailureContractReader struct {
	store.ContractReader
	existing int
	exists   bool
	err      error
	calls    int
	trace    *[]string
}

func (f *launchFailureContractReader) ReadExitCode(domain.TaskID) (int, bool, error) {
	f.calls++
	appendLifecycleTrace(f.trace, "read-exit-code")
	return f.existing, f.exists, f.err
}

type launchFailureContractWriter struct {
	contract.ContractWriter
	writeErr    error
	writeCalls  int
	written     []domain.ExitCode
	appendCalls int
	trace       *[]string
}

func (f *launchFailureContractWriter) WriteExitCode(_ domain.TaskID, exitCode domain.ExitCode) error {
	f.writeCalls++
	f.written = append(f.written, exitCode)
	appendLifecycleTrace(f.trace, "write-exit-code")
	return f.writeErr
}

func (f *launchFailureContractWriter) AppendEvent(_ domain.TaskID, _ domain.Event) error {
	f.appendCalls++
	appendLifecycleTrace(f.trace, "append-event")
	return nil
}

type launchFailureFixture struct {
	input   FailTaskLaunchInput
	store   *lifecycleRecordingTaskStore
	locker  *lifecycleRecordingTaskLocker
	reader  *launchFailureContractReader
	writer  *launchFailureContractWriter
	slots   *lifecycleRecordingSlotReleaser
	paths   *lifecycleRecordingPathLockReleaser
	trace   []string
	useCase *FailTaskLaunchUseCase
}

func newLaunchFailureFixture(t *testing.T, existing int, exists bool, readErr, writeErr error) *launchFailureFixture {
	t.Helper()
	f := &launchFailureFixture{}
	task := lifecycleTask(t, domain.SubcommandImpl)
	f.store = &lifecycleRecordingTaskStore{loads: []lifecycleLoadResult{{err: domain.ErrTaskNotFound}}, trace: &f.trace, loadName: "load", saveName: "save"}
	f.locker = &lifecycleRecordingTaskLocker{trace: &f.trace}
	f.reader = &launchFailureContractReader{existing: existing, exists: exists, err: readErr, trace: &f.trace}
	f.writer = &launchFailureContractWriter{writeErr: writeErr, trace: &f.trace}
	f.slots = &lifecycleRecordingSlotReleaser{trace: &f.trace}
	f.paths = &lifecycleRecordingPathLockReleaser{trace: &f.trace}
	f.useCase = NewFailTaskLaunchUseCase(f.store, f.locker, f.writer, f.reader, f.slots, f.paths, &lifecycleRecordingClock{now: testLifecycleTime, trace: &f.trace})
	f.input = FailTaskLaunchInput{Task: task, ResolvedTimeout: lifecycleTimeout(t), Model: "gpt-5", SandboxMode: "workspace-write", OccurredAt: testLifecycleTime}
	return f
}

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
	if err := uc.Execute(context.Background(), FailTaskLaunchInput{Task: task, Model: "gpt-5", SandboxMode: "workspace-write", OccurredAt: testLifecycleTime}); err == nil {
		t.Fatal("zero timeout was accepted")
	}
	if err := uc.Execute(context.Background(), FailTaskLaunchInput{Task: task, ResolvedTimeout: lifecycleTimeout(t), OccurredAt: testLifecycleTime}); err == nil {
		t.Fatal("empty model was accepted")
	}
	if err := uc.Execute(context.Background(), FailTaskLaunchInput{Task: task, ResolvedTimeout: lifecycleTimeout(t), Model: "gpt-5", SandboxMode: "danger-full-access", OccurredAt: testLifecycleTime}); err == nil {
		t.Fatal("invalid sandbox mode was accepted")
	}
}
func TestFailTaskLaunchInputPreservesOptionalReasoningEffort(t *testing.T) {
	input := FailTaskLaunchInput{ReasoningEffort: nil}
	if input.ReasoningEffort != nil {
		t.Fatal("nil reasoning effort changed")
	}
}

func TestFailTaskLaunchUseCasePersistsResolvedWorkingDir(t *testing.T) {
	f := newLaunchFailureFixture(t, 0, false, nil, nil)
	workingDir := t.TempDir()
	f.input.WorkingDir = &workingDir
	if _, err := f.useCase.ExecuteLocked(context.Background(), f.input); err != nil {
		t.Fatal(err)
	}
	if len(f.store.saved) != 1 || f.store.saved[0].WorkingDir == nil || *f.store.saved[0].WorkingDir != workingDir || f.store.saved[0].WorkingDir == &workingDir {
		t.Fatalf("saved working dir = %v", f.store.saved)
	}
}

func TestFailTaskLaunchUseCaseDoesNotBackfillSchemaVersion2WorkingDir(t *testing.T) {
	f := newLaunchFailureFixture(t, 0, false, nil, nil)
	storedTask := lifecycleTask(t, domain.SubcommandImpl)
	stored := lifecycleSnapshot(t, storedTask, domain.StateStarting)
	stored.SchemaVersion = 2
	stored.WorkingDir = nil
	f.store.loads = []lifecycleLoadResult{{snapshot: stored}}
	workingDir := t.TempDir()
	f.input.WorkingDir = &workingDir
	if _, err := f.useCase.ExecuteLocked(context.Background(), f.input); err != nil {
		t.Fatal(err)
	}
	if len(f.store.saved) != 1 || f.store.saved[0].SchemaVersion != 2 || f.store.saved[0].WorkingDir != nil {
		t.Fatalf("schema version 2 snapshot was backfilled: %#v", f.store.saved)
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
			if err := uc.Execute(context.Background(), FailTaskLaunchInput{Task: inputTask, ResolvedTimeout: lifecycleTimeout(t), Model: "gpt-5", SandboxMode: "workspace-write", OccurredAt: testLifecycleTime}); err != nil {
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

	result, err := uc.ExecuteLocked(context.Background(), FailTaskLaunchInput{Task: task, ResolvedTimeout: lifecycleTimeout(t), Model: "gpt-5", SandboxMode: "workspace-write", OccurredAt: testLifecycleTime})
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
	result, err := uc.ExecuteLocked(context.Background(), FailTaskLaunchInput{Task: task, ResolvedTimeout: lifecycleTimeout(t), Model: "gpt-5", SandboxMode: "workspace-write", OccurredAt: testLifecycleTime})
	if err == nil || !result.Terminal || result.Impl {
		t.Fatalf("ExecuteLocked() = %+v, %v", result, err)
	}
	if slots.calls != 0 || paths.calls != 0 {
		t.Fatal("ExecuteLocked released resources after a persistence failure")
	}
}

func TestFailTaskLaunchUseCaseClassifiesExitCodeErrorsAndReleasesResources(t *testing.T) {
	readErr := errors.New("read exit-code")
	writeErr := errors.New("write exit-code")
	const mismatchExisting = 2
	expectedExitCode := domain.NewExitCode(1)
	cases := []struct {
		name                    string
		existing                int
		exists                  bool
		readErr                 error
		writeErr                error
		wantContractWriteFailed bool
		wantWriteCalls          int
	}{
		{name: "read-error", readErr: readErr},
		{name: "mismatch", existing: mismatchExisting, exists: true},
		{name: "write-error", writeErr: writeErr, wantContractWriteFailed: true, wantWriteCalls: 1},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertError := func(t *testing.T, err error) {
				t.Helper()
				if err == nil {
					t.Fatal("expected exit-code error")
				}
				if got := errors.Is(err, domain.ErrContractWriteFailed); got != tc.wantContractWriteFailed {
					t.Fatalf("errors.Is(ErrContractWriteFailed) = %t, want %t: %v", got, tc.wantContractWriteFailed, err)
				}
				switch tc.name {
				case "read-error":
					if !errors.Is(err, readErr) {
						t.Fatalf("read error was not retained: %v", err)
					}
				case "mismatch":
					existing, attempted, ok := contract.ExitCodeMismatch(err)
					if !ok || existing != mismatchExisting || attempted != expectedExitCode.Raw() {
						t.Fatalf("ExitCodeMismatch() = (%d, %d, %t)", existing, attempted, ok)
					}
				case "write-error":
					if errors.Is(err, writeErr) {
						t.Fatalf("write error unexpectedly remained in the error chain: %v", err)
					}
				}
			}

			assertContractCalls := func(t *testing.T, f *launchFailureFixture) {
				t.Helper()
				if f.reader.calls != 1 || f.writer.writeCalls != tc.wantWriteCalls {
					t.Fatalf("contract calls: read=%d write=%d", f.reader.calls, f.writer.writeCalls)
				}
				if f.store.saveCalls != 0 || f.writer.appendCalls != 0 {
					t.Fatalf("terminal persistence continued: save=%d append=%d trace=%v", f.store.saveCalls, f.writer.appendCalls, f.trace)
				}
				if tc.wantWriteCalls == 1 {
					if len(f.writer.written) != 1 || f.writer.written[0].Raw() != expectedExitCode.Raw() {
						t.Fatalf("written exit codes=%v", f.writer.written)
					}
				} else if len(f.writer.written) != 0 {
					t.Fatalf("fatal error attempted exit-code write: %v", f.writer.written)
				}
			}

			locked := newLaunchFailureFixture(t, tc.existing, tc.exists, tc.readErr, tc.writeErr)
			result, err := locked.useCase.ExecuteLocked(context.Background(), locked.input)
			assertError(t, err)
			if !result.Terminal || !result.Impl {
				t.Fatalf("ExecuteLocked() result = %+v", result)
			}
			assertContractCalls(t, locked)
			if locked.locker.lockCalls != 0 || locked.locker.unlockCalls != 0 || locked.paths.calls != 0 || locked.slots.calls != 0 {
				t.Fatalf("ExecuteLocked managed resources: trace=%v", locked.trace)
			}

			executed := newLaunchFailureFixture(t, tc.existing, tc.exists, tc.readErr, tc.writeErr)
			err = executed.useCase.Execute(context.Background(), executed.input)
			assertError(t, err)
			assertContractCalls(t, executed)
			if executed.locker.lockCalls != 1 || executed.locker.unlockCalls != 1 {
				t.Fatalf("task mutex calls: lock=%d unlock=%d trace=%v", executed.locker.lockCalls, executed.locker.unlockCalls, executed.trace)
			}
			if executed.paths.calls != 1 || executed.slots.calls != 1 {
				t.Fatalf("resource release calls: path=%d slot=%d trace=%v", executed.paths.calls, executed.slots.calls, executed.trace)
			}
			if !lifecycleTraceSubsequence(executed.trace, "task-unlock", "release-path-lock", "release-slot") {
				t.Fatalf("release order=%v", executed.trace)
			}
		})
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
	result, err := uc.ExecuteLocked(context.Background(), FailTaskLaunchInput{Task: task, ResolvedTimeout: lifecycleTimeout(t), Model: "gpt-5", SandboxMode: "workspace-write", OccurredAt: testLifecycleTime})
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
