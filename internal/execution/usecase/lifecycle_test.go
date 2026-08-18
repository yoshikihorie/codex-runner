package usecase

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

var testLifecycleTime = time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

func appendLifecycleTrace(trace *[]string, name string) {
	if trace != nil {
		*trace = append(*trace, name)
	}
}

type lifecycleRecordingAcquireForChild struct {
	calls int
	paths []string
	file  *os.File
	err   error
	trace *[]string
}

func (f *lifecycleRecordingAcquireForChild) Acquire(path string) (*os.File, error) {
	f.calls++
	f.paths = append(f.paths, path)
	appendLifecycleTrace(f.trace, "acquire-for-child")
	return f.file, f.err
}

type lifecycleRecordingRecordStarting struct {
	calls int
	err   error
	trace *[]string
}

func (f *lifecycleRecordingRecordStarting) Execute(_ context.Context, _ *domain.Task, _ domain.Timeout, _ string, _ *string, _ domain.ExecutionRoute, _ string, _ time.Time) error {
	f.calls++
	appendLifecycleTrace(f.trace, "record-starting")
	return f.err
}

type lifecycleRecordingCreateWorktree struct {
	calls  int
	inputs []execution.CreateWorktreeInput
	output execution.CreateWorktreeOutput
	err    error
	trace  *[]string
}

func (f *lifecycleRecordingCreateWorktree) Execute(_ context.Context, input execution.CreateWorktreeInput) (execution.CreateWorktreeOutput, error) {
	f.calls++
	f.inputs = append(f.inputs, input)
	appendLifecycleTrace(f.trace, "create-worktree")
	return f.output, f.err
}

type lifecycleRecordingLauncher struct {
	calls     int
	contexts  []context.Context
	params    []execution.LaunchParams
	launched  *execution.LaunchedProcess
	err       error
	onExecute func(context.Context)
	trace     *[]string
}

func (f *lifecycleRecordingLauncher) Execute(ctx context.Context, p execution.LaunchParams) (*execution.LaunchedProcess, error) {
	f.calls++
	f.contexts = append(f.contexts, ctx)
	f.params = append(f.params, p)
	appendLifecycleTrace(f.trace, "launch")
	if f.onExecute != nil {
		f.onExecute(ctx)
	}
	return f.launched, f.err
}

type lifecycleRecordingRecordProcess struct {
	calls     int
	err       error
	onExecute func()
	trace     *[]string
}

func (f *lifecycleRecordingRecordProcess) Execute(_ context.Context, _ *domain.Task, _ *domain.ProcessHandle, _ time.Time) error {
	f.calls++
	appendLifecycleTrace(f.trace, "record-process")
	if f.onExecute != nil {
		f.onExecute()
	}
	return f.err
}

type lifecycleRecordingTimeoutArmer struct {
	calls       int
	taskIDs     []domain.TaskID
	deadlines   []time.Time
	seconds     []int
	generations []domain.LifecycleGeneration
	onArm       func()
	trace       *[]string
}

func (f *lifecycleRecordingTimeoutArmer) Arm(id domain.TaskID, deadline time.Time, seconds int, generation domain.LifecycleGeneration) {
	f.calls++
	f.taskIDs = append(f.taskIDs, id)
	f.deadlines = append(f.deadlines, deadline)
	f.seconds = append(f.seconds, seconds)
	f.generations = append(f.generations, generation)
	appendLifecycleTrace(f.trace, "timeout-arm")
	if f.onArm != nil {
		f.onArm()
	}
}

type lifecycleRecordingMonitor struct {
	calls   int
	taskIDs []domain.TaskID
	err     error
	trace   *[]string
}

func (f *lifecycleRecordingMonitor) Run(_ context.Context, id domain.TaskID, _ io.Reader) error {
	f.calls++
	f.taskIDs = append(f.taskIDs, id)
	appendLifecycleTrace(f.trace, "monitor")
	return f.err
}

type lifecycleFinalizeCall struct {
	taskID              domain.TaskID
	rawExitCode         int
	estimated           bool
	adoptedAfterRestart bool
	now                 time.Time
}
type lifecycleRecordingFinalizer struct {
	calls          []lifecycleFinalizeCall
	err            error
	prepareErr     error
	prepareCalls   int
	trace          *[]string
	releaseCalls   int
	prepared       lifecycleFinalizeCall
	prepareContext context.Context
	prepareBlocked chan struct{}
	allowPrepare   chan struct{}
}

func (f *lifecycleRecordingFinalizer) Prepare(ctx context.Context, in execution.FinalizeTaskInput) (execution.PreparedFinalizeTask, error) {
	f.prepareCalls++
	f.prepareContext = ctx
	f.prepared = lifecycleFinalizeCall{in.TaskID, in.RawExitCode, in.Estimated, in.AdoptedAfterRestart, in.OccurredAt}
	if f.prepareBlocked != nil {
		f.prepareBlocked <- struct{}{}
		<-f.allowPrepare
	}
	return execution.PreparedFinalizeTask{}, f.prepareErr
}

type lifecycleRecordingChangeNotifier struct {
	ch               chan struct{}
	subscribeCalls   int
	unsubscribeCalls int
	trace            *[]string
}

func (f *lifecycleRecordingChangeNotifier) Subscribe(domain.TaskID) (<-chan struct{}, func()) {
	f.subscribeCalls++
	return f.ch, func() {
		f.unsubscribeCalls++
	}
}

func (f *lifecycleRecordingFinalizer) ExecuteLocked(_ context.Context, _ execution.PreparedFinalizeTask) (execution.LockedFinalizeResult, error) {
	f.calls = append(f.calls, f.prepared)
	appendLifecycleTrace(f.trace, "finalize")
	return execution.LockedFinalizeResult{RecordExited: true}, f.err
}

func (f *lifecycleRecordingFinalizer) ReleaseAfterFinalization(_ context.Context, _ execution.LockedFinalizeResult, _ domain.TaskID) {
	f.releaseCalls++
	appendLifecycleTrace(f.trace, "release-after-finalization")
}

type lifecycleConfirmKilledCall struct {
	taskID      domain.TaskID
	rawExitCode int
	estimated   bool
	now         time.Time
}
type lifecycleRecordingKillConfirmer struct {
	calls              []lifecycleConfirmKilledCall
	wrapperCalls       int
	lockedCalls        int
	err                error
	lockedErr          error
	lockedResult       execution.LockedKillResult
	lockedResults      []execution.LockedKillResult
	lockedErrors       []error
	releaseCalls       int
	releaseWhileLocked func() bool
	releaseHook        func()
	trace              *[]string
}

func (f *lifecycleRecordingKillConfirmer) ConfirmKilled(_ context.Context, id domain.TaskID, raw int, estimated bool, now time.Time) error {
	f.wrapperCalls++
	f.calls = append(f.calls, lifecycleConfirmKilledCall{id, raw, estimated, now})
	appendLifecycleTrace(f.trace, "confirm-killed")
	return f.err
}
func (f *lifecycleRecordingKillConfirmer) ExecuteLocked(_ context.Context, in execution.ConfirmTaskKilledInput) (execution.LockedKillResult, error) {
	f.lockedCalls++
	f.calls = append(f.calls, lifecycleConfirmKilledCall{in.TaskID, in.RawExitCode, in.Estimated, in.OccurredAt})
	appendLifecycleTrace(f.trace, "confirm-killed-locked")
	if f.lockedCalls <= len(f.lockedResults) {
		var err error
		if f.lockedCalls <= len(f.lockedErrors) {
			err = f.lockedErrors[f.lockedCalls-1]
		}
		return f.lockedResults[f.lockedCalls-1], err
	}
	return f.lockedResult, f.lockedErr
}
func (f *lifecycleRecordingKillConfirmer) ReleaseAfterConfirmation(_ context.Context, _ execution.LockedKillResult, _ domain.TaskID) {
	f.releaseCalls++
	appendLifecycleTrace(f.trace, "release-after-confirmation")
	if f.releaseHook != nil {
		f.releaseHook()
	}
}

type lifecycleLoadResult struct {
	snapshot domain.TaskSnapshot
	err      error
}
type lifecycleRecordingTaskStore struct {
	store.TaskStore
	loadCalls int
	loadIDs   []domain.TaskID
	loads     []lifecycleLoadResult
	loadIndex int
	saveCalls int
	saved     []domain.TaskSnapshot
	saveErr   error
	trace     *[]string
	loadName  string
	saveName  string
	onLoad    func(int)
	onSave    func(int)
}

func (f *lifecycleRecordingTaskStore) Load(id domain.TaskID) (domain.TaskSnapshot, error) {
	f.loadCalls++
	if f.onLoad != nil {
		f.onLoad(f.loadCalls)
	}
	f.loadIDs = append(f.loadIDs, id)
	appendLifecycleTrace(f.trace, f.loadName)
	if f.loadIndex >= len(f.loads) {
		return domain.TaskSnapshot{}, domain.ErrTaskNotFound
	}
	r := f.loads[f.loadIndex]
	f.loadIndex++
	return r.snapshot, r.err
}
func (f *lifecycleRecordingTaskStore) Save(_ domain.TaskID, s domain.TaskSnapshot) error {
	f.saveCalls++
	if f.onSave != nil {
		f.onSave(f.saveCalls)
	}
	f.saved = append(f.saved, s)
	appendLifecycleTrace(f.trace, f.saveName)
	return f.saveErr
}

type lifecycleRecordingTaskLocker struct {
	lockCalls   int
	unlockCalls int
	trace       *[]string
}

func (f *lifecycleRecordingTaskLocker) Lock(domain.TaskID) {
	f.lockCalls++
	appendLifecycleTrace(f.trace, "task-lock")
}
func (f *lifecycleRecordingTaskLocker) Unlock(domain.TaskID) {
	f.unlockCalls++
	appendLifecycleTrace(f.trace, "task-unlock")
}

type lifecycleRecordingContractWriter struct {
	contract.ContractWriter
	appendCalls int
	exitCalls   int
	events      []domain.Event
	trace       *[]string
}

type lifecycleRecordingContractReader struct{}

func (lifecycleRecordingContractReader) ReadStderrLog(domain.TaskID) ([]byte, error) { return nil, nil }
func (lifecycleRecordingContractReader) ReadLastMessage(domain.TaskID) (bool, error) {
	return false, nil
}
func (lifecycleRecordingContractReader) ReadPromptContent(domain.TaskID) ([]byte, error) {
	return nil, nil
}
func (lifecycleRecordingContractReader) ReadLastMessageContent(domain.TaskID) ([]byte, error) {
	return nil, nil
}
func (lifecycleRecordingContractReader) ReadPartialOutputContent(domain.TaskID) ([]byte, error) {
	return nil, nil
}
func (lifecycleRecordingContractReader) ReadExitCode(domain.TaskID) (int, bool, error) {
	return 0, false, nil
}

func (f *lifecycleRecordingContractWriter) AppendEvent(_ domain.TaskID, e domain.Event) error {
	f.appendCalls++
	f.events = append(f.events, e)
	appendLifecycleTrace(f.trace, "append-event")
	return nil
}

func (f *lifecycleRecordingContractWriter) WriteExitCode(_ domain.TaskID, _ domain.ExitCode) error {
	f.exitCalls++
	appendLifecycleTrace(f.trace, "write-exit-code")
	return nil
}

type lifecycleRecordingSlotReleaser struct {
	calls   int
	taskIDs []domain.TaskID
	nows    []time.Time
	trace   *[]string
}

func (f *lifecycleRecordingSlotReleaser) ReleaseAndAdvance(_ context.Context, id domain.TaskID, now time.Time) {
	f.calls++
	f.taskIDs = append(f.taskIDs, id)
	f.nows = append(f.nows, now)
	appendLifecycleTrace(f.trace, "release-slot")
}

type lifecycleRecordingPathLockReleaser struct {
	calls int
	trace *[]string
}

func (f *lifecycleRecordingPathLockReleaser) Release(_ context.Context, _ domain.TaskID) error {
	f.calls++
	appendLifecycleTrace(f.trace, "release-path-lock")
	return nil
}

type lifecycleRecordingLivenessLock struct {
	calls        int
	paths        []string
	dead         bool
	err          error
	onTryAcquire func()
	trace        *[]string
	traceName    string
}

func (f *lifecycleRecordingLivenessLock) TryAcquire(path string) (bool, error) {
	f.calls++
	f.paths = append(f.paths, path)
	appendLifecycleTrace(f.trace, f.traceName)
	if f.onTryAcquire != nil {
		f.onTryAcquire()
	}
	return f.dead, f.err
}

type lifecycleRecordingTerminator struct {
	calls  int
	pids   []int
	graces []time.Duration
	err    error
	trace  *[]string
}

type lifecycleRecordingTerminationEnsurer struct {
	confirmCalls int
	sendCalls    int
	dead         bool
	err          error
	terminateErr error
	confirmErr   error
	authorities  []recovery.ProcessSignalAuthority
}

func (f *lifecycleRecordingTerminationEnsurer) Confirm(context.Context, domain.TaskID) (bool, error) {
	f.confirmCalls++
	return f.dead, f.err
}
func (f *lifecycleRecordingTerminationEnsurer) SendAndConfirm(_ context.Context, _ domain.TaskID, authority recovery.ProcessSignalAuthority, _ time.Duration) execution.TerminationAttemptResult {
	f.sendCalls++
	f.authorities = append(f.authorities, authority)
	terminateErr := f.terminateErr
	if terminateErr == nil {
		terminateErr = f.err
	}
	return execution.TerminationAttemptResult{Dead: f.dead, TerminateErr: terminateErr, ConfirmErr: f.confirmErr}
}

func (f *lifecycleRecordingTerminator) Terminate(pid int, grace time.Duration) error {
	f.calls++
	f.pids = append(f.pids, pid)
	f.graces = append(f.graces, grace)
	appendLifecycleTrace(f.trace, "terminate")
	return f.err
}

type lifecycleRecordingPendingRegistrar struct {
	calls           int
	taskIDs         []domain.TaskID
	dispositions    []recovery.PendingSendDisposition
	authorities     []*recovery.ProcessSignalAuthority
	registerErr     error
	claimCalls      int
	claimOutcomes   []recovery.ClaimOutcome
	completeCalls   int
	releaseCalls    int
	invalidateCalls int
	removeCalls     int
	trace           *[]string
	set             recovery.PendingReconciliationSet
}

var _ recovery.PendingRegistrar = (*lifecycleRecordingPendingRegistrar)(nil)

func (f *lifecycleRecordingPendingRegistrar) Register(id domain.TaskID, disposition recovery.PendingSendDisposition, authority *recovery.ProcessSignalAuthority) error {
	f.calls++
	f.taskIDs = append(f.taskIDs, id)
	f.dispositions = append(f.dispositions, disposition)
	f.authorities = append(f.authorities, nil)
	if authority != nil {
		copy := *authority
		f.authorities[len(f.authorities)-1] = &copy
	}
	appendLifecycleTrace(f.trace, "pending-register")
	if f.registerErr != nil {
		return f.registerErr
	}
	return f.set.Register(id, disposition, authority)
}
func (f *lifecycleRecordingPendingRegistrar) ClaimInitialSend(id domain.TaskID, authority recovery.ProcessSignalAuthority) (recovery.SendClaim, recovery.ClaimOutcome) {
	f.claimCalls++
	claim, outcome := f.set.ClaimInitialSend(id, authority)
	f.claimOutcomes = append(f.claimOutcomes, outcome)
	return claim, outcome
}

func (f *lifecycleRecordingPendingRegistrar) ClaimForSend(taskID domain.TaskID, authority recovery.ProcessSignalAuthority) (recovery.SendClaim, recovery.ClaimOutcome) {
	return f.set.ClaimForSend(taskID, authority)
}
func (f *lifecycleRecordingPendingRegistrar) CompleteSend(claim recovery.SendClaim) bool {
	f.completeCalls++
	return f.set.CompleteSend(claim)
}
func (f *lifecycleRecordingPendingRegistrar) ReleaseSend(claim recovery.SendClaim) bool {
	f.releaseCalls++
	return f.set.ReleaseSend(claim)
}
func (f *lifecycleRecordingPendingRegistrar) InvalidateSend(claim recovery.SendClaim) bool {
	f.invalidateCalls++
	return f.set.InvalidateSend(claim)
}
func (f *lifecycleRecordingPendingRegistrar) RemoveClaim(claim recovery.SendClaim) bool {
	f.removeCalls++
	return f.set.RemoveClaim(claim)
}

type lifecycleRecordingOwnership struct {
	acquireCalls int
	releaseCalls int
	acquired     bool
	trace        *[]string
}

type lifecycleRecordingLaunching struct {
	registerCalls   int
	unregisterCalls int
	trace           *[]string
}

func (f *lifecycleRecordingLaunching) Register(domain.TaskID, domain.TaskSnapshot) {
	f.registerCalls++
}
func (f *lifecycleRecordingLaunching) Unregister(domain.TaskID) {
	f.unregisterCalls++
	appendLifecycleTrace(f.trace, "launching-unregister")
}
func (f *lifecycleRecordingLaunching) Lookup(domain.TaskID) (domain.TaskSnapshot, bool) {
	return domain.TaskSnapshot{}, false
}

func (f *lifecycleRecordingOwnership) Acquire(domain.TaskID) (domain.LifecycleGeneration, func(), bool) {
	f.acquireCalls++
	appendLifecycleTrace(f.trace, "ownership-acquire")
	return 1, func() { f.releaseCalls++; appendLifecycleTrace(f.trace, "ownership-release") }, f.acquired
}
func (f *lifecycleRecordingOwnership) Current(domain.TaskID) (domain.LifecycleGeneration, bool) {
	return 1, f.acquired && f.releaseCalls == 0
}
func (f *lifecycleRecordingOwnership) WithCurrent(_ domain.TaskID, generation domain.LifecycleGeneration, action func() error) (bool, error) {
	if generation != 1 || !f.acquired || f.releaseCalls != 0 {
		return false, nil
	}
	return true, action()
}
func (f *lifecycleRecordingOwnership) IsOwned(domain.TaskID) bool {
	return f.acquired && f.releaseCalls == 0
}

type lifecycleRecordingStdoutOpener struct {
	calls int
	paths []string
	file  *os.File
	err   error
	trace *[]string
}

func (f *lifecycleRecordingStdoutOpener) Open(path string) (*os.File, error) {
	f.calls++
	f.paths = append(f.paths, path)
	appendLifecycleTrace(f.trace, "open-stdout")
	return f.file, f.err
}

type lifecycleRecordingClock struct {
	calls int
	now   time.Time
	step  time.Duration
	trace *[]string
}

func (f *lifecycleRecordingClock) Now() time.Time {
	f.calls++
	appendLifecycleTrace(f.trace, "clock-now")
	t := f.now
	f.now = f.now.Add(f.step)
	return t
}

type lifecycleRecordingWaiter struct {
	calls int
	raw   int
	err   error
	trace *[]string
}

func (f *lifecycleRecordingWaiter) Wait() (int, error) {
	f.calls++
	appendLifecycleTrace(f.trace, "wait")
	return f.raw, f.err
}

// lifecycleSynchronizedTaskStore is intentionally separate from the older
// recording store: these tests exercise real cross-goroutine mutex exclusion.
type lifecycleSynchronizedTaskStore struct {
	store.TaskStore
	mu             sync.Mutex
	snapshot       domain.TaskSnapshot
	loadSnapshots  []domain.TaskSnapshot
	loadCount      int
	blockLoad      int
	loadBlocked    chan struct{}
	allowLoad      chan struct{}
	loadEntered    chan struct{}
	saveStarted    chan struct{}
	allowFirstSave chan struct{}
	firstSave      bool
	trace          []string
}

func (s *lifecycleSynchronizedTaskStore) addTrace(name string) {
	s.mu.Lock()
	s.trace = append(s.trace, name)
	s.mu.Unlock()
}
func (s *lifecycleSynchronizedTaskStore) traceSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.trace...)
}
func (s *lifecycleSynchronizedTaskStore) Load(domain.TaskID) (domain.TaskSnapshot, error) {
	s.addTrace("load")
	select {
	case s.loadEntered <- struct{}{}:
	default:
	}
	s.mu.Lock()
	s.loadCount++
	loadCount := s.loadCount
	snapshot := s.snapshot
	if s.loadCount <= len(s.loadSnapshots) {
		snapshot = s.loadSnapshots[s.loadCount-1]
	}
	block := s.blockLoad == loadCount
	s.mu.Unlock()
	if block {
		s.loadBlocked <- struct{}{}
		<-s.allowLoad
	}
	return snapshot, nil
}
func (s *lifecycleSynchronizedTaskStore) Save(_ domain.TaskID, snapshot domain.TaskSnapshot) error {
	s.mu.Lock()
	first := !s.firstSave
	if first {
		s.firstSave = true
	}
	s.mu.Unlock()
	if first {
		s.addTrace("pid-save-started")
		s.saveStarted <- struct{}{}
		<-s.allowFirstSave
	}
	s.mu.Lock()
	s.snapshot = snapshot
	s.trace = append(s.trace, "save-complete")
	s.mu.Unlock()
	return nil
}

type lifecycleBlockingRecordProcess struct {
	tasks *lifecycleSynchronizedTaskStore
}

func (r *lifecycleBlockingRecordProcess) Execute(_ context.Context, task *domain.Task, handle *domain.ProcessHandle, now time.Time) error {
	snapshot, err := r.tasks.Load(task.ID())
	if err != nil {
		return err
	}
	restored, err := snapshot.Restore()
	if err != nil {
		return err
	}
	if _, err := restored.RecordProcessInfo(handle.PID, handle.ProcessStartedAt, now); err != nil {
		return err
	}
	updated, err := snapshot.WithTask(restored, now)
	if err != nil {
		return err
	}
	return r.tasks.Save(task.ID(), updated)
}

type lifecycleObservingTaskLocker struct {
	delegate *store.TaskMutex
	trace    *lifecycleSynchronizedTaskStore
	attempt  chan struct{}
}

type lifecycleReenteringPathLockReleaser struct {
	taskMu *store.TaskMutex
	id     domain.TaskID
	calls  int
}

func (r *lifecycleReenteringPathLockReleaser) Release(_ context.Context, _ domain.TaskID) error {
	r.taskMu.Lock(r.id)
	r.calls++
	r.taskMu.Unlock(r.id)
	return nil
}

type lifecycleReenteringSlotReleaser struct {
	taskMu *store.TaskMutex
	id     domain.TaskID
	calls  int
}

func (r *lifecycleReenteringSlotReleaser) ReleaseAndAdvance(_ context.Context, _ domain.TaskID, _ time.Time) {
	r.taskMu.Lock(r.id)
	r.calls++
	r.taskMu.Unlock(r.id)
}

func (l lifecycleObservingTaskLocker) Lock(id domain.TaskID) {
	l.trace.addTrace("task-lock-attempt")
	if l.attempt != nil {
		l.attempt <- struct{}{}
	}
	l.delegate.Lock(id)
	l.trace.addTrace("task-lock-acquired")
}
func (l lifecycleObservingTaskLocker) Unlock(id domain.TaskID) {
	l.delegate.Unlock(id)
	l.trace.addTrace("task-unlock")
}

func lifecycleTask(t *testing.T, subcommand domain.Subcommand) *domain.Task {
	t.Helper()
	id := testID(t, "lifecycle")
	slug, err := domain.NewSlug("lifecycle")
	if err != nil {
		t.Fatal(err)
	}
	task, _, err := domain.NewTask(id, subcommand, slug, nil, testLifecycleTime, 1)
	if err != nil {
		t.Fatal(err)
	}
	return task
}
func lifecycleTimeout(t *testing.T) domain.Timeout {
	t.Helper()
	timeout, err := domain.NewTimeout(nil, 1800)
	if err != nil {
		t.Fatal(err)
	}
	return timeout
}
func lifecycleSnapshot(t *testing.T, task *domain.Task, state domain.TaskState) domain.TaskSnapshot {
	return lifecycleSnapshotWithProcessStartedAt(t, task, state, testLifecycleTime)
}

func lifecycleSnapshotWithProcessStartedAt(t *testing.T, task *domain.Task, state domain.TaskState, processStartedAt time.Time) domain.TaskSnapshot {
	t.Helper()
	timeout := lifecycleTimeout(t)
	if _, err := task.Start(timeout, "gpt-5", testLifecycleTime); err != nil {
		t.Fatal(err)
	}
	// ConfirmTaskRunningUseCase restores this snapshot and then transitions it to
	// running. Record process information first so the resulting snapshot meets
	// the running-state invariant.
	if _, err := task.RecordProcessInfo(42, processStartedAt, testLifecycleTime); err != nil {
		t.Fatal(err)
	}
	snapshot, err := domain.NewInitialTaskSnapshot(domain.ExecutionRouteDaemon, nil).WithTask(task, testLifecycleTime)
	if err != nil {
		t.Fatal(err)
	}
	if state == domain.StateStarting {
		return snapshot
	}
	if err = task.ConfirmRunning(testLifecycleTime); err != nil {
		t.Fatal(err)
	}
	if state == domain.StateCancelling {
		if _, err = task.RequestCancel(false, testLifecycleTime); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err = snapshot.WithTask(task, testLifecycleTime)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

// lifecycleStartingSnapshotWithoutProcess preserves the launch-boundary
// precondition: RecordProcessInfo has not yet persisted a process identity.
func lifecycleStartingSnapshotWithoutProcess(t *testing.T, task *domain.Task) domain.TaskSnapshot {
	t.Helper()
	if _, err := task.Start(lifecycleTimeout(t), "gpt-5", testLifecycleTime); err != nil {
		t.Fatal(err)
	}
	snapshot, err := domain.NewInitialTaskSnapshot(domain.ExecutionRouteDaemon, nil).WithTask(task, testLifecycleTime)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

type lifecycleFixture struct {
	trace        []string
	input        TaskLifecycleInput
	acquire      *lifecycleRecordingAcquireForChild
	starting     *lifecycleRecordingRecordStarting
	worktree     *lifecycleRecordingCreateWorktree
	launch       *lifecycleRecordingLauncher
	process      *lifecycleRecordingRecordProcess
	timeout      *lifecycleRecordingTimeoutArmer
	monitor      *lifecycleRecordingMonitor
	finalizer    *lifecycleRecordingFinalizer
	changes      *lifecycleRecordingChangeNotifier
	killed       *lifecycleRecordingKillConfirmer
	tasks        *lifecycleRecordingTaskStore
	failStore    *lifecycleRecordingTaskStore
	failLocker   *lifecycleRecordingTaskLocker
	failWriter   *lifecycleRecordingContractWriter
	failSlots    *lifecycleRecordingSlotReleaser
	failLocks    *lifecycleRecordingPathLockReleaser
	confirmTasks *lifecycleRecordingTaskStore
	confirmLock  *lifecycleRecordingLivenessLock
	recordLock   *lifecycleRecordingLivenessLock
	taskMu       *lifecycleRecordingTaskLocker
	opener       *lifecycleRecordingStdoutOpener
	waiter       *lifecycleRecordingWaiter
	terminator   *lifecycleRecordingTerminator
	termination  *lifecycleRecordingTerminationEnsurer
	pending      *lifecycleRecordingPendingRegistrar
	ownership    *lifecycleRecordingOwnership
	launching    *lifecycleRecordingLaunching
	clock        *lifecycleRecordingClock
	orchestrator *TaskLifecycleOrchestrator
}

func newLifecycleFixture(t *testing.T) *lifecycleFixture {
	t.Helper()
	f := &lifecycleFixture{}
	file, err := os.Create(filepath.Join(t.TempDir(), "task.lock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	task := lifecycleTask(t, domain.SubcommandImpl)
	timeout := lifecycleTimeout(t)
	f.input = TaskLifecycleInput{TaskLaunchPayload: execution.TaskLaunchPayload{Task: task, Model: "gpt-5", PromptText: "prompt", ResolvedTimeout: timeout, SandboxMode: "workspace-write", SourceWorkingDir: "/private/tmp/source"}, TaskDirPath: "/private/tmp/task", Now: testLifecycleTime}
	f.acquire = &lifecycleRecordingAcquireForChild{file: file, trace: &f.trace}
	f.starting = &lifecycleRecordingRecordStarting{trace: &f.trace}
	f.worktree = &lifecycleRecordingCreateWorktree{output: execution.CreateWorktreeOutput{WorkingDir: "/private/tmp/worktree"}, trace: &f.trace}
	f.waiter = &lifecycleRecordingWaiter{raw: 23, trace: &f.trace}
	f.launch = &lifecycleRecordingLauncher{launched: &execution.LaunchedProcess{Handle: &domain.ProcessHandle{PID: 42, ProcessStartedAt: testLifecycleTime.Add(time.Minute)}, Waiter: f.waiter}, trace: &f.trace}
	f.process = &lifecycleRecordingRecordProcess{trace: &f.trace}
	f.timeout = &lifecycleRecordingTimeoutArmer{trace: &f.trace}
	f.monitor = &lifecycleRecordingMonitor{trace: &f.trace}
	f.finalizer = &lifecycleRecordingFinalizer{trace: &f.trace}
	f.changes = &lifecycleRecordingChangeNotifier{ch: make(chan struct{}, 1), trace: &f.trace}
	f.killed = &lifecycleRecordingKillConfirmer{lockedResult: execution.LockedKillResult{Confirmed: true}, trace: &f.trace}
	runningSnapshot := lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateRunning)
	f.tasks = &lifecycleRecordingTaskStore{loads: []lifecycleLoadResult{{snapshot: runningSnapshot}, {snapshot: runningSnapshot}, {snapshot: runningSnapshot}, {snapshot: runningSnapshot}}, trace: &f.trace, loadName: "load-final"}
	f.failStore = &lifecycleRecordingTaskStore{loads: []lifecycleLoadResult{{err: domain.ErrTaskNotFound}}, trace: &f.trace}
	f.failLocker = &lifecycleRecordingTaskLocker{trace: &f.trace}
	f.failWriter = &lifecycleRecordingContractWriter{trace: &f.trace}
	f.failSlots = &lifecycleRecordingSlotReleaser{trace: &f.trace}
	f.failLocks = &lifecycleRecordingPathLockReleaser{trace: &f.trace}
	f.taskMu = &lifecycleRecordingTaskLocker{trace: &f.trace}
	fail := NewFailTaskLaunchUseCase(f.failStore, f.taskMu, f.failWriter, &lifecycleRecordingContractReader{}, f.failSlots, f.failLocks, &lifecycleRecordingClock{now: testLifecycleTime, trace: &f.trace})
	f.confirmLock = &lifecycleRecordingLivenessLock{trace: &f.trace, traceName: "confirm-running"}
	f.confirmTasks = &lifecycleRecordingTaskStore{loads: []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateStarting)}}}
	confirm := NewConfirmTaskRunningUseCase(f.confirmTasks, &lifecycleRecordingTaskLocker{}, execution.NewCheckLivenessUseCase(f.confirmLock, func(domain.TaskID) string { return "/private/tmp/task.lock" }), &lifecycleRecordingContractWriter{})
	f.recordLock = &lifecycleRecordingLivenessLock{trace: &f.trace, traceName: "check-liveness"}
	stdout, err := os.Create(filepath.Join(t.TempDir(), "stdout.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stdout.Close() })
	f.opener = &lifecycleRecordingStdoutOpener{file: stdout, trace: &f.trace}
	f.terminator = &lifecycleRecordingTerminator{trace: &f.trace}
	f.termination = &lifecycleRecordingTerminationEnsurer{}
	f.pending = &lifecycleRecordingPendingRegistrar{trace: &f.trace}
	f.ownership = &lifecycleRecordingOwnership{acquired: true, trace: &f.trace}
	f.launching = &lifecycleRecordingLaunching{trace: &f.trace}
	f.clock = &lifecycleRecordingClock{now: testLifecycleTime.Add(2 * time.Hour), trace: &f.trace}
	deps := TaskLifecycleDependencies{AcquireForChild: f.acquire.Acquire, RecordStarting: f.starting, CreateWorktree: f.worktree, Launch: f.launch, RecordProcess: f.process, FailLaunch: fail, ConfirmRunning: confirm, CheckLiveness: execution.NewCheckLivenessUseCase(f.recordLock, func(domain.TaskID) string { return "/private/tmp/task.lock" }), TimeoutArmer: f.timeout, Monitor: f.monitor, Finalize: f.finalizer, ConfirmKilled: f.killed, Tasks: f.tasks, TaskMu: f.taskMu, Terminator: f.terminator, Termination: f.termination, Pending: f.pending, Ownership: f.ownership, Launching: f.launching, Changes: f.changes, OpenStdout: f.opener, Clock: f.clock}
	f.orchestrator, err = NewTaskLifecycleOrchestrator(deps, TaskLifecycleLaunchConfig{CodexBinaryPath: "/private/tmp/codex", PTYEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *lifecycleFixture) run() { f.orchestrator.Run(context.Background(), f.input) }

func TestTaskLifecycleLaunchConfigHasBinaryPath(t *testing.T) {
	if (TaskLifecycleLaunchConfig{}).CodexBinaryPath != "" {
		t.Fatal("zero config changed")
	}
}
func TestTaskLifecycleOrchestratorRejectsMissingOrRelativeBinaryPathBeforeDependencies(t *testing.T) {
	for _, path := range []string{"", "codex"} {
		if _, err := NewTaskLifecycleOrchestrator(TaskLifecycleDependencies{}, TaskLifecycleLaunchConfig{CodexBinaryPath: path}); err == nil {
			t.Fatalf("binary path %q was accepted", path)
		}
	}
}

func TestTaskLifecycleOrchestratorRejectsNilLaunchingRegistry(t *testing.T) {
	f := newLifecycleFixture(t)
	deps := f.orchestrator.deps
	deps.Launching = nil
	if _, err := NewTaskLifecycleOrchestrator(deps, f.orchestrator.launchConfig); err == nil {
		t.Fatal("nil launching registry was accepted")
	}
}

func TestTaskLifecycleOrchestratorRejectsNilTaskChangeNotifier(t *testing.T) {
	f := newLifecycleFixture(t)
	deps := f.orchestrator.deps
	deps.Changes = nil
	if _, err := NewTaskLifecycleOrchestrator(deps, f.orchestrator.launchConfig); err == nil {
		t.Fatal("nil task change notifier was accepted")
	}
}
func TestTaskLifecycleInputCarriesTaskScopedFields(t *testing.T) {
	input := TaskLifecycleInput{TaskDirPath: "/private/tmp/task", Now: testLifecycleTime}
	if input.TaskDirPath != "/private/tmp/task" || input.Now != testLifecycleTime {
		t.Fatalf("input=%+v", input)
	}
}

func TestTaskLifecycleRunImplSuccessOrdersLaunchAndFinalization(t *testing.T) {
	f := newLifecycleFixture(t)
	f.run()
	want := []string{"acquire-for-child", "record-starting", "create-worktree", "task-lock", "load-final", "task-unlock", "launch", "task-lock", "load-final", "record-process", "task-unlock", "confirm-running", "timeout-arm", "open-stdout", "monitor", "wait", "clock-now", "load-final", "task-lock", "load-final", "finalize", "task-unlock", "release-after-finalization", "launching-unregister"}
	if !reflect.DeepEqual(f.trace[1:len(f.trace)-1], want) {
		t.Fatalf("trace=%v", f.trace)
	}
	if f.ownership.acquireCalls != 1 || f.ownership.releaseCalls != 1 || f.launching.unregisterCalls != 1 || f.acquire.calls != 1 || f.starting.calls != 1 || f.worktree.calls != 1 || f.launch.calls != 1 || f.process.calls != 1 || f.confirmLock.calls != 1 || f.timeout.calls != 1 || f.monitor.calls != 1 || f.waiter.calls != 1 || len(f.finalizer.calls) != 1 || len(f.killed.calls) != 0 || f.pending.calls != 0 || f.terminator.calls != 0 || f.termination.confirmCalls != 0 || f.termination.sendCalls != 0 {
		t.Fatalf("unexpected call counts: %+v", f.trace)
	}
	p := f.launch.params[0]
	if p.WorkingDir != "/private/tmp/worktree" || !p.AllowResume || p.LivenessLockFile != f.acquire.file || p.CodexBinaryPath != "/private/tmp/codex" || !p.PTYEnabled {
		t.Fatalf("params=%+v", p)
	}
	if f.timeout.deadlines[0] != f.launch.launched.Handle.ProcessStartedAt.Add(1800*time.Second) || f.finalizer.calls[0].adoptedAfterRestart || f.finalizer.calls[0].now != f.clock.now {
		t.Fatalf("terminal values=%+v", f.finalizer.calls[0])
	}
}

func TestTaskLifecycleRunShutdownQuiescence(t *testing.T) {
	t.Run("pre-launch cancellation has no lifecycle side effects", func(t *testing.T) {
		f := newLifecycleFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		f.orchestrator.Run(ctx, f.input)

		if f.acquire.calls != 0 || f.starting.calls != 0 || f.launch.calls != 0 || f.tasks.loadCalls != 0 || f.tasks.saveCalls != 0 || f.failStore.loadCalls != 0 || f.failStore.saveCalls != 0 || f.killed.lockedCalls != 0 || f.terminator.calls != 0 || f.termination.confirmCalls != 0 || f.termination.sendCalls != 0 || f.pending.calls != 0 || f.pending.claimCalls != 0 || f.pending.completeCalls != 0 || f.pending.releaseCalls != 0 || f.pending.invalidateCalls != 0 || f.pending.removeCalls != 0 || f.waiter.calls != 0 || f.failSlots.calls != 0 {
			t.Fatalf("shutdown performed lifecycle side effects: trace=%v", f.trace)
		}
	})

	t.Run("launch receives detached context and stops before result evaluation", func(t *testing.T) {
		cases := []struct {
			name      string
			configure func(*lifecycleFixture)
		}{
			{"success", func(*lifecycleFixture) {}},
			{"launch error", func(f *lifecycleFixture) { f.launch.err = errors.New("launch") }},
			{"nil launch", func(f *lifecycleFixture) { f.launch.launched = nil }},
			{"nil handle", func(f *lifecycleFixture) { f.launch.launched.Handle = nil }},
			{"nil waiter", func(f *lifecycleFixture) { f.launch.launched.Waiter = nil }},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				f := newLifecycleFixture(t)
				ctx, cancel := context.WithCancel(context.Background())
				tc.configure(f)
				f.launch.onExecute = func(launchCtx context.Context) {
					if launchCtx.Err() != nil {
						t.Fatal("launch context inherited cancellation")
					}
					cancel()
				}

				f.orchestrator.Run(ctx, f.input)

				if ctx.Err() == nil || len(f.launch.contexts) != 1 || f.launch.contexts[0].Err() != nil || f.failStore.loadCalls != 0 || f.failStore.saveCalls != 0 || f.process.calls != 0 || f.terminator.calls != 0 || f.termination.confirmCalls != 0 || f.termination.sendCalls != 0 || f.killed.lockedCalls != 0 || f.pending.calls != 0 || f.pending.claimCalls != 0 || f.pending.completeCalls != 0 || f.pending.releaseCalls != 0 || f.pending.invalidateCalls != 0 || f.pending.removeCalls != 0 || f.waiter.calls != 0 {
					t.Fatalf("launch shutdown was not quiescent: trace=%v", f.trace)
				}
			})
		}
	})

	t.Run("record process return is checked after shutdown", func(t *testing.T) {
		cases := []struct {
			name      string
			configure func(*lifecycleFixture, context.CancelFunc)
		}{
			{"success", func(f *lifecycleFixture, cancel context.CancelFunc) { f.process.onExecute = cancel }},
			{"error", func(f *lifecycleFixture, cancel context.CancelFunc) {
				f.process.err = errors.New("record")
				f.process.onExecute = cancel
			}},
			{"cancelling", func(f *lifecycleFixture, cancel context.CancelFunc) {
				f.tasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateRunning)}, {snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)}}
				f.tasks.onLoad = func(call int) {
					if call == 2 {
						cancel()
					}
				}
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				f := newLifecycleFixture(t)
				ctx, cancel := context.WithCancel(context.Background())
				tc.configure(f, cancel)
				f.orchestrator.Run(ctx, f.input)
				if ctx.Err() == nil || f.confirmLock.calls != 0 || f.timeout.calls != 0 || f.monitor.calls != 0 || f.terminator.calls != 0 || f.termination.confirmCalls != 0 || f.termination.sendCalls != 0 || f.killed.lockedCalls != 0 || f.failStore.loadCalls != 0 || f.failStore.saveCalls != 0 || f.pending.calls != 0 || f.pending.claimCalls != 0 || f.waiter.calls != 0 {
					t.Fatalf("record-process shutdown was not quiescent: trace=%v", f.trace)
				}
			})
		}
	})

	t.Run("confirm running return is checked after shutdown", func(t *testing.T) {
		cases := []struct {
			name      string
			configure func(*lifecycleFixture, context.CancelFunc)
		}{
			{"success", func(f *lifecycleFixture, cancel context.CancelFunc) { f.confirmTasks.onSave = func(int) { cancel() } }},
			{"error", func(f *lifecycleFixture, cancel context.CancelFunc) {
				f.confirmLock.err = errors.New("liveness")
				f.confirmLock.onTryAcquire = cancel
			}},
			{"dead", func(f *lifecycleFixture, cancel context.CancelFunc) {
				f.confirmLock.dead = true
				f.confirmLock.onTryAcquire = cancel
			}},
			{"cancelling", func(f *lifecycleFixture, cancel context.CancelFunc) {
				f.confirmTasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)}}
				f.confirmTasks.onLoad = func(int) { cancel() }
			}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				f := newLifecycleFixture(t)
				ctx, cancel := context.WithCancel(context.Background())
				tc.configure(f, cancel)
				f.orchestrator.Run(ctx, f.input)
				if ctx.Err() == nil || f.timeout.calls != 0 || f.monitor.calls != 0 || f.terminator.calls != 0 || f.termination.confirmCalls != 0 || f.termination.sendCalls != 0 || f.killed.lockedCalls != 0 || f.pending.calls != 0 || f.pending.claimCalls != 0 || f.waiter.calls != 0 || len(f.finalizer.calls) != 0 {
					t.Fatalf("confirm-running shutdown was not quiescent: trace=%v", f.trace)
				}
			})
		}
	})

	t.Run("timeout arming checkpoint does not start monitoring", func(t *testing.T) {
		f := newLifecycleFixture(t)
		ctx, cancel := context.WithCancel(context.Background())
		f.timeout.onArm = cancel

		f.orchestrator.Run(ctx, f.input)

		if ctx.Err() == nil || f.timeout.calls != 1 || f.monitor.calls != 0 || f.terminator.calls != 0 || f.termination.confirmCalls != 0 || f.termination.sendCalls != 0 || f.killed.lockedCalls != 0 || f.pending.calls != 0 || f.pending.claimCalls != 0 || f.waiter.calls != 0 || len(f.finalizer.calls) != 0 {
			t.Fatalf("timeout shutdown was not quiescent: trace=%v", f.trace)
		}
	})
}
func TestTaskLifecycleRunRejectsDuplicateOwnershipBeforeLaunch(t *testing.T) {
	f := newLifecycleFixture(t)
	f.ownership.acquired = false
	f.run()
	if f.ownership.acquireCalls != 1 || f.ownership.releaseCalls != 0 || f.launching.unregisterCalls != 0 || f.acquire.calls != 0 || f.waiter.calls != 0 || f.failStore.saveCalls != 0 || len(f.finalizer.calls) != 0 || len(f.killed.calls) != 0 || f.pending.calls != 0 {
		t.Fatalf("unexpected duplicate ownership side effects")
	}
}
func TestTaskLifecycleRunLaunchPreparationFailuresFailAndStop(t *testing.T) {
	cases := []struct {
		name      string
		configure func(*lifecycleFixture)
	}{{"starting", func(f *lifecycleFixture) { f.starting.err = errors.New("starting") }}, {"worktree", func(f *lifecycleFixture) {
		f.worktree.err = errors.New("worktree")
		f.failStore.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateStarting)}}
	}}, {"launch", func(f *lifecycleFixture) {
		f.launch.err = errors.New("launch")
		f.failStore.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateStarting)}}
	}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFixture(t)
			tc.configure(f)
			f.run()
			if f.process.calls != 0 || f.confirmLock.calls != 0 || f.timeout.calls != 0 || f.monitor.calls != 0 || f.waiter.calls != 0 || len(f.finalizer.calls) != 0 || len(f.killed.calls) != 0 || f.failStore.saveCalls != 1 || f.failStore.saved[0].State != domain.StateFailed || f.failSlots.calls != 1 || f.launching.unregisterCalls != 1 || f.ownership.releaseCalls != 1 {
				t.Fatalf("failure handling mismatch: %+v", f.trace)
			}
		})
	}
}
func TestTaskLifecycleRunRecordProcessFailureHandlesLiveness(t *testing.T) {
	cases := []struct {
		name          string
		dead          bool
		livenessErr   error
		terminateErr  error
		invalidHandle bool
		wantFail      bool
		want          recovery.PendingSendDisposition
		wantAuthority bool
	}{
		{name: "dead", dead: true, wantFail: true},
		{name: "live after successful terminate", want: recovery.PendingSendSent},
		{name: "liveness error after successful terminate", livenessErr: errors.New("liveness"), want: recovery.PendingSendSent},
		{name: "terminate error with authority", terminateErr: errors.New("terminate"), want: recovery.PendingSendUnsent, wantAuthority: true},
		{name: "terminate error without authority", terminateErr: errors.New("terminate"), invalidHandle: true, want: recovery.PendingSendConfirmOnly},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFixture(t)
			f.process.err = errors.New("record")
			f.recordLock.dead = tc.dead
			f.recordLock.err = tc.livenessErr
			f.terminator.err = tc.terminateErr
			if tc.invalidHandle {
				f.launch.launched.Handle.ProcessStartedAt = time.Time{}
			}
			f.failStore.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateStarting)}}
			f.run()
			if f.terminator.calls != 1 || f.terminator.pids[0] != 42 || f.terminator.graces[0] != execution.TimeoutKillGrace || f.waiter.calls != 1 || f.confirmLock.calls != 0 || f.timeout.calls != 0 || f.opener.calls != 0 || f.monitor.calls != 0 || len(f.finalizer.calls) != 0 || len(f.killed.calls) != 0 {
				t.Fatalf("record failure calls=%+v", f.trace)
			}
			if !lifecycleTraceSubsequence(f.trace, "record-process", "terminate", "check-liveness") {
				t.Fatalf("record failure order=%v", f.trace)
			}
			if tc.wantFail {
				if f.failStore.saveCalls != 1 || f.failSlots.calls != 1 || f.pending.calls != 0 {
					t.Fatal("dead process was not failed")
				}
			} else if f.failStore.saveCalls != 0 || f.failSlots.calls != 0 || f.pending.calls != 1 || f.pending.dispositions[0] != tc.want {
				t.Fatal("live or unknown process was not pending")
			} else if tc.wantAuthority {
				got := f.pending.authorities[0]
				if got == nil || got.TaskID != f.input.Task.ID() || got.PID != 42 || got.ProcessStartedAt != f.launch.launched.Handle.ProcessStartedAt {
					t.Fatalf("authority=%+v", got)
				}
			} else if f.pending.authorities[0] != nil {
				t.Fatalf("authority=%+v", f.pending.authorities[0])
			}
		})
	}
}
func TestTaskLifecycleRunMonitorAndStdoutErrorsStillWait(t *testing.T) {
	for _, tc := range []struct {
		name   string
		stdout bool
	}{{"monitor", false}, {"stdout", true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFixture(t)
			if tc.stdout {
				f.opener.err = errors.New("open")
			} else {
				f.monitor.err = errors.New("monitor")
			}
			f.run()
			if f.waiter.calls != 1 || len(f.finalizer.calls) != 1 || f.finalizer.calls[0].rawExitCode != 23 || f.finalizer.calls[0].estimated {
				t.Fatalf("wait/finalize=%+v", f.finalizer.calls)
			}
			if tc.stdout {
				if f.monitor.calls != 0 || f.opener.paths[0] != filepath.Join(f.input.TaskDirPath, "stdout.log") {
					t.Fatal("stdout branch mismatch")
				}
			} else if f.monitor.calls != 1 {
				t.Fatal("monitor branch mismatch")
			}
		})
	}
}
func TestTaskLifecycleRunCancellingUsesKillConfirmation(t *testing.T) {
	f := newLifecycleFixture(t)
	f.tasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)}}
	f.run()
	if f.launch.calls != 0 || f.process.calls != 0 || f.waiter.calls != 0 || len(f.killed.calls) != 1 || len(f.finalizer.calls) != 0 || f.killed.calls[0].rawExitCode != 130 || !f.killed.calls[0].estimated || f.killed.calls[0].now != f.clock.now {
		t.Fatalf("kill=%+v", f.killed.calls)
	}
	if !lifecycleTraceSubsequence(f.trace, "task-lock", "load-final", "confirm-killed-locked", "task-unlock", "release-after-confirmation") {
		t.Fatalf("unexpected cancellation trace=%v", f.trace)
	}
}
func TestTaskLifecycleRunConvertsWaitErrorsToEstimatedExit(t *testing.T) {
	for _, tc := range []struct {
		name      string
		raw       int
		err       error
		wantRaw   int
		estimated bool
	}{{"success", 23, nil, 23, false}, {"error", 99, errors.New("wait"), 1, true}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFixture(t)
			f.waiter.raw, f.waiter.err = tc.raw, tc.err
			f.run()
			if f.waiter.calls != 1 || len(f.finalizer.calls) != 1 || len(f.killed.calls) != 0 {
				t.Fatalf("finalize=%+v", f.finalizer.calls)
			}
			got := f.finalizer.calls[0]
			if got.rawExitCode != tc.wantRaw || got.estimated != tc.estimated || got.adoptedAfterRestart {
				t.Fatalf("finalize=%+v", got)
			}
		})
	}
}

func TestTaskLifecycleStartingCancellationUsesEstimatedCancelledExitCode(t *testing.T) {
	f := newLifecycleFixture(t)
	cancelling := lifecycleSnapshotWithProcessStartedAt(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling, f.launch.launched.Handle.ProcessStartedAt)
	f.tasks.loads = []lifecycleLoadResult{{snapshot: cancelling}}
	f.termination.dead = true
	f.orchestrator.handleStartingCancellation(context.Background(), f.input.Task.ID(), f.launch.launched, true, domain.LifecycleGeneration(1))
	if f.pending.claimCalls != 1 || len(f.pending.claimOutcomes) != 1 || f.pending.claimOutcomes[0] != recovery.ClaimAcquired || f.pending.completeCalls != 1 || f.termination.sendCalls != 1 || f.termination.confirmCalls != 0 || f.waiter.calls != 1 || f.killed.lockedCalls != 1 || f.killed.wrapperCalls != 0 {
		t.Fatalf("unexpected cancellation handling: trace=%v", f.trace)
	}
	call := f.killed.calls[0]
	if call.rawExitCode != 130 || !call.estimated {
		t.Fatalf("kill confirmation=%+v", call)
	}
	if f.termination.sendCalls != 1 || f.termination.confirmCalls != 0 {
		t.Fatalf("termination calls send=%d confirm=%d", f.termination.sendCalls, f.termination.confirmCalls)
	}
}

func TestTaskLifecycleStartingCancellationCompletesClaimBySignalOutcome(t *testing.T) {
	cases := []struct {
		name           string
		dead           bool
		terminateErr   error
		confirmErr     error
		wantComplete   int
		wantRelease    int
		wantInvalidate int
	}{
		{name: "confirm error", confirmErr: errors.New("confirm"), wantComplete: 1},
		{name: "confirm live", wantComplete: 1},
		{name: "confirm dead unconfirmed", dead: true, wantComplete: 1},
		{name: "retryable send error", terminateErr: errors.New("send"), wantRelease: 1},
		{name: "authority invalid", terminateErr: recovery.ErrProcessSignalAuthorityInvalid, wantInvalidate: 1},
		{name: "send succeeds while live", wantComplete: 1},
		{name: "send succeeds dead unconfirmed", dead: true, wantComplete: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFixture(t)
			cancelling := lifecycleSnapshotWithProcessStartedAt(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling, f.launch.launched.Handle.ProcessStartedAt)
			f.tasks.loads = []lifecycleLoadResult{{snapshot: cancelling}}
			f.termination.dead, f.termination.terminateErr, f.termination.confirmErr = tc.dead, tc.terminateErr, tc.confirmErr
			f.killed.lockedResult = execution.LockedKillResult{}

			f.orchestrator.handleStartingCancellation(context.Background(), f.input.Task.ID(), f.launch.launched, true, domain.LifecycleGeneration(1))

			if f.pending.claimCalls != 1 || len(f.pending.claimOutcomes) != 1 || f.pending.claimOutcomes[0] != recovery.ClaimAcquired || f.pending.completeCalls != tc.wantComplete || f.pending.releaseCalls != tc.wantRelease || f.pending.invalidateCalls != tc.wantInvalidate || f.pending.removeCalls != 0 {
				t.Fatalf("pending=%+v", f.pending)
			}
			if f.termination.sendCalls != 1 || f.termination.confirmCalls != 0 {
				t.Fatalf("termination calls send=%d confirm=%d", f.termination.sendCalls, f.termination.confirmCalls)
			}
		})
	}
}

func TestTaskLifecycleCancellationConfirmationRegistersUnconfirmed(t *testing.T) {
	for _, confirmErr := range []error{nil, errors.New("confirm")} {
		for _, tc := range []struct {
			name        string
			disposition recovery.PendingSendDisposition
			invoke      func(*lifecycleFixture)
		}{
			{
				name:        "unlaunched",
				disposition: recovery.PendingSendConfirmOnly,
				invoke: func(f *lifecycleFixture) {
					f.tasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)}}
					if !f.orchestrator.confirmUnlaunchedCancellation(context.Background(), f.input.Task.ID()) {
						t.Fatal("cancellation was not detected")
					}
				},
			},
			{
				name:        "launch-failure",
				disposition: recovery.PendingSendConfirmOnly,
				invoke: func(f *lifecycleFixture) {
					f.tasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)}}
					f.orchestrator.fail(context.Background(), f.input)
				},
			},
			{
				name:        "starting",
				disposition: recovery.PendingSendSent,
				invoke: func(f *lifecycleFixture) {
					cancelling := lifecycleSnapshotWithProcessStartedAt(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling, f.launch.launched.Handle.ProcessStartedAt)
					f.tasks.loads = []lifecycleLoadResult{{snapshot: cancelling}}
					f.termination.dead = true
					f.orchestrator.handleStartingCancellation(context.Background(), f.input.Task.ID(), f.launch.launched, true, domain.LifecycleGeneration(1))
				},
			},
			{
				name:        "terminal",
				disposition: recovery.PendingSendSent,
				invoke: func(f *lifecycleFixture) {
					cancelling := lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)
					f.tasks.loads = []lifecycleLoadResult{{snapshot: cancelling}, {snapshot: cancelling}}
					f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)
				},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				f := newLifecycleFixture(t)
				f.killed.lockedResult = execution.LockedKillResult{}
				f.killed.lockedErr = confirmErr
				tc.invoke(f)
				if f.killed.lockedCalls != 1 || f.killed.releaseCalls != 0 {
					t.Fatalf("locked=%d release=%d pending=%+v", f.killed.lockedCalls, f.killed.releaseCalls, f.pending)
				}
				if tc.name == "starting" {
					if f.pending.claimCalls != 1 || f.pending.completeCalls != 1 || f.pending.calls != 0 {
						t.Fatalf("starting claim=%d complete=%d register=%d", f.pending.claimCalls, f.pending.completeCalls, f.pending.calls)
					}
				} else if f.pending.calls != 1 || f.pending.taskIDs[0] != f.input.Task.ID() || f.pending.dispositions[0] != tc.disposition || f.pending.authorities[0] != nil || !lifecycleTraceSubsequence(f.trace, "confirm-killed-locked", "task-unlock", "pending-register") {
					t.Fatalf("pending registration did not follow unlock: %v", f.trace)
				}
				if tc.name == "starting" && (f.killed.wrapperCalls != 0 || f.waiter.calls != 1) {
					t.Fatalf("wrapper=%d wait=%d", f.killed.wrapperCalls, f.waiter.calls)
				}
			})
		}
	}
}

func TestTaskLifecycleCancellationConfirmationReleasesConfirmedDespiteError(t *testing.T) {
	for _, tc := range []struct {
		name   string
		invoke func(*lifecycleFixture)
	}{
		{
			name: "unlaunched",
			invoke: func(f *lifecycleFixture) {
				f.tasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)}}
				f.orchestrator.confirmUnlaunchedCancellation(context.Background(), f.input.Task.ID())
			},
		},
		{
			name: "launch-failure",
			invoke: func(f *lifecycleFixture) {
				f.tasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)}}
				f.orchestrator.fail(context.Background(), f.input)
			},
		},
		{
			name: "starting",
			invoke: func(f *lifecycleFixture) {
				cancelling := lifecycleSnapshotWithProcessStartedAt(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling, f.launch.launched.Handle.ProcessStartedAt)
				f.tasks.loads = []lifecycleLoadResult{{snapshot: cancelling}}
				f.termination.dead = true
				f.orchestrator.handleStartingCancellation(context.Background(), f.input.Task.ID(), f.launch.launched, true, domain.LifecycleGeneration(1))
			},
		},
		{
			name: "terminal",
			invoke: func(f *lifecycleFixture) {
				cancelling := lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)
				f.tasks.loads = []lifecycleLoadResult{{snapshot: cancelling}, {snapshot: cancelling}}
				f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFixture(t)
			f.killed.lockedResult = execution.LockedKillResult{Confirmed: true, TerminalPersisted: false}
			f.killed.lockedErr = errors.New("persistence")
			tc.invoke(f)
			if f.killed.lockedCalls != 1 || f.killed.releaseCalls != 1 || f.pending.calls != 0 {
				t.Fatalf("locked=%d release=%d pending=%d", f.killed.lockedCalls, f.killed.releaseCalls, f.pending.calls)
			}
			if tc.name == "starting" && (f.pending.claimCalls != 1 || f.pending.completeCalls != 1) {
				t.Fatalf("starting claim=%d complete=%d", f.pending.claimCalls, f.pending.completeCalls)
			}
			if !lifecycleTraceSubsequence(f.trace, "confirm-killed-locked", "task-unlock", "release-after-confirmation") {
				t.Fatalf("release did not follow unlock: %v", f.trace)
			}
			if (tc.name == "starting" || tc.name == "terminal") && f.killed.wrapperCalls != 0 {
				t.Fatalf("wrapper calls=%d", f.killed.wrapperCalls)
			}
		})
	}
}

func TestTaskLifecycleCancellationConfirmationDoesNotRereleaseAfterConfirmedPersistenceError(t *testing.T) {
	f := newLifecycleFixture(t)
	cancelling := lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)
	f.tasks.loads = []lifecycleLoadResult{{snapshot: cancelling}, {snapshot: cancelling}, {snapshot: cancelling}, {snapshot: cancelling}}
	f.killed.lockedResults = []execution.LockedKillResult{{Confirmed: true}, {}}
	f.killed.lockedErrors = []error{errors.New("persistence"), errors.New("reevaluation")}

	f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)
	if f.killed.releaseCalls != 1 || f.pending.calls != 0 {
		t.Fatalf("first confirmation release=%d pending=%d", f.killed.releaseCalls, f.pending.calls)
	}
	f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)
	if f.killed.lockedCalls != 2 || f.killed.releaseCalls != 1 || f.pending.calls != 1 || f.pending.dispositions[0] != recovery.PendingSendSent || f.pending.authorities[0] != nil {
		t.Fatalf("locked=%d release=%d pending=%+v", f.killed.lockedCalls, f.killed.releaseCalls, f.pending)
	}
}

func TestTaskLifecycleFailReleasesConfirmedCancellationDespiteError(t *testing.T) {
	f := newLifecycleFixture(t)
	f.tasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)}}
	f.killed.lockedResult = execution.LockedKillResult{Confirmed: true}
	f.killed.lockedErr = errors.New("contract")
	f.orchestrator.fail(context.Background(), f.input)
	if f.killed.releaseCalls != 1 || !lifecycleTraceSubsequence(f.trace, "confirm-killed-locked", "task-unlock", "release-after-confirmation") {
		t.Fatalf("confirmed cancellation was not released after unlock: trace=%v", f.trace)
	}
}

func TestTaskLifecycleFailDoesNotReleaseUnconfirmedCancellation(t *testing.T) {
	for _, err := range []error{nil, errors.New("confirm")} {
		f := newLifecycleFixture(t)
		f.tasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)}}
		f.killed.lockedResult = execution.LockedKillResult{}
		f.killed.lockedErr = err
		f.orchestrator.fail(context.Background(), f.input)
		if f.killed.releaseCalls != 0 || f.pending.calls != 1 || f.pending.dispositions[0] != recovery.PendingSendConfirmOnly || f.pending.authorities[0] != nil {
			t.Fatalf("unconfirmed cancellation release=%d pending=%+v error=%v", f.killed.releaseCalls, f.pending, err)
		}
	}
}

func TestTaskLifecycleFailKeepsTaskMutexUntilFailureTransitionAndThenReleases(t *testing.T) {
	f := newLifecycleFixture(t)
	f.tasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateStarting)}}
	f.failStore.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateStarting)}}
	f.failStore.loadName = "load"
	f.failStore.saveName = "save"
	f.orchestrator.fail(context.Background(), f.input)
	if f.taskMu.lockCalls != 1 || f.taskMu.unlockCalls != 1 || f.failStore.saveCalls != 1 || f.failSlots.calls != 1 {
		t.Fatalf("failure transition was not completed in one task mutex section: trace=%v", f.trace)
	}
	if !lifecycleTraceSubsequence(f.trace, "task-lock", "load-final", "load", "save", "task-unlock", "release-path-lock", "release-slot") {
		t.Fatalf("failure release order=%v", f.trace)
	}
}

func TestTaskLifecycleRecordProcessAtLaunchBoundarySerializesCancelWithRealTaskMutex(t *testing.T) {
	f := newLifecycleFixture(t)
	task := lifecycleTask(t, domain.SubcommandImpl)
	snapshot := lifecycleStartingSnapshotWithoutProcess(t, task)
	tasks := &lifecycleSynchronizedTaskStore{
		snapshot:       snapshot,
		loadEntered:    make(chan struct{}, 2),
		saveStarted:    make(chan struct{}, 1),
		allowFirstSave: make(chan struct{}),
	}
	shared := store.NewTaskMutex()
	locker := lifecycleObservingTaskLocker{delegate: shared, trace: tasks, attempt: make(chan struct{}, 2)}
	f.orchestrator.deps.Tasks = tasks
	f.orchestrator.deps.TaskMu = locker
	f.orchestrator.deps.RecordProcess = &lifecycleBlockingRecordProcess{tasks: tasks}
	f.input.Task = task
	launched := &execution.LaunchedProcess{Handle: &domain.ProcessHandle{PID: 42, ProcessStartedAt: testLifecycleTime.Add(time.Minute)}}
	recorded := make(chan error, 1)
	go func() {
		_, err := f.orchestrator.recordProcessAtLaunchBoundary(context.Background(), f.input, launched)
		recorded <- err
	}()
	<-tasks.saveStarted
	<-locker.attempt // launch-boundary lock acquisition

	cancelled := make(chan error, 1)
	go func() {
		locker.Lock(task.ID())
		defer locker.Unlock(task.ID())
		snapshot, err := tasks.Load(task.ID())
		if err != nil {
			cancelled <- err
			return
		}
		restored, err := snapshot.Restore()
		if err != nil {
			cancelled <- err
			return
		}
		if _, err = restored.RequestCancel(false, testLifecycleTime.Add(2*time.Minute)); err != nil {
			cancelled <- err
			return
		}
		updated, err := snapshot.WithTask(restored, testLifecycleTime.Add(2*time.Minute))
		if err == nil {
			err = tasks.Save(task.ID(), updated)
		}
		cancelled <- err
	}()

	// The contender announces its lock attempt before entering the real mutex.
	<-locker.attempt
	close(tasks.allowFirstSave)
	select {
	case err := <-recorded:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("record process did not complete")
	}
	select {
	case err := <-cancelled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not complete")
	}
	final, err := tasks.Load(task.ID())
	if err != nil || final.State != domain.StateCancelling || final.PID == nil || *final.PID != 42 || final.ProcessStartedAt == nil {
		t.Fatalf("final snapshot=%+v err=%v", final, err)
	}
	trace := tasks.traceSnapshot()
	if !lifecycleTraceSubsequence(trace, "pid-save-started", "task-lock-attempt", "save-complete", "task-unlock", "task-lock-acquired") {
		t.Fatalf("PID persistence and cancel were not serialized: %v", trace)
	}
}

func TestTaskLifecycleConfirmRunningErrorWaitsAndConfirmsExactlyOneTerminalBranch(t *testing.T) {
	for _, state := range []domain.TaskState{domain.StateRunning, domain.StateCancelling} {
		t.Run(string(state), func(t *testing.T) {
			f := newLifecycleFixture(t)
			f.confirmLock.err = errors.New("liveness")
			// Run loads the lifecycle snapshot before launch, at the process
			// recording boundary, and twice when choosing the terminal branch.
			// Keep cancellation until the third and fourth loads so this test exercises the
			// ConfirmRunning error path rather than launch-time cancellation.
			startingSnapshot := lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateStarting)
			f.tasks.loads = []lifecycleLoadResult{
				{snapshot: startingSnapshot},
				{snapshot: startingSnapshot},
				{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), state)},
				{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), state)},
			}
			f.run()
			if f.waiter.calls != 1 {
				t.Fatalf("Wait() calls=%d", f.waiter.calls)
			}
			if state == domain.StateCancelling {
				if len(f.killed.calls) != 1 || len(f.finalizer.calls) != 0 {
					t.Fatalf("terminal calls kill=%d finalize=%d", len(f.killed.calls), len(f.finalizer.calls))
				}
			} else if len(f.finalizer.calls) != 1 || len(f.killed.calls) != 0 {
				t.Fatalf("terminal calls finalize=%d kill=%d", len(f.finalizer.calls), len(f.killed.calls))
			}
		})
	}
}

func TestTaskLifecycleConfirmRunningDeadRegistersPendingBeforeWaiting(t *testing.T) {
	f := newLifecycleFixture(t)
	f.confirmLock.dead = true

	f.run()

	if f.pending.calls != 1 {
		t.Fatalf("Pending.Register calls=%d", f.pending.calls)
	}
	if len(f.pending.taskIDs) != 1 || f.pending.taskIDs[0] != f.input.Task.ID() {
		t.Fatalf("Pending.Register task IDs=%v want %s", f.pending.taskIDs, f.input.Task.ID())
	}
	if len(f.pending.dispositions) != 1 || f.pending.dispositions[0] != recovery.PendingSendConfirmOnly {
		t.Fatalf("Pending.Register dispositions=%v want %v", f.pending.dispositions, recovery.PendingSendConfirmOnly)
	}
	if len(f.pending.authorities) != 1 || f.pending.authorities[0] != nil {
		t.Fatalf("Pending.Register authorities=%v want [nil]", f.pending.authorities)
	}
	if !lifecycleTraceSubsequence(f.trace, "pending-register", "wait") {
		t.Fatalf("Pending.Register did not precede Wait(): trace=%v", f.trace)
	}
	if f.waiter.calls != 1 || len(f.finalizer.calls) != 0 || len(f.killed.calls) != 0 || f.timeout.calls != 0 || f.monitor.calls != 0 || f.termination.confirmCalls != 0 || f.termination.sendCalls != 0 || f.terminator.calls != 0 {
		t.Fatalf("dead confirmation continued lifecycle: trace=%v", f.trace)
	}
}

func TestTaskLifecycleConfirmRunningDeadWaitsWhenPendingRegistrationFails(t *testing.T) {
	f := newLifecycleFixture(t)
	f.confirmLock.dead = true
	f.pending.registerErr = errors.New("pending registration failed")
	var logs bytes.Buffer
	f.orchestrator.logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))

	f.run()

	if f.pending.calls != 1 {
		t.Fatalf("Pending.Register calls=%d", f.pending.calls)
	}
	if f.waiter.calls != 1 {
		t.Fatalf("Wait() calls=%d", f.waiter.calls)
	}
	if !lifecycleTraceSubsequence(f.trace, "pending-register", "wait") {
		t.Fatalf("Pending.Register did not precede Wait(): trace=%v", f.trace)
	}
	if len(f.finalizer.calls) != 0 || len(f.killed.calls) != 0 || f.timeout.calls != 0 || f.monitor.calls != 0 || f.termination.confirmCalls != 0 || f.termination.sendCalls != 0 || f.terminator.calls != 0 {
		t.Fatalf("dead confirmation continued lifecycle: trace=%v", f.trace)
	}
	logOutput := logs.String()
	if strings.Count(logOutput, "register pending lifecycle reconciliation") != 1 {
		t.Fatalf("pending registration warning count=%d logs=%q", strings.Count(logOutput, "register pending lifecycle reconciliation"), logOutput)
	}
	if !strings.Contains(logOutput, f.input.Task.ID().String()) || !strings.Contains(logOutput, f.pending.registerErr.Error()) {
		t.Fatalf("pending registration warning missing task ID or error: logs=%q", logOutput)
	}
}

// RED-05: a cancellation committed while Prepare is blocked must confirm the
// kill without waiting for terminal artifact reading to return.
func TestTaskLifecycleRunTerminalRaceCancelWinsRoutesOnlyConfirmKilled(t *testing.T) {
	f := newLifecycleFixture(t)
	running := lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateRunning)
	tasks := &lifecycleSynchronizedTaskStore{
		snapshot:       running,
		loadEntered:    make(chan struct{}, 2),
		saveStarted:    make(chan struct{}, 1),
		allowFirstSave: make(chan struct{}),
	}
	close(tasks.allowFirstSave)
	shared := store.NewTaskMutex()
	f.orchestrator.deps.Tasks = tasks
	f.orchestrator.deps.TaskMu = shared
	f.finalizer.prepareBlocked = make(chan struct{}, 1)
	f.finalizer.allowPrepare = make(chan struct{})
	defer close(f.finalizer.allowPrepare)

	confirmed := make(chan struct{})
	go func() {
		f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)
		close(confirmed)
	}()
	<-f.finalizer.prepareBlocked
	shared.Lock(f.input.Task.ID())
	snapshot, err := tasks.Load(f.input.Task.ID())
	if err != nil {
		shared.Unlock(f.input.Task.ID())
		t.Fatal(err)
	}
	task, err := snapshot.Restore()
	if err != nil {
		shared.Unlock(f.input.Task.ID())
		t.Fatal(err)
	}
	if _, err = task.RequestCancel(false, testLifecycleTime); err != nil {
		shared.Unlock(f.input.Task.ID())
		t.Fatal(err)
	}
	updated, err := snapshot.WithTask(task, testLifecycleTime)
	if err != nil {
		shared.Unlock(f.input.Task.ID())
		t.Fatal(err)
	}
	if err = tasks.Save(f.input.Task.ID(), updated); err != nil {
		shared.Unlock(f.input.Task.ID())
		t.Fatal(err)
	}
	shared.Unlock(f.input.Task.ID())
	f.changes.ch <- struct{}{}
	select {
	case <-confirmed:
	case <-time.After(3 * time.Second):
		t.Fatal("terminal cancellation waited for blocked Prepare")
	}

	if f.killed.lockedCalls != 1 || len(f.finalizer.calls) != 0 {
		t.Fatalf("ConfirmKilled=%d Finalize=%d", f.killed.lockedCalls, len(f.finalizer.calls))
	}
	if f.changes.subscribeCalls != 1 || f.changes.unsubscribeCalls != 1 {
		t.Fatalf("subscribe=%d unsubscribe=%d", f.changes.subscribeCalls, f.changes.unsubscribeCalls)
	}
	select {
	case <-f.finalizer.prepareContext.Done():
	default:
		t.Fatal("Prepare context was not cancelled after cancellation notification")
	}
}

func TestTaskLifecycleConfirmTerminalCancellingSkipsPrepare(t *testing.T) {
	f := newLifecycleFixture(t)
	cancelling := lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)
	f.tasks.loads = []lifecycleLoadResult{{snapshot: cancelling}, {snapshot: cancelling}}

	f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)

	if f.finalizer.prepareCalls != 0 || f.killed.lockedCalls != 1 || len(f.finalizer.calls) != 0 {
		t.Fatalf("Prepare=%d ConfirmKilled=%d Finalize=%d", f.finalizer.prepareCalls, f.killed.lockedCalls, len(f.finalizer.calls))
	}
	if f.taskMu.lockCalls != f.taskMu.unlockCalls {
		t.Fatalf("task mutex lock=%d unlock=%d", f.taskMu.lockCalls, f.taskMu.unlockCalls)
	}
}

func TestTaskLifecycleConfirmTerminalNonCancellingPreparesOnce(t *testing.T) {
	f := newLifecycleFixture(t)
	running := lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateRunning)
	f.tasks.loads = []lifecycleLoadResult{{snapshot: running}, {snapshot: running}}

	f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)

	if f.finalizer.prepareCalls != 1 || len(f.finalizer.calls) != 1 || f.killed.lockedCalls != 0 {
		t.Fatalf("Prepare=%d Finalize=%d ConfirmKilled=%d", f.finalizer.prepareCalls, len(f.finalizer.calls), f.killed.lockedCalls)
	}
	if f.taskMu.lockCalls != f.taskMu.unlockCalls {
		t.Fatalf("task mutex lock=%d unlock=%d", f.taskMu.lockCalls, f.taskMu.unlockCalls)
	}
}

func TestTaskLifecycleConfirmTerminalPrepareRaceRoutesOnlyConfirmKilled(t *testing.T) {
	f := newLifecycleFixture(t)
	f.tasks.loads = []lifecycleLoadResult{
		{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateRunning)},
		{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)},
	}

	f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)

	if f.finalizer.prepareCalls != 1 || f.killed.lockedCalls != 1 || len(f.finalizer.calls) != 0 {
		t.Fatalf("Prepare=%d ConfirmKilled=%d Finalize=%d", f.finalizer.prepareCalls, f.killed.lockedCalls, len(f.finalizer.calls))
	}
	if f.taskMu.lockCalls != f.taskMu.unlockCalls {
		t.Fatalf("task mutex lock=%d unlock=%d", f.taskMu.lockCalls, f.taskMu.unlockCalls)
	}
}

func TestTaskLifecycleConfirmTerminalCancellingUsesClockReacquiredUnderLock(t *testing.T) {
	f := newLifecycleFixture(t)
	f.clock.step = time.Second
	base := f.clock.now
	f.tasks.loads = []lifecycleLoadResult{
		{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateRunning)},
		{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)},
	}

	f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)

	if f.finalizer.prepareCalls != 1 || f.killed.lockedCalls != 1 || len(f.finalizer.calls) != 0 {
		t.Fatalf("Prepare=%d ConfirmKilled=%d Finalize=%d", f.finalizer.prepareCalls, f.killed.lockedCalls, len(f.finalizer.calls))
	}
	if f.finalizer.prepared.now != base {
		t.Fatalf("Prepare occurredAt=%v want %v", f.finalizer.prepared.now, base)
	}
	if got := f.killed.calls[0].now; got != base.Add(time.Second) {
		t.Fatalf("ConfirmKilled occurredAt=%v want %v (must be re-acquired under lock)", got, base.Add(time.Second))
	}
	if f.taskMu.lockCalls != f.taskMu.unlockCalls {
		t.Fatalf("task mutex lock=%d unlock=%d", f.taskMu.lockCalls, f.taskMu.unlockCalls)
	}
}

func TestTaskLifecycleConfirmTerminalCancellingPreReadTerminalAuthoritativeLoadReturnsWithoutFinalization(t *testing.T) {
	f := newLifecycleFixture(t)
	task := lifecycleTask(t, domain.SubcommandImpl)
	timeout := lifecycleTimeout(t)
	if _, err := task.Start(timeout, "gpt-5", testLifecycleTime); err != nil {
		t.Fatal(err)
	}
	if _, err := task.RecordProcessInfo(42, testLifecycleTime, testLifecycleTime); err != nil {
		t.Fatal(err)
	}
	if err := task.ConfirmRunning(testLifecycleTime); err != nil {
		t.Fatal(err)
	}
	if _, err := task.RequestCancel(false, testLifecycleTime); err != nil {
		t.Fatal(err)
	}
	if _, err := task.ConfirmKilled(domain.NewExitCode(130), true, testLifecycleTime); err != nil {
		t.Fatal(err)
	}
	terminal, err := domain.NewInitialTaskSnapshot(domain.ExecutionRouteDaemon, nil).WithTask(task, testLifecycleTime)
	if err != nil {
		t.Fatal(err)
	}
	cancelling := lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)
	f.tasks.loads = []lifecycleLoadResult{{snapshot: cancelling}, {snapshot: terminal}}

	f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)

	if f.finalizer.prepareCalls != 0 || f.killed.lockedCalls != 0 || len(f.finalizer.calls) != 0 || f.finalizer.releaseCalls != 0 || f.pending.calls != 0 {
		t.Fatalf("Prepare=%d ConfirmKilled=%d Finalize=%d release=%d pending=%d", f.finalizer.prepareCalls, f.killed.lockedCalls, len(f.finalizer.calls), f.finalizer.releaseCalls, f.pending.calls)
	}
	if f.taskMu.lockCalls != f.taskMu.unlockCalls {
		t.Fatalf("task mutex lock=%d unlock=%d", f.taskMu.lockCalls, f.taskMu.unlockCalls)
	}
}

func TestTaskLifecycleConfirmTerminalDoesNotFallbackAfterSelectedBranchError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state domain.TaskState
		err   error
	}{
		{"running-invalid", domain.StateRunning, domain.ErrInvalidStateTransition},
		{"running-other", domain.StateRunning, errors.New("finalize")},
		{"cancelling-invalid", domain.StateCancelling, domain.ErrInvalidStateTransition},
		{"cancelling-other", domain.StateCancelling, errors.New("kill")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFixture(t)
			snapshot := lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), tc.state)
			f.tasks.loads = []lifecycleLoadResult{{snapshot: snapshot}, {snapshot: snapshot}}
			f.finalizer.err, f.killed.lockedErr = tc.err, tc.err
			f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)
			if tc.state == domain.StateCancelling {
				if f.killed.lockedCalls != 1 || f.killed.wrapperCalls != 0 || len(f.finalizer.calls) != 0 {
					t.Fatalf("locked=%d wrapper=%d finalize=%d", f.killed.lockedCalls, f.killed.wrapperCalls, len(f.finalizer.calls))
				}
			} else if len(f.finalizer.calls) != 1 || len(f.killed.calls) != 0 {
				t.Fatalf("finalize=%d kill=%d", len(f.finalizer.calls), len(f.killed.calls))
			}
			if f.taskMu.lockCalls != f.taskMu.unlockCalls {
				t.Fatalf("task mutex lock=%d unlock=%d", f.taskMu.lockCalls, f.taskMu.unlockCalls)
			}
		})
	}
}

func TestTaskLifecycleConfirmTerminalPrepareErrorInRaceStillConfirmsCancelling(t *testing.T) {
	for _, tc := range []struct {
		name           string
		result         execution.LockedKillResult
		raw            int
		waitErr        error
		wantRaw        int
		wantEstimated  bool
		wantRelease    int
		wantPending    int
		traceAfterLock string
	}{
		{name: "confirmed", result: execution.LockedKillResult{Confirmed: true}, raw: 23, wantRaw: 23, wantRelease: 1, traceAfterLock: "release-after-confirmation"},
		{name: "unconfirmed-estimated", result: execution.LockedKillResult{}, raw: 99, waitErr: errors.New("wait"), wantRaw: 1, wantEstimated: true, wantPending: 1, traceAfterLock: "pending-register"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFixture(t)
			f.finalizer.prepareErr = errors.New("prepare")
			f.tasks.loads = []lifecycleLoadResult{
				{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateRunning)},
				{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)},
			}
			f.killed.lockedResult = tc.result

			f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), tc.raw, tc.waitErr)

			if f.killed.lockedCalls != 1 || len(f.finalizer.calls) != 0 {
				t.Fatalf("locked=%d finalize=%d", f.killed.lockedCalls, len(f.finalizer.calls))
			}
			if f.finalizer.prepareCalls != 1 {
				t.Fatalf("Prepare() calls=%d", f.finalizer.prepareCalls)
			}
			got := f.killed.calls[0]
			if got.taskID != f.input.Task.ID() || got.rawExitCode != tc.wantRaw || got.estimated != tc.wantEstimated || got.now != f.clock.now {
				t.Fatalf("kill confirmation=%+v", got)
			}
			if f.killed.releaseCalls != tc.wantRelease || f.pending.calls != tc.wantPending {
				t.Fatalf("release=%d pending=%d", f.killed.releaseCalls, f.pending.calls)
			}
			if tc.wantPending == 1 && (f.pending.dispositions[0] != recovery.PendingSendSent || f.pending.authorities[0] != nil) {
				t.Fatal("pending registration was not sent")
			}
			if !lifecycleTraceSubsequence(f.trace, "confirm-killed-locked", "task-unlock", tc.traceAfterLock) {
				t.Fatalf("post-unlock handling trace=%v", f.trace)
			}
			if f.taskMu.lockCalls != f.taskMu.unlockCalls {
				t.Fatalf("task mutex lock=%d unlock=%d", f.taskMu.lockCalls, f.taskMu.unlockCalls)
			}
		})
	}
}

func TestTaskLifecycleConfirmTerminalPrepareErrorUnlocksNonCancelling(t *testing.T) {
	f := newLifecycleFixture(t)
	f.finalizer.prepareErr = errors.New("prepare")
	running := lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateRunning)
	f.tasks.loads = []lifecycleLoadResult{{snapshot: running}, {snapshot: running}}

	f.orchestrator.confirmTerminal(context.Background(), f.input.Task.ID(), 23, nil)

	if f.finalizer.prepareCalls != 1 || len(f.finalizer.calls) != 0 || f.killed.lockedCalls != 0 {
		t.Fatalf("Prepare=%d finalize=%d kill=%d", f.finalizer.prepareCalls, len(f.finalizer.calls), f.killed.lockedCalls)
	}
	if f.taskMu.lockCalls != f.taskMu.unlockCalls {
		t.Fatalf("task mutex lock=%d unlock=%d", f.taskMu.lockCalls, f.taskMu.unlockCalls)
	}
}

func TestTaskLifecycleRecordProcessFailureResolvesBeforeWait(t *testing.T) {
	for _, tc := range []struct {
		name string
		dead bool
		err  error
		want []string
	}{
		{"dead", true, nil, []string{"record-process", "terminate", "check-liveness", "save", "wait"}},
		{"live", false, nil, []string{"record-process", "terminate", "check-liveness", "pending-register", "wait"}},
		{"liveness-error", false, errors.New("liveness"), []string{"record-process", "terminate", "check-liveness", "pending-register", "wait"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFixture(t)
			f.process.err = errors.New("record")
			f.recordLock.dead, f.recordLock.err = tc.dead, tc.err
			f.failStore.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateStarting)}}
			f.failStore.saveName = "save"
			f.run()
			if f.waiter.calls != 1 || f.recordLock.calls != 1 || !lifecycleTraceSubsequence(f.trace, tc.want...) {
				t.Fatalf("trace=%v", f.trace)
			}
		})
	}
}

func TestTaskLifecycleAndFailTaskLaunchShareTheSameRealTaskMutex(t *testing.T) {
	shared := store.NewTaskMutex()
	f := newLifecycleFixture(t)
	fail := NewFailTaskLaunchUseCase(f.failStore, shared, f.failWriter, &lifecycleRecordingContractReader{}, f.failSlots, f.failLocks, f.clock)
	f.orchestrator.deps.TaskMu = shared
	f.orchestrator.deps.FailLaunch = fail
	if f.orchestrator.deps.TaskMu != fail.taskMu {
		t.Fatal("fixture did not retain the shared task mutex instance")
	}
	if fail.taskMu == store.NewTaskMutex() {
		t.Fatal("distinct task mutexes compared equal")
	}
}

func TestTaskLifecycleFailBranchesUseRealTaskMutexAndReleaseAfterUnlock(t *testing.T) {
	for _, tc := range []struct {
		name       string
		cancelling bool
		confirmed  bool
	}{
		{"failure", false, false},
		{"confirmed-cancellation", true, true},
		{"unconfirmed-cancellation", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFixture(t)
			shared := store.NewTaskMutex()
			f.orchestrator.deps.TaskMu = shared
			if tc.cancelling {
				f.tasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateCancelling)}}
				f.killed.lockedResult = execution.LockedKillResult{Confirmed: tc.confirmed}
				f.killed.releaseHook = func() { shared.Lock(f.input.Task.ID()); shared.Unlock(f.input.Task.ID()) }
			} else {
				f.tasks.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateStarting)}}
				f.failStore.loads = []lifecycleLoadResult{{snapshot: lifecycleSnapshot(t, lifecycleTask(t, domain.SubcommandImpl), domain.StateStarting)}}
				paths := &lifecycleReenteringPathLockReleaser{taskMu: shared, id: f.input.Task.ID()}
				slots := &lifecycleReenteringSlotReleaser{taskMu: shared, id: f.input.Task.ID()}
				f.orchestrator.deps.FailLaunch = NewFailTaskLaunchUseCase(f.failStore, shared, f.failWriter, &lifecycleRecordingContractReader{}, slots, paths, f.clock)
			}
			done := make(chan struct{})
			go func() { f.orchestrator.fail(context.Background(), f.input); close(done) }()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("failure branch did not complete; resource release may have reentered a held mutex")
			}
			if tc.cancelling {
				if f.killed.releaseCalls != map[bool]int{true: 1, false: 0}[tc.confirmed] {
					t.Fatalf("confirmation releases=%d", f.killed.releaseCalls)
				}
			} else if f.failStore.saveCalls != 1 {
				t.Fatalf("failure transition saves=%d", f.failStore.saveCalls)
			}
		})
	}
}
