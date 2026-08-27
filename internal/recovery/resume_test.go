package recovery

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
)

type resumeLauncherFake struct {
	params ResumeLaunchParams
	err    error
	calls  int
}

func (f *resumeLauncherFake) LaunchAndWait(_ context.Context, params ResumeLaunchParams) error {
	f.calls++
	f.params = params
	return f.err
}

type resumeReaderFake struct {
	present bool
	err     error
	calls   int
}

func (f *resumeReaderFake) ReadLastMessage(domain.TaskID) (bool, error) {
	f.calls++
	return f.present, f.err
}
func (*resumeReaderFake) ReadStderrLog(domain.TaskID) ([]byte, error) { return nil, nil }

func TestRecoveryAttemptAttemptsResumeAndChecksLastMessage(t *testing.T) {
	id, err := domain.NewTaskID("impl-20260813-120000-abcd-resume")
	if err != nil {
		t.Fatal(err)
	}
	session, err := domain.NewSessionRef("123e4567-e89b-12d3-a456-426614174000", time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC), false)
	if err != nil {
		t.Fatal(err)
	}
	launcher := &resumeLauncherFake{}
	reader := &resumeReaderFake{present: true}
	attempt := RecoveryAttempt{TaskID: id, SessionRef: session, CodexBinaryPath: "/usr/local/bin/codex"}
	out, err := attempt.Attempt(context.Background(), launcher, reader)
	if err != nil || !out.Succeeded || out.ExitCode.Raw() != 0 || out.PartialOutputSaved {
		t.Fatalf("result = (%+v, %v)", out, err)
	}
	if launcher.calls != 1 || reader.calls != 1 {
		t.Fatalf("calls = launcher:%d reader:%d", launcher.calls, reader.calls)
	}
	if launcher.params.SessionID != session.SessionID() || launcher.params.TaskID != id {
		t.Fatalf("params = %#v", launcher.params)
	}
}

func TestRecoveryAttemptDoesNotReadOutputAfterLaunchFailure(t *testing.T) {
	id, _ := domain.NewTaskID("impl-20260813-120001-abcd-resume")
	session, _ := domain.NewSessionRef("123e4567-e89b-12d3-a456-426614174000", time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC), false)
	launcher := &resumeLauncherFake{err: context.DeadlineExceeded}
	reader := &resumeReaderFake{present: true}
	_, err := (&RecoveryAttempt{TaskID: id, SessionRef: session, CodexBinaryPath: "/usr/local/bin/codex"}).Attempt(context.Background(), launcher, reader)
	if err == nil || reader.calls != 0 {
		t.Fatalf("err=%v reader calls=%d", err, reader.calls)
	}
}

func TestRecoveryAttemptFailureExitCodeDependsOnOrigin(t *testing.T) {
	for _, tc := range []struct {
		name      string
		origin    domain.RecoveryOrigin
		launcher  *resumeLauncherFake
		reader    *resumeReaderFake
		wantCode  int
		wantClass domain.ExitCodeClass
		wantErr   bool
	}{
		{name: "timeout launch error", origin: domain.RecoveryOriginTimeout, launcher: &resumeLauncherFake{err: errors.New("launch failed")}, reader: &resumeReaderFake{}, wantCode: 6, wantClass: domain.ExitCodeClassTimeout, wantErr: true},
		{name: "timeout read error", origin: domain.RecoveryOriginTimeout, launcher: &resumeLauncherFake{}, reader: &resumeReaderFake{err: errors.New("read failed")}, wantCode: 6, wantClass: domain.ExitCodeClassTimeout, wantErr: true},
		{name: "timeout missing last message", origin: domain.RecoveryOriginTimeout, launcher: &resumeLauncherFake{}, reader: &resumeReaderFake{}, wantCode: 6, wantClass: domain.ExitCodeClassTimeout},
		{name: "orphan launch error", origin: domain.RecoveryOriginOrphan, launcher: &resumeLauncherFake{err: errors.New("launch failed")}, reader: &resumeReaderFake{}, wantCode: 1, wantClass: domain.ExitCodeClassFailure, wantErr: true},
		{name: "orphan read error", origin: domain.RecoveryOriginOrphan, launcher: &resumeLauncherFake{}, reader: &resumeReaderFake{err: errors.New("read failed")}, wantCode: 1, wantClass: domain.ExitCodeClassFailure, wantErr: true},
		{name: "orphan missing last message", origin: domain.RecoveryOriginOrphan, launcher: &resumeLauncherFake{}, reader: &resumeReaderFake{}, wantCode: 1, wantClass: domain.ExitCodeClassFailure},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := (&RecoveryAttempt{TaskID: recoveryTestTaskID(t), Origin: tc.origin, SessionRef: recoveryTestSession(t), CodexBinaryPath: "/usr/local/bin/codex"}).Attempt(context.Background(), tc.launcher, tc.reader)
			if (err != nil) != tc.wantErr || result.ExitCode.Raw() != tc.wantCode || result.ExitCode.Class() != tc.wantClass {
				t.Fatalf("result=(%+v, %v), want code=%d class=%q err=%t", result, err, tc.wantCode, tc.wantClass, tc.wantErr)
			}
		})
	}
}

func TestRecoveryAttemptAppliesResumeRecoveryTimeout(t *testing.T) {
	id := recoveryTestTaskID(t)
	session := recoveryTestSession(t)
	launcher := &resumeLauncherFake{}
	launcherWithDeadline := resumeLauncherFunc(func(ctx context.Context, _ ResumeLaunchParams) error {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("resume context has no deadline")
		}
		remaining := time.Until(deadline)
		if remaining > resumeRecoveryTimeout || remaining < resumeRecoveryTimeout-time.Second {
			t.Fatalf("resume timeout remaining = %s", remaining)
		}
		launcher.calls++
		return nil
	})
	result, err := (&RecoveryAttempt{TaskID: id, SessionRef: session, CodexBinaryPath: "/usr/local/bin/codex"}).Attempt(context.Background(), launcherWithDeadline, &resumeReaderFake{present: true})
	if err != nil || !result.Succeeded || launcher.calls != 1 {
		t.Fatalf("result = (%+v, %v), calls = %d", result, err, launcher.calls)
	}
}

func TestResumeRecovererAllowsNilSessionRef(t *testing.T) {
	launcher := &resumeLauncherFake{}
	recoverer := NewResumeRecoverer(launcher, &resumeReaderFake{}, "/usr/local/bin/codex", "", domain.ClockFunc(time.Now))
	result, err := recoverer.Resume(context.Background(), recoveryTestTaskID(t), nil, domain.RecoveryOriginTimeout)
	if err != nil || result != (RecoveryResult{}) || launcher.calls != 0 {
		t.Fatalf("result=(%+v, %v), launches=%d", result, err, launcher.calls)
	}
}

func TestNewResumeRecovererRejectsTypedNilDependencies(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func()
	}{
		{"launcher", func() {
			NewResumeRecoverer((*resumeLauncherFake)(nil), &resumeReaderFake{}, "/usr/local/bin/codex", "", domain.ClockFunc(time.Now))
		}},
		{"reader", func() {
			NewResumeRecoverer(&resumeLauncherFake{}, (*resumeReaderFake)(nil), "/usr/local/bin/codex", "", domain.ClockFunc(time.Now))
		}},
		{"clock", func() {
			NewResumeRecoverer(&resumeLauncherFake{}, &resumeReaderFake{}, "/usr/local/bin/codex", "", (domain.ClockFunc)(nil))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.build()
		})
	}
}

func recoveryEvent[T domain.Event](t *testing.T, events []domain.Event) T {
	t.Helper()
	for _, event := range events {
		if typed, ok := event.(T); ok {
			return typed
		}
	}
	var zero T
	t.Fatalf("event %T not found in %#v", zero, events)
	return zero
}

type resumeLauncherFunc func(context.Context, ResumeLaunchParams) error

func (f resumeLauncherFunc) LaunchAndWait(ctx context.Context, params ResumeLaunchParams) error {
	return f(ctx, params)
}

type recovererFake struct {
	result RecoveryResult
	err    error
	calls  int
	origin domain.RecoveryOrigin
	mutex  *recoveryMutexFake
	cancel context.CancelFunc
}

func (f *recovererFake) Resume(_ context.Context, _ domain.TaskID, _ *domain.SessionRef, origin domain.RecoveryOrigin) (RecoveryResult, error) {
	if f.mutex != nil && f.mutex.held {
		panic("recoverer called while task mutex is held")
	}
	f.calls++
	f.origin = origin
	if f.cancel != nil {
		f.cancel()
	}
	return f.result, f.err
}

type recoveryStoreFake struct {
	snapshot  domain.TaskSnapshot
	loads     int
	saves     int
	saveErr   error
	saveErrOn int
}

func (f *recoveryStoreFake) Load(domain.TaskID) (domain.TaskSnapshot, error) {
	f.loads++
	return f.snapshot, nil
}

func (f *recoveryStoreFake) Save(_ domain.TaskID, snapshot domain.TaskSnapshot) error {
	f.saves++
	if f.saveErrOn == f.saves {
		return f.saveErr
	}
	f.snapshot = snapshot
	return nil
}

type recoveryWriterFake struct {
	events        []domain.Event
	markerCalls   int
	partialCalls  int
	lastPresent   bool
	stderr        []byte
	appendErr     error
	markerErr     error
	partialErr    error
	exitCodeErr   error
	exitCodeCalls int
	exitCode      domain.ExitCode
}

func (f *recoveryWriterFake) ReadLastMessage(domain.TaskID) (bool, error) { return f.lastPresent, nil }
func (f *recoveryWriterFake) ReadStderrLog(domain.TaskID) ([]byte, error) { return f.stderr, nil }
func (f *recoveryWriterFake) WritePartialOutput(domain.TaskID, string) error {
	f.partialCalls++
	return f.partialErr
}
func (f *recoveryWriterFake) WriteRecoveredMarker(domain.TaskID, time.Time) error {
	f.markerCalls++
	return f.markerErr
}
func (f *recoveryWriterFake) WriteExitCode(_ domain.TaskID, exitCode domain.ExitCode) error {
	f.exitCodeCalls++
	f.exitCode = exitCode
	return f.exitCodeErr
}
func (f *recoveryWriterFake) AppendEvent(_ domain.TaskID, event domain.Event) error {
	f.events = append(f.events, event)
	return f.appendErr
}

type recoveryMetricsFake struct {
	inputs []metrics.RecordTaskMetricsInput
	record metrics.RecordTaskMetricsOutput
	ctxErr error
}

func (f *recoveryMetricsFake) Execute(ctx context.Context, in metrics.RecordTaskMetricsInput) metrics.RecordTaskMetricsOutput {
	f.inputs = append(f.inputs, in)
	f.ctxErr = ctx.Err()
	return f.record
}

type recoveryStalledTrackerFake struct {
	calls  int
	taskID domain.TaskID
	total  int
	trace  *[]string
}

func (f *recoveryStalledTrackerFake) LeaveStalled(domain.TaskID, time.Time) int { return 0 }
func (f *recoveryStalledTrackerFake) TakeTotal(taskID domain.TaskID) int {
	f.calls++
	f.taskID = taskID
	if f.trace != nil {
		*f.trace = append(*f.trace, "take")
	}
	return f.total
}

type recoverySlotFake struct {
	calls  int
	at     time.Time
	ctxErr error
}

func (f *recoverySlotFake) ReleaseAndAdvance(ctx context.Context, _ domain.TaskID, at time.Time) {
	f.calls++
	f.at = at
	f.ctxErr = ctx.Err()
}

type recoveryMutexFake struct {
	locks, unlocks int
	held           bool
}

func (f *recoveryMutexFake) Lock(domain.TaskID) {
	if f.held {
		panic("task mutex double lock")
	}
	f.locks++
	f.held = true
}
func (f *recoveryMutexFake) Unlock(domain.TaskID) {
	if !f.held {
		panic("task mutex unlock without lock")
	}
	f.unlocks++
	f.held = false
}

type recoveryClockFake struct{ values []time.Time }

func (f *recoveryClockFake) Now() time.Time {
	if len(f.values) == 0 {
		panic("clock exhausted")
	}
	value := f.values[0]
	f.values = f.values[1:]
	return value
}

func recoveryTestTaskID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260813-120002-abcd-resume")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func recoveryTestSession(t *testing.T) domain.SessionRef {
	t.Helper()
	session, err := domain.NewSessionRef("123e4567-e89b-12d3-a456-426614174000", time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC), false)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func recoverySnapshot(t *testing.T, state domain.TaskState, session *domain.SessionRef) domain.TaskSnapshot {
	t.Helper()
	id := recoveryTestTaskID(t)
	slug, err := domain.NewSlug("resume")
	if err != nil {
		t.Fatal(err)
	}
	requestedAt := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	task, _, err := domain.NewTask(id, domain.SubcommandImpl, slug, nil, requestedAt, 1)
	if err != nil {
		t.Fatal(err)
	}
	timeout, err := domain.NewTimeout(nil, 1800)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.Start(timeout, "gpt-5", requestedAt); err != nil {
		t.Fatal(err)
	}
	snapshot, err := domain.NewTaskSnapshotFromAdmission(task, timeout, "gpt-5", nil, domain.ExecutionRouteDaemon, requestedAt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.RecordProcessInfo(123, requestedAt, requestedAt); err != nil {
		t.Fatal(err)
	}
	if err := task.ConfirmRunning(requestedAt); err != nil {
		t.Fatal(err)
	}
	switch state {
	case domain.StateTimeout:
		if _, err := task.MarkTimedOut(session, requestedAt); err != nil {
			t.Fatal(err)
		}
	case domain.StateOrphaned:
		if _, err := task.DetectOrphan("running", requestedAt); err != nil {
			t.Fatal(err)
		}
	case domain.StateRunning:
	default:
		t.Fatalf("unsupported recovery fixture state %q", state)
	}
	snapshot, err = snapshot.WithTask(task, requestedAt)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func newRecoveryUseCaseFixture(t *testing.T, state domain.TaskState, session *domain.SessionRef, result RecoveryResult) (*RecoverViaResumeUseCase, *recoveryStoreFake, *recoveryWriterFake, *recovererFake, *recoveryMetricsFake, *recoverySlotFake, *recoveryMutexFake) {
	t.Helper()
	store := &recoveryStoreFake{snapshot: recoverySnapshot(t, state, session)}
	writer := &recoveryWriterFake{}
	mutex := &recoveryMutexFake{}
	recoverer := &recovererFake{result: result, mutex: mutex}
	metricsRecorder := &recoveryMetricsFake{}
	tracker := &recoveryStalledTrackerFake{total: 41}
	slots := &recoverySlotFake{}
	clock := &recoveryClockFake{values: []time.Time{
		time.Date(2026, 8, 13, 12, 1, 0, 0, time.UTC),
		time.Date(2026, 8, 13, 12, 2, 0, 0, time.UTC),
	}}
	partial := NewSavePartialOutputUseCase(writer, writer)
	uc := NewRecoverViaResumeUseCase(store, writer, recoverer, partial, slots, metricsRecorder, tracker, mutex, clock)
	return uc, store, writer, recoverer, metricsRecorder, slots, mutex
}

func TestRecoverViaResumeUseCaseTakesStalledTotalForEveryTerminal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		state     domain.TaskState
		session   bool
		result    RecoveryResult
		wantState domain.TaskState
		saveErr   error
		recorded  bool
	}{
		{"recovered", domain.StateTimeout, true, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)}, domain.StateRecovered, nil, true},
		{"timeout lost", domain.StateTimeout, false, RecoveryResult{}, domain.StateTimeoutLost, nil, true},
		{"lost", domain.StateOrphaned, true, RecoveryResult{ExitCode: domain.NewExitCode(1)}, domain.StateLost, nil, true},
		{"metrics fail soft", domain.StateOrphaned, true, RecoveryResult{ExitCode: domain.NewExitCode(1)}, domain.StateLost, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var session *domain.SessionRef
			if tc.session {
				value := recoveryTestSession(t)
				session = &value
			}
			uc, store, _, _, recorder, slots, _ := newRecoveryUseCaseFixture(t, tc.state, session, tc.result)
			store.saveErr, store.saveErrOn = tc.saveErr, 2
			out, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: session, Origin: map[domain.TaskState]domain.RecoveryOrigin{domain.StateTimeout: domain.RecoveryOriginTimeout, domain.StateOrphaned: domain.RecoveryOriginOrphan}[tc.state], OccurredAt: time.Now()})
			if err != nil || out.FinalState != tc.wantState || len(recorder.inputs) != 1 || !recorder.inputs[0].Estimated || recorder.inputs[0].StalledTotalMs != 41 || slots.calls != 1 {
				t.Fatalf("out=(%+v,%v) metrics=%+v slots=%d", out, err, recorder.inputs, slots.calls)
			}
		})
	}
}

func TestRecoverViaResumeUseCaseSaveFailureIsNotTerminal(t *testing.T) {
	session := recoveryTestSession(t)
	uc, store, writer, recoverer, recorded, slots, mutex := newRecoveryUseCaseFixture(t, domain.StateTimeout, &session, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)})
	store.saveErr, store.saveErrOn = errors.New("save failed"), 2

	out, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: &session, Origin: domain.RecoveryOriginTimeout, OccurredAt: time.Now()})
	if err == nil || err.Error() != "recovery: terminal transition was not persisted" {
		t.Fatalf("err = %v, want terminal transition not persisted error", err)
	}
	if out != (RecoverViaResumeOutput{}) {
		t.Fatalf("out = %+v, want zero value", out)
	}
	if len(recorded.inputs) != 0 {
		t.Fatalf("metrics recorded despite unpersisted terminal state: %+v", recorded.inputs)
	}
	if slots.calls != 0 {
		t.Fatalf("slot released despite unpersisted terminal state: calls=%d", slots.calls)
	}
	if writer.exitCodeCalls != 0 {
		t.Fatalf("exit code written despite unpersisted terminal state: calls=%d", writer.exitCodeCalls)
	}
	// begin() saves the recovering-transition snapshot (call 1); finish()
	// attempts to persist the terminal snapshot and fails (call 2).
	if store.saves != 2 {
		t.Fatalf("save calls = %d, want 2 (begin + failed finish attempt)", store.saves)
	}
	if recoverer.calls != 1 || mutex.locks != 2 || mutex.unlocks != 2 {
		t.Fatalf("recoverer calls=%d locks=%d unlocks=%d", recoverer.calls, mutex.locks, mutex.unlocks)
	}
	// The recovered marker must only be written after the terminal snapshot
	// save succeeds; otherwise a marker file could exist while the
	// persisted task state is still stuck in "recovering".
	if writer.markerCalls != 0 {
		t.Fatalf("recovered marker written despite unpersisted terminal state: calls=%d", writer.markerCalls)
	}
	if len(writer.events) != 1 || writer.events[0].Type() != "RecoveryAttempted" {
		t.Fatalf("events = %#v, want only RecoveryAttempted from begin()", writer.events)
	}
}

func TestRecoverViaResumeUseCaseCompletesTerminalCleanupAfterContextCancellation(t *testing.T) {
	session := recoveryTestSession(t)
	uc, store, _, recoverer, recorder, slots, _ := newRecoveryUseCaseFixture(t, domain.StateTimeout, &session, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)})
	ctx, cancel := context.WithCancel(context.Background())
	recoverer.cancel = cancel

	out, err := uc.Execute(ctx, RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: &session, Origin: domain.RecoveryOriginTimeout, OccurredAt: time.Now()})
	if err != nil || out.FinalState != domain.StateRecovered || store.snapshot.State != domain.StateRecovered {
		t.Fatalf("out=(%+v, %v) snapshot=%#v", out, err, store.snapshot)
	}
	if len(recorder.inputs) != 1 || recorder.ctxErr != nil || slots.calls != 1 || slots.ctxErr != nil {
		t.Fatalf("metrics=%+v metrics_ctx_err=%v slots=%d slot_ctx_err=%v", recorder.inputs, recorder.ctxErr, slots.calls, slots.ctxErr)
	}
}

func TestRecoverViaResumeUseCaseSavesPartialOutputAfterContextCancellation(t *testing.T) {
	session := recoveryTestSession(t)
	uc, store, writer, recoverer, recorder, slots, _ := newRecoveryUseCaseFixture(t, domain.StateTimeout, &session, RecoveryResult{ExitCode: domain.NewExitCode(1)})
	writer.stderr = []byte("incomplete output")
	ctx, cancel := context.WithCancel(context.Background())
	recoverer.cancel = cancel

	out, err := uc.Execute(ctx, RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: &session, Origin: domain.RecoveryOriginTimeout, OccurredAt: time.Now()})
	if err != nil || out.FinalState != domain.StateTimeoutLost || !out.PartialOutputSaved || store.snapshot.State != domain.StateTimeoutLost {
		t.Fatalf("out=(%+v, %v) snapshot=%#v", out, err, store.snapshot)
	}
	if writer.partialCalls != 1 || len(recorder.inputs) != 1 || recorder.ctxErr != nil || slots.calls != 1 || slots.ctxErr != nil {
		t.Fatalf("partial=%d metrics=%+v metrics_ctx_err=%v slots=%d slot_ctx_err=%v", writer.partialCalls, recorder.inputs, recorder.ctxErr, slots.calls, slots.ctxErr)
	}
}

func TestRecoverViaResumeUseCaseRejectsNilStalledTracker(t *testing.T) {
	for _, tracker := range []stalledTimeTracker{nil, (*recoveryStalledTrackerFake)(nil)} {
		t.Run("nil", func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			NewRecoverViaResumeUseCase(&recoveryStoreFake{}, &recoveryWriterFake{}, &recovererFake{}, NewSavePartialOutputUseCase(&recoveryWriterFake{}, &recoveryWriterFake{}), &recoverySlotFake{}, &recoveryMetricsFake{}, tracker, &recoveryMutexFake{}, domain.ClockFunc(time.Now))
		})
	}
}

func TestRecoverViaResumeUseCaseTimeoutSuccess(t *testing.T) {
	session := recoveryTestSession(t)
	uc, store, writer, recoverer, recorded, slots, mutex := newRecoveryUseCaseFixture(t, domain.StateTimeout, &session, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)})
	out, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: &session, Origin: domain.RecoveryOriginTimeout, OccurredAt: time.Date(2026, 8, 13, 12, 0, 30, 0, time.UTC)})
	if err != nil || !out.Succeeded || out.FinalState != domain.StateRecovered {
		t.Fatalf("result = (%+v, %v)", out, err)
	}
	if recoverer.calls != 1 || recoverer.origin != domain.RecoveryOriginTimeout || writer.markerCalls != 1 || writer.exitCodeCalls != 1 || writer.exitCode.Raw() != 0 || store.loads != 2 || store.saves != 2 {
		t.Fatalf("calls = recoverer:%d marker:%d exit:%d loads:%d saves:%d", recoverer.calls, writer.markerCalls, writer.exitCodeCalls, store.loads, store.saves)
	}
	if len(recorded.inputs) != 1 || !recorded.inputs[0].Estimated || recorded.inputs[0].FinalState != domain.StateRecovered || slots.calls != 1 || mutex.locks != 2 || mutex.unlocks != 2 {
		t.Fatalf("metrics=%+v slots=%d locks=%d unlocks=%d", recorded.inputs, slots.calls, mutex.locks, mutex.unlocks)
	}
	if len(writer.events) != 2 || writer.events[0].Type() != "RecoveryAttempted" || writer.events[1].Type() != "RecoverySucceeded" {
		t.Fatalf("events = %#v", writer.events)
	}
}

func TestRecoverViaResumeUseCaseOrphanSuccess(t *testing.T) {
	session := recoveryTestSession(t)
	uc, _, writer, recoverer, recorded, slots, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, &session, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)})
	out, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: &session, Origin: domain.RecoveryOriginOrphan, OccurredAt: time.Now()})
	if err != nil || out.FinalState != domain.StateRecovered || recoverer.origin != domain.RecoveryOriginOrphan || writer.markerCalls != 1 || slots.calls != 1 || len(recorded.inputs) != 1 || !recorded.inputs[0].Estimated {
		t.Fatalf("result=(%+v, %v), origin=%q marker=%d slots=%d metrics=%+v", out, err, recoverer.origin, writer.markerCalls, slots.calls, recorded.inputs)
	}
	if attempted := recoveryEvent[domain.RecoveryAttempted](t, writer.events); attempted.Origin != domain.RecoveryOriginOrphan {
		t.Fatalf("attempted origin=%q", attempted.Origin)
	}
}

func TestRecoverViaResumeUseCaseFailureUsesDomainOrigin(t *testing.T) {
	for _, tc := range []struct {
		name            string
		state           domain.TaskState
		inputOrigin     domain.RecoveryOrigin
		wantState       domain.TaskState
		wantPartialCall int
	}{
		{name: "timeout", state: domain.StateTimeout, inputOrigin: domain.RecoveryOriginTimeout, wantState: domain.StateTimeoutLost, wantPartialCall: 1},
		{name: "orphan", state: domain.StateOrphaned, inputOrigin: domain.RecoveryOriginOrphan, wantState: domain.StateLost, wantPartialCall: 0},
		{name: "derived origin overrides input", state: domain.StateTimeout, inputOrigin: domain.RecoveryOriginOrphan, wantState: domain.StateTimeoutLost, wantPartialCall: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session := recoveryTestSession(t)
			uc, _, writer, recoverer, recorded, slots, _ := newRecoveryUseCaseFixture(t, tc.state, &session, RecoveryResult{ExitCode: domain.NewExitCode(1)})
			writer.stderr = []byte("incomplete output")
			out, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: &session, Origin: tc.inputOrigin, OccurredAt: time.Now()})
			if err != nil || out.Succeeded || out.FinalState != tc.wantState || writer.partialCalls != tc.wantPartialCall || slots.calls != 1 {
				t.Fatalf("result=(%+v, %v), partial=%d slots=%d", out, err, writer.partialCalls, slots.calls)
			}
			if recoverer.origin != map[domain.TaskState]domain.RecoveryOrigin{domain.StateTimeout: domain.RecoveryOriginTimeout, domain.StateOrphaned: domain.RecoveryOriginOrphan}[tc.state] || len(recorded.inputs) != 1 || recorded.inputs[0].FinalState != tc.wantState {
				t.Fatalf("origin=%q metrics=%+v", recoverer.origin, recorded.inputs)
			}
			failed := recoveryEvent[domain.RecoveryFailed](t, writer.events)
			if failed.Origin != recoverer.origin || failed.PartialOutputSaved != (tc.wantPartialCall == 1) {
				t.Fatalf("failed event=%#v", failed)
			}
		})
	}
}

func TestRecoverViaResumeUseCaseNilSessionTransitionsToTimeoutLostWithoutResume(t *testing.T) {
	uc, store, writer, recoverer, recorded, slots, mutex := newRecoveryUseCaseFixture(t, domain.StateTimeout, nil, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)})
	out, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), Origin: domain.RecoveryOriginTimeout, OccurredAt: time.Now()})
	if err != nil || out.Succeeded || out.ExitCode.Raw() != 6 || out.FinalState != domain.StateTimeoutLost || recoverer.calls != 0 || slots.calls != 1 || mutex.locks != 2 || mutex.unlocks != 2 {
		t.Fatalf("result=(%+v, %v), resume=%d slots=%d locks=%d unlocks=%d", out, err, recoverer.calls, slots.calls, mutex.locks, mutex.unlocks)
	}
	if store.snapshot.SessionRef != nil || len(writer.events) != 2 || len(recorded.inputs) != 1 {
		t.Fatalf("session=%v events=%d metrics=%d", store.snapshot.SessionRef, len(writer.events), len(recorded.inputs))
	}
	attempted := recoveryEvent[domain.RecoveryAttempted](t, writer.events)
	failed := recoveryEvent[domain.RecoveryFailed](t, writer.events)
	if attempted.SessionRef != nil || failed.Origin != domain.RecoveryOriginTimeout {
		t.Fatalf("attempted=%#v failed=%#v", attempted, failed)
	}
}

func TestRecoverViaResumeUseCaseOrphanFailureKeepsFailureExitCode(t *testing.T) {
	for _, tc := range []struct {
		name    string
		session bool
	}{
		{name: "nil session"},
		{name: "resume error", session: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var session *domain.SessionRef
			if tc.session {
				value := recoveryTestSession(t)
				session = &value
			}
			uc, _, writer, recoverer, _, _, _ := newRecoveryUseCaseFixture(t, domain.StateOrphaned, session, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)})
			if tc.session {
				recoverer.err = errors.New("resume failed")
			}
			out, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: session, Origin: domain.RecoveryOriginOrphan, OccurredAt: time.Now()})
			if err != nil || out.Succeeded || out.ExitCode.Raw() != 1 || writer.exitCode.Raw() != 1 {
				t.Fatalf("result=(%+v, %v), writer code=%d", out, err, writer.exitCode.Raw())
			}
		})
	}
}

func TestRecoverViaResumeUseCaseRejectsInvalidStateWithoutSideEffects(t *testing.T) {
	session := recoveryTestSession(t)
	uc, store, writer, recoverer, recorded, slots, mutex := newRecoveryUseCaseFixture(t, domain.StateRunning, &session, RecoveryResult{})
	_, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: &session, Origin: domain.RecoveryOriginTimeout, OccurredAt: time.Now()})
	if !errors.Is(err, domain.ErrInvalidStateTransition) {
		t.Fatalf("error = %v", err)
	}
	if store.saves != 0 || len(writer.events) != 0 || recoverer.calls != 0 || len(recorded.inputs) != 0 || slots.calls != 0 || mutex.locks != 1 || mutex.unlocks != 1 {
		t.Fatalf("unexpected side effects: saves=%d events=%d resume=%d metrics=%d slots=%d locks=%d unlocks=%d", store.saves, len(writer.events), recoverer.calls, len(recorded.inputs), slots.calls, mutex.locks, mutex.unlocks)
	}
}

func TestRecoverViaResumeUseCaseCompletionTimeIsTakenAfterResume(t *testing.T) {
	session := recoveryTestSession(t)
	uc, _, _, _, recorded, slots, _ := newRecoveryUseCaseFixture(t, domain.StateTimeout, &session, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)})
	_, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: &session, Origin: domain.RecoveryOriginTimeout, OccurredAt: time.Date(2026, 8, 13, 11, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 8, 13, 12, 1, 0, 0, time.UTC)
	if len(recorded.inputs) != 1 || !recorded.inputs[0].OccurredAt.Equal(want) || !slots.at.Equal(time.Date(2026, 8, 13, 12, 2, 0, 0, time.UTC)) {
		t.Fatalf("metrics=%+v slot at=%s", recorded.inputs, slots.at)
	}
}

func TestRecoverViaResumeUseCaseTimeoutDuringResumeFailsTerminally(t *testing.T) {
	session := recoveryTestSession(t)
	uc, _, writer, recoverer, recorded, slots, _ := newRecoveryUseCaseFixture(t, domain.StateTimeout, &session, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)})
	recoverer.err = context.DeadlineExceeded
	writer.stderr = []byte("incomplete output")
	out, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: &session, Origin: domain.RecoveryOriginTimeout, OccurredAt: time.Now()})
	if err != nil || out.Succeeded || out.FinalState != domain.StateTimeoutLost || writer.partialCalls != 1 || slots.calls != 1 || len(recorded.inputs) != 1 {
		t.Fatalf("result=(%+v, %v), partial=%d slots=%d metrics=%+v", out, err, writer.partialCalls, slots.calls, recorded.inputs)
	}
	if failed := recoveryEvent[domain.RecoveryFailed](t, writer.events); !failed.PartialOutputSaved {
		t.Fatalf("failed event=%#v", failed)
	}
	if writer.exitCodeCalls != 1 || writer.exitCode.Raw() != 6 {
		t.Fatalf("exit code writes=%d code=%d", writer.exitCodeCalls, writer.exitCode.Raw())
	}
}

func TestRecoverViaResumeUseCaseWriterFailuresDoNotSkipMetricsOrSlotRelease(t *testing.T) {
	session := recoveryTestSession(t)
	uc, _, writer, _, recorded, slots, mutex := newRecoveryUseCaseFixture(t, domain.StateTimeout, &session, RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)})
	writer.markerErr = errors.New("marker write failed")
	writer.appendErr = errors.New("event append failed")
	out, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: &session, Origin: domain.RecoveryOriginTimeout, OccurredAt: time.Now()})
	if err != nil || !out.Succeeded || out.FinalState != domain.StateRecovered || len(recorded.inputs) != 1 || slots.calls != 1 || mutex.locks != 2 || mutex.unlocks != 2 {
		t.Fatalf("result=(%+v, %v), metrics=%+v slots=%d locks=%d unlocks=%d", out, err, recorded.inputs, slots.calls, mutex.locks, mutex.unlocks)
	}
}

func TestRecoverViaResumeUseCaseRejectsInvalidOriginBeforeLocking(t *testing.T) {
	session := recoveryTestSession(t)
	uc, store, writer, recoverer, recorded, slots, mutex := newRecoveryUseCaseFixture(t, domain.StateTimeout, &session, RecoveryResult{})
	_, err := uc.Execute(context.Background(), RecoverViaResumeInput{TaskID: recoveryTestTaskID(t), SessionRef: &session, Origin: domain.RecoveryOrigin("running"), OccurredAt: time.Now()})
	if err == nil {
		t.Fatal("invalid origin was accepted")
	}
	if store.loads != 0 || store.saves != 0 || len(writer.events) != 0 || recoverer.calls != 0 || len(recorded.inputs) != 0 || slots.calls != 0 || mutex.locks != 0 || mutex.unlocks != 0 {
		t.Fatalf("unexpected side effects: loads=%d saves=%d events=%d resume=%d metrics=%d slots=%d locks=%d unlocks=%d", store.loads, store.saves, len(writer.events), recoverer.calls, len(recorded.inputs), slots.calls, mutex.locks, mutex.unlocks)
	}
}
