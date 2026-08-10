package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type timeoutTimerFake struct {
	mu        sync.Mutex
	durations []time.Duration
	callbacks []func()
	stopped   []bool
}

func (f *timeoutTimerFake) AfterFunc(d time.Duration, callback func()) CancelFunc {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := len(f.callbacks)
	f.durations, f.callbacks, f.stopped = append(f.durations, d), append(f.callbacks, callback), append(f.stopped, false)
	return func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.stopped[index] {
			return false
		}
		f.stopped[index] = true
		return true
	}
}
func (f *timeoutTimerFake) fire(index int) {
	f.mu.Lock()
	callback := f.callbacks[index]
	f.mu.Unlock()
	callback()
}

type timeoutStoreFake struct {
	mu               sync.Mutex
	snapshot         domain.TaskSnapshot
	loadErr, saveErr error
	loads, saves     int
	trace            *[]string
}

func (f *timeoutStoreFake) Load(domain.TaskID) (domain.TaskSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loads++
	return f.snapshot, f.loadErr
}
func (f *timeoutStoreFake) Save(_ domain.TaskID, s domain.TaskSnapshot) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saves++
	if f.trace != nil {
		*f.trace = append(*f.trace, "save")
	}
	if f.saveErr == nil {
		f.snapshot = s
	}
	return f.saveErr
}
func (*timeoutStoreFake) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	return nil, nil
}
func (*timeoutStoreFake) Reserve(domain.TaskID) error { return nil }
func (*timeoutStoreFake) Release(domain.TaskID) error { return nil }

type timeoutWriterFake struct {
	mu        sync.Mutex
	events    []domain.Event
	appendErr error
	trace     *[]string
}

func (*timeoutWriterFake) WritePrompt(domain.TaskID, []byte) error         { return nil }
func (*timeoutWriterFake) WriteReviewInput(domain.TaskID, []byte) error    { return nil }
func (*timeoutWriterFake) WriteCombinedPrompt(domain.TaskID, []byte) error { return nil }
func (*timeoutWriterFake) OpenExecutionLogs(domain.TaskID) (*contract.ExecutionLogs, error) {
	return nil, nil
}
func (*timeoutWriterFake) WriteExitCode(domain.TaskID, domain.ExitCode) error  { return nil }
func (*timeoutWriterFake) WritePartialOutput(domain.TaskID, string) error      { return nil }
func (*timeoutWriterFake) WriteRecoveredMarker(domain.TaskID, time.Time) error { return nil }
func (*timeoutWriterFake) WriteAdoptedMarker(domain.TaskID, time.Time) error   { return nil }
func (f *timeoutWriterFake) AppendEvent(_ domain.TaskID, e domain.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
	if f.trace != nil {
		*f.trace = append(*f.trace, "append-event")
	}
	return f.appendErr
}
func (*timeoutWriterFake) AppendRawEvent(domain.TaskID, string, json.RawMessage) error { return nil }

type timeoutProcessFake struct {
	mu    sync.Mutex
	calls int
	pid   int
	grace time.Duration
	err   error
	trace *[]string
}

func (*timeoutProcessFake) Launch(context.Context, LaunchParams) (*LaunchedProcess, error) {
	return nil, errors.New("unused")
}
func (f *timeoutProcessFake) Terminate(pid int, grace time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.trace != nil {
		*f.trace = append(*f.trace, "terminate")
	}
	f.pid, f.grace = pid, grace
	return f.err
}

type timeoutRecoveryFake struct {
	mu      sync.Mutex
	calls   int
	ctx     context.Context
	input   recovery.RecoverViaResumeInput
	err     error
	started chan struct{}
	release <-chan struct{}
}

func (f *timeoutRecoveryFake) Execute(ctx context.Context, in recovery.RecoverViaResumeInput) (recovery.RecoverViaResumeOutput, error) {
	f.mu.Lock()
	f.calls++
	f.ctx = ctx
	f.input = in
	started, release, err := f.started, f.release, f.err
	f.mu.Unlock()
	if started != nil {
		close(started)
	}
	if release != nil {
		<-release
	}
	return recovery.RecoverViaResumeOutput{}, err
}

type timeoutPendingFake struct {
	mu         sync.Mutex
	calls      int
	signalSent bool
	err        error
}

func (f *timeoutPendingFake) Register(_ domain.TaskID, signalSent bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.signalSent = signalSent
	return f.err
}

type timeoutPathStoreFake struct {
	mu      sync.Mutex
	deleted []domain.TaskID
	err     error
}

func (*timeoutPathStoreFake) List() ([]PathLockSnapshot, error)                 { return nil, nil }
func (*timeoutPathStoreFake) Save(domain.TaskID, []domain.NormalizedPath) error { return nil }
func (f *timeoutPathStoreFake) Delete(id domain.TaskID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, id)
	return f.err
}

func timeoutID(t *testing.T, suffix string) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260810-120000-abcd-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func timeoutSnapshot(t *testing.T, state domain.TaskState, subcommand domain.Subcommand, session *domain.SessionRef) domain.TaskSnapshot {
	t.Helper()
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	pid := 321
	return domain.TaskSnapshot{TaskID: timeoutID(t, string(state)), Subcommand: subcommand, PID: &pid, ProcessStartedAt: &now, ResolvedTimeoutSeconds: 1800, Model: "gpt-5", RequestedAt: now, Route: domain.ExecutionRouteDaemon, State: state, StateUpdatedAt: now, SessionRef: session, SchemaVersion: 1}
}
func timeoutLiveness(results ...struct {
	dead bool
	err  error
}) *CheckLivenessUseCase {
	return timeoutLivenessWithCalls(nil, results...)
}
func timeoutLivenessWithCalls(calls *int, results ...struct {
	dead bool
	err  error
}) *CheckLivenessUseCase {
	var mu sync.Mutex
	index := 0
	return NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		if calls != nil {
			*calls = *calls + 1
		}
		result := results[index]
		if index < len(results)-1 {
			index++
		}
		return result.dead, result.err
	}), func(domain.TaskID) string { return "timeout-test.lock" })
}
func timeoutEnsurer(results ...struct {
	dead bool
	err  error
}) *TerminationEnsurer {
	return NewTerminationEnsurer(timeoutLiveness(results...), &timeoutProcessFake{}, domain.ClockFunc(time.Now), func(context.Context, time.Duration) {})
}
func timeoutUseCase(t *testing.T, snapshot domain.TaskSnapshot, liveness ...struct {
	dead bool
	err  error
}) (*EnforceTaskTimeoutUseCase, *timeoutStoreFake, *timeoutWriterFake, *timeoutProcessFake, *timeoutRecoveryFake, *timeoutPendingFake, *timeoutPathStoreFake) {
	t.Helper()
	tasks := &timeoutStoreFake{snapshot: snapshot}
	writer := &timeoutWriterFake{}
	proc := &timeoutProcessFake{}
	recoverer := &timeoutRecoveryFake{}
	pending := &timeoutPendingFake{}
	paths := &timeoutPathStoreFake{}
	ensurer := NewTerminationEnsurer(timeoutLiveness(liveness...), proc, domain.ClockFunc(time.Now), func(context.Context, time.Duration) {})
	return NewEnforceTaskTimeoutUseCase(tasks, writer, proc, recoverer, ensurer, pending, NewReleasePathLockUseCase(paths), store.NewTaskMutex(), domain.ClockFunc(time.Now)), tasks, writer, proc, recoverer, pending, paths
}

func TestTimeoutWatcherArmDisarmAndClose(t *testing.T) {
	factory := &timeoutTimerFake{}
	clock := domain.ClockFunc(func() time.Time { return time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC) })
	watcher := &TimeoutWatcher{clock: clock, timerFactory: factory, baseCtx: context.Background(), logger: nil, timers: make(map[domain.TaskID]armedTimer)}
	id := timeoutID(t, "watcher")
	watcher.Arm(id, clock.Now().Add(time.Minute), 1800)
	watcher.Arm(id, clock.Now().Add(2*time.Minute), 1800)
	watcher.Disarm(id)
	if err := watcher.Close(); err != nil {
		t.Fatal(err)
	}
	watcher.Arm(id, clock.Now().Add(time.Minute), 1800)
	factory.mu.Lock()
	defer factory.mu.Unlock()
	if len(factory.durations) != 2 || !factory.stopped[0] || !factory.stopped[1] {
		t.Fatalf("timer state = durations:%v stopped:%v", factory.durations, factory.stopped)
	}
}

func TestTimeoutWatcherGenerationAndCallbackInput(t *testing.T) {
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandReview, nil)
	uc, _, _, _, recoveryFake, _, _ := timeoutUseCase(t, snapshot, struct {
		dead bool
		err  error
	}{dead: true})
	factory := &timeoutTimerFake{}
	baseCtx := context.WithValue(context.Background(), "timeout-test-context", "base")
	watcher := NewTimeoutWatcher(uc, domain.ClockFunc(func() time.Time { return now }), factory, baseCtx, slog.New(slog.NewTextHandler(io.Discard, nil)))
	id := snapshot.TaskID
	watcher.Arm(id, now.Add(time.Minute), 1800)
	watcher.Arm(id, now, 1900)
	factory.fire(0)
	watcher.mu.Lock()
	_, oldDeletedNew := watcher.timers[id]
	watcher.mu.Unlock()
	if !oldDeletedNew {
		t.Fatal("old callback deleted new timer")
	}
	factory.fire(1)
	recoveryFake.mu.Lock()
	defer recoveryFake.mu.Unlock()
	if recoveryFake.calls != 1 || recoveryFake.input.TaskID != id || recoveryFake.input.OccurredAt != now {
		t.Fatalf("recovery=%+v calls=%d", recoveryFake.input, recoveryFake.calls)
	}
	if recoveryFake.ctx != baseCtx {
		t.Fatal("callback did not receive the watcher's base context")
	}
	if factory.durations[1] != 0 {
		t.Fatalf("past deadline duration=%v", factory.durations[1])
	}
}

func TestTimeoutWatcherDisarmNoopAndDoesNotInterruptRunningCallback(t *testing.T) {
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandReview, nil)
	uc, _, _, _, recoverer, _, _ := timeoutUseCase(t, snapshot, struct {
		dead bool
		err  error
	}{dead: true})
	factory := &timeoutTimerFake{}
	watcher := NewTimeoutWatcher(uc, domain.ClockFunc(func() time.Time { return now }), factory, context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	watcher.Disarm(timeoutID(t, "not-armed"))
	recoverer.started = make(chan struct{})
	release := make(chan struct{})
	recoverer.release = release
	watcher.Arm(snapshot.TaskID, now, 1800)
	finished := make(chan struct{})
	go func() {
		factory.fire(0)
		close(finished)
	}()
	select {
	case <-recoverer.started:
	case <-time.After(time.Second):
		t.Fatal("callback did not begin recovery")
	}
	watcher.Disarm(snapshot.TaskID)
	select {
	case <-finished:
		t.Fatal("Disarm interrupted an executing callback")
	default:
	}
	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("callback did not finish")
	}
}

func TestTimeoutWatcherCloseWaitsForCallback(t *testing.T) {
	watcher := &TimeoutWatcher{timers: make(map[domain.TaskID]armedTimer)}
	started, unblock, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	watcher.wg.Add(1)
	go func() { close(started); <-unblock; watcher.wg.Done() }()
	<-started
	go func() { done <- watcher.Close() }()
	select {
	case <-done:
		t.Fatal("Close returned before callback")
	default:
	}
	close(unblock)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutWatcherLogsCallbackErrorOnce(t *testing.T) {
	snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandReview, nil)
	uc, tasks, _, _, _, _, _ := timeoutUseCase(t, snapshot, struct {
		dead bool
		err  error
	}{dead: true})
	tasks.loadErr = errors.New("load failed")
	var logs bytes.Buffer
	factory := &timeoutTimerFake{}
	watcher := NewTimeoutWatcher(uc, domain.ClockFunc(time.Now), factory, context.Background(), slog.New(slog.NewTextHandler(&logs, nil)))
	watcher.Arm(snapshot.TaskID, time.Now(), 1800)
	factory.fire(0)
	if got := bytes.Count(logs.Bytes(), []byte("enforce task timeout")); got != 1 {
		t.Fatalf("timeout callback error log count=%d logs=%q", got, logs.String())
	}
}

func TestTerminationEnsurerConfirmAndSendOnce(t *testing.T) {
	cases := []struct {
		name    string
		results []struct {
			dead bool
			err  error
		}
		cancel   bool
		wantDead bool
		wantErr  bool
		waits    int
	}{
		{"dead-first", []struct {
			dead bool
			err  error
		}{{dead: true}}, false, true, false, 0},
		{"dead-second", []struct {
			dead bool
			err  error
		}{{}, {dead: true}}, false, true, false, 1},
		{"alive", []struct {
			dead bool
			err  error
		}{{}, {}}, false, false, false, 1},
		{"io-error", []struct {
			dead bool
			err  error
		}{{err: errors.New("io")}}, false, false, true, 0},
		{"cancelled", []struct {
			dead bool
			err  error
		}{{}, {dead: true}}, true, false, true, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var waits int
			var waitedFor time.Duration
			proc := &timeoutProcessFake{err: errors.New("send")}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			e := NewTerminationEnsurer(timeoutLiveness(tc.results...), proc, domain.ClockFunc(time.Now), func(ctx context.Context, d time.Duration) {
				waits++
				waitedFor = d
				if tc.cancel {
					cancel()
					<-ctx.Done()
				}
			})
			dead, err := e.Confirm(ctx, timeoutID(t, tc.name))
			if dead != tc.wantDead || (err != nil) != tc.wantErr || waits != tc.waits {
				t.Fatalf("dead=%v err=%v waits=%d", dead, err, waits)
			}
			if waits > 0 && waitedFor != timeoutKillGrace {
				t.Fatalf("wait duration=%v want=%v", waitedFor, timeoutKillGrace)
			}
			if tc.name == "dead-second" {
				sendEnsurer := NewTerminationEnsurer(timeoutLiveness(struct {
					dead bool
					err  error
				}{dead: true}), proc, domain.ClockFunc(time.Now), func(context.Context, time.Duration) {})
				sentDead, sendErr := sendEnsurer.SendAndConfirm(context.Background(), timeoutID(t, "send"), 44, time.Second)
				if !sentDead || sendErr != nil {
					t.Fatalf("SendAndConfirm dead=%v err=%v", sentDead, sendErr)
				}
				proc.mu.Lock()
				calls := proc.calls
				proc.mu.Unlock()
				if calls != 1 {
					t.Fatalf("terminate calls=%d", calls)
				}
			}
		})
	}
}

func TestTerminationEnsurerGracePeriodBoundary(t *testing.T) {
	for _, tc := range []struct {
		name    string
		results []struct {
			dead bool
			err  error
		}
		wantDead  bool
		wantWaits int
	}{
		{"SCN-exec-06-04-before-grace", []struct {
			dead bool
			err  error
		}{{dead: true}}, true, 0},
		{"SCN-exec-06-04-at-grace", []struct {
			dead bool
			err  error
		}{{dead: true}}, true, 0},
		{"SCN-exec-06-04-after-grace", []struct {
			dead bool
			err  error
		}{{}, {dead: false}}, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			waits := 0
			proc := &timeoutProcessFake{}
			ensurer := NewTerminationEnsurer(timeoutLiveness(tc.results...), proc, domain.ClockFunc(time.Now), func(_ context.Context, d time.Duration) {
				waits++
				if d != timeoutKillGrace {
					t.Fatalf("wait duration=%v want=%v", d, timeoutKillGrace)
				}
			})
			dead, err := ensurer.SendAndConfirm(context.Background(), timeoutID(t, "grace-boundary"), 321, timeoutKillGrace)
			if err != nil || dead != tc.wantDead || waits != tc.wantWaits {
				t.Fatalf("dead=%v err=%v waits=%d", dead, err, waits)
			}
			if proc.calls != 1 || proc.pid != 321 || proc.grace != timeoutKillGrace {
				t.Fatalf("terminate calls=%d pid=%d grace=%v", proc.calls, proc.pid, proc.grace)
			}
		})
	}
}

func TestEnforceTaskTimeoutScenarios(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state domain.TaskState
		dead  bool
		want  string
	}{
		{"SCN-exec-06-01-running", domain.StateRunning, true, "timed-out"}, {"SCN-exec-06-02-stalled", domain.StateStalled, true, "timed-out"}, {"SCN-exec-06-03-terminal", domain.StateCompleted, true, "already-terminal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := timeoutSnapshot(t, tc.state, domain.SubcommandImpl, nil)
			uc, tasks, writer, proc, recoverer, pending, paths := timeoutUseCase(t, snapshot, struct {
				dead bool
				err  error
			}{}, struct {
				dead bool
				err  error
			}{dead: tc.dead})
			at := time.Date(2026, time.August, 10, 13, 0, 0, 0, time.UTC)
			out, err := uc.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: snapshot.TaskID, ResolvedTimeoutSeconds: 1800, OccurredAt: at})
			if err != nil || out.Outcome != tc.want {
				t.Fatalf("out=%+v err=%v", out, err)
			}
			if tc.want == "already-terminal" {
				if tasks.saves != 0 || proc.calls != 0 || recoverer.calls != 0 {
					t.Fatalf("terminal calls save=%d terminate=%d recovery=%d", tasks.saves, proc.calls, recoverer.calls)
				}
				return
			}
			if tasks.snapshot.State != domain.StateTimeout || len(writer.events) != 1 {
				t.Fatalf("snapshot=%+v events=%#v", tasks.snapshot, writer.events)
			}
			event, ok := writer.events[0].(domain.TaskTimedOut)
			if !ok || event.OccurredAt != at || event.ResolvedTimeoutSeconds != 1800 {
				t.Fatalf("event=%#v", writer.events[0])
			}
			if tc.dead {
				if recoverer.calls != 1 || len(paths.deleted) != 1 || pending.calls != 0 {
					t.Fatalf("recover=%d paths=%d pending=%d", recoverer.calls, len(paths.deleted), pending.calls)
				}
			} else if pending.calls != 1 || !pending.signalSent || recoverer.calls != 0 || len(paths.deleted) != 0 {
				t.Fatalf("pending=%d recovery=%d paths=%d", pending.calls, recoverer.calls, len(paths.deleted))
			}
		})
	}
}

func TestEnforceTaskTimeoutPendingUsesReconciliationSet(t *testing.T) {
	snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandImpl, nil)
	tasks := &timeoutStoreFake{snapshot: snapshot}
	proc := &timeoutProcessFake{}
	pending := &recovery.PendingReconciliationSet{}
	uc := NewEnforceTaskTimeoutUseCase(
		tasks,
		&timeoutWriterFake{},
		proc,
		&timeoutRecoveryFake{},
		NewTerminationEnsurer(timeoutLiveness(struct {
			dead bool
			err  error
		}{}, struct {
			dead bool
			err  error
		}{}), proc, domain.ClockFunc(time.Now), func(context.Context, time.Duration) {}),
		pending,
		NewReleasePathLockUseCase(&timeoutPathStoreFake{}),
		store.NewTaskMutex(),
		domain.ClockFunc(time.Now),
	)
	out, err := uc.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: snapshot.TaskID, OccurredAt: time.Now()})
	if err != nil || out.Outcome != "timed-out" {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if len(pending.List()) != 1 {
		t.Fatalf("pending entries=%d", len(pending.List()))
	}
	if claimed, found := pending.ClaimForSend(snapshot.TaskID); claimed || !found {
		t.Fatalf("pending signalSent state claimed=%v found=%v", claimed, found)
	}
}

func TestEnforceTaskTimeoutNilSessionAndTerminateFailure(t *testing.T) {
	snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandReview, nil)
	uc, _, _, proc, recoverer, _, _ := timeoutUseCase(t, snapshot, struct {
		dead bool
		err  error
	}{dead: true})
	proc.err = errors.New("terminate")
	out, err := uc.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: snapshot.TaskID, ResolvedTimeoutSeconds: 1800, OccurredAt: time.Now()})
	if err != nil || out.Outcome != "timed-out" || recoverer.calls != 1 || recoverer.input.SessionRef != nil {
		t.Fatalf("out=%+v err=%v recovery=%+v", out, err, recoverer.input)
	}
}

func TestEnforceTaskTimeoutAppendEventFailureContinuesTerminationAndRecovery(t *testing.T) {
	snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandReview, nil)
	tasks := &timeoutStoreFake{snapshot: snapshot}
	writer := &timeoutWriterFake{}
	proc := &timeoutProcessFake{}
	recoverer := &timeoutRecoveryFake{}
	pending := &timeoutPendingFake{}
	confirmCalls := 0
	uc := NewEnforceTaskTimeoutUseCase(tasks, writer, proc, recoverer, NewTerminationEnsurer(timeoutLivenessWithCalls(&confirmCalls, struct {
		dead bool
		err  error
	}{dead: true}), proc, domain.ClockFunc(time.Now), func(context.Context, time.Duration) {}), pending, NewReleasePathLockUseCase(&timeoutPathStoreFake{}), store.NewTaskMutex(), domain.ClockFunc(time.Now))
	appendErr := fmt.Errorf("%w: append event", domain.ErrContractWriteFailed)
	writer.appendErr = appendErr

	out, err := uc.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: snapshot.TaskID, OccurredAt: time.Now()})
	if out.Outcome != "timed-out" || !errors.Is(err, appendErr) || !errors.Is(err, domain.ErrContractWriteFailed) {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if tasks.saves != 1 || proc.calls != 1 || recoverer.calls != 1 || confirmCalls != 1 {
		t.Fatalf("save=%d terminate=%d recovery=%d confirm=%d", tasks.saves, proc.calls, recoverer.calls, confirmCalls)
	}
}

func TestEnforceTaskTimeoutPersistsBeforeTermination(t *testing.T) {
	snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandReview, nil)
	trace := []string{}
	uc, tasks, writer, proc, _, _, _ := timeoutUseCase(t, snapshot, struct {
		dead bool
		err  error
	}{dead: true})
	tasks.trace, writer.trace, proc.trace = &trace, &trace, &trace
	if _, err := uc.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: snapshot.TaskID, OccurredAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if got, want := strings.Join(trace, ","), "save,append-event,terminate"; got != want {
		t.Fatalf("call order=%q want=%q", got, want)
	}
}

func TestTimeoutWatcherDeadlineBoundary(t *testing.T) {
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandReview, nil)
	uc, _, _, _, _, _, _ := timeoutUseCase(t, snapshot, struct {
		dead bool
		err  error
	}{dead: true})
	for _, tc := range []struct {
		name     string
		deadline time.Time
		want     time.Duration
	}{
		{"before", now.Add(-time.Second), 0},
		{"at", now, 0},
		{"after", now.Add(time.Second), time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			factory := &timeoutTimerFake{}
			watcher := NewTimeoutWatcher(uc, domain.ClockFunc(func() time.Time { return now }), factory, context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))
			watcher.Arm(snapshot.TaskID, tc.deadline, 1800)
			if factory.durations[0] != tc.want {
				t.Fatalf("delay=%v want=%v", factory.durations[0], tc.want)
			}
		})
	}
}

func TestEnforceTaskTimeoutPendingSkipsReleaseAndRecovery(t *testing.T) {
	for _, tc := range []struct {
		name    string
		results []struct {
			dead bool
			err  error
		}
	}{
		{"dead-false", []struct {
			dead bool
			err  error
		}{{}, {}}},
		{"liveness-error", []struct {
			dead bool
			err  error
		}{{err: errors.New("liveness")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandImpl, nil)
			tasks := &timeoutStoreFake{snapshot: snapshot}
			proc := &timeoutProcessFake{}
			paths := &timeoutPathStoreFake{}
			recoverer := &timeoutRecoveryFake{}
			pending := &recovery.PendingReconciliationSet{}
			ensurer := NewTerminationEnsurer(timeoutLiveness(tc.results...), proc, domain.ClockFunc(time.Now), func(context.Context, time.Duration) {})
			uc := NewEnforceTaskTimeoutUseCase(tasks, &timeoutWriterFake{}, proc, recoverer, ensurer, pending, NewReleasePathLockUseCase(paths), store.NewTaskMutex(), domain.ClockFunc(time.Now))
			_, _ = uc.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: snapshot.TaskID, OccurredAt: time.Now()})
			if len(pending.List()) != 1 || recoverer.calls != 0 || len(paths.deleted) != 0 {
				t.Fatalf("pending=%d recovery=%d paths=%d", len(pending.List()), recoverer.calls, len(paths.deleted))
			}
		})
	}
}

func TestEnforceTaskTimeoutReleasesPathLockFileBeforeRecovery(t *testing.T) {
	snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandImpl, nil)
	pathStore := store.NewPathLockFileStore(t.TempDir())
	path, err := domain.NewNormalizedPath(t.TempDir() + "/work")
	if err != nil {
		t.Fatal(err)
	}
	if err := pathStore.Save(snapshot.TaskID, []domain.NormalizedPath{path}); err != nil {
		t.Fatal(err)
	}
	proc := &timeoutProcessFake{}
	uc := NewEnforceTaskTimeoutUseCase(
		&timeoutStoreFake{snapshot: snapshot}, &timeoutWriterFake{}, proc, &timeoutRecoveryFake{},
		NewTerminationEnsurer(timeoutLiveness(struct {
			dead bool
			err  error
		}{dead: true}), proc, domain.ClockFunc(time.Now), func(context.Context, time.Duration) {}),
		&timeoutPendingFake{}, NewReleasePathLockUseCase(pathStore), store.NewTaskMutex(), domain.ClockFunc(time.Now),
	)
	if _, err := uc.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: snapshot.TaskID, OccurredAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	locks, err := pathStore.List()
	if err != nil || len(locks) != 0 {
		t.Fatalf("locks=%v err=%v", locks, err)
	}
}

func TestEnforceTaskTimeoutSaveFailureStopsBeforeTermination(t *testing.T) {
	snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandReview, nil)
	uc, tasks, _, proc, recoverer, _, _ := timeoutUseCase(t, snapshot, struct {
		dead bool
		err  error
	}{dead: true})
	saveErr := errors.New("save failed")
	tasks.saveErr = saveErr

	if _, err := uc.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: snapshot.TaskID, OccurredAt: time.Now()}); !errors.Is(err, saveErr) {
		t.Fatalf("err=%v", err)
	}
	if proc.calls != 0 || recoverer.calls != 0 {
		t.Fatalf("terminate=%d recovery=%d", proc.calls, recoverer.calls)
	}
}

func TestEnforceTaskTimeoutNotFoundAndPendingSet(t *testing.T) {
	snapshot := timeoutSnapshot(t, domain.StateRunning, domain.SubcommandReview, nil)
	uc, tasks, _, proc, recoverer, _, _ := timeoutUseCase(t, snapshot, struct {
		dead bool
		err  error
	}{dead: true})
	tasks.loadErr = domain.ErrTaskNotFound
	if _, err := uc.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: snapshot.TaskID, OccurredAt: time.Now()}); !errors.Is(err, domain.ErrTaskNotFound) || proc.calls != 0 || recoverer.calls != 0 {
		t.Fatalf("not-found err=%v terminate=%d recovery=%d", err, proc.calls, recoverer.calls)
	}

	tasks.loadErr = nil
	pending := &recovery.PendingReconciliationSet{}
	ensurer := NewTerminationEnsurer(timeoutLiveness(struct {
		dead bool
		err  error
	}{}, struct {
		dead bool
		err  error
	}{}), proc, domain.ClockFunc(time.Now), func(context.Context, time.Duration) {})
	uc = NewEnforceTaskTimeoutUseCase(tasks, &timeoutWriterFake{}, proc, &timeoutRecoveryFake{}, ensurer, pending, NewReleasePathLockUseCase(&timeoutPathStoreFake{}), store.NewTaskMutex(), domain.ClockFunc(time.Now))
	if _, err := uc.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: snapshot.TaskID, OccurredAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if len(pending.List()) != 1 {
		t.Fatalf("pending entries=%d", len(pending.List()))
	}
	if claimed, found := pending.ClaimForSend(snapshot.TaskID); claimed || !found {
		t.Fatalf("pending signalSent state claimed=%v found=%v", claimed, found)
	}
}

func TestEnforceTaskTimeoutConstructorRejectsNilPathLockReleaser(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	NewEnforceTaskTimeoutUseCase(&timeoutStoreFake{}, &timeoutWriterFake{}, &timeoutProcessFake{}, &timeoutRecoveryFake{}, timeoutEnsurer(struct {
		dead bool
		err  error
	}{dead: true}), &timeoutPendingFake{}, nil, store.NewTaskMutex(), domain.ClockFunc(time.Now))
}

func TestTerminationEnsurerConstructorRejectsNilDependencies(t *testing.T) {
	validLiveness := timeoutEnsurer(struct {
		dead bool
		err  error
	}{dead: true}).liveness
	validProc := &timeoutProcessFake{}
	validClock := domain.ClockFunc(time.Now)
	validWait := func(context.Context, time.Duration) {}
	for _, tc := range []struct {
		name string
		make func()
	}{
		{"liveness", func() { NewTerminationEnsurer(nil, validProc, validClock, validWait) }},
		{"proc", func() { NewTerminationEnsurer(validLiveness, nil, validClock, validWait) }},
		{"clock", func() { NewTerminationEnsurer(validLiveness, validProc, nil, validWait) }},
		{"wait", func() { NewTerminationEnsurer(validLiveness, validProc, validClock, nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.make()
		})
	}
}

var _ contract.ContractWriter = (*timeoutWriterFake)(nil)
var _ store.TaskStore = (*timeoutStoreFake)(nil)
var _ ProcessRunner = (*timeoutProcessFake)(nil)
var _ RecoveryInvoker = (*timeoutRecoveryFake)(nil)
var _ recovery.PendingRegistrar = (*timeoutPendingFake)(nil)
