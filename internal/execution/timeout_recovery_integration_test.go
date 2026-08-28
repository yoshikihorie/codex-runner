package execution

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type timeoutRecoveryIntegrationRecoverer struct{ calls int }

func (r *timeoutRecoveryIntegrationRecoverer) Resume(context.Context, domain.TaskID, *domain.SessionRef, domain.RecoveryOrigin) (recovery.RecoveryResult, error) {
	r.calls++
	return recovery.RecoveryResult{}, nil
}

type timeoutRecoveryBlockingRecoverer struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (r *timeoutRecoveryBlockingRecoverer) Resume(context.Context, domain.TaskID, *domain.SessionRef, domain.RecoveryOrigin) (recovery.RecoveryResult, error) {
	r.started <- struct{}{}
	<-r.release
	return recovery.RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)}, nil
}

type timeoutRecoveryLiveness struct {
	mu    sync.Mutex
	calls int
}

func (l *timeoutRecoveryLiveness) Execute(context.Context, domain.TaskID) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	return true, nil
}

func (l *timeoutRecoveryLiveness) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

type timeoutRecoveryObservedMutex struct {
	delegate      recovery.TaskMutex
	mu            sync.Mutex
	locks         int
	observedLocks chan<- struct{}
}

func (m *timeoutRecoveryObservedMutex) Lock(id domain.TaskID) {
	m.delegate.Lock(id)
	m.mu.Lock()
	m.locks++
	locks := m.locks
	m.mu.Unlock()
	if locks == 2 || locks == 3 {
		m.observedLocks <- struct{}{}
	}
}

func (m *timeoutRecoveryObservedMutex) Unlock(id domain.TaskID) {
	m.delegate.Unlock(id)
}

// metricsAcceptanceFixture deliberately uses the file-backed metrics dependencies.
// Terminal-route unit tests cover each route's state transition; these tests keep the
// acceptance mapping focused on the persisted JSON Lines contract.
const (
	metricsAcceptanceSuccessExitCode = 0
	metricsAcceptanceFailureExitCode = 1
	metricsAcceptanceKilledExitCode  = 130
	metricsAcceptanceBarrierTimeout  = 2 * time.Second
)

type metricsTerminalRoute int

const (
	metricsRouteFinalize metricsTerminalRoute = iota
	metricsRouteConfirmKilled
	metricsRouteResume
	metricsRouteAdoptRecovering
)

type metricsAcceptanceSlots struct {
	mu    sync.Mutex
	calls int
}

func (s *metricsAcceptanceSlots) ReleaseAndAdvance(context.Context, domain.TaskID, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
}
func (s *metricsAcceptanceSlots) Reset(reservations map[domain.TaskID]domain.Subcommand) {
}
func (s *metricsAcceptanceSlots) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }

type metricsAcceptanceDisarmer struct {
	mu    sync.Mutex
	calls int
}

func (d *metricsAcceptanceDisarmer) Disarm(domain.TaskID) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
}
func (d *metricsAcceptanceDisarmer) count() int { d.mu.Lock(); defer d.mu.Unlock(); return d.calls }

type metricsAcceptancePathLocks struct {
	mu    sync.Mutex
	calls int
}

func (*metricsAcceptancePathLocks) List() ([]PathLockSnapshot, error)                 { return nil, nil }
func (*metricsAcceptancePathLocks) Save(domain.TaskID, []domain.NormalizedPath) error { return nil }
func (p *metricsAcceptancePathLocks) Delete(domain.TaskID) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return nil
}
func (p *metricsAcceptancePathLocks) count() int { p.mu.Lock(); defer p.mu.Unlock(); return p.calls }

type metricsAcceptanceRecorder struct {
	mu       sync.Mutex
	delegate *metrics.RecordTaskMetricsUseCase
	inputs   []metrics.RecordTaskMetricsInput
	outputs  []metrics.RecordTaskMetricsOutput
}

func (r *metricsAcceptanceRecorder) Execute(ctx context.Context, in metrics.RecordTaskMetricsInput) metrics.RecordTaskMetricsOutput {
	out := r.delegate.Execute(ctx, in)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inputs = append(r.inputs, in)
	r.outputs = append(r.outputs, out)
	return out
}
func (r *metricsAcceptanceRecorder) single() (metrics.RecordTaskMetricsInput, metrics.RecordTaskMetricsOutput, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.inputs) != 1 || len(r.outputs) != 1 {
		return metrics.RecordTaskMetricsInput{}, metrics.RecordTaskMetricsOutput{}, false
	}
	return r.inputs[0], r.outputs[0], true
}

type metricsAcceptanceFailingWriter struct {
	mu    sync.Mutex
	calls int
	err   error
}

type metricsAcceptanceConcurrentWriter struct {
	mu       sync.Mutex
	delegate metrics.MetricsWriter
	ctx      context.Context
	want     int
	current  int
	max      int
	gate     chan struct{}
}

func newMetricsAcceptanceConcurrentWriter(ctx context.Context, delegate metrics.MetricsWriter, want int) *metricsAcceptanceConcurrentWriter {
	return &metricsAcceptanceConcurrentWriter{ctx: ctx, delegate: delegate, want: want, gate: make(chan struct{})}
}
func (w *metricsAcceptanceConcurrentWriter) Append(id domain.TaskID, month string, line []byte) error {
	w.mu.Lock()
	w.current++
	if w.current > w.max {
		w.max = w.current
	}
	if w.current == w.want {
		close(w.gate)
	}
	gate := w.gate
	w.mu.Unlock()
	select {
	case <-gate:
	case <-w.ctx.Done():
		w.mu.Lock()
		w.current--
		w.mu.Unlock()
		return w.ctx.Err()
	}
	err := w.delegate.Append(id, month, line)
	w.mu.Lock()
	w.current--
	w.mu.Unlock()
	return err
}
func (w *metricsAcceptanceConcurrentWriter) maximum() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.max
}

func (w *metricsAcceptanceFailingWriter) Append(domain.TaskID, string, []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.calls++
	return w.err
}
func (w *metricsAcceptanceFailingWriter) count() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.calls
}

type metricsAcceptanceRecoverer struct{ result recovery.RecoveryResult }

func (r *metricsAcceptanceRecoverer) Resume(context.Context, domain.TaskID, *domain.SessionRef, domain.RecoveryOrigin) (recovery.RecoveryResult, error) {
	return r.result, nil
}

type metricsAcceptanceLiveness struct{}

func (metricsAcceptanceLiveness) Execute(context.Context, domain.TaskID) (bool, error) {
	return true, nil
}

type metricsAcceptanceFinalizer struct{}

func (metricsAcceptanceFinalizer) Finalize(domain.TaskID, int, bool, bool, time.Time) error {
	return nil
}

type metricsAcceptanceTermination struct{}

func (metricsAcceptanceTermination) Confirm(context.Context, domain.TaskID) (bool, error) {
	return true, nil
}
func (metricsAcceptanceTermination) SendAndConfirm(context.Context, domain.TaskID, recovery.ProcessSignalAuthority, time.Duration) recovery.TerminationAttemptResult {
	return recovery.TerminationAttemptResult{Dead: true}
}

type metricsAcceptanceKilled struct{}

func (metricsAcceptanceKilled) ConfirmKilled(context.Context, domain.TaskID, int, bool, time.Time) error {
	return nil
}

type metricsAcceptanceFixture struct {
	root, logs string
	tasks      *store.FileTaskStore
	writer     contract.ContractWriter
	reader     *store.FileContractReader
	events     *store.FileEventReader
	mutex      *store.TaskMutex
	slots      *metricsAcceptanceSlots
	disarmer   *metricsAcceptanceDisarmer
	pathLocks  *metricsAcceptancePathLocks
	tracker    *metrics.StalledTimeTracker
	now        time.Time
}

func newMetricsAcceptanceFixture(t *testing.T) metricsAcceptanceFixture {
	t.Helper()
	root := t.TempDir()
	logs := filepath.Join(root, "metrics")
	if err := os.Mkdir(logs, 0o700); err != nil {
		t.Fatal(err)
	}
	tasks, err := store.NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	writer := contract.NewFileContractWriter(root, domain.ClockFunc(func() time.Time { return now }))
	return metricsAcceptanceFixture{root: root, logs: logs, tasks: tasks, writer: writer, reader: store.NewFileContractReader(root), events: store.NewFileEventReader(root), mutex: store.NewTaskMutex(), slots: &metricsAcceptanceSlots{}, disarmer: &metricsAcceptanceDisarmer{}, pathLocks: &metricsAcceptancePathLocks{}, tracker: &metrics.StalledTimeTracker{}, now: now}
}

func (f metricsAcceptanceFixture) newRecorder(content bool, writer metrics.MetricsWriter) *metricsAcceptanceRecorder {
	if writer == nil {
		writer = metrics.NewFileMetricsWriter(f.logs, 1<<20)
	}
	return &metricsAcceptanceRecorder{delegate: metrics.NewRecordTaskMetricsUseCase(f.tasks, f.events, f.reader, writer, content, domain.ClockFunc(func() time.Time { return f.now }), "daemon-test", nil, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))}
}

func (f metricsAcceptanceFixture) prepare(t *testing.T, suffix string, state domain.TaskState, occurredAt time.Time, lastMessage []byte) (domain.TaskID, *domain.SessionRef) {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260814-120000-a1b2-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.tasks.Reserve(id); err != nil {
		t.Fatal(err)
	}
	slug, err := domain.NewSlug(suffix)
	if err != nil {
		t.Fatal(err)
	}
	requested := occurredAt.Add(-2 * time.Minute)
	task, _, err := domain.NewTask(id, domain.SubcommandImpl, slug, nil, requested, 1)
	if err != nil {
		t.Fatal(err)
	}
	timeout, err := domain.NewTimeout(nil, 1800)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = task.Start(timeout, "test-model", requested); err != nil {
		t.Fatal(err)
	}
	snapshot, err := domain.NewTaskSnapshotFromAdmission(task, timeout, "test-model", nil, domain.ExecutionRouteDaemon, requested)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = task.RecordProcessInfo(1, occurredAt.Add(-time.Minute), occurredAt.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = task.ConfirmRunning(occurredAt.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	sessionValue, err := domain.NewSessionRef("123e4567-e89b-12d3-a456-426614174000", requested, false)
	if err != nil {
		t.Fatal(err)
	}
	session := &sessionValue
	switch state {
	case domain.StateRunning:
	case domain.StateCancelling:
		_, err = task.RequestCancel(false, occurredAt)
	case domain.StateTimeout:
		_, err = task.MarkTimedOut(session, occurredAt)
	case domain.StateOrphaned:
		_, err = task.DetectOrphan("running", occurredAt)
	case domain.StateRecovering:
		_, err = task.MarkTimedOut(session, occurredAt)
		if err == nil {
			_, err = task.BeginRecovery(session, occurredAt)
		}
	default:
		t.Fatalf("unsupported terminal preparation state %q", state)
	}
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err = snapshot.WithTask(task, occurredAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.tasks.Save(id, snapshot); err != nil {
		t.Fatal(err)
	}
	if err := f.writer.WritePrompt(id, []byte("acceptance prompt")); err != nil {
		t.Fatal(err)
	}
	if lastMessage != nil {
		if err := os.WriteFile(filepath.Join(f.root, id.String(), "last-message.md"), lastMessage, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return id, session
}

type metricsAcceptanceResult struct {
	TaskID   domain.TaskID
	State    domain.TaskState
	Snapshot domain.TaskSnapshot
	Finalize FinalizeTaskOutput
	Input    metrics.RecordTaskMetricsInput
	Output   metrics.RecordTaskMetricsOutput
	Err      error
}

func (f metricsAcceptanceFixture) finishE(ctx context.Context, id domain.TaskID, session *domain.SessionRef, route metricsTerminalRoute, expected domain.TaskState, occurredAt time.Time, content bool, writer metrics.MetricsWriter) metricsAcceptanceResult {
	capture := f.newRecorder(content, writer)
	result := metricsAcceptanceResult{TaskID: id, State: expected}
	switch route {
	case metricsRouteFinalize:
		raw := metricsAcceptanceSuccessExitCode
		if expected == domain.StateFailed {
			raw = metricsAcceptanceFailureExitCode
		}
		uc := NewFinalizeTaskUseCase(f.tasks, f.writer, f.reader, domain.ClockFunc(func() time.Time { return f.now }), f.mutex, f.slots, f.disarmer, NewReleasePathLockUseCase(f.pathLocks), capture, f.tracker)
		result.Finalize, result.Err = uc.Execute(ctx, FinalizeTaskInput{TaskID: id, RawExitCode: raw, OccurredAt: occurredAt})
	case metricsRouteConfirmKilled:
		uc := NewConfirmTaskKilledUseCase(f.tasks, f.writer, f.reader, f.mutex, f.disarmer, NewReleasePathLockUseCase(f.pathLocks), f.slots, domain.ClockFunc(func() time.Time { return f.now }), capture, f.tracker, &recovery.PendingReconciliationSet{})
		var locked LockedKillResult
		f.mutex.Lock(id)
		locked, result.Err = uc.ExecuteLocked(ctx, ConfirmTaskKilledInput{TaskID: id, RawExitCode: metricsAcceptanceKilledExitCode, OccurredAt: occurredAt})
		f.mutex.Unlock(id)
		if locked.Confirmed {
			uc.ReleaseAfterConfirmation(ctx, locked, id)
		}
	case metricsRouteResume:
		recoveryResult := recovery.RecoveryResult{ExitCode: domain.NewExitCode(metricsAcceptanceFailureExitCode)}
		if expected == domain.StateRecovered {
			recoveryResult = recovery.RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(metricsAcceptanceSuccessExitCode)}
		}
		recoverer := &metricsAcceptanceRecoverer{result: recoveryResult}
		uc := recovery.NewRecoverViaResumeUseCase(f.tasks, f.writer, recoverer, recovery.NewSavePartialOutputUseCase(f.reader, f.writer), f.slots, capture, f.tracker, f.mutex, domain.ClockFunc(func() time.Time { return f.now }))
		origin := domain.RecoveryOriginTimeout
		if expected == domain.StateLost {
			origin = domain.RecoveryOriginOrphan
		}
		_, result.Err = uc.Execute(ctx, recovery.RecoverViaResumeInput{TaskID: id, SessionRef: session, Origin: origin, OccurredAt: occurredAt})
	case metricsRouteAdoptRecovering:
		dummy := recovery.NewRecoverViaResumeUseCase(f.tasks, f.writer, &metricsAcceptanceRecoverer{}, recovery.NewSavePartialOutputUseCase(f.reader, f.writer), f.slots, capture, f.tracker, f.mutex, domain.ClockFunc(func() time.Time { return f.now }))
		adopt := recovery.NewAdoptRunningTasksUseCase(f.tasks, metricsAcceptanceLiveness{}, f.reader, f.writer, metricsAcceptanceFinalizer{}, dummy, f.slots, f.slots, metricsAcceptanceTermination{}, metricsAcceptanceKilled{}, NewReleasePathLockUseCase(f.pathLocks), &recovery.PendingReconciliationSet{}, f.mutex, domain.ClockFunc(func() time.Time { return f.now }), f.tracker, capture)
		_, result.Err = adopt.Execute(ctx)
	}
	result.Snapshot, _ = f.tasks.Load(id)
	result.Input, result.Output, _ = capture.single()
	return result
}

func (f metricsAcceptanceFixture) record(t *testing.T, suffix string, route metricsTerminalRoute, expected domain.TaskState, occurredAt time.Time, lastMessage []byte, content bool) metricsAcceptanceResult {
	t.Helper()
	preState := domain.StateRunning
	if route == metricsRouteConfirmKilled {
		preState = domain.StateCancelling
	}
	if route == metricsRouteResume {
		if expected == domain.StateLost {
			preState = domain.StateOrphaned
		} else {
			preState = domain.StateTimeout
		}
	}
	if route == metricsRouteAdoptRecovering {
		preState = domain.StateRecovering
	}
	id, session := f.prepare(t, suffix, preState, occurredAt, lastMessage)
	result := f.finishE(context.Background(), id, session, route, expected, occurredAt, content, nil)
	if result.Err != nil {
		t.Fatal(result.Err)
	}
	return result
}

func (f metricsAcceptanceFixture) line(t *testing.T, occurredAt time.Time) map[string]any {
	t.Helper()
	path := filepath.Join(f.logs, "task-metrics-"+occurredAt.Format("2006-01")+".jsonl")
	bytes, err := os.ReadFile(path)
	if err != nil || !strings.HasSuffix(string(bytes), "\n") {
		t.Fatalf("metrics file=%s err=%v", path, err)
	}
	lines := strings.Split(strings.TrimSuffix(string(bytes), "\n"), "\n")
	if len(lines) != 1 {
		t.Fatalf("metrics lines=%d", len(lines))
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatal(err)
	}
	if len(record) != 41 {
		t.Fatalf("metrics keys=%d", len(record))
	}
	return record
}

func requireMetricsRecord(t *testing.T, f metricsAcceptanceFixture, id domain.TaskID, state domain.TaskState, at time.Time) map[string]any {
	t.Helper()
	record := f.line(t, at)
	if record["task_id"] != id.String() || record["final_state"] != string(state) {
		t.Fatalf("record identity=%#v", record)
	}
	return record
}

// This file covers the terminal routes assigned here; SCN-metrics-01-12,
// SCN-metrics-01-13, and SCN-metrics-01-15 are covered by their concrete tests.
func TestMetricsAcceptance_SCNMetrics0101_CompletedFinalizationWritesOneRecord(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	result := f.record(t, "scn0101", metricsRouteFinalize, domain.StateCompleted, f.now, []byte("acceptance answer"), true)
	if result.Snapshot.State != domain.StateCompleted || !result.Output.Recorded || result.Input.Estimated || requireMetricsRecord(t, f, result.TaskID, domain.StateCompleted, f.now)["estimated"] != false || f.slots.count() != 1 || f.disarmer.count() != 1 {
		t.Fatal("completed metrics record was not persisted as non-estimated")
	}
	info, err := os.Stat(filepath.Join(f.logs, "task-metrics-2026-08.jsonl"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("metrics permissions=%v err=%v", info.Mode(), err)
	}
}

func TestMetricsAcceptance_SCNMetrics0102_FailedWithoutLastMessageWritesZeroLengthRecord(t *testing.T) {
	for _, tc := range []struct {
		name        string
		lastMessage []byte
	}{
		{name: "scn0102missing", lastMessage: nil},
		{name: "scn0102empty", lastMessage: []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newMetricsAcceptanceFixture(t)
			result := f.record(t, tc.name, metricsRouteFinalize, domain.StateFailed, f.now, tc.lastMessage, false)
			record := requireMetricsRecord(t, f, result.TaskID, domain.StateFailed, f.now)
			if result.Snapshot.State != domain.StateFailed || !result.Output.Recorded || record["last_message_bytes"] != float64(0) || record["last_message_lines"] != float64(0) || record["last_message_body"] != nil || record["last_message_sha256"] != nil {
				t.Fatal("missing last message was not represented as zero length")
			}
		})
	}
}

func TestMetricsAcceptance_SCNMetrics0103_RecoveryAndKilledTerminalRoutesShareContract(t *testing.T) {
	for _, tc := range []struct {
		state domain.TaskState
		route metricsTerminalRoute
	}{{domain.StateRecovered, metricsRouteResume}, {domain.StateTimeoutLost, metricsRouteResume}, {domain.StateLost, metricsRouteResume}, {domain.StateKilled, metricsRouteConfirmKilled}} {
		t.Run(string(tc.state), func(t *testing.T) {
			f := newMetricsAcceptanceFixture(t)
			result := f.record(t, "scn0103"+string(tc.state), tc.route, tc.state, f.now, []byte("acceptance answer"), true)
			if result.Snapshot.State != tc.state || !result.Output.Recorded || f.slots.count() != 1 {
				t.Fatal("terminal route did not record")
			}
			if tc.state == domain.StateKilled && (f.disarmer.count() != 1 || f.pathLocks.count() != 1) {
				t.Fatal("killed cleanup was not released")
			}
			requireMetricsRecord(t, f, result.TaskID, tc.state, f.now)
		})
	}
}

func TestMetricsAcceptance_SCNMetrics0104_MetricsFailureDoesNotChangeTerminalResultOrReleases(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	failing := &metricsAcceptanceFailingWriter{err: errors.New("metrics append failed")}
	id, _ := f.prepare(t, "scn0104", domain.StateRunning, f.now, []byte("acceptance answer"))
	result := f.finishE(context.Background(), id, nil, metricsRouteFinalize, domain.StateCompleted, f.now, true, failing)
	if result.Err != nil || result.Output.Recorded || failing.count() != 1 || result.Finalize.ResultState != domain.StateCompleted || f.slots.count() != 1 || f.disarmer.count() != 1 {
		t.Fatalf("result=%#v finalize=%#v append=%d slots=%d disarms=%d", result, result.Finalize, failing.count(), f.slots.count(), f.disarmer.count())
	}
	if snapshot, err := f.tasks.Load(id); err != nil || snapshot.State != domain.StateCompleted {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
}

func TestMetricsAcceptance_SCNMetrics0105_AdoptionRecordsEstimatedAndAllowsRestartGap(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	result := f.record(t, "scn0105", metricsRouteAdoptRecovering, domain.StateRecovered, f.now, []byte("acceptance answer"), true)
	record := requireMetricsRecord(t, f, result.TaskID, domain.StateRecovered, f.now)
	if !result.Output.Recorded || !result.Input.Estimated || !result.Snapshot.AdoptedAfterRestart || record["estimated"] != true || record["stalled_total_ms"] != float64(0) {
		t.Fatal("empty restart tracker must allow zero stalled total")
	}
}

func TestMetricsAcceptance_SCNMetrics0106_OccurredAtSelectsMonthlyFile(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	previous := time.Date(2026, time.July, 31, 23, 59, 0, 0, time.UTC)
	current := time.Date(2026, time.August, 1, 0, 1, 0, 0, time.UTC)
	f.record(t, "scn0106a", metricsRouteFinalize, domain.StateCompleted, previous, []byte("acceptance answer"), true)
	f.record(t, "scn0106b", metricsRouteFinalize, domain.StateCompleted, current, []byte("acceptance answer"), true)
	for _, month := range []string{"2026-07", "2026-08"} {
		if _, err := os.Stat(filepath.Join(f.logs, "task-metrics-"+month+".jsonl")); err != nil {
			t.Fatal(err)
		}
	}
}

func TestMetricsAcceptance_SCNMetrics0107_ContentDisabledOmitsBodiesButKeepsDerivatives(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	result := f.record(t, "scn0107", metricsRouteFinalize, domain.StateCompleted, f.now, []byte("acceptance answer"), false)
	record := requireMetricsRecord(t, f, result.TaskID, domain.StateCompleted, f.now)
	if record["prompt_body"] != nil || record["last_message_body"] != nil || record["prompt_bytes"] == float64(0) || record["prompt_sha256"] == "" {
		t.Fatal("content-disabled record lost its derivatives")
	}
}

func TestMetricsAcceptance_SCNMetrics0108_ContentEnabledStoresBodiesAndDerivatives(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	result := f.record(t, "scn0108", metricsRouteFinalize, domain.StateCompleted, f.now, []byte("acceptance answer"), true)
	record := requireMetricsRecord(t, f, result.TaskID, domain.StateCompleted, f.now)
	if record["prompt_body"] == nil || record["last_message_body"] == nil || record["last_message_sha256"] == nil {
		t.Fatal("content-enabled record omitted content or derivatives")
	}
}

func TestMetricsAcceptance_SCNMetrics0109_FourConcurrentTerminalTasksWriteIndependentLines(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	type preparedTask struct {
		id      domain.TaskID
		session *domain.SessionRef
	}
	prepared := make([]preparedTask, 0, 4)
	for _, suffix := range []string{"scn0109a", "scn0109b", "scn0109c", "scn0109d"} {
		id, session := f.prepare(t, suffix, domain.StateRunning, f.now, []byte("acceptance answer"))
		prepared = append(prepared, preparedTask{id: id, session: session})
	}
	ctx, cancel := context.WithTimeout(context.Background(), metricsAcceptanceBarrierTimeout)
	defer cancel()
	start := make(chan struct{})
	results := make(chan metricsAcceptanceResult, len(prepared))
	concurrentWriter := newMetricsAcceptanceConcurrentWriter(ctx, metrics.NewFileMetricsWriter(f.logs, 1<<20), len(prepared))
	for _, task := range prepared {
		go func(task preparedTask) {
			<-start
			results <- f.finishE(ctx, task.id, task.session, metricsRouteFinalize, domain.StateCompleted, f.now, true, concurrentWriter)
		}(task)
	}
	close(start)
	seen := make(map[string]struct{}, len(prepared))
	for range prepared {
		result := <-results
		if result.Err != nil || result.Snapshot.State != domain.StateCompleted || !result.Output.Recorded {
			t.Fatalf("result=%#v", result)
		}
		seen[result.TaskID.String()] = struct{}{}
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("metrics barrier timed out: %v", err)
	}
	if len(seen) != len(prepared) || concurrentWriter.maximum() < 2 || f.slots.count() != len(prepared) || f.disarmer.count() != len(prepared) {
		t.Fatalf("unique=%d concurrent=%d slots=%d disarms=%d", len(seen), concurrentWriter.maximum(), f.slots.count(), f.disarmer.count())
	}
	bytes, err := os.ReadFile(filepath.Join(f.logs, "task-metrics-2026-08.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if lines := strings.Split(strings.TrimSuffix(string(bytes), "\n"), "\n"); len(lines) != 4 {
		t.Fatalf("lines=%d", len(lines))
	} else {
		lineIDs := make(map[string]struct{}, len(lines))
		for _, line := range lines {
			var record map[string]any
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				t.Fatal(err)
			}
			id, _ := record["task_id"].(string)
			if _, ok := seen[id]; !ok || record["final_state"] != string(domain.StateCompleted) {
				t.Fatalf("record=%#v", record)
			}
			lineIDs[id] = struct{}{}
		}
		if len(lineIDs) != len(prepared) {
			t.Fatalf("line task IDs=%d", len(lineIDs))
		}
	}
}

func TestMetricsAcceptanceConcurrentWriterCancelsWhenBarrierIsNotReached(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), metricsAcceptanceBarrierTimeout)
	defer cancel()
	delegate := &metricsAcceptanceFailingWriter{err: errors.New("delegate must not be called")}
	writer := newMetricsAcceptanceConcurrentWriter(ctx, delegate, 2)
	id, _ := domain.NewTaskID("impl-20260814-120000-a1b2-scn0109-barrier")
	result := make(chan error, 1)
	go func() { result <- writer.Append(id, "2026-08", []byte("record")) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("barrier wait error=%v", err)
		}
	case <-time.After(2 * metricsAcceptanceBarrierTimeout):
		t.Fatal("metrics barrier did not stop at the context deadline")
	}
	if delegate.count() != 0 {
		t.Fatalf("delegate calls=%d", delegate.count())
	}
}

func TestMetricsAcceptance_SCNMetrics0110_NonTerminalStateIsRejectedWithoutAppend(t *testing.T) {
	f := newMetricsAcceptanceFixture(t)
	id, _ := f.prepare(t, "scn0110", domain.StateRunning, f.now, []byte("acceptance answer"))
	recorder := f.newRecorder(false, nil)
	if out := recorder.Execute(context.Background(), metrics.RecordTaskMetricsInput{TaskID: id, FinalState: domain.StateRunning, OccurredAt: f.now}); out.Recorded {
		t.Fatal("non-terminal state was recorded")
	}
}

func TestMetricsAcceptance_SCNMetrics0111_MultipleStalledIntervalsAndSelfLoopAccumulate(t *testing.T) {
	tracker := &metrics.StalledTimeTracker{}
	id, _ := domain.NewTaskID("impl-20260814-120000-a1b2-scn0111")
	t1 := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	tracker.EnterStalled(id, t1)
	tracker.EnterStalled(id, t1.Add(time.Second))
	tracker.LeaveStalled(id, t1.Add(2*time.Second))
	tracker.EnterStalled(id, t1.Add(3*time.Second))
	tracker.LeaveStalled(id, t1.Add(5*time.Second))
	if total := tracker.TakeTotal(id); total != 4000 {
		t.Fatalf("stalled total=%d", total)
	}
}

func TestMetricsAcceptance_SCNMetrics0114_EmptyTrackerAdoptionLeaveIsNoop(t *testing.T) {
	tracker := &metrics.StalledTimeTracker{}
	id, _ := domain.NewTaskID("impl-20260814-120000-a1b2-scn0114")
	if tracker.LeaveStalled(id, time.Now()) != 0 || tracker.TakeTotal(id) != 0 {
		t.Fatal("empty tracker was not a no-op")
	}
}

type timeoutRecoveryIntegrationMetrics struct {
	inputs []metrics.RecordTaskMetricsInput
}

func (m *timeoutRecoveryIntegrationMetrics) Execute(_ context.Context, in metrics.RecordTaskMetricsInput) metrics.RecordTaskMetricsOutput {
	m.inputs = append(m.inputs, in)
	return metrics.RecordTaskMetricsOutput{Recorded: true}
}

type timeoutRecoveryIntegrationSlots struct {
	calls  int
	taskID domain.TaskID
	at     time.Time
}

func (s *timeoutRecoveryIntegrationSlots) ReleaseAndAdvance(_ context.Context, taskID domain.TaskID, at time.Time) {
	s.calls++
	s.taskID = taskID
	s.at = at
}

type timeoutRecoveryIntegrationOwnership struct {
	mu         sync.Mutex
	calls      int
	taskID     domain.TaskID
	generation domain.LifecycleGeneration
}

func (o *timeoutRecoveryIntegrationOwnership) WithCurrent(taskID domain.TaskID, generation domain.LifecycleGeneration, action func() error) (bool, error) {
	o.mu.Lock()
	o.calls++
	o.taskID, o.generation = taskID, generation
	o.mu.Unlock()
	return true, action()
}

func (o *timeoutRecoveryIntegrationOwnership) observed() (int, domain.TaskID, domain.LifecycleGeneration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.calls, o.taskID, o.generation
}

// SCN-exec-06-05: a timed-out task without a session transitions through
// recovering to timeout-lost without launching a resume process.
func TestTimeoutRecoveryIntegrationNilSessionTransitionsToTimeoutLost(t *testing.T) {
	root := t.TempDir()
	id := timeoutID(t, "nil-session-integration")
	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	tasks, err := store.NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := tasks.Reserve(id); err != nil {
		t.Fatal(err)
	}
	pid := 123
	snapshot := domain.TaskSnapshot{TaskID: id, Subcommand: domain.SubcommandImpl, PID: &pid, ProcessStartedAt: &now, ResolvedTimeoutSeconds: 1800, Model: "gpt-5", RequestedAt: now, Route: domain.ExecutionRouteDaemon, State: domain.StateRunning, StateUpdatedAt: now, SchemaVersion: 1}
	if err := tasks.Save(id, snapshot); err != nil {
		t.Fatal(err)
	}
	writer := contract.NewFileContractWriter(root, domain.ClockFunc(func() time.Time { return now }))
	reader := store.NewFileContractReader(root)
	sharedMutex := store.NewTaskMutex()
	recoverer := &timeoutRecoveryIntegrationRecoverer{}
	metricsRecorder := &timeoutRecoveryIntegrationMetrics{}
	slots := &timeoutRecoveryIntegrationSlots{}
	stalledTracker := &metrics.StalledTimeTracker{}
	recoveryUseCase := recovery.NewRecoverViaResumeUseCase(tasks, writer, recoverer, recovery.NewSavePartialOutputUseCase(reader, writer), slots, metricsRecorder, stalledTracker, sharedMutex, domain.ClockFunc(func() time.Time { return now }))
	proc := &timeoutProcessFake{}
	liveness := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(domain.TaskID) string { return filepath.Join(root, "unused.lock") })
	validator := recovery.NewProcessSignalAuthorityValidator(tasks, sharedMutex, timeoutAuthorityOwnershipFake{})
	enforce := NewEnforceTaskTimeoutUseCase(tasks, writer, proc, recoveryUseCase, NewTerminationEnsurer(liveness, proc, domain.ClockFunc(func() time.Time { return now }), func(context.Context, time.Duration) {}, validator), validator, &recovery.PendingReconciliationSet{}, NewReleasePathLockUseCase(&timeoutPathStoreFake{}), sharedMutex, domain.ClockFunc(func() time.Time { return now }), stalledTracker)
	if _, err := enforce.Execute(context.Background(), EnforceTaskTimeoutInput{TaskID: id, ResolvedTimeoutSeconds: 1800, OccurredAt: now}); err != nil {
		t.Fatal(err)
	}
	stored, err := tasks.Load(id)
	if err != nil || stored.State != domain.StateTimeoutLost || recoverer.calls != 0 || proc.calls != 1 || proc.pid != pid {
		t.Fatalf("snapshot=%#v err=%v resume=%d terminate=%d pid=%d", stored, err, recoverer.calls, proc.calls, proc.pid)
	}
	if slots.calls != 1 || slots.taskID != id || !slots.at.Equal(now) || len(metricsRecorder.inputs) != 1 {
		t.Fatalf("slots=%#v metrics=%#v", slots, metricsRecorder.inputs)
	}
	metric := metricsRecorder.inputs[0]
	if metric.TaskID != id || metric.FinalState != domain.StateTimeoutLost || !metric.Estimated || !metric.OccurredAt.Equal(now) {
		t.Fatalf("metric=%#v", metric)
	}
	events, err := os.ReadFile(filepath.Join(root, id.String(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var records []struct {
		EventType string          `json:"event_type"`
		Raw       json.RawMessage `json:"raw"`
	}
	for _, line := range bytes.Split(bytes.TrimSpace(events), []byte{'\n'}) {
		var record struct {
			EventType string          `json:"event_type"`
			Raw       json.RawMessage `json:"raw"`
		}
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	if len(records) != 3 || records[0].EventType != "TaskTimedOut" || records[1].EventType != "RecoveryAttempted" || records[2].EventType != "RecoveryFailed" {
		t.Fatalf("records=%#v", records)
	}
	var timedOut, attempted struct {
		SessionRef *domain.SessionRef `json:"session_ref"`
	}
	var failed struct {
		Origin             domain.RecoveryOrigin `json:"origin"`
		PartialOutputSaved bool                  `json:"partial_output_saved"`
	}
	if err := json.Unmarshal(records[0].Raw, &timedOut); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(records[1].Raw, &attempted); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(records[2].Raw, &failed); err != nil {
		t.Fatal(err)
	}
	if timedOut.SessionRef != nil || attempted.SessionRef != nil || failed.Origin != domain.RecoveryOriginTimeout || failed.PartialOutputSaved {
		t.Fatalf("timedOut=%#v attempted=%#v failed=%#v", timedOut, attempted, failed)
	}
}

func TestTimeoutRecoveryIntegrationCarriesLifecycleGeneration(t *testing.T) {
	root := t.TempDir()
	id := timeoutID(t, "lifecycle-generation")
	now := time.Date(2026, time.August, 14, 12, 0, 0, 0, time.UTC)
	pid := 123
	tasks, err := store.NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := tasks.Reserve(id); err != nil {
		t.Fatal(err)
	}
	snapshot := domain.TaskSnapshot{TaskID: id, Subcommand: domain.SubcommandReview, PID: &pid, ProcessStartedAt: &now, ResolvedTimeoutSeconds: 1800, Model: "gpt-5", RequestedAt: now, Route: domain.ExecutionRouteDaemon, State: domain.StateRunning, StateUpdatedAt: now, SchemaVersion: 1}
	if err := tasks.Save(id, snapshot); err != nil {
		t.Fatal(err)
	}
	writer := contract.NewFileContractWriter(root, domain.ClockFunc(func() time.Time { return now }))
	taskMu := store.NewTaskMutex()
	ownership := &timeoutRecoveryIntegrationOwnership{}
	validator := recovery.NewProcessSignalAuthorityValidator(tasks, taskMu, ownership)
	proc := &timeoutProcessFake{}
	liveness := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(domain.TaskID) string { return filepath.Join(root, "unused.lock") })
	enforce := NewEnforceTaskTimeoutUseCase(tasks, writer, proc, &timeoutRecoveryFake{}, NewTerminationEnsurer(liveness, proc, domain.ClockFunc(func() time.Time { return now }), func(context.Context, time.Duration) {}, validator), validator, &recovery.PendingReconciliationSet{}, NewReleasePathLockUseCase(&timeoutPathStoreFake{}), taskMu, domain.ClockFunc(func() time.Time { return now }), &metrics.StalledTimeTracker{})
	factory := &timeoutTimerFake{}
	watcher := NewTimeoutWatcher(enforce, domain.ClockFunc(func() time.Time { return now }), factory, context.Background(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	expectedGeneration := domain.LifecycleGeneration(17)
	watcher.Arm(id, now, snapshot.ResolvedTimeoutSeconds, expectedGeneration)
	factory.fire(0)
	calls, observedTaskID, observedGeneration := ownership.observed()
	if calls != 1 || observedTaskID != id || observedGeneration != expectedGeneration {
		t.Fatalf("ownership calls=%d task_id=%s generation=%d want=%d", calls, observedTaskID.String(), observedGeneration, expectedGeneration)
	}
	if proc.calls != 1 || proc.pid != pid {
		t.Fatalf("terminate calls=%d pid=%d", proc.calls, proc.pid)
	}
}

func TestTimeoutRecoveryIntegrationReconcileSkipsOwnedRecoveringTask(t *testing.T) {
	fixture := newMetricsAcceptanceFixture(t)
	id, session := fixture.prepare(t, "owned-recovery", domain.StateTimeout, fixture.now, nil)
	pending := &recovery.PendingReconciliationSet{}
	if err := pending.Register(id, recovery.PendingSendConfirmOnly, nil); err != nil {
		t.Fatal(err)
	}
	registry := recovery.NewRecoveryOwnershipRegistry()
	observedLocks := make(chan struct{}, 2)
	sharedMutex := &timeoutRecoveryObservedMutex{delegate: store.NewTaskMutex(), observedLocks: observedLocks}
	started := make(chan struct{}, 1)
	releaseRecovery := make(chan struct{})
	recoverer := &timeoutRecoveryBlockingRecoverer{started: started, release: releaseRecovery}
	metricsRecorder := &timeoutRecoveryIntegrationMetrics{}
	slots := &timeoutRecoveryIntegrationSlots{}
	resume := recovery.NewRecoverViaResumeUseCase(fixture.tasks, fixture.writer, recoverer, recovery.NewSavePartialOutputUseCase(fixture.reader, fixture.writer), slots, metricsRecorder, fixture.tracker, sharedMutex, domain.ClockFunc(func() time.Time { return fixture.now })).WithRecoveryOwnership(registry)
	liveness := &timeoutRecoveryLiveness{}
	reconcile := recovery.NewReconcilePendingUseCase(pending, fixture.tasks, liveness, fixture.reader, fixture.writer, metricsAcceptanceFinalizer{}, metricsAcceptanceTermination{}, metricsAcceptanceKilled{}, NewReleasePathLockUseCase(&timeoutPathStoreFake{}), resume, slots, sharedMutex, domain.ClockFunc(func() time.Time { return fixture.now }), fixture.tracker, metricsRecorder, time.Nanosecond, time.Second, slog.New(slog.NewTextHandler(io.Discard, nil))).WithRecoveryOwnership(registry)

	resumeDone := make(chan error, 1)
	go func() {
		_, err := resume.Execute(context.Background(), recovery.RecoverViaResumeInput{TaskID: id, SessionRef: session, Origin: domain.RecoveryOriginTimeout, OccurredAt: fixture.now})
		if err == nil {
			pending.Remove(id)
		}
		resumeDone <- err
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	reconcileDone := make(chan struct{})
	go func() {
		reconcile.Run(ctx)
		close(reconcileDone)
	}()
	<-observedLocks
	<-observedLocks
	stored, err := fixture.tasks.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != domain.StateRecovering || len(pending.List()) != 1 || liveness.count() != 0 {
		t.Fatalf("state=%s pending=%+v liveness=%d", stored.State, pending.List(), liveness.count())
	}
	cancel()
	<-reconcileDone

	close(releaseRecovery)
	if err := <-resumeDone; err != nil {
		t.Fatal(err)
	}
	stored, err = fixture.tasks.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != domain.StateRecovered || len(pending.List()) != 0 {
		t.Fatalf("state=%s pending=%+v", stored.State, pending.List())
	}
	code, exists, err := fixture.reader.ReadExitCode(id)
	if err != nil || !exists || code != 0 {
		t.Fatalf("exit code=(%d, %t, %v)", code, exists, err)
	}
	events, err := os.ReadFile(filepath.Join(fixture.root, id.String(), "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(events, []byte("RecoverySucceeded")) {
		t.Fatalf("RecoverySucceeded event is absent: %s", events)
	}
}
