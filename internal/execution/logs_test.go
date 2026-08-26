package execution

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

func TestValidateEvictLogsInput(t *testing.T) {
	for _, in := range []EvictLogsInput{
		{Trigger: TriggerExplicit},
		{Trigger: "other", OccurredAt: time.Now()},
	} {
		if err := validateEvictLogsInput(in); err == nil {
			t.Fatalf("validateEvictLogsInput(%#v) error = nil", in)
		}
	}
}

func TestMonthAge(t *testing.T) {
	now := time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)
	if got := monthAge("2025-08", now); got != 12 {
		t.Fatalf("monthAge = %d, want 12", got)
	}
}

func TestDaemonStateRejectsOversizedPingResponse(t *testing.T) {
	paths := testLogPaths(t)
	if err := os.WriteFile(paths.SocketPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	uc, err := NewEvictLogsUseCase(&logStoreStub{}, aliveLocks(), testLogPolicy(), paths)
	if err != nil {
		t.Fatal(err)
	}
	client, daemon := net.Pipe()
	uc.dial = func(context.Context, string, string) (net.Conn, error) { return client, nil }
	go func() {
		defer daemon.Close()
		var request transport.Request
		if err := json.NewDecoder(bufio.NewReader(daemon)).Decode(&request); err != nil {
			return
		}
		response := struct {
			ProtocolVersion string         `json:"protocol_version"`
			RequestID       string         `json:"request_id"`
			OK              bool           `json:"ok"`
			Result          map[string]any `json:"result"`
		}{
			ProtocolVersion: transport.ProtocolVersion,
			RequestID:       request.RequestID,
			OK:              true,
			Result:          map[string]any{"padding": strings.Repeat("x", protocolLineMaxBytes+1)},
		}
		_ = json.NewEncoder(daemon).Encode(response)
	}()

	if got := uc.daemonState(context.Background()); got != daemonUnknown {
		t.Fatalf("daemonState() = %v, want daemonUnknown for oversized response", got)
	}
}

func TestNewEvictLogsUseCaseRejectsNonPositiveRetention(t *testing.T) {
	for _, policy := range []LogEvictionPolicy{
		{RotationMaxSize: 1, RotationInterval: time.Second, RetentionCount: 1, MetricsRetention: 1},
		{RotationMaxSize: 1, RotationInterval: time.Second, RetentionDays: 1, MetricsRetention: 1},
		{RotationMaxSize: 1, RotationInterval: time.Second, RetentionDays: 1, RetentionCount: 1},
	} {
		if _, err := NewEvictLogsUseCase(&logStoreStub{}, aliveLocks(), policy, testLogPaths(t)); err == nil {
			t.Fatalf("NewEvictLogsUseCase(%+v) error = nil", policy)
		}
	}
}

func TestEvictLogsPlanSkipsTaskWhenLivenessLockIsMissing(t *testing.T) {
	id := testLogTaskID(t)
	path := filepath.Join(t.TempDir(), "stdout.log")
	logs := &logStoreStub{perTask: map[domain.TaskID][]string{id: {path}}, ages: map[string]int{path: 2}}
	uc, err := NewEvictLogsUseCase(logs, NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) {
		return false, domain.ErrTaskNotFound
	}), func(domain.TaskID) string { return filepath.Join(t.TempDir(), "task.lock") }), testLogPolicy(), testLogPaths(t))
	if err != nil {
		t.Fatal(err)
	}
	out, err := uc.Plan(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Candidates) != 0 || len(out.Skipped) != 1 || out.Skipped[0].Reason != LogSkipLivenessCheckFailed {
		t.Fatalf("Plan() = %+v, want liveness failure skip without candidates", out)
	}
}

func TestEvictLogsDeleteCandidatesSkipsMissingLivenessLock(t *testing.T) {
	id := testLogTaskID(t)
	paths := testLogPaths(t)
	path := filepath.Join(paths.TaskLogsRoot, id.String(), "stdout.log")
	logs := &logStoreStub{}
	uc, err := NewEvictLogsUseCase(logs, NewCheckLivenessUseCase(nil, func(domain.TaskID) string { return filepath.Join(t.TempDir(), "task.lock") }), testLogPolicy(), paths)
	if err != nil {
		t.Fatal(err)
	}
	deleted, skipped, err := uc.deleteCandidates(context.Background(), []LogDeletionCandidate{{Path: path, Category: LogCategoryPerTaskLog, TaskID: &id}})
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 0 || logs.removes != 0 || len(skipped) != 1 || skipped[0].Reason != LogSkipLivenessCheckFailed {
		t.Fatalf("deleteCandidates() = deleted=%v skipped=%v removes=%d", deleted, skipped, logs.removes)
	}
}

func TestEvictLogsExecuteRejectsConfirmedPerTaskCandidateWithoutTaskID(t *testing.T) {
	paths := testLogPaths(t)
	path := filepath.Join(paths.TaskLogsRoot, testLogTaskID(t).String(), "stdout.log")
	logs := &logStoreStub{}
	uc, err := NewEvictLogsUseCase(logs, aliveLocks(), testLogPolicy(), paths)
	if err != nil {
		t.Fatal(err)
	}

	_, err = uc.Execute(context.Background(), EvictLogsInput{Trigger: TriggerExplicit, OccurredAt: time.Now()}, []LogDeletionCandidate{{Path: path, Category: LogCategoryPerTaskLog}})
	if err == nil {
		t.Fatal("Execute() error = nil, want invalid confirmed candidate rejection")
	}
	if logs.removes != 0 {
		t.Fatalf("Remove calls = %d, want 0", logs.removes)
	}
}

func TestEvictLogsPlanUsesEvictionLock(t *testing.T) {
	paths := testLogPaths(t)
	uc, err := NewEvictLogsUseCase(&logStoreStub{}, aliveLocks(), testLogPolicy(), paths)
	if err != nil {
		t.Fatal(err)
	}
	held := store.NewFileMutex(paths.LockPath)
	if err := held.Lock(); err != nil {
		t.Fatal(err)
	}
	defer held.Unlock()
	completed := make(chan error, 1)
	go func() {
		_, err := uc.Plan(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: time.Now()})
		completed <- err
	}()
	select {
	case err := <-completed:
		t.Fatalf("Plan completed while eviction lock held: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if err := held.Unlock(); err != nil {
		t.Fatal(err)
	}
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
}

func TestEvictLogsPlanCompressesPastMetricsAndSkipsCompressedGenerations(t *testing.T) {
	root := t.TempDir()
	oldMetric := filepath.Join(root, "task-metrics-2026-07.jsonl")
	compressedGeneration := filepath.Join(root, "codexd.log.old.gz")
	uncompressedGeneration := filepath.Join(root, "codexd.log.older")
	logs := &logStoreStub{
		generations: map[string][]string{},
		metrics:     []string{oldMetric},
		compressed:  map[string]string{oldMetric: oldMetric + ".gz", uncompressedGeneration: uncompressedGeneration + ".gz"},
	}
	paths := testLogPaths(t)
	logs.generations[paths.CodexdLog] = []string{uncompressedGeneration, compressedGeneration}
	uc, err := NewEvictLogsUseCase(logs, aliveLocks(), testLogPolicy(), paths)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := uc.Plan(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: time.Date(2026, time.August, 1, 0, 0, 0, 0, time.UTC)}); err != nil {
		t.Fatal(err)
	}
	if logs.compressedCalls[compressedGeneration] != 0 || logs.compressedCalls[oldMetric] != 1 {
		t.Fatalf("CompressGeneration calls = %#v", logs.compressedCalls)
	}
}

func TestEvictLogsPlanDoesNotDeleteGenerationWhoseCompressionFailed(t *testing.T) {
	paths := testLogPaths(t)
	failed := paths.CodexdLog + ".20260820T120000.000000000Z"
	newest := paths.CodexdLog + ".20260821T120000.000000000Z"
	logs := &logStoreStub{
		generations:  map[string][]string{paths.CodexdLog: {failed, newest}},
		ages:         map[string]int{failed: 2, newest: 0},
		compressErrs: map[string]error{failed: errors.New("compress failed")},
	}
	uc, err := NewEvictLogsUseCase(logs, aliveLocks(), testLogPolicy(), paths)
	if err != nil {
		t.Fatal(err)
	}

	out, err := uc.Plan(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range out.Candidates {
		if candidate.Path == failed {
			t.Fatalf("failed compression generation was selected for deletion: %+v", out.Candidates)
		}
	}
}

func TestEvictLogsPlanReportsActivePathForDaemonWideRotation(t *testing.T) {
	paths := testLogPaths(t)
	rotated := paths.CodexdLog + ".20260821T120000.000000000Z"
	logs := &logStoreStub{
		sizes:   map[string]int64{paths.CodexdLog: 1},
		rotated: map[string]string{paths.CodexdLog: rotated},
	}
	uc, err := NewEvictLogsUseCase(logs, aliveLocks(), testLogPolicy(), paths)
	if err != nil {
		t.Fatal(err)
	}

	out, err := uc.Plan(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.RotatedDaemonWide) != 1 || out.RotatedDaemonWide[0] != paths.CodexdLog {
		t.Fatalf("RotatedDaemonWide = %v, want [%q]", out.RotatedDaemonWide, paths.CodexdLog)
	}
}

func TestEvictLogsDoesNotDeleteLiveTaskLogsAcrossTriggers(t *testing.T) {
	paths, id := testLogPaths(t), testLogTaskID(t)
	files := testTaskLogs(paths, id)
	logs := &logStoreStub{perTask: map[domain.TaskID][]string{id: files}, ages: testAges(files, 2)}
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }))
	for _, in := range []EvictLogsInput{{Trigger: TriggerAutomatic, OccurredAt: testLogNow()}, {Trigger: TriggerExplicit, OccurredAt: testLogNow()}} {
		var out EvictLogsOutput
		var err error
		if in.Trigger == TriggerAutomatic {
			out, err = uc.Execute(context.Background(), in, nil)
		} else {
			out, err = uc.Plan(context.Background(), in)
		}
		if err != nil || len(out.Candidates) != 0 || len(out.Deleted) != 0 {
			t.Fatalf("%s: out=%+v err=%v", in.Trigger, out, err)
		}
	}
}

func TestEvictLogsSkipsAgeEvaluationAndPreservesOversizedLiveTaskLog(t *testing.T) {
	paths, id := testLogPaths(t), testLogTaskID(t)
	stdout := filepath.Join(paths.TaskLogsRoot, id.String(), "stdout.log")
	logs := &logStoreStub{perTask: map[domain.TaskID][]string{id: {stdout}}, sizes: map[string]int64{stdout: testLogPolicy().RotationMaxSize * 100}}
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }))
	out, err := uc.Execute(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: testLogNow()}, nil)
	if err != nil || logs.ageCalls[stdout] != 0 || len(out.Deleted) != 0 {
		t.Fatalf("out=%+v ageCalls=%v err=%v", out, logs.ageCalls, err)
	}
}

func TestEvictLogsAutomaticDeletesOnlyExpiredTaskLogs(t *testing.T) {
	paths := testLogPaths(t)
	a, b := testLogTaskID(t), testLogTaskIDWithSuffix(t, "b2c3")
	aFiles, bFiles := testTaskLogs(paths, a), testTaskLogs(paths, b)
	ages := testAges(aFiles, 1)
	for path, age := range testAges(bFiles, 2) {
		ages[path] = age
	}
	logs := &logStoreStub{perTask: map[domain.TaskID][]string{a: aFiles, b: bFiles}, ages: ages}
	uc := testDeadTaskEvictLogsUseCase(t, logs, paths, a, b)
	out, err := uc.Execute(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: testLogNow()}, nil)
	if err != nil || !samePaths(out.Deleted, bFiles) {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestEvictLogsAutomaticReturnsNonNilEmptySlicesWithoutCandidates(t *testing.T) {
	paths := testLogPaths(t)
	uc := testEvictLogsUseCase(t, &logStoreStub{}, paths, domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }))
	out, err := uc.Execute(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: testLogNow()}, nil)
	if err != nil || out.Candidates == nil || out.Deleted == nil || len(out.Candidates) != 0 || len(out.Deleted) != 0 {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestEvictLogsExecuteExplicitEmptyConfirmedReturnsNonNilSlices(t *testing.T) {
	paths := testLogPaths(t)
	uc := testEvictLogsUseCase(t, &logStoreStub{}, paths, domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }))
	out, err := uc.Execute(context.Background(), EvictLogsInput{Trigger: TriggerExplicit, OccurredAt: testLogNow()}, []LogDeletionCandidate{})
	if err != nil || out.RotatedDaemonWide == nil || out.Candidates == nil || out.Deleted == nil || out.Skipped == nil || len(out.RotatedDaemonWide) != 0 || len(out.Candidates) != 0 || len(out.Deleted) != 0 || len(out.Skipped) != 0 {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestEvictLogsPlanRotatesDaemonLogAtIntervalBelowSizeLimit(t *testing.T) {
	paths := testLogPaths(t)
	at := testLogNow()
	rotated := paths.CodexdLog + ".20260820T000000.000000000Z"
	logs := &logStoreStub{sizes: map[string]int64{paths.CodexdLog: 0}, lastRotation: map[string]time.Time{paths.CodexdLog: at.Add(-2 * time.Second)}, rotated: map[string]string{paths.CodexdLog: rotated}}
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }))
	out, err := uc.Plan(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: at})
	if err != nil || !samePaths(out.RotatedDaemonWide, []string{paths.CodexdLog}) {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestEvictLogsAutomaticDeletesExpiredMetricsAndPreservesCurrentMonth(t *testing.T) {
	paths := testLogPaths(t)
	current := filepath.Join(paths.LogsRoot, "task-metrics-2026-08.jsonl")
	old := filepath.Join(paths.LogsRoot, "task-metrics-2025-07.jsonl")
	logs := &logStoreStub{metrics: []string{current, old}, compressed: map[string]string{old: old + ".gz"}}
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }))
	out, err := uc.Execute(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: testLogNow()}, nil)
	if err != nil || !samePaths(out.Deleted, []string{old + ".gz"}) || logs.compressedCalls[current] != 0 {
		t.Fatalf("out=%+v compressed=%v err=%v", out, logs.compressedCalls, err)
	}
}

func TestEvictLogsDeleteCandidatesContinuesAfterRemoveFailure(t *testing.T) {
	paths := testLogPaths(t)
	a := testGeneration(paths.CodexdLog, "20260818")
	b := testGeneration(paths.CodexdLog, "20260819")
	c := testGeneration(paths.CodexdLog, "20260820")
	var buffer bytes.Buffer
	logs := &logStoreStub{removeErrs: map[string]error{a: errors.New("permission denied")}}
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), slog.New(slog.NewTextHandler(&buffer, nil)))
	deleted, skipped, err := uc.deleteCandidates(context.Background(), testGenerationCandidates(a, b, c))
	if err != nil || !samePaths(deleted, []string{b, c}) || len(skipped) != 1 || skipped[0].Reason != LogSkipRemoveFailed || !strings.Contains(buffer.String(), logRotationFailedCode) || !strings.Contains(buffer.String(), a) {
		t.Fatalf("deleted=%v skipped=%v log=%q err=%v", deleted, skipped, buffer.String(), err)
	}
}

func TestEvictLogsPlanLogsLivenessCheckFailure(t *testing.T) {
	paths, id := testLogPaths(t), testLogTaskID(t)
	files := testTaskLogs(paths, id)
	var buffer bytes.Buffer
	logs := &logStoreStub{perTask: map[domain.TaskID][]string{id: files}, ages: testAges(files, 2)}
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(func(string) (bool, error) { return false, errors.New("broken lock") }), slog.New(slog.NewTextHandler(&buffer, nil)))
	out, err := uc.Plan(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: testLogNow()})
	if err != nil || len(out.Candidates) != 0 || len(out.Skipped) != 1 || out.Skipped[0].Reason != LogSkipLivenessCheckFailed || !strings.Contains(buffer.String(), logRotationFailedCode) || !strings.Contains(buffer.String(), "broken lock") {
		t.Fatalf("out=%+v log=%q err=%v", out, buffer.String(), err)
	}
}

func TestEvictLogsExplicitRechecksLivenessBeforeDeleting(t *testing.T) {
	paths := testLogPaths(t)
	d, other := testLogTaskID(t), testLogTaskIDWithSuffix(t, "b2c3")
	dFile := testTaskLogs(paths, d)[0]
	otherFile := testTaskLogs(paths, other)[0]
	logs := &logStoreStub{}
	uc := testDeadTaskEvictLogsUseCase(t, logs, paths, d, other)
	held, err := AcquireExistingForChild(filepath.Join(paths.TaskLogsRoot, d.String()))
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	out, err := uc.Execute(context.Background(), EvictLogsInput{Trigger: TriggerExplicit, OccurredAt: testLogNow()}, []LogDeletionCandidate{{Path: dFile, Category: LogCategoryPerTaskLog, TaskID: &d}, {Path: otherFile, Category: LogCategoryPerTaskLog, TaskID: &other}})
	if err != nil || !samePaths(out.Deleted, []string{otherFile}) || len(out.Skipped) != 1 || out.Skipped[0].Path != dFile || out.Skipped[0].Reason != LogSkipStillAlive {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestEvictLogsDeleteCandidatesTreatsMissingFileAsSuccess(t *testing.T) {
	paths := testLogPaths(t)
	missing, next := testGeneration(paths.CodexdLog, "20260818"), testGeneration(paths.CodexdLog, "20260819")
	logs := store.NewFileLogStore(nil)
	if err := os.WriteFile(next, []byte("next"), 0o600); err != nil {
		t.Fatal(err)
	}
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }))
	deleted, skipped, err := uc.deleteCandidates(context.Background(), testGenerationCandidates(missing, next))
	if err != nil || !samePaths(deleted, []string{missing, next}) || len(skipped) != 0 {
		t.Fatalf("deleted=%v skipped=%v err=%v", deleted, skipped, err)
	}
}

func TestEvictLogsPlanEvictsOldestGenerationOverRetentionCount(t *testing.T) {
	paths := testLogPaths(t)
	oldest, newest := testGeneration(paths.CodexdLog, "20260818"), testGeneration(paths.CodexdLog, "20260819")
	logs := &logStoreStub{generations: map[string][]string{paths.CodexdLog: {oldest, newest}}, ages: map[string]int{oldest: 0, newest: 0}}
	policy := testLogPolicy()
	policy.Compress = false
	uc := testEvictLogsUseCaseWithPolicy(t, logs, paths, policy, domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }))
	out, err := uc.Plan(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: testLogNow()})
	if err != nil || len(out.Candidates) != 1 || out.Candidates[0].Path != oldest {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestEvictLogsCompressesOldDaemonGenerationButDeletesTaskLogDirectly(t *testing.T) {
	paths, id := testLogPaths(t), testLogTaskID(t)
	old, newest := testGeneration(paths.CodexdLog, "20260818"), testGeneration(paths.CodexdLog, "20260819")
	taskFile := testTaskLogs(paths, id)[0]
	logs := &logStoreStub{generations: map[string][]string{paths.CodexdLog: {old, newest}}, ages: map[string]int{old: 2, newest: 0, taskFile: 2}, compressed: map[string]string{old: old + ".gz"}, perTask: map[domain.TaskID][]string{id: {taskFile}}}
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }))
	out, err := uc.Plan(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: testLogNow()})
	if err != nil || logs.compressedCalls[old] != 1 || logs.compressedCalls[newest] != 0 || logs.compressedCalls[taskFile] != 0 || !hasCandidate(out.Candidates, taskFile) {
		t.Fatalf("out=%+v compressed=%v err=%v", out, logs.compressedCalls, err)
	}
}

func TestEvictLogsAutomaticResumesAfterInterruptedDeletion(t *testing.T) {
	paths := testLogPaths(t)
	ids := []domain.TaskID{testLogTaskID(t), testLogTaskIDWithSuffix(t, "b2c3"), testLogTaskIDWithSuffix(t, "c3d4"), testLogTaskIDWithSuffix(t, "d4e5"), testLogTaskIDWithSuffix(t, "e5f6")}
	files := make([]string, 0, len(ids))
	for _, id := range ids {
		root := filepath.Join(paths.TaskLogsRoot, id.String())
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
		lock, err := AcquireForChild(root)
		if err != nil {
			t.Fatal(err)
		}
		if err := lock.Close(); err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, "stdout.log")
		if err := os.WriteFile(path, []byte("complete"), 0o600); err != nil {
			t.Fatal(err)
		}
		files = append(files, path)
	}
	logs := store.NewFileLogStore(nil)
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(store.TryAcquireLiveness))
	first := []LogDeletionCandidate{{Path: files[0], Category: LogCategoryPerTaskLog, TaskID: &ids[0]}, {Path: files[1], Category: LogCategoryPerTaskLog, TaskID: &ids[1]}}
	if _, _, err := uc.deleteCandidates(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(files[2]); err != nil || string(got) != "complete" {
		t.Fatalf("remaining file = %q, err=%v", got, err)
	}
	out, err := uc.Execute(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: time.Now().Add(48 * time.Hour)}, nil)
	if err != nil || !samePaths(out.Deleted, files[2:]) {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestEvictLogsDeletesOnlyPerTaskLogFiles(t *testing.T) {
	paths, id := testLogPaths(t), testLogTaskID(t)
	root := filepath.Join(paths.TaskLogsRoot, id.String())
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	lock, err := AcquireForChild(root)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	logs := store.NewFileLogStore(nil)
	names := []string{"stdout.log", "stderr.log", "events.jsonl", "task.json", "prompt.md", "exit-code", "last-message.md", "extra.txt"}
	candidates := make([]LogDeletionCandidate, 0, 3)
	for _, name := range names {
		path := filepath.Join(root, name)
		if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
		if name == "stdout.log" || name == "stderr.log" || name == "events.jsonl" {
			candidates = append(candidates, LogDeletionCandidate{Path: path, Category: LogCategoryPerTaskLog, TaskID: &id})
		}
	}
	if err := lock.Close(); err != nil {
		t.Fatal(err)
	}
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(store.TryAcquireLiveness))
	deleted, _, err := uc.deleteCandidates(context.Background(), candidates)
	if err != nil || len(deleted) != 3 {
		t.Fatalf("deleted=%v err=%v", deleted, err)
	}
	for _, name := range names[3:] {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Fatalf("%s unexpectedly removed: %v", name, err)
		}
	}
	if _, err := os.Stat(root); err != nil {
		t.Fatal(err)
	}
}

func TestEvictLogsPlanContinuesAfterCodexdRotationFailure(t *testing.T) {
	paths := testLogPaths(t)
	fallback := paths.RouteFallback + ".20260818T000000.000000000Z"
	logs := &logStoreStub{sizes: map[string]int64{paths.CodexdLog: 1, paths.RouteFallback: 1}, rotateErrs: map[string]error{paths.CodexdLog: errors.New("rename denied")}, rotated: map[string]string{paths.RouteFallback: fallback}}
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }))
	out, err := uc.Plan(context.Background(), EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: testLogNow()})
	if err != nil || !samePaths(out.RotatedDaemonWide, []string{paths.RouteFallback}) || len(out.Skipped) == 0 || out.Skipped[0].Path != paths.CodexdLog {
		t.Fatalf("out=%+v err=%v", out, err)
	}
}

func TestEvictLogsExplicitSkipsCodexdRotationWhenDaemonRunning(t *testing.T) {
	paths := testLogPaths(t)
	fallback := paths.RouteFallback + ".20260818T000000.000000000Z"
	if err := os.WriteFile(paths.SocketPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	logs := &logStoreStub{sizes: map[string]int64{paths.CodexdLog: 1, paths.RouteFallback: 1}, rotated: map[string]string{paths.RouteFallback: fallback}, generations: map[string][]string{paths.RouteFallback: {fallback}}, ages: map[string]int{fallback: 2}}
	uc := testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }))
	client, daemon := net.Pipe()
	uc.dial = func(context.Context, string, string) (net.Conn, error) { return client, nil }
	go testPingServer(daemon)
	out, err := uc.Plan(context.Background(), EvictLogsInput{Trigger: TriggerExplicit, OccurredAt: testLogNow()})
	if err != nil || len(logs.rotateCalls) != 1 || logs.rotateCalls[0] != paths.RouteFallback || len(logs.reopenCalls) != 0 || !hasCandidate(out.Candidates, fallback) {
		t.Fatalf("out=%+v rotate=%v reopen=%v err=%v", out, logs.rotateCalls, logs.reopenCalls, err)
	}
}

func testLogNow() time.Time { return time.Date(2026, time.August, 21, 12, 0, 0, 0, time.UTC) }

func testLogTaskIDWithSuffix(t *testing.T, suffix string) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260821-120000-" + suffix + "-test")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func testTaskLogs(paths LogPaths, id domain.TaskID) []string {
	root := filepath.Join(paths.TaskLogsRoot, id.String())
	return []string{filepath.Join(root, "stdout.log"), filepath.Join(root, "stderr.log"), filepath.Join(root, "events.jsonl")}
}

func testAges(paths []string, age int) map[string]int {
	ages := make(map[string]int, len(paths))
	for _, path := range paths {
		ages[path] = age
	}
	return ages
}

func testEvictLogsUseCase(t *testing.T, logs LogStore, paths LogPaths, lock domain.LivenessLock, loggers ...*slog.Logger) *EvictLogsUseCase {
	t.Helper()
	return testEvictLogsUseCaseWithPolicy(t, logs, paths, testLogPolicy(), lock, loggers...)
}

func testEvictLogsUseCaseWithPolicy(t *testing.T, logs LogStore, paths LogPaths, policy LogEvictionPolicy, lock domain.LivenessLock, loggers ...*slog.Logger) *EvictLogsUseCase {
	t.Helper()
	uc, err := NewEvictLogsUseCase(logs, NewCheckLivenessUseCase(lock, func(id domain.TaskID) string { return filepath.Join(paths.TaskLogsRoot, id.String(), "task.lock") }), policy, paths, loggers...)
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

func testDeadTaskEvictLogsUseCase(t *testing.T, logs LogStore, paths LogPaths, ids ...domain.TaskID) *EvictLogsUseCase {
	t.Helper()
	for _, id := range ids {
		root := filepath.Join(paths.TaskLogsRoot, id.String())
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
		lock, err := AcquireForChild(root)
		if err != nil {
			t.Fatal(err)
		}
		if err := lock.Close(); err != nil {
			t.Fatal(err)
		}
	}
	return testEvictLogsUseCase(t, logs, paths, domain.LivenessLockFunc(store.TryAcquireLiveness))
}

func testGeneration(active, date string) string { return active + "." + date + "T000000.000000000Z" }

func testGenerationCandidates(paths ...string) []LogDeletionCandidate {
	candidates := make([]LogDeletionCandidate, 0, len(paths))
	for _, path := range paths {
		candidates = append(candidates, LogDeletionCandidate{Path: path, Category: LogCategoryDaemonWideGeneration})
	}
	return candidates
}

func samePaths(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for index := range got {
		if got[index] != want[index] {
			return false
		}
	}
	return true
}

func hasCandidate(candidates []LogDeletionCandidate, path string) bool {
	for _, candidate := range candidates {
		if candidate.Path == path {
			return true
		}
	}
	return false
}

func testPingServer(conn net.Conn) {
	defer conn.Close()
	var request transport.Request
	if json.NewDecoder(bufio.NewReader(conn)).Decode(&request) != nil {
		return
	}
	_ = json.NewEncoder(conn).Encode(transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: request.RequestID, OK: true})
}

func testLogPolicy() LogEvictionPolicy {
	return LogEvictionPolicy{RotationMaxSize: 1, RotationInterval: time.Second, RetentionDays: 1, RetentionCount: 1, Compress: true, MetricsRetention: 1}
}

func testLogPaths(t *testing.T) LogPaths {
	t.Helper()
	root := t.TempDir()
	return LogPaths{LogsRoot: root, CodexdLog: filepath.Join(root, "codexd.log"), RouteFallback: filepath.Join(root, "route.jsonl"), TaskLogsRoot: filepath.Join(root, "tasks"), SocketPath: filepath.Join(root, "codexd.sock"), LockPath: filepath.Join(root, "evict.lock")}
}

func testLogTaskID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260821-120000-a1b2-test")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func aliveLocks() *CheckLivenessUseCase {
	return NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(domain.TaskID) string { return "/tmp/task.lock" })
}

type logStoreStub struct {
	perTask         map[domain.TaskID][]string
	ages            map[string]int
	metrics         []string
	generations     map[string][]string
	compressed      map[string]string
	compressedCalls map[string]int
	compressErrs    map[string]error
	sizes           map[string]int64
	rotated         map[string]string
	lastRotation    map[string]time.Time
	ageErrs         map[string]error
	ageCalls        map[string]int
	removeErrs      map[string]error
	removeCalls     []string
	rotateErrs      map[string]error
	rotateCalls     []string
	reopenErrs      map[string]error
	reopenCalls     []string
	removes         int32
}

func (s *logStoreStub) Size(path string) (int64, error) {
	if size, ok := s.sizes[path]; ok {
		return size, nil
	}
	return 0, fs.ErrNotExist
}
func (s *logStoreStub) RotateNow(path string) (string, error) {
	s.rotateCalls = append(s.rotateCalls, path)
	if err := s.rotateErrs[path]; err != nil {
		return "", err
	}
	return s.rotated[path], nil
}
func (s *logStoreStub) ListRotatedGenerations(path string) ([]string, error) {
	return s.generations[path], nil
}
func (s *logStoreStub) CompressGeneration(path string) (string, error) {
	if s.compressedCalls == nil {
		s.compressedCalls = map[string]int{}
	}
	s.compressedCalls[path]++
	if err, ok := s.compressErrs[path]; ok {
		return "", err
	}
	if compressed, ok := s.compressed[path]; ok {
		return compressed, nil
	}
	return path + ".gz", nil
}
func (s *logStoreStub) AgeDays(path string, _ time.Time) (int, error) {
	if s.ageCalls == nil {
		s.ageCalls = map[string]int{}
	}
	s.ageCalls[path]++
	if err := s.ageErrs[path]; err != nil {
		return 0, err
	}
	return s.ages[path], nil
}
func (s *logStoreStub) ListMonthlyMetricsFiles(string) ([]string, error) { return s.metrics, nil }
func (s *logStoreStub) ListPerTaskLogFiles(string) (map[domain.TaskID][]string, error) {
	return s.perTask, nil
}
func (s *logStoreStub) Remove(path string) error {
	s.removeCalls = append(s.removeCalls, path)
	atomic.AddInt32(&s.removes, 1)
	return s.removeErrs[path]
}
func (s *logStoreStub) ReopenActiveHandle(path string) error {
	s.reopenCalls = append(s.reopenCalls, path)
	return s.reopenErrs[path]
}
func (s *logStoreStub) LastRotationAt(path string) (time.Time, error) {
	return s.lastRotation[path], nil
}
