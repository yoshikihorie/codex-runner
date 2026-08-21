package execution

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
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
	removes         int32
}

func (s *logStoreStub) Size(path string) (int64, error) {
	if size, ok := s.sizes[path]; ok {
		return size, nil
	}
	return 0, fs.ErrNotExist
}
func (s *logStoreStub) RotateNow(path string) (string, error) { return s.rotated[path], nil }
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
func (s *logStoreStub) AgeDays(path string, _ time.Time) (int, error)    { return s.ages[path], nil }
func (s *logStoreStub) ListMonthlyMetricsFiles(string) ([]string, error) { return s.metrics, nil }
func (s *logStoreStub) ListPerTaskLogFiles(string) (map[domain.TaskID][]string, error) {
	return s.perTask, nil
}
func (s *logStoreStub) Remove(string) error                      { atomic.AddInt32(&s.removes, 1); return nil }
func (s *logStoreStub) ReopenActiveHandle(string) error          { return nil }
func (s *logStoreStub) LastRotationAt(string) (time.Time, error) { return time.Now(), nil }
