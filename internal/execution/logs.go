package execution

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	storepkg "github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

const (
	logRotationFailedCode = "LOG_ROTATION_FAILED"

	// Canonical source: validation-rules.md PROTOCOL_LINE_MAX_BYTES.
	protocolLineMaxBytes = 1048576
)

const rotatedGenerationTimestampLayout = "20060102T150405.000000000Z"

var (
	monthlyMetricsLogName = regexp.MustCompile(
		`^task-metrics-[0-9]{4}-(0[1-9]|1[0-2])(?:\.(?:[2-9]|[1-9][0-9]+))?\.jsonl(?:\.gz)?$`,
	)
	perTaskLogNames = map[string]struct{}{
		"stdout.log":   {},
		"stderr.log":   {},
		"events.jsonl": {},
	}
)

type LogCategory string

const (
	LogCategoryDaemonWideGeneration LogCategory = "daemon-wide-generation"
	LogCategoryMonthlyMetrics       LogCategory = "monthly-metrics"
	LogCategoryPerTaskLog           LogCategory = "per-task-log"
)

type LogSkipReason string

const (
	LogSkipStillAlive          LogSkipReason = "still_alive"
	LogSkipLivenessCheckFailed LogSkipReason = "liveness_check_failed"
	LogSkipBelowAgeThreshold   LogSkipReason = "below_age_threshold"
	LogSkipRemoveFailed        LogSkipReason = "remove_failed"
	LogSkipRotationFailed      LogSkipReason = "rotation_failed"
	LogSkipDaemonStateUnknown  LogSkipReason = "daemon_state_unknown"
	LogSkipDaemonRunning       LogSkipReason = "daemon_running"
)

type LogDeletionCandidate struct {
	Path     string
	Category LogCategory
	TaskID   *domain.TaskID
}

type LogSkipped struct {
	Path   string
	Reason LogSkipReason
}

type EvictLogsInput struct {
	Trigger    string
	OccurredAt time.Time
}

type EvictLogsOutput struct {
	RotatedDaemonWide []string
	Candidates        []LogDeletionCandidate
	Deleted           []string
	Skipped           []LogSkipped
}

// LogStore is the execution boundary for the fixed set of log lifecycle operations.
type LogStore interface {
	Size(path string) (int64, error)
	RotateNow(path string) (string, error)
	ListRotatedGenerations(path string) ([]string, error)
	CompressGeneration(path string) (string, error)
	AgeDays(path string, now time.Time) (int, error)
	ListMonthlyMetricsFiles(dir string) ([]string, error)
	ListPerTaskLogFiles(root string) (map[domain.TaskID][]string, error)
	Remove(path string) error
	ReopenActiveHandle(path string) error
	LastRotationAt(path string) (time.Time, error)
}

type LogEvictionPolicy struct {
	RotationMaxSize  int64
	RotationInterval time.Duration
	RetentionDays    int
	RetentionCount   int
	Compress         bool
	MetricsRetention int
}

type LogPaths struct {
	LogsRoot      string
	CodexdLog     string
	RouteFallback string
	TaskLogsRoot  string
	SocketPath    string
	LockPath      string
}

type logTicker interface {
	C() <-chan time.Time
	Stop()
}
type logTickerFactory interface{ NewTicker(time.Duration) logTicker }
type realLogTickerFactory struct{}
type realLogTicker struct{ *time.Ticker }

func (realLogTickerFactory) NewTicker(d time.Duration) logTicker {
	return realLogTicker{time.NewTicker(d)}
}
func (t realLogTicker) C() <-chan time.Time { return t.Ticker.C }

type EvictLogsUseCase struct {
	store       LogStore
	locks       *CheckLivenessUseCase
	policy      LogEvictionPolicy
	paths       LogPaths
	logger      *slog.Logger
	dial        func(context.Context, string, string) (net.Conn, error)
	pingTimeout time.Duration
	tickers     logTickerFactory
	lockFactory func(string) *storepkg.FileMutex
	requestSeq  atomic.Uint64
}

func NewEvictLogsUseCase(logs LogStore, locks *CheckLivenessUseCase, policy LogEvictionPolicy, paths LogPaths, loggers ...*slog.Logger) (*EvictLogsUseCase, error) {
	if logs == nil || locks == nil {
		return nil, fmt.Errorf("log store and liveness use case must not be nil")
	}
	if policy.RotationMaxSize <= 0 || policy.RotationInterval <= 0 || policy.RetentionDays <= 0 || policy.RetentionCount <= 0 || policy.MetricsRetention <= 0 {
		return nil, fmt.Errorf("log eviction policy must be positive")
	}
	for _, path := range []string{paths.LogsRoot, paths.CodexdLog, paths.RouteFallback, paths.TaskLogsRoot, paths.SocketPath, paths.LockPath} {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
			return nil, fmt.Errorf("log paths must be normalized absolute paths")
		}
	}
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	dialer := &net.Dialer{}
	return &EvictLogsUseCase{store: logs, locks: locks, policy: policy, paths: paths, logger: logger, dial: dialer.DialContext, pingTimeout: time.Second, tickers: realLogTickerFactory{}, lockFactory: storepkg.NewFileMutex}, nil
}

func DefaultLogPaths() (LogPaths, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return LogPaths{}, fmt.Errorf("resolve user home directory: %w", err)
	}
	logs := filepath.Join(home, ".claude", "logs")
	return LogPaths{LogsRoot: logs, CodexdLog: filepath.Join(logs, "codexd.log"), RouteFallback: filepath.Join(logs, "route-fallback.jsonl"), TaskLogsRoot: taskPlacementRoot, SocketPath: filepath.Join(home, ".claude", "run", "codexd.sock"), LockPath: filepath.Join(home, ".claude", "run", "log-eviction.lock")}, nil
}

func validateEvictLogsInput(in EvictLogsInput) error {
	if in.Trigger != TriggerExplicit && in.Trigger != TriggerAutomatic {
		return fmt.Errorf("invalid trigger: %q", in.Trigger)
	}
	if in.OccurredAt.IsZero() {
		return fmt.Errorf("occurredAt must not be zero")
	}
	return nil
}

// Plan performs non-destructive candidate selection after any required rotation.
func (uc *EvictLogsUseCase) Plan(ctx context.Context, in EvictLogsInput) (EvictLogsOutput, error) {
	if err := validateEvictLogsInput(in); err != nil {
		return EvictLogsOutput{}, err
	}
	return uc.withEvictionLock(func() (EvictLogsOutput, error) {
		return uc.plan(ctx, in)
	})
}

func (uc *EvictLogsUseCase) withEvictionLock(run func() (EvictLogsOutput, error)) (out EvictLogsOutput, err error) {
	mutex := uc.lockFactory(uc.paths.LockPath)
	if err := mutex.Lock(); err != nil {
		return out, fmt.Errorf("lock log eviction: %w", err)
	}
	defer func() { err = errors.Join(err, mutex.Unlock()) }()
	return run()
}

func (uc *EvictLogsUseCase) Execute(ctx context.Context, in EvictLogsInput, confirmed []LogDeletionCandidate) (out EvictLogsOutput, err error) {
	if err := validateEvictLogsInput(in); err != nil {
		return out, err
	}
	if in.Trigger == TriggerAutomatic && confirmed != nil {
		return out, fmt.Errorf("automatic trigger must not receive confirmed candidates")
	}
	if in.Trigger == TriggerExplicit && confirmed == nil {
		return out, fmt.Errorf("explicit trigger requires confirmed candidates")
	}
	return uc.withEvictionLock(func() (EvictLogsOutput, error) {
		return uc.execute(ctx, in, confirmed)
	})
}

func (uc *EvictLogsUseCase) execute(ctx context.Context, in EvictLogsInput, confirmed []LogDeletionCandidate) (out EvictLogsOutput, err error) {
	if confirmed == nil {
		out, err = uc.plan(ctx, in)
		if err != nil {
			return out, err
		}
		deleted, skipped, deleteErr := uc.deleteCandidates(ctx, out.Candidates)
		out.Deleted, out.Skipped = deleted, append(out.Skipped, skipped...)
		return out, deleteErr
	}
	if err := uc.validateDeletionCandidates(confirmed); err != nil {
		return out, fmt.Errorf("invalid confirmed candidate: %w", err)
	}
	out.RotatedDaemonWide = []string{}
	out.Candidates = []LogDeletionCandidate{}
	out.Candidates = append(out.Candidates, confirmed...)
	deleted, skipped, err := uc.deleteCandidates(ctx, confirmed)
	out.Deleted, out.Skipped = deleted, skipped
	return out, err
}

func (uc *EvictLogsUseCase) plan(ctx context.Context, in EvictLogsInput) (EvictLogsOutput, error) {
	out := EvictLogsOutput{RotatedDaemonWide: []string{}, Candidates: []LogDeletionCandidate{}, Deleted: []string{}, Skipped: []LogSkipped{}}
	for _, path := range []string{uc.paths.CodexdLog, uc.paths.RouteFallback} {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		rotated, skipped := uc.rotateIfNeeded(ctx, in, path)
		if rotated != "" {
			out.RotatedDaemonWide = append(out.RotatedDaemonWide, path)
		}
		if skipped != nil {
			out.Skipped = append(out.Skipped, *skipped)
		}
	}
	for _, path := range []string{uc.paths.CodexdLog, uc.paths.RouteFallback} {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		generations, err := uc.store.ListRotatedGenerations(path)
		if err != nil {
			out.Skipped = append(out.Skipped, uc.skipFailure(path, LogSkipRotationFailed, err))
			continue
		}
		compressionFailed := make(map[string]struct{})
		if uc.policy.Compress && len(generations) > 1 {
			for _, generation := range generations[:len(generations)-1] {
				if err := ctx.Err(); err != nil {
					return out, err
				}
				if strings.HasSuffix(generation, ".gz") {
					continue
				}
				if _, err := uc.store.CompressGeneration(generation); err != nil {
					compressionFailed[generation] = struct{}{}
					out.Skipped = append(out.Skipped, uc.skipFailure(generation, LogSkipRotationFailed, err))
				}
			}
			generations, err = uc.store.ListRotatedGenerations(path)
			if err != nil {
				out.Skipped = append(out.Skipped, uc.skipFailure(path, LogSkipRotationFailed, err))
				continue
			}
		}
		for index, generation := range generations {
			if _, failed := compressionFailed[generation]; failed {
				continue
			}
			age, err := uc.store.AgeDays(generation, in.OccurredAt)
			if err != nil {
				out.Skipped = append(out.Skipped, uc.skipFailure(generation, LogSkipRotationFailed, err))
				continue
			}
			if age > uc.policy.RetentionDays || index < len(generations)-uc.policy.RetentionCount {
				out.Candidates = append(out.Candidates, LogDeletionCandidate{Path: generation, Category: LogCategoryDaemonWideGeneration})
			}
		}
	}
	metrics, err := uc.store.ListMonthlyMetricsFiles(uc.paths.LogsRoot)
	if err != nil {
		out.Skipped = append(out.Skipped, uc.skipFailure(uc.paths.LogsRoot, LogSkipRotationFailed, err))
	} else {
		for _, path := range metrics {
			age := monthAge(metricMonth(path), in.OccurredAt)
			if age <= 0 {
				continue
			}
			if uc.policy.Compress && !strings.HasSuffix(path, ".gz") {
				compressed, compressErr := uc.store.CompressGeneration(path)
				if compressErr != nil {
					out.Skipped = append(out.Skipped, uc.skipFailure(path, LogSkipRotationFailed, compressErr))
					continue
				}
				path = compressed
			}
			if age > uc.policy.MetricsRetention {
				out.Candidates = append(out.Candidates, LogDeletionCandidate{Path: path, Category: LogCategoryMonthlyMetrics})
			}
		}
	}
	perTask, err := uc.store.ListPerTaskLogFiles(uc.paths.TaskLogsRoot)
	if err != nil {
		out.Skipped = append(out.Skipped, uc.skipFailure(uc.paths.TaskLogsRoot, LogSkipLivenessCheckFailed, err))
		return out, nil
	}
	ids := make([]domain.TaskID, 0, len(perTask))
	for id := range perTask {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].String() < ids[j].String() })
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		dead, err := uc.locks.Execute(ctx, id)
		if err != nil {
			out.Skipped = append(out.Skipped, uc.skipFailure(filepath.Join(uc.paths.TaskLogsRoot, id.String()), LogSkipLivenessCheckFailed, err))
			continue
		}
		if !dead {
			for _, path := range perTask[id] {
				out.Skipped = append(out.Skipped, LogSkipped{Path: path, Reason: LogSkipStillAlive})
			}
			continue
		}
		for _, path := range perTask[id] {
			age, err := uc.store.AgeDays(path, in.OccurredAt)
			if err != nil {
				out.Skipped = append(out.Skipped, uc.skipFailure(path, LogSkipRotationFailed, err))
				continue
			}
			if age > uc.policy.RetentionDays {
				copied := id
				out.Candidates = append(out.Candidates, LogDeletionCandidate{Path: path, Category: LogCategoryPerTaskLog, TaskID: &copied})
			} else {
				out.Skipped = append(out.Skipped, LogSkipped{Path: path, Reason: LogSkipBelowAgeThreshold})
			}
		}
	}
	return out, nil
}

func (uc *EvictLogsUseCase) rotateIfNeeded(ctx context.Context, in EvictLogsInput, path string) (string, *LogSkipped) {
	size, err := uc.store.Size(path)
	if errors.Is(err, fs.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		s := uc.skipFailure(path, LogSkipRotationFailed, err)
		return "", &s
	}
	last, err := uc.store.LastRotationAt(path)
	if err != nil {
		s := uc.skipFailure(path, LogSkipRotationFailed, err)
		return "", &s
	}
	if size < uc.policy.RotationMaxSize && (last.IsZero() || in.OccurredAt.Before(last.Add(uc.policy.RotationInterval))) {
		return "", nil
	}
	if in.Trigger == TriggerExplicit && path == uc.paths.CodexdLog {
		state := uc.daemonState(ctx)
		if state != daemonStopped {
			reason := LogSkipDaemonStateUnknown
			if state == daemonRunning {
				reason = LogSkipDaemonRunning
			}
			return "", &LogSkipped{Path: path, Reason: reason}
		}
	}
	rotated, err := uc.store.RotateNow(path)
	if err != nil {
		s := uc.skipFailure(path, LogSkipRotationFailed, err)
		return "", &s
	}
	if in.Trigger == TriggerAutomatic && path == uc.paths.CodexdLog {
		if err := uc.store.ReopenActiveHandle(path); err != nil {
			s := uc.skipFailure(path, LogSkipRotationFailed, err)
			return rotated, &s
		}
	}
	return rotated, nil
}

func (uc *EvictLogsUseCase) deleteCandidates(ctx context.Context, candidates []LogDeletionCandidate) (deleted []string, skipped []LogSkipped, err error) {
	deleted = []string{}
	skipped = []LogSkipped{}
	if err := uc.validateDeletionCandidates(candidates); err != nil {
		return deleted, skipped, err
	}
	for _, candidate := range candidates {
		if err := ctx.Err(); err != nil {
			return deleted, skipped, err
		}
		if candidate.Category == LogCategoryPerTaskLog && candidate.TaskID != nil {
			lease, dead, leaseErr := uc.locks.AcquireDeathLease(*candidate.TaskID)
			if leaseErr != nil {
				skipped = append(skipped, uc.skipFailure(candidate.Path, LogSkipLivenessCheckFailed, leaseErr))
				continue
			}
			if !dead || lease == nil {
				skipped = append(skipped, LogSkipped{Path: candidate.Path, Reason: LogSkipStillAlive})
				continue
			}
			if lease != nil {
				removeErr := uc.store.Remove(candidate.Path)
				closeErr := lease.Close()
				if removeErr != nil {
					skipped = append(skipped, uc.skipFailure(candidate.Path, LogSkipRemoveFailed, removeErr))
				} else {
					deleted = append(deleted, candidate.Path)
				}
				if closeErr != nil {
					skipped = append(skipped, uc.skipFailure(candidate.Path, LogSkipRemoveFailed, closeErr))
				}
				continue
			}
		}
		if err := uc.store.Remove(candidate.Path); err != nil {
			skipped = append(skipped, uc.skipFailure(candidate.Path, LogSkipRemoveFailed, err))
			continue
		}
		deleted = append(deleted, candidate.Path)
	}
	return deleted, skipped, nil
}

func (uc *EvictLogsUseCase) validateDeletionCandidates(candidates []LogDeletionCandidate) error {
	for _, candidate := range candidates {
		if err := uc.validateDeletionCandidate(candidate); err != nil {
			return err
		}
	}
	return nil
}

func (uc *EvictLogsUseCase) validateDeletionCandidate(candidate LogDeletionCandidate) error {
	if candidate.Path == "" || !filepath.IsAbs(candidate.Path) || filepath.Clean(candidate.Path) != candidate.Path {
		return fmt.Errorf("candidate path must be normalized and absolute: %q", candidate.Path)
	}
	switch candidate.Category {
	case LogCategoryDaemonWideGeneration:
		if candidate.TaskID != nil || !uc.isDaemonWideGeneration(candidate.Path) {
			return fmt.Errorf("invalid daemon-wide generation candidate: %q", candidate.Path)
		}
	case LogCategoryMonthlyMetrics:
		if candidate.TaskID != nil || filepath.Dir(candidate.Path) != uc.paths.LogsRoot || !monthlyMetricsLogName.MatchString(filepath.Base(candidate.Path)) {
			return fmt.Errorf("invalid monthly metrics candidate: %q", candidate.Path)
		}
	case LogCategoryPerTaskLog:
		if candidate.TaskID == nil {
			return fmt.Errorf("per-task log candidate requires task ID: %q", candidate.Path)
		}
		if _, ok := perTaskLogNames[filepath.Base(candidate.Path)]; !ok || filepath.Dir(candidate.Path) != filepath.Join(uc.paths.TaskLogsRoot, candidate.TaskID.String()) {
			return fmt.Errorf("invalid per-task log candidate: %q", candidate.Path)
		}
	default:
		return fmt.Errorf("unknown log deletion category: %q", candidate.Category)
	}
	return nil
}

func (uc *EvictLogsUseCase) isDaemonWideGeneration(path string) bool {
	for _, active := range []string{uc.paths.CodexdLog, uc.paths.RouteFallback} {
		if filepath.Dir(path) != uc.paths.LogsRoot || filepath.Dir(path) != filepath.Dir(active) {
			continue
		}
		suffix := strings.TrimPrefix(filepath.Base(path), filepath.Base(active)+".")
		if suffix == filepath.Base(path) {
			continue
		}
		suffix = strings.TrimSuffix(suffix, ".gz")
		if _, err := time.Parse(rotatedGenerationTimestampLayout, suffix); err == nil {
			return true
		}
	}
	return false
}

func (uc *EvictLogsUseCase) skipFailure(path string, reason LogSkipReason, err error) LogSkipped {
	uc.logger.Warn("log lifecycle operation failed", "code", logRotationFailedCode, "path", path, "reason", reason, "error", err)
	return LogSkipped{Path: path, Reason: reason}
}

func metricMonth(path string) string {
	name := filepath.Base(path)
	name = strings.TrimSuffix(name, ".gz")
	name = strings.TrimSuffix(name, ".jsonl")
	name = strings.TrimPrefix(name, "task-metrics-")
	if index := strings.Index(name, "."); index >= 0 {
		name = name[:index]
	}
	return name
}
func monthAge(month string, now time.Time) int {
	parsed, err := time.Parse("2006-01", month)
	if err != nil {
		return -1
	}
	return (now.Year()-parsed.Year())*12 + int(now.Month()) - int(parsed.Month())
}

type daemonLiveness int

const (
	daemonUnknown daemonLiveness = iota
	daemonStopped
	daemonRunning
)

func (uc *EvictLogsUseCase) daemonState(ctx context.Context) daemonLiveness {
	if _, err := os.Lstat(uc.paths.SocketPath); errors.Is(err, fs.ErrNotExist) {
		return daemonStopped
	} else if err != nil {
		return daemonUnknown
	}
	timed, cancel := context.WithTimeout(ctx, uc.pingTimeout)
	defer cancel()
	conn, err := uc.dial(timed, "unix", uc.paths.SocketPath)
	if err != nil {
		return daemonUnknown
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(uc.pingTimeout)); err != nil {
		return daemonUnknown
	}
	id := fmt.Sprintf("log-eviction-%d", uc.requestSeq.Add(1))
	if err := json.NewEncoder(conn).Encode(transport.Request{ProtocolVersion: transport.ProtocolVersion, Verb: string(domain.ProtocolVerbPing), RequestID: id}); err != nil {
		return daemonUnknown
	}
	scanner := bufio.NewScanner(conn)
	// Reserve one byte so Scanner can inspect the terminating newline.
	scanner.Buffer(make([]byte, 64*1024), protocolLineMaxBytes+1)
	if !scanner.Scan() {
		return daemonUnknown
	}
	var response transport.Response
	if err := json.Unmarshal(scanner.Bytes(), &response); err != nil {
		return daemonUnknown
	}
	if response.OK && response.RequestID == id && response.ProtocolVersion == transport.ProtocolVersion {
		return daemonRunning
	}
	return daemonUnknown
}

// Run periodically triggers automatic eviction until ctx is cancelled.
func (uc *EvictLogsUseCase) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := uc.tickers.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case at := <-ticker.C():
			if ctx.Err() != nil {
				return
			}
			if _, err := uc.Execute(ctx, EvictLogsInput{Trigger: TriggerAutomatic, OccurredAt: at}, nil); err != nil {
				uc.logger.Warn("log eviction scan failed", "code", logRotationFailedCode, "error", err)
			}
		}
	}
}
