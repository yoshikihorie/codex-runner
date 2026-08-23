// Command codexd runs the foreground Unix-socket daemon.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/config"
	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/execution/usecase"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
	"github.com/yoshikihorie/codex-runner/internal/transport/client"
	transportusecase "github.com/yoshikihorie/codex-runner/internal/transport/usecase"
)

// daemonVersion is replaced by the release build without requiring a source edit.
var daemonVersion = "development"

var (
	runDaemonMode         = runMain
	newClientRequest      = client.NewRequest
	dialAndSend           = client.DialAndSend
	statsUserHomeDir      = os.UserHomeDir
	newStatsMetricsReader = func() store.MetricsReader { return store.NewFileMetricsReader() }
)

const (
	daemonLogFileName          = "codexd.log"
	daemonInstanceLockFileName = "codexd.lock"
	routeFallbackLogFileName   = "route-fallback.jsonl"
	logEvictionLockFileName    = "log-eviction.lock"
	codexVersionProbeTimeout   = 5 * time.Second
	daemonInstanceLockFilePerm = 0o600
)

const (
	machineCodeStatsInvalidDateRange        = "STATS_INVALID_DATE_RANGE"
	machineCodeStatsInvalidSubcommandFilter = "STATS_INVALID_SUBCOMMAND_FILTER"
)

const (
	// Canonical source: validation-rules.md SOCKET_CONNECT_TIMEOUT_SECONDS.
	clientDefaultConnectTimeoutSeconds = 5
	// Canonical source: validation-rules.md CLIENT_PING_TOTAL_TIMEOUT_SECONDS.
	clientPingTotalTimeoutSeconds = 5
	// Canonical source: validation-rules.md PROTOCOL_LINE_MAX_BYTES.
	clientProtocolLineMaxBytes int64 = 1_048_576
)

const (
	taskPlacementRoot = "/tmp/codex-tasks"
	reconcileInterval = time.Minute
	shutdownGrace     = 5 * time.Second
)

type versionResolver func(context.Context, string) (*string, error)

// resolveCodexVersion is intentionally replaceable in command tests. Version
// collection is optional metrics metadata and must never prevent startup.
var resolveCodexVersion versionResolver = resolveCodexCLIVersion

func startCodexVersionProbe(ctx context.Context, binary string, logger *slog.Logger) {
	go func() {
		version, err := resolveCodexVersion(ctx, binary)
		if err != nil || version == nil {
			logger.Warn("codex CLI version unavailable")
			return
		}
		logger.Info("codex CLI version resolved", "version", *version)
	}()
}

type afterFuncTimerFactory struct{}

func (afterFuncTimerFactory) AfterFunc(d time.Duration, f func()) execution.CancelFunc {
	return time.AfterFunc(d, f).Stop
}

type osStdoutFileOpener struct{}

func (osStdoutFileOpener) Open(path string) (*os.File, error) { return os.Open(path) }

// pidTerminator preserves process identity while waiting between SIGTERM and
// SIGKILL, so a recycled PID cannot receive the latter signal.
type pidTerminator struct {
	runner    execution.ProcessRunner
	watchExit func(int) (proc.ExitWatcher, error)
}

func (t pidTerminator) Terminate(pid int, grace time.Duration) error {
	terminateErr := t.runner.SendTerminate(pid)
	watchExit := t.watchExit
	if watchExit == nil {
		watchExit = proc.WatchExitWithoutReaping
	}
	watcher, watcherErr := watchExit(pid)
	if watcherErr != nil {
		killErr := t.runner.SendKill(pid)
		return errors.Join(terminateErr, watcherErr, killErr)
	}
	defer watcher.Close()

	graceCtx, cancel := context.WithTimeout(context.Background(), grace)
	waitErr := watcher.Wait(graceCtx)
	cancel()
	if waitErr == nil {
		return terminateErr
	}
	if !errors.Is(waitErr, context.DeadlineExceeded) {
		return errors.Join(terminateErr, waitErr)
	}

	killErr := t.runner.SendKill(pid)
	graceCtx, cancel = context.WithTimeout(context.Background(), grace)
	waitErr = watcher.Wait(graceCtx)
	cancel()
	if waitErr == nil {
		return errors.Join(terminateErr, killErr)
	}
	if errors.Is(waitErr, context.DeadlineExceeded) {
		return errors.Join(terminateErr, killErr, errors.New("process was not observed to exit after SIGKILL"))
	}
	return errors.Join(terminateErr, killErr, waitErr)
}

type deferredLifecycleRunner struct {
	target *usecase.TaskLifecycleOrchestrator
}

func (r *deferredLifecycleRunner) Run(ctx context.Context, input usecase.TaskLifecycleInput) {
	if r.target == nil {
		slog.Error("task lifecycle runner was not initialized")
		return
	}
	r.target.Run(ctx, input)
}

type serveResult struct {
	serveErr        error
	socketRemoveErr error
}

// daemonInstanceLease exclusively identifies the foreground daemon process.
// It uses a non-blocking flock so a duplicate invocation fails instead of
// waiting indefinitely for the existing daemon to exit.
type daemonInstanceLease struct {
	file *os.File
}

func acquireDaemonInstanceLease(path string) (*daemonInstanceLease, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, daemonInstanceLockFilePerm)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(daemonInstanceLockFilePerm); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &daemonInstanceLease{file: file}, nil
}

func (l *daemonInstanceLease) Unlock() error {
	if l == nil || l.file == nil {
		return errors.New("daemon instance lease is not locked")
	}
	file := l.file
	l.file = nil
	err := syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	closeErr := file.Close()
	return errors.Join(err, closeErr)
}

func resolveCodexCLIVersion(ctx context.Context, binary string) (*string, error) {
	probeCtx, cancel := context.WithTimeout(ctx, codexVersionProbeTimeout)
	defer cancel()
	command := exec.CommandContext(probeCtx, binary, "--version")
	command.Env = proc.SafeChildEnv()
	output, err := command.Output()
	if err != nil {
		return nil, err
	}
	version := strings.TrimSpace(string(output))
	if version == "" {
		return nil, fmt.Errorf("empty Codex CLI version")
	}
	return &version, nil
}

type daemonConfigArgs struct {
	configPath string
}

type reportedError struct {
	cause error
}

func (e *reportedError) Error() string { return e.cause.Error() }

func (e *reportedError) Unwrap() error { return e.cause }

func main() {
	os.Exit(runEntrypoint(context.Background(), os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func runEntrypoint(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "client" {
		return runClient(ctx, args[1:], stdin, stdout, stderr)
	}
	if len(args) > 0 && args[0] == "stats" {
		return runStats(ctx, args[1:], stdout, stderr)
	}
	if err := runDaemonMode(ctx, args, stderr); err != nil {
		reportMainError(stderr, err)
		return 1
	}
	return 0
}

func runStats(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	_ = ctx
	flags := flag.NewFlagSet("stats", flag.ContinueOnError)
	flags.SetOutput(stderr)
	since := flags.String("since", "", "start month (YYYY-MM)")
	until := flags.String("until", "", "end month (YYYY-MM)")
	subcommand := flags.String("subcommand", "", "subcommand")
	jsonOut := flags.Bool("json", false, "output JSON")
	if err := flags.Parse(args); err != nil {
		return 1
	}
	if len(flags.Args()) != 0 {
		fmt.Fprintf(stderr, "stats: unexpected positional arguments: %v\n", flags.Args())
		return 1
	}
	query := buildStatsQueryFromFlags(optionalStatsFlag(since, specifiedFlags(flags)["since"]), optionalStatsFlag(until, specifiedFlags(flags)["until"]), optionalStatsFlag(subcommand, specifiedFlags(flags)["subcommand"]), *jsonOut)
	if !validStatsMonth(query.Since) || !validStatsMonth(query.Until) || (query.Since != nil && query.Until != nil && *query.Until < *query.Since) {
		fmt.Fprintf(stderr, "%s %s\n", machineCodeStatsInvalidDateRange, metrics.MessageKeyStatsInvalidDateRange)
		return 1
	}
	if query.SubcommandFilter != nil && !isStatsSubcommand(*query.SubcommandFilter) {
		fmt.Fprintf(stderr, "%s %s\n", machineCodeStatsInvalidSubcommandFilter, metrics.MessageKeyStatsInvalidSubcommand)
		return 1
	}
	home, err := statsUserHomeDir()
	if err != nil {
		reportMainError(stderr, err)
		return 1
	}
	report, err := metrics.NewComputeTaskStatsUseCase(newStatsMetricsReader(), filepath.Join(home, ".claude", "logs")).Execute(query)
	if err != nil {
		reportMainError(stderr, err)
		return 1
	}
	if query.JSON {
		encoded, err := json.Marshal(report)
		if err == nil {
			_, err = fmt.Fprintln(stdout, string(encoded))
		}
		if err != nil {
			reportMainError(stderr, err)
			return 1
		}
		return 0
	}
	if err := writeStatsText(stdout, report); err != nil {
		reportMainError(stderr, err)
		return 1
	}
	return 0
}

func optionalStatsFlag(value *string, specified bool) *string {
	if !specified {
		return nil
	}
	return value
}

func buildStatsQueryFromFlags(since, until, subcommand *string, jsonOut bool) metrics.StatsQuery {
	query := metrics.StatsQuery{Since: since, Until: until, JSON: jsonOut}
	if subcommand != nil {
		value := domain.Subcommand(*subcommand)
		query.SubcommandFilter = &value
	}
	return query
}

func validStatsMonth(value *string) bool {
	if value == nil || len(*value) != len("2006-01") || (*value)[4] != '-' {
		return value == nil
	}
	for _, index := range []int{0, 1, 2, 3} {
		if (*value)[index] < '0' || (*value)[index] > '9' {
			return false
		}
	}
	return ((*value)[5] == '0' && (*value)[6] >= '1' && (*value)[6] <= '9') || ((*value)[5] == '1' && (*value)[6] >= '0' && (*value)[6] <= '2')
}

func isStatsSubcommand(value domain.Subcommand) bool {
	switch value {
	case domain.SubcommandImpl, domain.SubcommandReview, domain.SubcommandPlan, domain.SubcommandResearch, domain.SubcommandRead, domain.SubcommandStatus, domain.SubcommandLogs, domain.SubcommandCancel, domain.SubcommandDoctor, domain.SubcommandCleanup, domain.SubcommandStats:
		return true
	default:
		return false
	}
}

func writeStatsText(out io.Writer, report metrics.StatsReport) error {
	lines := []string{
		fmt.Sprintf("matched_files: %d", report.MatchedFiles), fmt.Sprintf("total_records: %d", report.TotalRecords),
		"queue_wait_median: " + statsInt(report.QueueWaitMedian), "queue_wait_p95: " + statsInt(report.QueueWaitP95),
		"startup_median: " + statsInt(report.StartupMedian), "startup_p95: " + statsInt(report.StartupP95),
		"execution_median: " + statsInt(report.ExecutionMedian), "execution_p95: " + statsInt(report.ExecutionP95),
		"prompt_length_to_output_length_correlation: " + statsFloat(report.PromptLengthToOutputLengthCorrelation), "prompt_length_to_output_tokens_correlation: " + statsFloat(report.PromptLengthToOutputTokensCorrelation),
		"max_event_gap_median: " + statsFloat(report.MaxEventGapMedian), "max_event_gap_p95: " + statsFloat(report.MaxEventGapP95), "max_event_gap_max: " + statsFloat(report.MaxEventGapMax),
		fmt.Sprintf("timeout_count: %d", report.TimeoutCount), fmt.Sprintf("recovery_attempted_count: %d", report.RecoveryAttemptedCount), fmt.Sprintf("recovery_succeeded_count: %d", report.RecoverySucceededCount), "recovery_success_rate: " + statsFloat(report.RecoverySuccessRate),
		"success_rate_by_subcommand:",
	}
	keys := make([]string, 0, len(report.SuccessRateBySubcommand))
	for key := range report.SuccessRateBySubcommand {
		keys = append(keys, string(key))
	}
	sort.Strings(keys)
	for _, key := range keys {
		lines = append(lines, fmt.Sprintf("  %s: %s", key, statsSuccess(report.SuccessRateBySubcommand[domain.Subcommand(key)])))
	}
	lines = append(lines, "success_rate_by_model:")
	keys = keys[:0]
	for key := range report.SuccessRateByModel {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		lines = append(lines, fmt.Sprintf("  %s: %s", key, statsSuccess(report.SuccessRateByModel[key])))
	}
	if report.SkippedLines > 0 {
		lines = append(lines, fmt.Sprintf("%s: %d", metrics.MessageKeyStatsSkippedLines, report.SkippedLines))
	}
	for _, line := range lines {
		if _, err := fmt.Fprintln(out, line); err != nil {
			return err
		}
	}
	return nil
}
func statsInt(value *int) string {
	if value == nil {
		return "null"
	}
	return fmt.Sprintf("%d", *value)
}
func statsFloat(value *float64) string {
	if value == nil {
		return "null"
	}
	return fmt.Sprintf("%v", *value)
}
func statsSuccess(value metrics.SuccessStat) string {
	return fmt.Sprintf("total=%d success=%d rate=%s", value.Total, value.Success, statsFloat(value.Rate))
}

type clientConfig struct {
	socketPath string
	timeouts   client.Timeouts
}

func runClient(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		return clientUsageError(stderr, "client verb is required")
	}

	switch args[0] {
	case string(domain.ProtocolVerbSubmit):
		return runSubmitClient(ctx, args[1:], stdin, stdout, stderr)
	case string(domain.ProtocolVerbTail):
		return runTaskClient(ctx, domain.ProtocolVerbTail, args[1:], stdout, stderr)
	case string(domain.ProtocolVerbStatus):
		return runTaskClient(ctx, domain.ProtocolVerbStatus, args[1:], stdout, stderr)
	case string(domain.ProtocolVerbCancel):
		return runCancelClient(ctx, args[1:], stdout, stderr)
	case string(domain.ProtocolVerbPing):
		return runPingClient(ctx, args[1:], stdout, stderr)
	default:
		return clientUsageError(stderr, "unknown client verb")
	}
}

func runSubmitClient(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	flags, socketPath, connectTimeout := newClientFlagSet("submit", stderr)
	subcommand := flags.String("subcommand", "", "subcommand")
	model := flags.String("model", "", "model")
	timeout := flags.Int("timeout", 0, "timeout seconds")
	requestFile := flags.String("request-file", "", "request JSON file")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if len(flags.Args()) != 0 || *subcommand == "" {
		return clientUsageError(stderr, "submit requires --subcommand and no positional arguments")
	}
	specified := specifiedFlags(flags)
	var input io.Reader = stdin
	if specified["request-file"] {
		if *requestFile == "" || !filepath.IsAbs(*requestFile) {
			return clientUsageError(stderr, "request file path must be absolute")
		}
		file, err := os.Open(*requestFile)
		if err != nil {
			return clientUsageError(stderr, "open request file")
		}
		defer file.Close()
		input = file
	}
	params, err := decodeClientParams(input)
	if err != nil {
		return clientUsageError(stderr, "invalid submit params")
	}
	params["subcommand"], _ = json.Marshal(*subcommand)
	if specified["model"] {
		params["model"], _ = json.Marshal(*model)
	}
	if specified["timeout"] {
		params["requested_timeout_seconds"], _ = json.Marshal(*timeout)
	}
	rawParams, err := json.Marshal(params)
	if err != nil {
		return clientUsageError(stderr, "encode submit params")
	}
	cfg, err := resolveClientConfig(*socketPath, *connectTimeout, specified["socket-path"])
	if err != nil {
		return clientUsageError(stderr, err.Error())
	}
	return sendClientRequest(ctx, cfg, domain.ProtocolVerbSubmit, "", rawParams, stdout, stderr)
}

func runTaskClient(ctx context.Context, verb domain.ProtocolVerb, args []string, stdout, stderr io.Writer) int {
	flags, socketPath, connectTimeout := newClientFlagSet(string(verb), stderr)
	taskID := flags.String("task-id", "", "task ID")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if len(flags.Args()) != 0 || *taskID == "" {
		return clientUsageError(stderr, "task command requires --task-id and no positional arguments")
	}
	cfg, err := resolveClientConfig(*socketPath, *connectTimeout, specifiedFlags(flags)["socket-path"])
	if err != nil {
		return clientUsageError(stderr, err.Error())
	}
	return sendClientRequest(ctx, cfg, verb, *taskID, nil, stdout, stderr)
}

func runCancelClient(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags, socketPath, connectTimeout := newClientFlagSet("cancel", stderr)
	taskID := flags.String("task-id", "", "task ID")
	force := flags.Bool("force", false, "force cancellation")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if len(flags.Args()) != 0 || *taskID == "" {
		return clientUsageError(stderr, "cancel requires --task-id and no positional arguments")
	}
	params, err := json.Marshal(map[string]bool{"force": *force})
	if err != nil {
		return clientUsageError(stderr, "encode cancel params")
	}
	cfg, err := resolveClientConfig(*socketPath, *connectTimeout, specifiedFlags(flags)["socket-path"])
	if err != nil {
		return clientUsageError(stderr, err.Error())
	}
	return sendClientRequest(ctx, cfg, domain.ProtocolVerbCancel, *taskID, params, stdout, stderr)
}

func runPingClient(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags, socketPath, connectTimeout := newClientFlagSet("ping", stderr)
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if len(flags.Args()) != 0 {
		return clientUsageError(stderr, "ping accepts no positional arguments")
	}
	cfg, err := resolveClientConfig(*socketPath, *connectTimeout, specifiedFlags(flags)["socket-path"])
	if err != nil {
		return clientUsageError(stderr, err.Error())
	}
	return sendClientRequest(ctx, cfg, domain.ProtocolVerbPing, "", nil, stdout, stderr)
}

func newClientFlagSet(name string, stderr io.Writer) (*flag.FlagSet, *string, *int) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	socketPath := flags.String("socket-path", "", "absolute socket path")
	connectTimeout := flags.Int("connect-timeout-seconds", clientDefaultConnectTimeoutSeconds, "connection timeout in seconds")
	return flags, socketPath, connectTimeout
}

func resolveClientConfig(socketPath string, connectTimeout int, socketPathSpecified bool) (clientConfig, error) {
	if socketPath == "" {
		if socketPathSpecified {
			return clientConfig{}, errors.New("socket path must be absolute")
		}
		var err error
		socketPath, err = config.DefaultSocketPath()
		if err != nil {
			return clientConfig{}, fmt.Errorf("resolve default socket path: %w", err)
		}
	}
	if !filepath.IsAbs(socketPath) {
		return clientConfig{}, errors.New("socket path must be absolute")
	}
	if connectTimeout <= 0 {
		return clientConfig{}, errors.New("connect timeout must be positive")
	}
	if int64(connectTimeout) > int64(time.Duration(1<<63-1)/time.Second) {
		return clientConfig{}, errors.New("connect timeout is too large")
	}
	return clientConfig{
		socketPath: socketPath,
		timeouts: client.Timeouts{
			Connect:   time.Duration(connectTimeout) * time.Second,
			PingTotal: clientPingTotalTimeoutSeconds * time.Second,
		},
	}, nil
}

func specifiedFlags(flags *flag.FlagSet) map[string]bool {
	specified := make(map[string]bool)
	flags.Visit(func(item *flag.Flag) { specified[item.Name] = true })
	return specified
}

func decodeClientParams(input io.Reader) (map[string]json.RawMessage, error) {
	contents, err := io.ReadAll(io.LimitReader(input, clientProtocolLineMaxBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(contents)) > clientProtocolLineMaxBytes || len(strings.TrimSpace(string(contents))) == 0 {
		return nil, errors.New("request params must be a non-empty JSON object within the protocol limit")
	}
	decoder := json.NewDecoder(strings.NewReader(string(contents)))
	params := make(map[string]json.RawMessage)
	if err := decoder.Decode(&params); err != nil || params == nil {
		return nil, errors.New("request params must be a JSON object")
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, errors.New("request params must contain one JSON value")
	}
	return params, nil
}

func sendClientRequest(ctx context.Context, cfg clientConfig, verb domain.ProtocolVerb, taskID string, params json.RawMessage, stdout, stderr io.Writer) int {
	req, err := newClientRequest(verb, taskID, params)
	if err != nil {
		return clientUsageError(stderr, "create client request")
	}
	_, code, err := dialAndSend(ctx, cfg.socketPath, cfg.timeouts, req, stdout)
	if err != nil {
		fmt.Fprintln(stderr, "client communication failed")
	}
	return code
}

func clientUsageError(stderr io.Writer, message string) int {
	fmt.Fprintln(stderr, message)
	return 2
}

func reportMainError(stderr io.Writer, err error) {
	var reported *reportedError
	if !errors.As(err, &reported) {
		fmt.Fprintln(stderr, err)
	}
}

// runMain owns the startup boundary.  Dependency construction is deliberately
// below configuration and filesystem validation so an invalid environment can
// never create task state or open a listener.
func runMain(ctx context.Context, args []string, stderr io.Writer) error {
	home, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("resolve user home directory: %w", err)
	}
	logsDir := filepath.Join(home, ".claude", "logs")
	if err := ensureManagedPrivateDir(logsDir); err != nil {
		return err
	}
	logWriter, err := openDaemonLog(logsDir)
	if err != nil {
		return err
	}
	defer logWriter.Close()
	logger := slog.New(slog.NewJSONHandler(logWriter, nil))
	restoreDefaultLogger := installDefaultLogger(logger)
	defer restoreDefaultLogger()

	cfg, err := loadConfig(args)
	if err != nil {
		logger.Error("configuration load failed", safeConfigErrorAttributes(err)...)
		fmt.Fprintln(stderr, safeConfigErrorMessage(err))
		return &reportedError{cause: err}
	}
	managedRunDir := filepath.Join(home, ".claude", "run")
	if err := ensureManagedPrivateDir(managedRunDir); err != nil {
		return err
	}
	daemonLease, err := acquireDaemonInstanceLease(filepath.Join(managedRunDir, daemonInstanceLockFileName))
	if err != nil {
		logger.Error("acquire daemon instance lock", "error", err)
		fmt.Fprintln(stderr, "another codexd process may already be running")
		return &reportedError{cause: fmt.Errorf("acquire daemon instance lock (another codexd may be running): %w", err)}
	}
	defer func() { _ = daemonLease.Unlock() }()
	if err := ensureManagedPrivateDir(taskPlacementRoot); err != nil {
		return err
	}
	if err := ensureSocketParent(filepath.Dir(cfg.SocketPath()), managedRunDir); err != nil {
		return err
	}

	shutdownCtx, stopSignals := proc.NewShutdownContext(ctx)
	defer stopSignals()
	baseCtx, cancelBase := context.WithCancel(shutdownCtx)
	deps, err := buildDependencies(baseCtx, cfg, home, logsDir, logWriter.Reopen, logger)
	if err != nil {
		cancelBase()
		return err
	}
	if _, err := deps.adoption.Execute(baseCtx); err != nil {
		cancelBase()
		return errors.Join(fmt.Errorf("adopt running tasks: %w", err), deps.watcher.Close())
	}
	startCodexVersionProbe(baseCtx, cfg.CodexBinaryPath(), logger)

	serveResultCh := make(chan serveResult, 1)
	go func() { serveResultCh <- deps.serve(baseCtx) }()
	if err := waitForUnixListener(baseCtx, cfg.SocketPath(), serveResultCh); err != nil {
		cancelBase()
		result := <-serveResultCh
		return errors.Join(err, result.serveErr, result.socketRemoveErr, deps.watcher.Close())
	}
	var background sync.WaitGroup
	startBackground := func(run func(context.Context)) {
		background.Add(1)
		go func() { defer background.Done(); run(baseCtx) }()
	}
	startBackground(deps.stall.Run)
	startBackground(deps.reconcile.Run)
	evictLogsInterval := time.Duration(cfg.LogEvictionScanIntervalSeconds()) * time.Second
	startBackground(func(ctx context.Context) { deps.evictLogs.Run(ctx, evictLogsInterval) })

	var result serveResult
	serveReturned := false
	select {
	case <-shutdownCtx.Done():
	case result = <-serveResultCh:
		serveReturned = true
	}
	deps.shutdownStarter(context.Background())
	cancelBase()
	deps.finalizer.Finalize(shutdownGrace)
	if !serveReturned {
		result = <-serveResultCh
	}
	background.Wait()
	return errors.Join(result.serveErr, result.socketRemoveErr, deps.watcher.Close())
}

type daemonDependencies struct {
	adoption        *recovery.AdoptRunningTasksUseCase
	stall           interface{ Run(context.Context) }
	reconcile       *recovery.ReconcilePendingUseCase
	evictLogs       *execution.EvictLogsUseCase
	watcher         *execution.TimeoutWatcher
	starter         execution.TaskLifecycleStarter
	shutdownStarter func(context.Context)
	finalizer       transport.ShutdownFinalizer
	serve           func(context.Context) serveResult
}

// buildDependencies constructs every stateful collaborator once and shares the
// resulting instance through each use case that needs it.
func buildDependencies(baseCtx context.Context, cfg config.Config, home, logsDir string, reopenLog func(string) error, logger *slog.Logger) (daemonDependencies, error) {
	clock := domain.ClockFunc(time.Now)
	notifier := execution.NewTaskChangeNotifier()
	rawTasks, err := store.NewFileTaskStore(taskPlacementRoot)
	if err != nil {
		return daemonDependencies{}, err
	}
	tasks := execution.NewNotifyingTaskStore(rawTasks, notifier)
	writer := execution.NewNotifyingContractWriter(contract.NewFileContractWriter(taskPlacementRoot, clock), notifier)
	reader := store.NewFileContractReader(taskPlacementRoot)
	events := store.NewFileEventReader(taskPlacementRoot)
	taskMu := store.NewTaskMutex()
	queueMu := &sync.Mutex{}
	queue := execution.NewTaskQueue()
	registry := execution.NewActiveTaskRegistry()
	launching := execution.NewLaunchingTaskRegistry()
	ownership := execution.NewLifecycleOwnershipRegistry()
	pending := &recovery.PendingReconciliationSet{}
	pathLocksDir, pathLocksMutexPath, err := defaultPathLockLocations()
	if err != nil {
		return daemonDependencies{}, err
	}
	pathStore := store.NewPathLockFileStore(pathLocksDir)
	livenessLock := domain.LivenessLockFunc(store.TryAcquireLiveness)
	liveness := execution.NewCheckLivenessUseCase(livenessLock, execution.DefaultLockPathResolver)
	evictLogs, err := newEvictLogsUseCase(cfg, home, logsDir, reopenLog, liveness, logger)
	if err != nil {
		return daemonDependencies{}, err
	}
	pathAcquire := execution.NewAcquirePathLockUseCase(store.NewFileMutex(pathLocksMutexPath), pathStore, livenessLock, store.NormalizePath, tasks, logger)
	pathRelease := execution.NewReleasePathLockUseCase(pathStore, logger)
	processRunner := execution.NewProcessRunner(writer)
	validator := recovery.NewProcessSignalAuthorityValidator(tasks, taskMu, ownership)
	termination := execution.NewTerminationEnsurer(liveness, processRunner, clock, waitForContext, validator)
	metricRecorder := metrics.NewRecordTaskMetricsUseCase(tasks, events, reader, metrics.NewFileMetricsWriter(logsDir, cfg.MetricsMaxFileBytes()), cfg.MetricsRecordContentEnabled(), clock, daemonVersion, nil, logger)
	stalled := &metrics.StalledTimeTracker{}
	runner := &deferredLifecycleRunner{}
	starterConcrete, err := usecase.NewTaskLifecycleStarter(runner, taskPlacementRoot, baseCtx, clock, logger)
	if err != nil {
		return daemonDependencies{}, err
	}
	starter := execution.TaskLifecycleStarter(starterConcrete)
	advance := usecase.NewAdvanceQueueUseCase(queue, registry, launching, queueMu, cfg.MaxConcurrentTasks(), cfg.MaxConcurrentImplTasks())
	slots := usecase.NewSlotReleaser(advance, starter, logger)
	partial := recovery.NewSavePartialOutputUseCase(reader, writer, logger)
	resume := recovery.NewRecoverViaResumeUseCase(tasks, writer, recovery.NewResumeRecoverer(execution.NewResumeLauncher(processRunner, logger), reader, cfg.CodexBinaryPath(), clock), partial, slots, metricRecorder, stalled, taskMu, clock, logger)
	enforce := execution.NewEnforceTaskTimeoutUseCase(tasks, writer, processRunner, resume, termination, validator, pending, pathRelease, taskMu, clock, stalled)
	watcher := execution.NewTimeoutWatcher(enforce, clock, afterFuncTimerFactory{}, baseCtx, logger)
	finalize := execution.NewFinalizeTaskUseCase(tasks, writer, reader, clock, taskMu, slots, watcher, metricRecorder, stalled, logger)
	killed := execution.NewConfirmTaskKilledUseCase(tasks, writer, reader, taskMu, watcher, pathRelease, slots, clock, metricRecorder, stalled, pending, logger)
	failLaunch := usecase.NewFailTaskLaunchUseCase(tasks, taskMu, writer, reader, slots, usecase.NewPathLockReleaser(pathRelease), clock, logger)
	worktree, err := execution.NewCreateWorktreeUseCase(store.NewWorktreeFileStore(), filepath.Join(home, ".codex-worktrees-cli"))
	if err != nil {
		return daemonDependencies{}, err
	}
	orchestrator, err := usecase.NewTaskLifecycleOrchestrator(usecase.TaskLifecycleDependencies{
		AcquireForChild: execution.AcquireForChild, RecordStarting: usecase.NewRecordTaskStartingUseCase(tasks, writer, logger), CreateWorktree: worktree,
		Launch: usecase.NewLaunchWithPTYUseCase(processRunner), RecordProcess: usecase.NewRecordTaskProcessUseCase(tasks, writer, logger), FailLaunch: failLaunch,
		ConfirmRunning: usecase.NewConfirmTaskRunningUseCase(tasks, taskMu, liveness, writer, logger), CheckLiveness: liveness, TimeoutArmer: watcher,
		Monitor: usecase.NewMonitorTaskEventsUseCase(execution.NewEventMonitor(), tasks, taskMu, writer, clock, stalled, logger), Finalize: finalize, ConfirmKilled: killed,
		Tasks: tasks, TaskMu: taskMu, Termination: termination, Terminator: pidTerminator{runner: processRunner}, Pending: pending, Ownership: ownership, Launching: launching,
		Changes: notifier, OpenStdout: osStdoutFileOpener{}, Clock: clock,
	}, usecase.TaskLifecycleLaunchConfig{CodexBinaryPath: cfg.CodexBinaryPath(), PTYEnabled: cfg.PtyEnabled()}, logger)
	if err != nil {
		return daemonDependencies{}, err
	}
	runner.target = orchestrator
	admit := usecase.NewAdmitTaskUseCase(queue, registry, launching, queueMu, cfg.MaxConcurrentTasks(), cfg.MaxConcurrentImplTasks(), cfg.QueueMaxDepth())
	provider := execution.NewTaskSnapshotProvider(tasks, launching, queue, queueMu)
	submit := transportusecase.NewSubmitTaskUseCase(tasks, pathAcquire, pathRelease, admit, cfg.QueueMaxDepth(), starter, cfg, clock, logger)
	status := transportusecase.NewGetTaskStatusUseCase(provider, clock, logger)
	cancel := transportusecase.NewCancelTaskUseCase(tasks, queue, queueMu, taskMu, writer, processRunner, termination, pending, watcher, killed, stalled, ownership, clock, logger)
	dispatch, err := transport.NewDispatcher(submit.Handle, status.Handle, cancel.Handle, (&transportusecase.PingUseCase{}).Handle)
	if err != nil {
		return daemonDependencies{}, err
	}
	adoption := recovery.NewAdoptRunningTasksUseCase(tasks, liveness, reader, writer, finalize, resume, slots, registry, termination, killed, pathRelease, pending, taskMu, clock, stalled, metricRecorder, logger)
	reconcile := recovery.NewReconcilePendingUseCase(pending, tasks, liveness, reader, writer, finalize, termination, killed, pathRelease, resume, slots, taskMu, clock, stalled, metricRecorder, reconcileInterval, execution.TimeoutKillGrace, logger)
	stall := usecase.NewCheckStallUseCase(tasks, taskMu, liveness, writer, clock, stalled, ownership, logger)
	connections := &sync.WaitGroup{}
	tailConns := transport.NewTailConnRegistry()
	acceptDone := make(chan struct{})
	tail := transportusecase.NewTailTaskUseCase(provider, events, notifier)
	serve := func(ctx context.Context) serveResult {
		result := serveResult{serveErr: transport.Serve(ctx, cfg.SocketPath(), dispatch.Dispatch, tail.Handle, connections, tailConns, acceptDone)}
		if err := os.Remove(cfg.SocketPath()); !errors.Is(err, fs.ErrNotExist) {
			result.socketRemoveErr = err
		}
		return result
	}
	return daemonDependencies{adoption: adoption, stall: stall, reconcile: reconcile, evictLogs: evictLogs, watcher: watcher, starter: starter, shutdownStarter: starterConcrete.Shutdown, finalizer: transport.NewShutdownFinalizer(connections, tailConns, acceptDone), serve: serve}, nil
}

func newEvictLogsUseCase(cfg config.Config, home, logsDir string, reopenLog func(string) error, liveness *execution.CheckLivenessUseCase, logger *slog.Logger) (*execution.EvictLogsUseCase, error) {
	policy := execution.LogEvictionPolicy{
		RotationMaxSize:  cfg.LogRotationMaxSizeBytes(),
		RotationInterval: time.Duration(cfg.LogRotationIntervalSeconds()) * time.Second,
		RetentionDays:    cfg.LogRotationRetentionDays(),
		RetentionCount:   cfg.LogRotationRetentionCount(),
		Compress:         cfg.LogRotationCompress(),
		MetricsRetention: cfg.MetricsRetentionMonths(),
	}
	paths := execution.LogPaths{
		LogsRoot:      logsDir,
		CodexdLog:     filepath.Join(logsDir, daemonLogFileName),
		RouteFallback: filepath.Join(logsDir, routeFallbackLogFileName),
		TaskLogsRoot:  taskPlacementRoot,
		SocketPath:    cfg.SocketPath(),
		LockPath:      filepath.Join(home, ".claude", "run", logEvictionLockFileName),
	}
	return execution.NewEvictLogsUseCase(store.NewFileLogStore(reopenLog), liveness, policy, paths, logger)
}

func waitForContext(ctx context.Context, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// waitForUnixListener is the post-Serve synchronization gate. It observes both
// cancellation and a server that failed before publishing its listener.
func waitForUnixListener(ctx context.Context, socketPath string, served chan serveResult) error {
	for {
		conn, err := net.DialTimeout("unix", socketPath, 25*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-served:
			served <- result
			return errors.Join(result.serveErr, result.socketRemoveErr)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func loadConfig(args []string) (config.Config, error) {
	parsed, err := parseDaemonConfigArgs(args)
	if err != nil {
		return config.Config{}, err
	}
	if parsed.configPath == "" {
		return config.LoadDefault()
	}
	return config.LoadExplicit(parsed.configPath)
}

func parseDaemonConfigArgs(args []string) (daemonConfigArgs, error) {
	if len(args) == 0 {
		return daemonConfigArgs{}, nil
	}
	if len(args) != 2 || args[0] != "--config" || !filepath.IsAbs(args[1]) {
		return daemonConfigArgs{}, fmt.Errorf("invalid daemon arguments")
	}
	return daemonConfigArgs{configPath: args[1]}, nil
}

type daemonLogWriter struct {
	mu   sync.RWMutex
	file *os.File
}

func openDaemonLog(logsDir string) (*daemonLogWriter, error) {
	file, err := openDaemonLogFile(filepath.Join(logsDir, daemonLogFileName))
	if err != nil {
		return nil, err
	}
	return &daemonLogWriter{file: file}, nil
}

func openDaemonLogFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open daemon log: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure daemon log: %w", err)
	}
	return file, nil
}

func (w *daemonLogWriter) Write(p []byte) (int, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if w.file == nil {
		return 0, os.ErrClosed
	}
	return w.file.Write(p)
}

func (w *daemonLogWriter) Reopen(path string) error {
	file, err := openDaemonLogFile(path)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	old := w.file
	w.file = file
	if old == nil {
		return nil
	}
	return old.Close()
}

func (w *daemonLogWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

func installDefaultLogger(logger *slog.Logger) func() {
	previousLogger := slog.Default()
	slog.SetDefault(logger)
	return func() { slog.SetDefault(previousLogger) }
}

func defaultPathLockLocations() (dir, mutexPath string, err error) {
	dir, err = store.DefaultPathLocksDir()
	if err != nil {
		return "", "", err
	}
	mutexPath, err = store.DefaultPathLocksMutexPath()
	if err != nil {
		return "", "", err
	}
	return dir, mutexPath, nil
}

func ensureManagedPrivateDir(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("create runtime directory: %w", err)
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return fmt.Errorf("inspect runtime directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("runtime path is not a real directory")
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open runtime directory safely: %w", err)
	}
	dir := os.NewFile(uintptr(fd), path)
	defer dir.Close()
	if err := dir.Chmod(0o700); err != nil {
		return fmt.Errorf("secure runtime directory: %w", err)
	}
	return nil
}

func ensureSocketParent(parent, managedRunDir string) error {
	if filepath.Clean(parent) == filepath.Clean(managedRunDir) {
		return ensureManagedPrivateDir(parent)
	}
	info, err := os.Lstat(parent)
	if err != nil {
		return fmt.Errorf("inspect socket parent: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("socket parent is not a real directory")
	}
	if info.Mode().Perm() != 0o700 {
		return fmt.Errorf("socket parent must already have permission 0700")
	}
	return nil
}

func safeConfigErrorAttributes(err error) []any {
	var loadErr *config.LoadError
	if errors.As(err, &loadErr) && loadErr.Key != "" {
		return []any{"key", loadErr.Key}
	}
	return nil
}

func safeConfigErrorMessage(err error) string {
	var loadErr *config.LoadError
	if errors.As(err, &loadErr) && loadErr.Key != "" {
		return "configuration load failed for key " + loadErr.Key
	}
	return "configuration load failed"
}
