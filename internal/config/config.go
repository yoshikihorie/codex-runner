// Package config loads and validates codexd's TOML configuration.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const (
	defaultMaxConcurrentTasks                   = 4
	defaultMaxConcurrentImplTasks               = 4
	defaultQueueMaxDepth                        = 50
	defaultMetricsRecordContentEnabled          = false
	defaultLogRotationMaxSizeBytes        int64 = 10_000_000
	defaultLogRotationIntervalSeconds           = 86_400
	defaultLogEvictionScanIntervalSeconds       = 3_600
	defaultLogRotationRetentionDays             = 14
	defaultLogRotationRetentionCount            = 5
	defaultLogRotationCompress                  = true
	defaultMetricsRetentionMonths               = 12
	defaultMetricsMaxFileBytes            int64 = 20_000_000
	defaultTaskPlacementRetentionDays           = 14
	defaultTotalTaskDiskBudgetMB                = 5_000
	defaultModel                                = "gpt-5.6-terra"
	defaultPtyEnabled                           = false
	maxConcurrentTasksMin                       = 1
	maxConcurrentTasksMax                       = 16
)

var (
	ErrInvalidConfig          = errors.New("config: invalid value")
	ErrExplicitConfigNotFound = errors.New("config: explicit config file not found")

	allowedModels           = []string{"gpt-5.6-terra", "gpt-5.6-sol"}
	allowedReasoningEfforts = []string{"low", "medium", "high", "xhigh"}

	// This is a variable so package tests can replace the candidates safely.
	codexBinaryPathCandidates = []string{
		"/opt/homebrew/bin/codex",
		"/usr/local/bin/codex",
	}
	userHomeDir = os.UserHomeDir
)

// LoadError identifies the configuration key that prevented startup.
type LoadError struct {
	Key    string
	Reason string
	Err    error
}

func (e *LoadError) Error() string {
	switch {
	case e.Key == "" && e.Err == nil:
		return fmt.Sprintf("config: %s", e.Reason)
	case e.Key == "":
		return fmt.Sprintf("config: %s: %v", e.Reason, e.Err)
	case e.Err == nil:
		return fmt.Sprintf("config: key %q: %s", e.Key, e.Reason)
	default:
		return fmt.Sprintf("config: key %q: %s: %v", e.Key, e.Reason, e.Err)
	}
}

func (e *LoadError) Unwrap() error { return e.Err }

// Config is an immutable, validated codexd configuration value.
type Config struct {
	maxConcurrentTasks             int
	maxConcurrentImplTasks         int
	queueMaxDepth                  int
	metricsRecordContentEnabled    bool
	logRotationMaxSizeBytes        int64
	logRotationIntervalSeconds     int
	logEvictionScanIntervalSeconds int
	logRotationRetentionDays       int
	logRotationRetentionCount      int
	logRotationCompress            bool
	metricsRetentionMonths         int
	metricsMaxFileBytes            int64
	taskPlacementRetentionDays     int
	totalTaskDiskBudgetMB          int
	socketPath                     string
	codexBinaryPath                string
	model                          string
	modelOverrides                 map[domain.Subcommand]string
	reasoningEffort                *string
	reasoningEffortOverrides       map[domain.Subcommand]string
	ptyEnabled                     bool
}

type rawConfig struct {
	MaxConcurrentTasks             *int              `toml:"max_concurrent_tasks"`
	MaxConcurrentImplTasks         *int              `toml:"max_concurrent_impl_tasks"`
	QueueMaxDepth                  *int              `toml:"queue_max_depth"`
	MetricsRecordContentEnabled    *bool             `toml:"metrics_record_content_enabled"`
	LogRotationMaxSizeBytes        *int64            `toml:"log_rotation_max_size_bytes"`
	LogRotationIntervalSeconds     *int              `toml:"log_rotation_interval_seconds"`
	LogEvictionScanIntervalSeconds *int              `toml:"log_eviction_scan_interval_seconds"`
	LogRotationRetentionDays       *int              `toml:"log_rotation_retention_days"`
	LogRotationRetentionCount      *int              `toml:"log_rotation_retention_count"`
	LogRotationCompress            *bool             `toml:"log_rotation_compress"`
	MetricsRetentionMonths         *int              `toml:"metrics_retention_months"`
	MetricsMaxFileBytes            *int64            `toml:"metrics_max_file_bytes"`
	TaskPlacementRetentionDays     *int              `toml:"task_placement_retention_days"`
	TotalTaskDiskBudgetMB          *int              `toml:"total_task_disk_budget_mb"`
	SocketPath                     *string           `toml:"socket_path"`
	CodexBinaryPath                *string           `toml:"codex_binary_path"`
	Model                          *string           `toml:"model"`
	ModelOverrides                 map[string]string `toml:"model_overrides"`
	ReasoningEffort                *string           `toml:"reasoning_effort"`
	ReasoningEffortOverrides       map[string]string `toml:"reasoning_effort_overrides"`
	PtyEnabled                     *bool             `toml:"pty_enabled"`
}

type missingFilePolicy bool

const (
	allowMissingFile  missingFilePolicy = true
	rejectMissingFile missingFilePolicy = false
)

// LoadDefault reads ~/.claude/codexd/config.toml. A missing file uses defaults.
func LoadDefault() (Config, error) {
	home, err := userHomeDir()
	if err != nil {
		return Config{}, invalid("", "resolve home directory", err)
	}
	return load(filepath.Join(home, ".claude", "codexd", "config.toml"), allowMissingFile)
}

// LoadExplicit reads an explicitly supplied absolute configuration path.
func LoadExplicit(path string) (Config, error) {
	if !filepath.IsAbs(path) {
		return Config{}, invalid("", "explicit config path must be absolute", nil)
	}
	return load(path, rejectMissingFile)
}

func load(path string, missingPolicy missingFilePolicy) (Config, error) {
	raw := rawConfig{}
	md, err := toml.DecodeFile(path, &raw)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			if missingPolicy == allowMissingFile {
				return resolve(raw)
			}
			return Config{}, &LoadError{Reason: "explicit config file not found", Err: ErrExplicitConfigNotFound}
		}
		return Config{}, invalid("", "decode TOML configuration", err)
	}
	for _, key := range md.Undecoded() {
		keyPath := key.String()
		if keyPath == "daemon_route_enabled" || strings.HasPrefix(keyPath, "daemon_route_enabled.") {
			continue
		}
		return Config{}, invalid(keyPath, "unknown configuration key", nil)
	}
	return resolve(raw)
}

func resolve(raw rawConfig) (Config, error) {
	socketPath := ""
	if raw.SocketPath != nil {
		socketPath = *raw.SocketPath
	} else {
		var err error
		socketPath, err = defaultSocketPath()
		if err != nil {
			return Config{}, err
		}
	}
	c := Config{
		maxConcurrentTasks: defaultMaxConcurrentTasks, maxConcurrentImplTasks: defaultMaxConcurrentImplTasks,
		queueMaxDepth: defaultQueueMaxDepth, metricsRecordContentEnabled: defaultMetricsRecordContentEnabled,
		logRotationMaxSizeBytes: defaultLogRotationMaxSizeBytes, logRotationIntervalSeconds: defaultLogRotationIntervalSeconds,
		logEvictionScanIntervalSeconds: defaultLogEvictionScanIntervalSeconds, logRotationRetentionDays: defaultLogRotationRetentionDays,
		logRotationRetentionCount: defaultLogRotationRetentionCount, logRotationCompress: defaultLogRotationCompress,
		metricsRetentionMonths: defaultMetricsRetentionMonths, metricsMaxFileBytes: defaultMetricsMaxFileBytes,
		taskPlacementRetentionDays: defaultTaskPlacementRetentionDays, totalTaskDiskBudgetMB: defaultTotalTaskDiskBudgetMB,
		socketPath: socketPath, model: defaultModel,
		modelOverrides: make(map[domain.Subcommand]string), reasoningEffortOverrides: make(map[domain.Subcommand]string),
		ptyEnabled: defaultPtyEnabled,
	}
	if raw.MaxConcurrentTasks != nil {
		c.maxConcurrentTasks = *raw.MaxConcurrentTasks
	}
	if raw.MaxConcurrentImplTasks != nil {
		c.maxConcurrentImplTasks = *raw.MaxConcurrentImplTasks
	}
	if raw.QueueMaxDepth != nil {
		c.queueMaxDepth = *raw.QueueMaxDepth
	}
	if raw.MetricsRecordContentEnabled != nil {
		c.metricsRecordContentEnabled = *raw.MetricsRecordContentEnabled
	}
	if raw.LogRotationMaxSizeBytes != nil {
		c.logRotationMaxSizeBytes = *raw.LogRotationMaxSizeBytes
	}
	if raw.LogRotationIntervalSeconds != nil {
		c.logRotationIntervalSeconds = *raw.LogRotationIntervalSeconds
	}
	if raw.LogEvictionScanIntervalSeconds != nil {
		c.logEvictionScanIntervalSeconds = *raw.LogEvictionScanIntervalSeconds
	}
	if raw.LogRotationRetentionDays != nil {
		c.logRotationRetentionDays = *raw.LogRotationRetentionDays
	}
	if raw.LogRotationRetentionCount != nil {
		c.logRotationRetentionCount = *raw.LogRotationRetentionCount
	}
	if raw.LogRotationCompress != nil {
		c.logRotationCompress = *raw.LogRotationCompress
	}
	if raw.MetricsRetentionMonths != nil {
		c.metricsRetentionMonths = *raw.MetricsRetentionMonths
	}
	if raw.MetricsMaxFileBytes != nil {
		c.metricsMaxFileBytes = *raw.MetricsMaxFileBytes
	}
	if raw.TaskPlacementRetentionDays != nil {
		c.taskPlacementRetentionDays = *raw.TaskPlacementRetentionDays
	}
	if raw.TotalTaskDiskBudgetMB != nil {
		c.totalTaskDiskBudgetMB = *raw.TotalTaskDiskBudgetMB
	}
	if raw.Model != nil {
		c.model = *raw.Model
	}
	if raw.ReasoningEffort != nil {
		value := *raw.ReasoningEffort
		c.reasoningEffort = &value
	}
	if raw.PtyEnabled != nil {
		c.ptyEnabled = *raw.PtyEnabled
	}

	if err := validateRanges(c); err != nil {
		return Config{}, err
	}
	if c.maxConcurrentImplTasks > c.maxConcurrentTasks {
		return Config{}, invalid("max_concurrent_impl_tasks", "must not exceed max_concurrent_tasks", nil)
	}
	if err := copyOverrides(raw.ModelOverrides, c.modelOverrides, "model_overrides"); err != nil {
		return Config{}, err
	}
	if err := copyOverrides(raw.ReasoningEffortOverrides, c.reasoningEffortOverrides, "reasoning_effort_overrides"); err != nil {
		return Config{}, err
	}
	if !IsModelAllowed(c.model) {
		return Config{}, invalid("model", "model is not allowed", nil)
	}
	if c.reasoningEffort != nil && !IsReasoningEffortAllowed(*c.reasoningEffort) {
		return Config{}, invalid("reasoning_effort", "reasoning effort is not allowed", nil)
	}
	if err := validateOverrideValues(c.modelOverrides, "model_overrides", IsModelAllowed, "model is not allowed"); err != nil {
		return Config{}, err
	}
	if err := validateOverrideValues(c.reasoningEffortOverrides, "reasoning_effort_overrides", IsReasoningEffortAllowed, "reasoning effort is not allowed"); err != nil {
		return Config{}, err
	}
	if raw.CodexBinaryPath != nil {
		if *raw.CodexBinaryPath == "" || !filepath.IsAbs(*raw.CodexBinaryPath) {
			return Config{}, invalid("codex_binary_path", "must be a non-empty absolute path", nil)
		}
		info, err := os.Stat(*raw.CodexBinaryPath)
		if err != nil {
			return Config{}, invalid("codex_binary_path", "must be an executable regular file", err)
		}
		if !isExecutableRegularFile(info) {
			return Config{}, invalid("codex_binary_path", "must be an executable regular file", nil)
		}
		c.codexBinaryPath = *raw.CodexBinaryPath
	} else {
		path, err := findCodexBinary()
		if err != nil {
			return Config{}, err
		}
		c.codexBinaryPath = path
	}
	if !filepath.IsAbs(c.socketPath) {
		return Config{}, invalid("socket_path", "must be an absolute path", nil)
	}
	return c, nil
}

func validateRanges(c Config) error {
	values := []struct {
		key   string
		value int64
		max   int64
	}{
		{"max_concurrent_tasks", int64(c.maxConcurrentTasks), maxConcurrentTasksMax}, {"max_concurrent_impl_tasks", int64(c.maxConcurrentImplTasks), 0}, {"queue_max_depth", int64(c.queueMaxDepth), 0}, {"log_rotation_max_size_bytes", c.logRotationMaxSizeBytes, 0}, {"log_rotation_interval_seconds", int64(c.logRotationIntervalSeconds), 0}, {"log_eviction_scan_interval_seconds", int64(c.logEvictionScanIntervalSeconds), 0}, {"log_rotation_retention_days", int64(c.logRotationRetentionDays), 0}, {"log_rotation_retention_count", int64(c.logRotationRetentionCount), 0}, {"metrics_retention_months", int64(c.metricsRetentionMonths), 0}, {"metrics_max_file_bytes", c.metricsMaxFileBytes, 0}, {"task_placement_retention_days", int64(c.taskPlacementRetentionDays), 0}, {"total_task_disk_budget_mb", int64(c.totalTaskDiskBudgetMB), 0},
	}
	for _, item := range values {
		if item.value < maxConcurrentTasksMin || (item.max > 0 && item.value > item.max) {
			return invalid(item.key, "must be within the allowed range", nil)
		}
	}
	return nil
}

func copyOverrides(source map[string]string, destination map[domain.Subcommand]string, key string) error {
	names := make([]string, 0, len(source))
	for name := range source {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := source[name]
		subcommand := domain.Subcommand(name)
		if !domain.IsSubmittable(subcommand) {
			return invalid(key+"."+name, "subcommand is not submittable", nil)
		}
		destination[subcommand] = value
	}
	return nil
}

func validateOverrideValues(overrides map[domain.Subcommand]string, key string, allowed func(string) bool, reason string) error {
	subcommands := make([]string, 0, len(overrides))
	for subcommand := range overrides {
		subcommands = append(subcommands, string(subcommand))
	}
	sort.Strings(subcommands)
	for _, subcommand := range subcommands {
		if !allowed(overrides[domain.Subcommand(subcommand)]) {
			return invalid(key+"."+subcommand, reason, nil)
		}
	}
	return nil
}

func defaultSocketPath() (string, error) {
	home, err := userHomeDir()
	if err != nil {
		return "", invalid("socket_path", "resolve home directory", err)
	}
	return filepath.Join(home, ".claude", "run", "codexd.sock"), nil
}

func findCodexBinary() (string, error) {
	candidates := codexBinaryPathCandidates
	if home, err := userHomeDir(); err == nil {
		candidates = append([]string{filepath.Join(home, ".npm-global", "bin", "codex")}, candidates...)
	}
	for _, candidate := range candidates {
		if !filepath.IsAbs(candidate) {
			continue
		}
		info, err := os.Stat(candidate)
		if err == nil && isExecutableRegularFile(info) {
			return candidate, nil
		}
	}
	return "", invalid("codex_binary_path", "CHILD_PROCESS_LAUNCH_FAILED: no codex binary found", nil)
}

func isExecutableRegularFile(info fs.FileInfo) bool {
	return info.Mode().IsRegular() && info.Mode()&0o111 != 0
}

func invalid(key, reason string, cause error) *LoadError {
	if cause == nil {
		cause = ErrInvalidConfig
	} else {
		cause = fmt.Errorf("%w: %v", ErrInvalidConfig, cause)
	}
	return &LoadError{Key: key, Reason: reason, Err: cause}
}

func IsModelAllowed(model string) bool {
	for _, allowed := range allowedModels {
		if model == allowed {
			return true
		}
	}
	return false
}
func IsReasoningEffortAllowed(effort string) bool {
	for _, allowed := range allowedReasoningEfforts {
		if effort == allowed {
			return true
		}
	}
	return false
}

// ResolveModel applies request, subcommand, then global defaults.
func (c Config) ResolveModel(subcommand domain.Subcommand, requested *string) (string, bool) {
	model := c.model
	if override, ok := c.modelOverrides[subcommand]; ok {
		model = override
	}
	if requested != nil {
		model = *requested
	}
	return model, IsModelAllowed(model)
}

// ResolveReasoningEffort applies request, subcommand, then global defaults.
func (c Config) ResolveReasoningEffort(subcommand domain.Subcommand, requested *string) (*string, bool) {
	var effort *string
	if c.reasoningEffort != nil {
		value := *c.reasoningEffort
		effort = &value
	}
	if override, ok := c.reasoningEffortOverrides[subcommand]; ok {
		value := override
		effort = &value
	}
	if requested != nil {
		value := *requested
		effort = &value
	}
	if effort == nil {
		return nil, true
	}
	return effort, IsReasoningEffortAllowed(*effort)
}

func (c Config) MaxConcurrentTasks() int             { return c.maxConcurrentTasks }
func (c Config) MaxConcurrentImplTasks() int         { return c.maxConcurrentImplTasks }
func (c Config) QueueMaxDepth() int                  { return c.queueMaxDepth }
func (c Config) MetricsRecordContentEnabled() bool   { return c.metricsRecordContentEnabled }
func (c Config) LogRotationMaxSizeBytes() int64      { return c.logRotationMaxSizeBytes }
func (c Config) LogRotationIntervalSeconds() int     { return c.logRotationIntervalSeconds }
func (c Config) LogEvictionScanIntervalSeconds() int { return c.logEvictionScanIntervalSeconds }
func (c Config) LogRotationRetentionDays() int       { return c.logRotationRetentionDays }
func (c Config) LogRotationRetentionCount() int      { return c.logRotationRetentionCount }
func (c Config) LogRotationCompress() bool           { return c.logRotationCompress }
func (c Config) MetricsRetentionMonths() int         { return c.metricsRetentionMonths }
func (c Config) MetricsMaxFileBytes() int64          { return c.metricsMaxFileBytes }
func (c Config) TaskPlacementRetentionDays() int     { return c.taskPlacementRetentionDays }
func (c Config) TotalTaskDiskBudgetMB() int          { return c.totalTaskDiskBudgetMB }
func (c Config) SocketPath() string                  { return c.socketPath }
func (c Config) CodexBinaryPath() string             { return c.codexBinaryPath }
func (c Config) Model() string                       { return c.model }
func (c Config) PtyEnabled() bool                    { return c.ptyEnabled }
func (c Config) ReasoningEffort() (string, bool) {
	if c.reasoningEffort == nil {
		return "", false
	}
	return *c.reasoningEffort, true
}
func (c Config) ModelOverrides() map[domain.Subcommand]string {
	return copyOverrideMap(c.modelOverrides)
}
func (c Config) ReasoningEffortOverrides() map[domain.Subcommand]string {
	return copyOverrideMap(c.reasoningEffortOverrides)
}
func copyOverrideMap(source map[domain.Subcommand]string) map[domain.Subcommand]string {
	copied := make(map[domain.Subcommand]string, len(source))
	for key, value := range source {
		copied[key] = value
	}
	return copied
}
