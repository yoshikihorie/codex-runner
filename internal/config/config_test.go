package config

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestResolveSubmitOptions(t *testing.T) {
	requestedModel := "gpt-5.6-sol"
	requestedEffort := "high"
	c := Config{
		model: "gpt-5.6-terra", modelOverrides: map[domain.Subcommand]string{domain.SubcommandReview: "gpt-5.6-sol"},
		reasoningEffort: &requestedEffort, reasoningEffortOverrides: map[domain.Subcommand]string{domain.SubcommandReview: "low"},
	}
	if model, ok := c.ResolveModel(domain.SubcommandReview, &requestedModel); !ok || model != requestedModel {
		t.Fatalf("model = %q, %t", model, ok)
	}
	if model, ok := c.ResolveModel(domain.SubcommandReview, nil); !ok || model != "gpt-5.6-sol" {
		t.Fatalf("override model = %q, %t", model, ok)
	}
	if effort, ok := c.ResolveReasoningEffort(domain.SubcommandReview, nil); !ok || effort == nil || *effort != "low" {
		t.Fatalf("override effort = %v, %t", effort, ok)
	}
	if effort, ok := c.ResolveReasoningEffort(domain.SubcommandImpl, nil); !ok || effort == nil || *effort != "high" {
		t.Fatalf("default effort = %v, %t", effort, ok)
	}
	if effort, ok := (Config{}).ResolveReasoningEffort(domain.SubcommandImpl, nil); !ok || effort != nil {
		t.Fatalf("absent effort = %v, %t", effort, ok)
	}
	bad := "invalid"
	if _, ok := c.ResolveModel(domain.SubcommandImpl, &bad); ok {
		t.Fatal("invalid requested model was accepted")
	}
	if _, ok := c.ResolveReasoningEffort(domain.SubcommandImpl, &bad); ok {
		t.Fatal("invalid requested effort was accepted")
	}
}

func TestLoadExplicitDefaultsAndOverrides(t *testing.T) {
	codex := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(codex, []byte(""), 0o755); err != nil {
		t.Fatal(err)
	}
	config := `
max_concurrent_tasks = 2
max_concurrent_impl_tasks = 1
metrics_record_content_enabled = true
log_rotation_compress = false
pty_enabled = true
socket_path = "/tmp/codexd.sock"
codex_binary_path = "` + codex + `"
model = "gpt-5.6-sol"
reasoning_effort = "high"
[model_overrides]
review = "gpt-5.6-terra"
[reasoning_effort_overrides]
read = "low"
`
	c := loadExplicitFile(t, config)
	if c.MaxConcurrentTasks() != 2 || c.MaxConcurrentImplTasks() != 1 || !c.MetricsRecordContentEnabled() || c.LogRotationCompress() || !c.PtyEnabled() {
		t.Fatal("explicit values were not applied")
	}
	if c.Model() != "gpt-5.6-sol" || c.CodexBinaryPath() != codex {
		t.Fatal("string values were not applied")
	}
	if effort, ok := c.ReasoningEffort(); !ok || effort != "high" {
		t.Fatal("reasoning effort was not applied")
	}
	if got := c.ModelOverrides()[domain.SubcommandReview]; got != "gpt-5.6-terra" {
		t.Fatalf("model override = %q", got)
	}
	if got := c.ReasoningEffortOverrides()[domain.SubcommandRead]; got != "low" {
		t.Fatalf("reasoning override = %q", got)
	}
}

func TestLoadExplicitUsesExplicitPathsWithoutHomeDirectory(t *testing.T) {
	codex := withCodexBinaryCandidate(t)
	originalHomeDir := userHomeDir
	userHomeDir = func() (string, error) { return "", errors.New("home unavailable") }
	t.Cleanup(func() { userHomeDir = originalHomeDir })

	c := loadExplicitFile(t, `socket_path = "/tmp/codexd.sock"
codex_binary_path = "`+codex+`"`)
	if c.SocketPath() != "/tmp/codexd.sock" || c.CodexBinaryPath() != codex {
		t.Fatalf("explicit paths were not used: %#v", c)
	}
}

func TestLoadExplicitRejectsNonExecutableCodexBinaryPaths(t *testing.T) {
	dir := t.TempDir()
	nonExecutable := filepath.Join(dir, "not-executable")
	if err := os.WriteFile(nonExecutable, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		path string
	}{
		{"missing", filepath.Join(dir, "missing")},
		{"not executable", nonExecutable},
		{"directory", dir},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadExplicit(writeConfig(t, `socket_path = "/tmp/codexd.sock"
codex_binary_path = "`+tc.path+`"`))
			var loadError *LoadError
			if !errors.As(err, &loadError) || loadError.Key != "codex_binary_path" || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("unexpected error: %#v", err)
			}
		})
	}
}

func TestLoadExplicitCoversAllConfigurationSettings(t *testing.T) {
	codex := withCodexBinaryCandidate(t)
	for _, tc := range []struct {
		name     string
		contents string
		assert   func(*testing.T, Config)
	}{
		{"max concurrent tasks", "max_concurrent_tasks = 2\nmax_concurrent_impl_tasks = 2", func(t *testing.T, c Config) {
			if c.MaxConcurrentTasks() != 2 {
				t.Fatal(c.MaxConcurrentTasks())
			}
		}},
		{"max concurrent impl tasks", "max_concurrent_impl_tasks = 2", func(t *testing.T, c Config) {
			if c.MaxConcurrentImplTasks() != 2 {
				t.Fatal(c.MaxConcurrentImplTasks())
			}
		}},
		{"queue max depth", "queue_max_depth = 2", func(t *testing.T, c Config) {
			if c.QueueMaxDepth() != 2 {
				t.Fatal(c.QueueMaxDepth())
			}
		}},
		{"metrics record content", "metrics_record_content_enabled = true", func(t *testing.T, c Config) {
			if !c.MetricsRecordContentEnabled() {
				t.Fatal("false")
			}
		}},
		{"log rotation max size", "log_rotation_max_size_bytes = 2", func(t *testing.T, c Config) {
			if c.LogRotationMaxSizeBytes() != 2 {
				t.Fatal(c.LogRotationMaxSizeBytes())
			}
		}},
		{"log rotation interval", "log_rotation_interval_seconds = 2", func(t *testing.T, c Config) {
			if c.LogRotationIntervalSeconds() != 2 {
				t.Fatal(c.LogRotationIntervalSeconds())
			}
		}},
		{"log eviction interval", "log_eviction_scan_interval_seconds = 2", func(t *testing.T, c Config) {
			if c.LogEvictionScanIntervalSeconds() != 2 {
				t.Fatal(c.LogEvictionScanIntervalSeconds())
			}
		}},
		{"log retention days", "log_rotation_retention_days = 2", func(t *testing.T, c Config) {
			if c.LogRotationRetentionDays() != 2 {
				t.Fatal(c.LogRotationRetentionDays())
			}
		}},
		{"log retention count", "log_rotation_retention_count = 2", func(t *testing.T, c Config) {
			if c.LogRotationRetentionCount() != 2 {
				t.Fatal(c.LogRotationRetentionCount())
			}
		}},
		{"log rotation compress", "log_rotation_compress = false", func(t *testing.T, c Config) {
			if c.LogRotationCompress() {
				t.Fatal("true")
			}
		}},
		{"metrics retention months", "metrics_retention_months = 2", func(t *testing.T, c Config) {
			if c.MetricsRetentionMonths() != 2 {
				t.Fatal(c.MetricsRetentionMonths())
			}
		}},
		{"metrics max file bytes", "metrics_max_file_bytes = 2", func(t *testing.T, c Config) {
			if c.MetricsMaxFileBytes() != 2 {
				t.Fatal(c.MetricsMaxFileBytes())
			}
		}},
		{"task placement retention", "task_placement_retention_days = 2", func(t *testing.T, c Config) {
			if c.TaskPlacementRetentionDays() != 2 {
				t.Fatal(c.TaskPlacementRetentionDays())
			}
		}},
		{"total task disk budget", "total_task_disk_budget_mb = 2", func(t *testing.T, c Config) {
			if c.TotalTaskDiskBudgetMB() != 2 {
				t.Fatal(c.TotalTaskDiskBudgetMB())
			}
		}},
		{"socket path", "socket_path = \"/tmp/explicit.sock\"", func(t *testing.T, c Config) {
			if c.SocketPath() != "/tmp/explicit.sock" {
				t.Fatal(c.SocketPath())
			}
		}},
		{"codex binary path", "codex_binary_path = \"" + codex + "\"", func(t *testing.T, c Config) {
			if c.CodexBinaryPath() != codex {
				t.Fatal(c.CodexBinaryPath())
			}
		}},
		{"model", "model = \"gpt-5.6-sol\"", func(t *testing.T, c Config) {
			if c.Model() != "gpt-5.6-sol" {
				t.Fatal(c.Model())
			}
		}},
		{"model overrides", "[model_overrides]\nreview = \"gpt-5.6-sol\"", func(t *testing.T, c Config) {
			if c.ModelOverrides()[domain.SubcommandReview] != "gpt-5.6-sol" {
				t.Fatal(c.ModelOverrides())
			}
		}},
		{"reasoning effort", "reasoning_effort = \"high\"", func(t *testing.T, c Config) {
			if got, ok := c.ReasoningEffort(); !ok || got != "high" {
				t.Fatal(got, ok)
			}
		}},
		{"reasoning effort overrides", "[reasoning_effort_overrides]\nread = \"high\"", func(t *testing.T, c Config) {
			if c.ReasoningEffortOverrides()[domain.SubcommandRead] != "high" {
				t.Fatal(c.ReasoningEffortOverrides())
			}
		}},
		{"pty enabled", "pty_enabled = true", func(t *testing.T, c Config) {
			if !c.PtyEnabled() {
				t.Fatal("false")
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.assert(t, loadExplicitFile(t, tc.contents)) })
	}
}

func TestLoadExplicitRejectsWrongTypesForAllConfigurationSettings(t *testing.T) {
	withCodexBinaryCandidate(t)
	for _, tc := range []struct {
		name     string
		contents string
	}{
		{"max concurrent tasks", `max_concurrent_tasks = "wrong"`},
		{"max concurrent impl tasks", `max_concurrent_impl_tasks = "wrong"`},
		{"queue max depth", `queue_max_depth = "wrong"`},
		{"metrics record content", `metrics_record_content_enabled = "wrong"`},
		{"log rotation max size", `log_rotation_max_size_bytes = "wrong"`},
		{"log rotation interval", `log_rotation_interval_seconds = "wrong"`},
		{"log eviction interval", `log_eviction_scan_interval_seconds = "wrong"`},
		{"log retention days", `log_rotation_retention_days = "wrong"`},
		{"log retention count", `log_rotation_retention_count = "wrong"`},
		{"log rotation compress", `log_rotation_compress = "wrong"`},
		{"metrics retention months", `metrics_retention_months = "wrong"`},
		{"metrics max file bytes", `metrics_max_file_bytes = "wrong"`},
		{"task placement retention", `task_placement_retention_days = "wrong"`},
		{"total task disk budget", `total_task_disk_budget_mb = "wrong"`},
		{"socket path", `socket_path = 1`},
		{"codex binary path", `codex_binary_path = 1`},
		{"model", `model = 1`},
		{"model overrides", `[model_overrides]
review = 1`},
		{"reasoning effort", `reasoning_effort = 1`},
		{"reasoning effort overrides", `[reasoning_effort_overrides]
read = 1`},
		{"pty enabled", `pty_enabled = "wrong"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadExplicit(writeConfig(t, tc.contents))
			if !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("wrong type was accepted: %v", err)
			}
		})
	}
}

func TestLoadExplicitRejectsOutOfRangeNumericSettings(t *testing.T) {
	withCodexBinaryCandidate(t)
	for _, key := range []string{
		"max_concurrent_tasks", "max_concurrent_impl_tasks", "queue_max_depth", "log_rotation_max_size_bytes",
		"log_rotation_interval_seconds", "log_eviction_scan_interval_seconds", "log_rotation_retention_days",
		"log_rotation_retention_count", "metrics_retention_months", "metrics_max_file_bytes",
		"task_placement_retention_days", "total_task_disk_budget_mb",
	} {
		t.Run(key, func(t *testing.T) {
			_, err := LoadExplicit(writeConfig(t, key+" = 0"))
			var loadError *LoadError
			if !errors.As(err, &loadError) || loadError.Key != key || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("unexpected error: %#v", err)
			}
		})
	}
}

func TestLoadExplicitRejectsInvalidValues(t *testing.T) {
	for _, test := range []struct{ name, contents, key string }{
		{"range", "max_concurrent_tasks = 0", "max_concurrent_tasks"},
		{"upper range", "max_concurrent_tasks = 17", "max_concurrent_tasks"},
		{"relative socket", `socket_path = "relative/socket"`, "socket_path"},
		{"empty binary", `codex_binary_path = ""`, "codex_binary_path"},
		{"model", `model = "not-allowed"`, "model"},
		{"effort", `reasoning_effort = "not-allowed"`, "reasoning_effort"},
		{"override key", "[model_overrides]\nstatus = \"gpt-5.6-sol\"", "model_overrides.status"},
		{"unknown", "unknown_key = true", "unknown_key"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadExplicit(writeConfig(t, test.contents))
			if err == nil {
				t.Fatal("LoadExplicit succeeded")
			}
			var loadError *LoadError
			if !errors.As(err, &loadError) || loadError.Key != test.key || !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("unexpected error: %#v", err)
			}
		})
	}
}

func TestLoadExplicitMissingAndRelativePath(t *testing.T) {
	_, err := LoadExplicit(filepath.Join(t.TempDir(), "missing.toml"))
	if !errors.Is(err, ErrExplicitConfigNotFound) {
		t.Fatalf("unexpected missing error: %v", err)
	}
	_, err = LoadExplicit("config.toml")
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unexpected relative error: %v", err)
	}
}

func TestDaemonRouteEnabledIsIgnored(t *testing.T) {
	loadExplicitFile(t, "[daemon_route_enabled]\nanything = [1, 2, 3]")
}

func TestOverrideAccessorsReturnCopies(t *testing.T) {
	c := loadExplicitFile(t, "[model_overrides]\nreview = \"gpt-5.6-sol\"\n[reasoning_effort_overrides]\nread = \"high\"")
	models := c.ModelOverrides()
	models[domain.SubcommandReview] = "changed"
	efforts := c.ReasoningEffortOverrides()
	efforts[domain.SubcommandRead] = "changed"
	if c.ModelOverrides()[domain.SubcommandReview] != "gpt-5.6-sol" || c.ReasoningEffortOverrides()[domain.SubcommandRead] != "high" {
		t.Fatal("accessor exposed internal map")
	}
}

func TestAllowedValues(t *testing.T) {
	if !IsModelAllowed("gpt-5.6-terra") || !IsModelAllowed("gpt-5.6-sol") || IsModelAllowed("other") {
		t.Fatal("model allowlist is incorrect")
	}
	if !IsReasoningEffortAllowed("low") || !IsReasoningEffortAllowed("medium") || !IsReasoningEffortAllowed("high") || !IsReasoningEffortAllowed("xhigh") || IsReasoningEffortAllowed("other") {
		t.Fatal("reasoning effort allowlist is incorrect")
	}
}

func TestFindCodexBinarySkipsRelativeCandidatesWhenHomeIsEmpty(t *testing.T) {
	if os.Getenv("CONFIG_FIND_CODEX_BINARY_HELPER") == "1" {
		if path, err := findCodexBinary(); err == nil || path != "" {
			t.Fatalf("findCodexBinary() = %q, %v; want no relative candidate", path, err)
		}
		return
	}
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, ".npm-global", "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".npm-global", "bin", "codex"), nil, 0o755); err != nil {
		t.Fatal(err)
	}
	env := make([]string, 0, len(os.Environ())+2)
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "HOME=") && !strings.HasPrefix(value, "CONFIG_FIND_CODEX_BINARY_HELPER=") {
			env = append(env, value)
		}
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestFindCodexBinarySkipsRelativeCandidatesWhenHomeIsEmpty$")
	cmd.Dir = dir
	cmd.Env = append(env, "HOME=", "CONFIG_FIND_CODEX_BINARY_HELPER=1")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("helper process failed: %v\n%s", err, output)
	}
}

func loadExplicitFile(t *testing.T, contents string) Config {
	t.Helper()
	withCodexBinaryCandidate(t)
	c, err := LoadExplicit(writeConfig(t, contents))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func withCodexBinaryCandidate(t *testing.T) string {
	t.Helper()
	codex := filepath.Join(t.TempDir(), "codex")
	if err := os.WriteFile(codex, nil, 0o755); err != nil {
		t.Fatal(err)
	}
	originalCandidates := codexBinaryPathCandidates
	codexBinaryPathCandidates = []string{codex}
	t.Cleanup(func() { codexBinaryPathCandidates = originalCandidates })
	return codex
}
func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.toml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
