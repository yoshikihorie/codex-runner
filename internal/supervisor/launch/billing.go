package launch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const maxBillingSettingsBytes = 16 * 1024 * 1024

var billingEnvNames = []string{"ANTHROPIC_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL", "CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY", "AWS_BEARER_TOKEN_BEDROCK"} // SUPERVISOR_BILLING_ENV_NAMES
var supervisorStrippedEnvNames = []string{"CLAUDE_CONFIG_DIR", "CODEX_HOME"}                                                                                                                                                    // SUPERVISOR_STRIPPED_ENV_NAMES additions from L26

// BillingGuard permits replacing the system managed-settings path for tests.
type BillingGuard struct{ ManagedSettingsPath string }

// Check fails closed unless childEnv is free of alternate billing selectors.
func (g BillingGuard) Check(cwd string, childEnv []string) error {
	if !filepath.IsAbs(cwd) {
		return billingFailure("project_settings")
	}
	for _, item := range childEnv {
		key, _, _ := strings.Cut(item, "=")
		if hasName(billingEnvNames, key) || hasName(supervisorStrippedEnvNames, key) {
			return billingFailure("child_env")
		}
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return billingFailure("user_settings")
	}
	checks := []struct{ path, kind string }{
		{filepath.Join(home, ".claude", "settings.json"), "user_settings"},
		{filepath.Join(cwd, ".claude", "settings.json"), "project_settings"},
		{filepath.Join(cwd, ".claude", "settings.local.json"), "project_local_settings"},
		{filepath.Join(home, ".claude.json"), "claude_json"},
	}
	for _, check := range checks {
		if err := inspectSettings(check.path, check.kind); err != nil {
			return err
		}
	}
	managed := g.ManagedSettingsPath
	if managed == "" {
		managed = "/Library/Application Support/ClaudeCode/managed-settings.json"
	}
	if err := inspectSettings(managed, "managed"); err != nil {
		return err
	}
	return inspectCodexAuth(filepath.Join(home, ".codex", "auth.json"))
}

func inspectSettings(path, kind string) error {
	doc, missing, err := readJSONFile(path, false, kind)
	if err != nil || missing {
		return err
	}
	if doc == nil {
		return billingFailure(kind)
	}
	if _, ok := doc["apiKeyHelper"]; ok {
		return billingFailure(kind)
	}
	if kind == "claude_json" {
		if raw, ok := doc["primaryApiKey"]; ok && rawNonEmpty(raw) {
			return billingFailure(kind)
		}
	}
	if raw, ok := doc["env"]; ok {
		var env map[string]json.RawMessage
		if err := json.Unmarshal(raw, &env); err != nil || env == nil {
			return billingFailure(kind)
		}
		for key := range env {
			if hasName(billingEnvNames, key) {
				return billingFailure(kind)
			}
		}
	}
	return nil
}

func inspectCodexAuth(path string) error {
	doc, _, err := readJSONFile(path, true, "codex_auth")
	if err != nil {
		return err
	}
	if doc == nil {
		return billingFailure("codex_auth")
	}
	var authMode string
	if raw, ok := doc["auth_mode"]; !ok || json.Unmarshal(raw, &authMode) != nil || authMode != "chatgpt" {
		return billingFailure("codex_auth")
	}
	if raw, ok := doc["OPENAI_API_KEY"]; ok && rawNonEmpty(raw) {
		return billingFailure("codex_auth")
	}
	return nil
}

func readJSONFile(path string, requireFile bool, kind string) (map[string]json.RawMessage, bool, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && !requireFile {
			return nil, true, nil
		}
		return nil, false, billingFailure(kind)
	}
	if !info.Mode().IsRegular() || info.Size() > maxBillingSettingsBytes {
		return nil, false, billingFailure(kind)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, false, billingFailure(kind)
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil || !openedInfo.Mode().IsRegular() || openedInfo.Size() > maxBillingSettingsBytes {
		return nil, false, billingFailure(kind)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxBillingSettingsBytes+1))
	if err != nil || len(data) > maxBillingSettingsBytes {
		return nil, false, billingFailure(kind)
	}
	var doc map[string]json.RawMessage
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&doc); err != nil || doc == nil {
		return nil, false, billingFailure(kind)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return nil, false, billingFailure(kind)
	}
	return doc, false, nil
}

func rawNonEmpty(raw json.RawMessage) bool {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return false
	}
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return value != ""
	}
	return true
}

func hasName(names []string, name string) bool {
	for _, n := range names {
		if n == name {
			return true
		}
	}
	return false
}

func billingFailure(kind string) error { return fmt.Errorf("SUP_BILLING_GUARD_FAILED: %s", kind) }
