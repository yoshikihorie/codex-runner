package launch

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestBuildLaunchArgv(t *testing.T) {
	tests := []struct {
		name, cli, resume       string
		relativeBinary, wantErr bool
	}{
		{name: "new", cli: "claude"},
		{name: "resume", cli: "claude", resume: "123e4567-e89b-12d3-a456-426614174000"},
		{name: "invalid resume", cli: "claude", resume: "--dangerously-skip-permissions", wantErr: true},
		{name: "resume whitespace", cli: "claude", resume: "abc def", wantErr: true},
		{name: "relative cli path rejected", cli: "claude", relativeBinary: true, wantErr: true},
		{name: "codex rejected", cli: "codex", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			binary := filepath.Join(root, "claude")
			if tc.relativeBinary {
				binary = "claude"
			}
			settings := filepath.Join(root, "settings.json")
			got, err := BuildLaunchArgv(tc.cli, binary, settings, tc.resume)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v", err)
			}
			if err == nil {
				want := []string{binary, "--settings", settings, "--permission-mode", "default", "--model", "claude-opus-5-5"}
				if tc.resume != "" {
					want = append(want, "--resume", tc.resume)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("got %#v want %#v", got, want)
				}
				if strings.Contains(strings.Join(got, " "), "--continue") {
					t.Fatal("--continue present")
				}
				if !containsPair(got, "--permission-mode", "default") {
					t.Fatal("permission mode default missing")
				}
			}
		})
	}
}

func TestValidateProjectModel(t *testing.T) {
	model := "gpt-6-luna"
	if err := ValidateProjectModel(&model); err == nil {
		t.Fatal("project model accepted")
	}
	if err := ValidateProjectModel(nil); err != nil {
		t.Fatalf("nil model rejected: %v", err)
	}
}

func containsPair(a []string, x, y string) bool {
	for i := 0; i+1 < len(a); i++ {
		if a[i] == x && a[i+1] == y {
			return true
		}
	}
	return false
}

func TestSettingsJSONAndWrite(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "ssettings-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	state := filepath.Join(home, "state")
	bin := filepath.Join(home, "codexd")
	data, err := BuildSettingsJSON(bin, "session-x", state)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		StatusLine struct {
			Type    string `json:"type"`
			Command string `json:"command"`
		} `json:"statusLine"`
		Permissions struct {
			Deny []string `json:"deny"`
		} `json:"permissions"`
	}
	if err = json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.StatusLine.Type != "command" || got.StatusLine.Command != bin+" supervise-statusline --state-dir "+state+" --session session-x" {
		t.Fatalf("statusline: %#v", got.StatusLine)
	}
	quoted, err := BuildSettingsJSON(bin+" path", "session 'quoted", state)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(quoted, &got); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.StatusLine.Command, "'\\''") || !strings.Contains(got.StatusLine.Command, "session '\\''quoted") {
		t.Fatalf("statusline not shell-quoted: %q", got.StatusLine.Command)
	}
	if _, err := BuildSettingsJSON(bin, "session-x", state+string(os.PathSeparator)); err == nil {
		t.Fatal("unclean stateDir accepted")
	}
	if len(got.Permissions.Deny) != len(supervisorDenyRules) {
		t.Fatalf("deny count %d", len(got.Permissions.Deny))
	}
	wantRules := []string{"Bash(git push --force*)", "Bash(git push -f*)", "Bash(git push*--force*)", "Bash(git push*+*)", "Bash(git push*--mirror*)", "Bash(git push*--delete*)", "Bash(gh pr create*)", "Bash(gh pr merge*)", "Bash(gh release*)", "Bash(*deploy*)", "Bash(codexd supervisor-client*)", "Bash(*codexd-supervisor*)", "Edit(/" + state + "/**)", "Bash(*" + state + "*)"}
	if len(got.Permissions.Deny) != len(wantRules) {
		t.Fatalf("deny count = %d, want %d", len(got.Permissions.Deny), len(wantRules))
	}
	for i, want := range wantRules {
		if got.Permissions.Deny[i] != want {
			t.Errorf("deny[%d]=%q want %q", i, got.Permissions.Deny[i], want)
		}
	}
	path := filepath.Join(home, "settings.json")
	if err := WriteSettingsFile(path, data); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("mode %o", info.Mode().Perm())
	}
	if err := WriteSettingsFile(path, []byte(`{"updated":true}`)); err != nil {
		t.Fatal(err)
	}
	linkTarget := filepath.Join(home, "target.json")
	if err := os.WriteFile(linkTarget, []byte("target"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(linkTarget, path); err != nil {
		t.Fatal(err)
	}
	if err := WriteSettingsFile(path, data); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(path); err != nil || info.Mode()&os.ModeSymlink != 0 {
		t.Fatalf("destination symlink was not replaced: info=%v err=%v", info, err)
	}
	if contents, err := os.ReadFile(linkTarget); err != nil || string(contents) != "target" {
		t.Fatalf("symlink target changed: %q err=%v", contents, err)
	}
	leftovers, err := filepath.Glob(filepath.Join(home, ".settings-*.tmp"))
	if err != nil || len(leftovers) != 0 {
		t.Fatalf("temporary files remain: %v err=%v", leftovers, err)
	}
}

func TestBillingGuard(t *testing.T) {
	home, cwd := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	g := BillingGuard{ManagedSettingsPath: filepath.Join(home, "managed.json")}
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(`{"auth_mode":"chatgpt"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := g.Check(cwd, nil); err != nil {
		t.Fatalf("clean config: %v", err)
	}
	if err := g.Check(cwd, []string{"OPENAI_API_KEY=secret"}); err == nil || !strings.Contains(err.Error(), "child_env") {
		t.Fatalf("child env was not rejected: %v", err)
	}
	for _, tc := range []struct{ name, path, body string }{{"user apiKeyHelper", filepath.Join(home, ".claude", "settings.json"), `{"apiKeyHelper":"helper"}`}, {"project billing env", filepath.Join(cwd, ".claude", "settings.local.json"), `{"env":{"OPENAI_API_KEY":"x"}}`}, {"claude json billing env", filepath.Join(home, ".claude.json"), `{"env":{"ANTHROPIC_API_KEY":"x"}}`}, {"managed billing env", filepath.Join(home, "managed.json"), `{"env":{"CLAUDE_CODE_USE_VERTEX":"1"}}`}, {"codex api key marker", filepath.Join(home, ".codex", "auth.json"), `{"OPENAI_API_KEY":"x"}`}, {"broken settings JSON", filepath.Join(home, ".claude", "settings.json"), `{`}, {"codex api_key method", filepath.Join(home, ".codex", "auth.json"), `{"auth_mode":"api_key"}`}} {
		t.Run(tc.name, func(t *testing.T) {
			os.RemoveAll(filepath.Join(home, ".claude"))
			os.RemoveAll(filepath.Join(cwd, ".claude"))
			os.Remove(filepath.Join(home, ".claude.json"))
			os.Remove(filepath.Join(home, "managed.json"))
			os.RemoveAll(filepath.Join(home, ".codex"))
			if err := os.MkdirAll(filepath.Dir(tc.path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(tc.path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			if err := g.Check(cwd, nil); err == nil || !strings.Contains(err.Error(), "SUP_BILLING_GUARD_FAILED") {
				t.Fatalf("expected guard failure, got %v", err)
			}
		})
	}
}

func TestBillingGuardUnreadableManagedSettings(t *testing.T) {
	home, cwd := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	managed := filepath.Join(home, "managed.json")
	if err := os.Mkdir(managed, 0700); err != nil {
		t.Fatal(err)
	}
	err := (BillingGuard{ManagedSettingsPath: managed}).Check(cwd, nil)
	if err == nil || !strings.Contains(err.Error(), "SUP_BILLING_GUARD_FAILED") {
		t.Fatalf("expected fail-closed error, got %v", err)
	}
}

func TestInspectCodexAuth(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		missing bool
		mode    os.FileMode
		wantErr bool
	}{
		{name: "chatgpt with null key and tokens", body: `{"auth_mode":"chatgpt","OPENAI_API_KEY":null,"tokens":{"access_token":"dummy","account_id":"dummy","id_token":"dummy","refresh_token":"dummy"},"last_refresh":"dummy"}`},
		{name: "chatgpt without key", body: `{"auth_mode":"chatgpt"}`},
		{name: "chatgpt with empty key", body: `{"auth_mode":"chatgpt","OPENAI_API_KEY":""}`},
		{name: "non-empty key", body: `{"auth_mode":"chatgpt","OPENAI_API_KEY":"sk-dummy"}`, wantErr: true},
		{name: "apikey mode", body: `{"auth_mode":"apikey"}`, wantErr: true},
		{name: "api_key mode", body: `{"auth_mode":"api_key"}`, wantErr: true},
		{name: "missing auth mode", body: `{"OPENAI_API_KEY":null}`, wantErr: true},
		{name: "numeric auth mode", body: `{"auth_mode":1}`, wantErr: true},
		{name: "nested key name does not determine auth mode", body: `{"auth_mode":"chatgpt","tokens":{"OPENAI_API_KEY":null}}`},
		{name: "broken JSON", body: `{`, wantErr: true},
		{name: "unreadable file", body: `{"auth_mode":"chatgpt"}`, mode: 0000, wantErr: true},
		{name: "missing file", missing: true, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "auth.json")
			if !tc.missing {
				mode := tc.mode
				if mode == 0 {
					mode = 0600
				}
				if err := os.WriteFile(path, []byte(tc.body), mode); err != nil {
					t.Fatal(err)
				}
				if tc.name == "unreadable file" {
					if err := os.Chmod(path, 0000); err != nil {
						t.Fatal(err)
					}
					if _, err := os.ReadFile(path); err == nil {
						t.Skip("current user can read mode-000 files")
					}
				}
			}
			err := inspectCodexAuth(path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("inspectCodexAuth() error = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.wantErr && !strings.Contains(err.Error(), "SUP_BILLING_GUARD_FAILED") {
				t.Fatalf("error = %v, want SUP_BILLING_GUARD_FAILED", err)
			}
		})
	}
}

func TestBillingGuardFailsClosedWithoutAuthAndUsesKinds(t *testing.T) {
	home, cwd := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	g := BillingGuard{ManagedSettingsPath: filepath.Join(home, "managed.json")}
	for _, env := range []string{"CLAUDE_CONFIG_DIR=/elsewhere", "CODEX_HOME=/elsewhere"} {
		err := g.Check(cwd, []string{env})
		if err == nil || !strings.Contains(err.Error(), "child_env") || strings.Contains(err.Error(), home) {
			t.Errorf("env %q: %v", env, err)
		}
	}
	if err := g.Check(cwd, []string{"FOO=1"}); err == nil || !strings.Contains(err.Error(), "codex_auth") {
		t.Fatalf("missing auth accepted: %v", err)
	}
	if err := g.Check("relative", nil); err == nil || !strings.Contains(err.Error(), "project_settings") {
		t.Fatalf("relative cwd accepted: %v", err)
	}
	for _, body := range []string{"null", `{"primaryApiKey":"key"}`} {
		if err := os.WriteFile(filepath.Join(home, ".claude.json"), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(home, ".codex"), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(`{"auth_mode":"chatgpt"}`), 0600); err != nil {
			t.Fatal(err)
		}
		err := g.Check(cwd, nil)
		if err == nil || !strings.Contains(err.Error(), "claude_json") || strings.Contains(err.Error(), home) {
			t.Errorf("body %s: %v", body, err)
		}
		_ = os.Remove(filepath.Join(home, ".claude.json"))
	}
}

func TestBillingGuardRejectsMalformedAndNonRegularSettings(t *testing.T) {
	home, cwd := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	g := BillingGuard{ManagedSettingsPath: filepath.Join(home, "managed.json")}
	for _, tc := range []struct {
		name, path, body string
		directory        bool
	}{
		{"user unreadable path", filepath.Join(home, ".claude", "settings.json"), "", true},
		{"project settings malformed", filepath.Join(cwd, ".claude", "settings.json"), "{", false},
		{"project local bedrock", filepath.Join(cwd, ".claude", "settings.local.json"), `{"env":{"CLAUDE_CODE_USE_BEDROCK":"1"}}`, false},
		{"claude json malformed", filepath.Join(home, ".claude.json"), "{", false},
		{"claude json api key helper", filepath.Join(home, ".claude.json"), `{"apiKeyHelper":"helper"}`, false},
		{"managed base url", filepath.Join(home, "managed.json"), `{"env":{"ANTHROPIC_BASE_URL":"url"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_ = os.RemoveAll(filepath.Join(home, ".claude"))
			_ = os.Remove(filepath.Join(cwd, ".claude"))
			_ = os.Remove(filepath.Join(home, ".claude.json"))
			_ = os.Remove(filepath.Join(home, "managed.json"))
			if err := os.MkdirAll(filepath.Dir(tc.path), 0700); err != nil {
				t.Fatal(err)
			}
			if tc.directory {
				if err := os.Mkdir(tc.path, 0700); err != nil {
					t.Fatal(err)
				}
			} else if err := os.WriteFile(tc.path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			err := g.Check(cwd, nil)
			if err == nil || strings.Contains(err.Error(), tc.path) {
				t.Fatalf("expected path-free guard error, got %v", err)
			}
		})
	}
}

func TestBillingGuardRejectsOversizedAndSymlinkedAuth(t *testing.T) {
	home, cwd := t.TempDir(), t.TempDir()
	t.Setenv("HOME", home)
	authDir := filepath.Join(home, ".codex")
	if err := os.MkdirAll(authDir, 0700); err != nil {
		t.Fatal(err)
	}
	authPath := filepath.Join(authDir, "auth.json")
	if err := os.WriteFile(authPath, []byte(strings.Repeat(" ", maxBillingSettingsBytes+1)), 0600); err != nil {
		t.Fatal(err)
	}
	g := BillingGuard{ManagedSettingsPath: filepath.Join(home, "managed.json")}
	if err := g.Check(cwd, nil); err == nil || !strings.Contains(err.Error(), "codex_auth") {
		t.Fatalf("oversized auth accepted: %v", err)
	}
	if err := os.Remove(authPath); err != nil {
		t.Fatal(err)
	}
	other := filepath.Join(home, "other.json")
	if err := os.WriteFile(other, []byte(`{"auth_mode":"chatgpt"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(other, authPath); err != nil {
		t.Fatal(err)
	}
	if err := g.Check(cwd, nil); err == nil || !strings.Contains(err.Error(), "codex_auth") {
		t.Fatalf("symlinked auth accepted: %v", err)
	}
}

func TestInspectSettingsRejectsNullAndAcceptsUnrelatedEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	if err := os.WriteFile(path, []byte(`{"FOO":"1"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := inspectSettings(path, "user_settings"); err != nil {
		t.Fatalf("unrelated env rejected: %v", err)
	}
	if err := os.WriteFile(path, []byte(`null`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := inspectSettings(path, "user_settings"); err == nil {
		t.Fatal("top-level null accepted")
	}
}

func TestLunaImplAllowed(t *testing.T) {
	for _, tc := range []struct {
		name, contents string
		want           bool
		reason         SupervisorLunaCheckReason
	}{{"allowed", `model = "gpt-6-luna"
[model_allowlist]
gpt-6-luna=["impl"]`, true, ""}, {"not allowed", `model = "gpt-6-luna"
[model_allowlist]
gpt-6-luna=["read"]`, false, LunaNotAllowed}, {"missing", "", false, LunaConfigMissing}, {"broken", "model_allowlist = [", false, LunaConfigInvalid}} {
		t.Run(tc.name, func(t *testing.T) {
			home, err := os.MkdirTemp("/tmp", "sluna-old-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(home) })
			t.Setenv("HOME", home)
			path := filepath.Join(home, ".claude", "codexd", "config.toml")
			if tc.contents != "" {
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(tc.contents), 0600); err != nil {
					t.Fatal(err)
				}
			}
			before, beforeErr := os.ReadFile(path)
			got, reason := LunaImplAllowed()
			after, afterErr := os.ReadFile(path)
			if tc.contents != "" && (beforeErr != nil || afterErr != nil || string(before) != string(after)) {
				t.Fatalf("allowlist changed during read: before=%v after=%v", beforeErr, afterErr)
			}
			if got != tc.want || reason != tc.reason {
				t.Fatalf("got %v want %v, reason: %s", got, tc.want, reason)
			}
		})
	}
}
