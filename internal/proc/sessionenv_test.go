package proc

import (
	"strings"
	"testing"
)

func TestBuildSessionEnvStripsExactNamesAndSetsSessionValues(t *testing.T) {
	parent := []string{
		"ANTHROPIC_API_KEY=secret", "OPENAI_API_KEY=secret", "ANTHROPIC_AUTH_TOKEN=secret",
		"ANTHROPIC_BASE_URL=https://example.invalid", "CLAUDE_CODE_USE_BEDROCK=1", "CLAUDE_CODE_USE_VERTEX=1",
		"CLAUDE_CODE_USE_FOUNDRY=1", "AWS_BEARER_TOKEN_BEDROCK=secret", "CLAUDECODE=1",
		"CLAUDE_CODE_ENTRYPOINT=x", "CLAUDE_CODE_MESSAGING_SOCKET=x", "CLAUDE_CODE_MESSAGING_TOKEN=x",
		"CLAUDE_CODE_EXECPATH=x", "CLAUDE_CODE_SESSION_ID=x", "CLAUDE_CODE_CHILD_SESSION=1",
		"CLAUDE_CODE_SESSION_ATTENDED=1", "CLAUDE_PID=1", "CLAUDE_EFFORT=high",
		"CLAUDE_CODE_SESSION_IDX=keep", "MY_ANTHROPIC_API_KEY=keep", "anthropic_api_key=keep",
		"CLAUDE_CODE_USE_BEDROCK_X=keep", "CLAUDECODE_X=keep", "TERM=old", "CODEXD_SUPERVISED=old", "PATH=/bin",
	}
	got := BuildSessionEnv(parent)
	for forbidden := range strippedSessionEnvNames {
		for _, entry := range got {
			if strings.HasPrefix(entry, forbidden+"=") {
				t.Errorf("forbidden variable remains: %s", forbidden)
			}
		}
	}
	values := make(map[string]string)
	for _, entry := range got {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			t.Fatalf("invalid environment entry %q", entry)
		}
		values[key] = value
	}
	if values["TERM"] != "xterm-256color" || values["CODEXD_SUPERVISED"] != "1" || values["COLUMNS"] != "200" || values["LINES"] != "50" {
		t.Fatalf("session values: %#v", values)
	}
	if values["CLAUDE_CODE_SESSION_IDX"] != "keep" || values["MY_ANTHROPIC_API_KEY"] != "keep" || values["anthropic_api_key"] != "keep" || values["CLAUDE_CODE_USE_BEDROCK_X"] != "keep" || values["CLAUDECODE_X"] != "keep" {
		t.Fatalf("similar variables removed: %#v", values)
	}
	if err := ValidateSessionEnv(got); err != nil {
		t.Fatal(err)
	}
}

func TestValidateSessionEnvRejectsForbiddenAndDuplicates(t *testing.T) {
	if err := ValidateSessionEnv([]string{"ANTHROPIC_API_KEY=x"}); err == nil {
		t.Fatal("expected forbidden key rejection")
	}
	if err := ValidateSessionEnv([]string{"TERM=x", "TERM=y"}); err == nil {
		t.Fatal("expected duplicate rejection")
	}
}

func TestValidateSessionEnvRejectsEachForbiddenName(t *testing.T) {
	for name := range strippedSessionEnvNames {
		t.Run(name, func(t *testing.T) {
			if err := ValidateSessionEnv([]string{name + "=x"}); err == nil {
				t.Fatalf("ValidateSessionEnv accepted forbidden name %q", name)
			}
		})
	}
}

func TestSessionEnvRemovalContractHasTwentyExactNames(t *testing.T) {
	want := []string{
		"ANTHROPIC_API_KEY", "OPENAI_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL",
		"CLAUDE_CODE_USE_BEDROCK", "CLAUDE_CODE_USE_VERTEX", "CLAUDE_CODE_USE_FOUNDRY", "AWS_BEARER_TOKEN_BEDROCK",
		"CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT", "CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_MESSAGING_TOKEN",
		"CLAUDE_CODE_EXECPATH", "CLAUDE_CODE_SESSION_ID", "CLAUDE_CODE_CHILD_SESSION", "CLAUDE_CODE_SESSION_ATTENDED",
		"CLAUDE_PID", "CLAUDE_EFFORT", "CLAUDE_CONFIG_DIR", "CODEX_HOME",
	}
	if len(want) != 20 || len(strippedSessionEnvNames) != 20 {
		t.Fatalf("removal names: specification=%d implementation=%d, want 20", len(want), len(strippedSessionEnvNames))
	}
	for _, name := range want {
		if _, ok := strippedSessionEnvNames[name]; !ok {
			t.Errorf("implementation is missing specified removal name %q", name)
		}
	}
	for _, name := range []string{"anthropic_api_key", "CLAUDE_CODE_USE_BEDROCK_X", "CLAUDECODE_X", "MY_ANTHROPIC_API_KEY"} {
		if _, ok := strippedSessionEnvNames[name]; ok {
			t.Errorf("similar but distinct key %q is stripped", name)
		}
	}
}

func TestValidateSessionEnvRejectsMalformedAndDuplicateEntries(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
	}{
		{"missing separator", []string{"FOO"}},
		{"empty name", []string{"=x"}},
		{"duplicate", []string{"FOO=one", "FOO=two"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateSessionEnv(tc.env); err == nil {
				t.Fatalf("ValidateSessionEnv accepted %#v", tc.env)
			}
		})
	}
	if err := ValidateSessionEnv([]string{"FOO="}); err != nil {
		t.Fatalf("empty value rejected: %v", err)
	}
}
