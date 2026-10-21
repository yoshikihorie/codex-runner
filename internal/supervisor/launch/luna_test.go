package launch

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLunaImplAllowed_SCNSup0557Reasons(t *testing.T) {
	for _, tc := range []struct {
		name, config string
		reason       SupervisorLunaCheckReason
		allowed      bool
	}{
		{"missing", "", "config_missing", false},
		{"invalid", "model_allowlist = [", "config_invalid", false},
		{"not_allowed", "model = \"gpt-6-luna\"\n[model_allowlist]\ngpt-6-luna=[\"read\"]", "not_allowed", false},
		{"allowed", "model = \"gpt-6-luna\"\n[model_allowlist]\ngpt-6-luna=[\"impl\"]", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			home, err := os.MkdirTemp("/tmp", "sluna-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(home) })
			t.Setenv("HOME", home)
			if tc.config != "" {
				path := filepath.Join(home, ".claude", "codexd", "config.toml")
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(tc.config), 0600); err != nil {
					t.Fatal(err)
				}
			}
			allowed, reason := LunaImplAllowed()
			if allowed != tc.allowed || reason != tc.reason {
				t.Fatalf("allowed=%v reason=%q", allowed, reason)
			}
		})
	}
}
