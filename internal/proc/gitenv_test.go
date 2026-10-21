package proc

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGitOpsEnvPurposeIsolation(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "agent.sock")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	parent := []string{"HOME=" + t.TempDir(), "SSH_AUTH_SOCK=" + sock, "GH_TOKEN=dummy", "GITHUB_TOKEN=dummy", "AWS_ACCESS_KEY_ID=dummy", "ANTHROPIC_API_KEY=dummy", "CLAUDE_CONFIG_DIR=/tmp/claude", "CODEX_HOME=/tmp/codex", "KEEP_ME=yes"}
	for _, purpose := range []GitEnvPurpose{GitEnvVerify, GitEnvLocal, GitEnvPush, GitEnvNotify} {
		env, err := GitOpsEnv(purpose, true, parent, t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		values := map[string]string{}
		for _, entry := range env {
			k, v, _ := strings.Cut(entry, "=")
			values[k] = v
		}
		if values["PATH"] != "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin" || values["GIT_CONFIG_NOSYSTEM"] != "1" || values["GIT_CONFIG_GLOBAL"] != "/dev/null" || values["GIT_TERMINAL_PROMPT"] != "0" {
			t.Fatalf("fixed env mismatch for %s: %#v", purpose, values)
		}
		if values["GH_TOKEN"] != "" || values["AWS_ACCESS_KEY_ID"] != "" || values["ANTHROPIC_API_KEY"] != "" || values["CLAUDE_CONFIG_DIR"] != "" || values["CODEX_HOME"] != "" {
			t.Fatalf("secret leaked for %s: %#v", purpose, values)
		}
		if purpose == GitEnvPush && values["SSH_AUTH_SOCK"] != sock {
			t.Fatalf("push sock missing: %#v", values)
		}
		if purpose != GitEnvPush && values["SSH_AUTH_SOCK"] != "" {
			t.Fatalf("sock leaked for %s", purpose)
		}
	}
}

func TestGitOpsEnvInvalidSocketIsDroppedAndConfigSourcesAreIsolated(t *testing.T) {
	state := t.TempDir()
	home := t.TempDir()
	for _, p := range []string{filepath.Join(home, ".config", "git")} {
		if err := os.MkdirAll(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(home, ".config", "git", "ignore"), []byte("evil\n"), 0600); err != nil {
		t.Fatal(err)
	}
	env, err := GitOpsEnv(GitEnvPush, true, []string{"HOME=" + home, "SSH_AUTH_SOCK=/no/such/socket", "GIT_DIR=/tmp/evil", "GIT_CONFIG_GLOBAL=/tmp/evil"}, state)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	for _, item := range env {
		k, v, _ := strings.Cut(item, "=")
		values[k] = v
	}
	if values["SSH_AUTH_SOCK"] != "" || values["GIT_DIR"] != "" || values["GIT_CONFIG_GLOBAL"] != "/dev/null" {
		t.Fatalf("unsafe environment: %#v", values)
	}
	if values["XDG_CONFIG_HOME"] != filepath.Join(state, "git-xdg-empty") || values["HOME"] != home {
		t.Fatalf("unexpected isolated dirs: %#v", values)
	}
	env, err = GitOpsEnv(GitEnvPush, false, []string{"SSH_AUTH_SOCK=/private/tmp/fake.sock"}, state)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range env {
		if strings.HasPrefix(item, "SSH_AUTH_SOCK=") {
			t.Fatalf("socket passed when disabled: %s", item)
		}
	}
}

func TestGitOpsEnvRejectsPopulatedOrLinkedXDG(t *testing.T) {
	for _, setup := range []struct {
		name string
		make func(string) error
	}{
		{"populated", func(p string) error {
			if err := os.Mkdir(p, 0700); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(p, "config"), []byte("trap"), 0600)
		}},
		{"symlink", func(p string) error { return os.Symlink(t.TempDir(), p) }},
	} {
		t.Run(setup.name, func(t *testing.T) {
			state := t.TempDir()
			if err := setup.make(filepath.Join(state, "git-xdg-empty")); err != nil {
				t.Fatal(err)
			}
			if _, err := GitOpsEnv(GitEnvLocal, false, []string{"HOME=" + t.TempDir()}, state); err == nil {
				t.Fatal("accepted unsafe XDG directory")
			}
		})
	}
}
