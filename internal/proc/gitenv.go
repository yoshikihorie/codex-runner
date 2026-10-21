package proc

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

type GitEnvPurpose string

const (
	GitEnvVerify GitEnvPurpose = "verify"
	GitEnvLocal  GitEnvPurpose = "git_local"
	GitEnvPush   GitEnvPurpose = "git_push"
	GitEnvNotify GitEnvPurpose = "notify"
	gitOpsPath                 = "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin"
)

// GitOpsEnv creates the small environment used by verification and git operations.
func GitOpsEnv(purpose GitEnvPurpose, passSSHAuthSock bool, parent []string, stateDir string) ([]string, error) {
	if purpose != GitEnvVerify && purpose != GitEnvLocal && purpose != GitEnvPush && purpose != GitEnvNotify {
		return nil, fmt.Errorf("invalid git environment purpose")
	}
	values := make(map[string]string, len(parent))
	for _, entry := range parent {
		key, value, ok := strings.Cut(entry, "=")
		if ok && key != "" {
			values[key] = value
		}
	}
	out := []string{"PATH=" + gitOpsPath, "GIT_TERMINAL_PROMPT=0", "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null"}
	if stateDir == "" || !filepath.IsAbs(stateDir) {
		return nil, fmt.Errorf("invalid state directory")
	}
	xdg := filepath.Join(stateDir, "git-xdg-empty")
	if err := os.Mkdir(xdg, 0700); err != nil && !os.IsExist(err) {
		return nil, fmt.Errorf("cannot create git xdg directory")
	}
	info, err := os.Lstat(xdg)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("unsafe git xdg directory")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return nil, fmt.Errorf("git xdg directory not owned by current user")
	}
	entries, err := os.ReadDir(xdg)
	if err != nil || len(entries) != 0 {
		return nil, fmt.Errorf("git xdg directory is not empty")
	}
	out = append(out, "XDG_CONFIG_HOME="+xdg)
	if home, ok := values["HOME"]; ok {
		out = append(out, "HOME="+home)
	}
	if purpose == GitEnvPush {
		out = append(out, `GIT_SSH_COMMAND=ssh -F /dev/null -o BatchMode=yes -o StrictHostKeyChecking=yes`)
		for _, key := range []string{"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy", "SSL_CERT_FILE", "NODE_EXTRA_CA_CERTS"} {
			if value := values[key]; value != "" && validateAllowedEnvValue(key, value) == nil {
				out = append(out, key+"="+value)
			}
		}
		sock := values["SSH_AUTH_SOCK"]
		if passSSHAuthSock && sock != "" {
			if !filepath.IsAbs(sock) {
				goto invalidSocket
			}
			info, err := os.Stat(sock)
			if err != nil || info.Mode()&os.ModeSocket == 0 {
				goto invalidSocket
			}
			if err := validateAllowedEnvValue("SSH_AUTH_SOCK", sock); err != nil {
				goto invalidSocket
			}
			out = append(out, "SSH_AUTH_SOCK="+sock)
		}
	}
	return out, nil

invalidSocket:
	slog.Warn("excluded invalid SSH_AUTH_SOCK")
	return out, nil
}
