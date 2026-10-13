package launch

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

var supervisorDenyRules = []string{
	"Bash(git push --force*)", "Bash(git push -f*)", "Bash(git push*--force*)", "Bash(git push*+*)", "Bash(git push*--mirror*)", "Bash(git push*--delete*)",
	"Bash(gh pr create*)", "Bash(gh pr merge*)", "Bash(gh release*)", "Bash(*deploy*)", "Bash(codexd supervisor-client*)", "Bash(*codexd-supervisor*)",
	"Write(<state_dir>/**)", "Edit(<state_dir>/**)", "Bash(*<state_dir>*)",
}

const supervisorStateDirPlaceholder = "<state_dir>"

type settingsDocument struct {
	StatusLine struct {
		Type    string `json:"type"`
		Command string `json:"command"`
	} `json:"statusLine"`
	Permissions struct {
		Deny []string `json:"deny"`
	} `json:"permissions"`
}

// BuildSettingsJSON creates the settings passed to the supervised Claude CLI.
func BuildSettingsJSON(codexdPath, sessionID, stateDir string) ([]byte, error) {
	if !filepath.IsAbs(codexdPath) || !filepath.IsAbs(stateDir) || filepath.Clean(stateDir) != stateDir || sessionID == "" || strings.ContainsAny(sessionID, "\r\n\x00") {
		return nil, fmt.Errorf("invalid settings arguments")
	}
	var doc settingsDocument
	doc.StatusLine.Type = "command"
	doc.StatusLine.Command = shellQuote(codexdPath) + " supervise-statusline --state-dir " + shellQuote(stateDir) + " --session " + shellQuote(sessionID)
	doc.Permissions.Deny = make([]string, len(supervisorDenyRules))
	for i, rule := range supervisorDenyRules {
		replacement := "/" + stateDir
		if strings.HasPrefix(rule, "Bash(*<state_dir>") {
			replacement = stateDir
		}
		doc.Permissions.Deny[i] = strings.ReplaceAll(rule, supervisorStateDirPlaceholder, replacement)
	}
	return json.Marshal(doc)
}

// WriteSettingsFile atomically writes settings with owner-only permissions.
func WriteSettingsFile(path string, data []byte) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("settings path must be absolute")
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".settings-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(tmp)
		}
	}()
	if err = f.Chmod(0600); err != nil {
		return err
	}
	if _, err = f.Write(data); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	ok = true
	dirFile, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer dirFile.Close()
	if err := dirFile.Sync(); err != nil {
		return err
	}
	return nil
}

func shellQuote(value string) string {
	if value != "" && strings.Trim(value, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./-") == "" {
		return value
	}
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}
