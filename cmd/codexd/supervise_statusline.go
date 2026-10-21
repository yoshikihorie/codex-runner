package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"time"

	corestore "github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

var superviseClaudeUUID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// FD-sup-01 §5.3b and validation-rules.yaml: decimal/exponent notation is not an integer literal.
var superviseStatuslineInteger = regexp.MustCompile(`^-?[0-9]+$`)

const superviseStatuslineSchemaVersion = 1 // FD-sup-01 §5.3b statusline record.

type superviseStatuslineInput struct {
	SessionID     string `json:"session_id"`
	ContextWindow struct {
		UsedPercentage    json.RawMessage `json:"used_percentage"`
		ContextWindowSize int             `json:"context_window_size"`
		CurrentUsage      *struct {
			InputTokens              int `json:"input_tokens"`
			OutputTokens             int `json:"output_tokens"`
			CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
			CacheReadInputTokens     int `json:"cache_read_input_tokens"`
		} `json:"current_usage"`
	} `json:"context_window"`
}

func runSuperviseStatusline(args []string, stdin io.Reader, stdout io.Writer) int {
	defer func() { _, _ = io.WriteString(stdout, "\n") }()
	flags := flag.NewFlagSet("supervise-statusline", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	stateDir := flags.String("state-dir", "", "")
	session := flags.String("session", "", "")
	if flags.Parse(args) != nil || len(flags.Args()) != 0 || !filepath.IsAbs(*stateDir) || *stateDir == "" {
		return 0
	}
	if _, err := supervisor.NewSessionID(*session); err != nil {
		return 0
	}
	info, err := os.Lstat(*stateDir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return 0
	}
	var input superviseStatuslineInput
	decoder := json.NewDecoder(io.LimitReader(stdin, 64*1024+1))
	if err := decoder.Decode(&input); err != nil {
		return 0
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return 0
	}
	rawUsed := bytes.TrimSpace(input.ContextWindow.UsedPercentage)
	var used *int
	if superviseStatuslineInteger.Match(rawUsed) {
		var value int
		// An integer too large for int is also out of range: preserve the no-write rule.
		if json.Unmarshal(rawUsed, &value) != nil {
			return 0
		}
		used = &value
	}
	if len(rawUsed) == 0 || bytes.Equal(rawUsed, []byte("null")) {
		// FD-sup-01 §5.3b: missing usage without a fallback must not refresh the record.
		if input.ContextWindow.ContextWindowSize <= 0 || input.ContextWindow.CurrentUsage == nil {
			return 0
		}
		current := input.ContextWindow.CurrentUsage
		value := 100 * (current.InputTokens + current.OutputTokens + current.CacheCreationInputTokens + current.CacheReadInputTokens) / input.ContextWindow.ContextWindowSize
		used = &value
	}
	if used != nil && (*used < 0 || *used > 100) {
		return 0
	}
	claudeID := input.SessionID
	if !superviseClaudeUUID.MatchString(claudeID) {
		claudeID = ""
	}
	if used == nil && claudeID == "" {
		return 0
	}

	record := struct {
		SchemaVersion   int       `json:"schema_version"`
		ObservedAt      time.Time `json:"observed_at"`
		UsedPercentage  *int      `json:"used_percentage,omitempty"`
		ClaudeSessionID string    `json:"claude_session_id"`
	}{superviseStatuslineSchemaVersion, time.Now().UTC(), used, claudeID}
	data, err := json.Marshal(record)
	if err != nil {
		return 0
	}
	statusDir := filepath.Join(*stateDir, "statusline")
	if err := os.MkdirAll(statusDir, 0700); err != nil {
		return 0
	}
	statusInfo, err := os.Lstat(statusDir)
	if err != nil || !statusInfo.IsDir() || statusInfo.Mode()&os.ModeSymlink != 0 {
		return 0
	}
	_ = corestore.WriteAtomic(filepath.Join(statusDir, *session+".json"), append(data, '\n'), 0600)
	return 0
}
