package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSuperviseStatusline_SCNSup0126(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sline-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	state := filepath.Join(home, "state")
	if err := os.MkdirAll(state, 0700); err != nil {
		t.Fatal(err)
	}
	input := []byte(`{"session_id":"123e4567-e89b-12d3-a456-426614174000","context_window":{"used_percentage":42},"cwd":"secret"}`)
	var out, stderr bytes.Buffer
	if code := runEntrypoint(context.Background(), []string{"supervise-statusline", "--state-dir", state, "--session", "demo-1"}, bytes.NewReader(input), &out, &stderr); code != 0 || out.String() != "\n" || stderr.Len() != 0 {
		t.Fatalf("code=%d out=%q err=%q", code, out.String(), stderr.String())
	}
	data, err := os.ReadFile(filepath.Join(state, "statusline", "demo-1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 || got["used_percentage"] != float64(42) || got["claude_session_id"] != "123e4567-e89b-12d3-a456-426614174000" {
		t.Fatalf("record=%v", got)
	}
	baseline := append([]byte(nil), data...)
	for _, args := range [][]string{{"--session", "demo-1"}, {"--state-dir", "", "--session", "demo-1"}, {"--state-dir", "relative", "--session", "demo-1"}, {"--state-dir", filepath.Join(home, "missing"), "--session", "demo-1"}, {"--state-dir", state, "--session", "../escape"}, {"--state-dir", state, "--session", "slash/name"}, {"--state-dir", state, "--session", ""}} {
		out.Reset()
		stderr.Reset()
		if code := runEntrypoint(context.Background(), append([]string{"supervise-statusline"}, args...), bytes.NewReader(input), &out, &stderr); code != 0 || out.String() != "\n" || stderr.Len() != 0 {
			t.Fatalf("args=%v code=%d out=%q err=%q", args, code, out.String(), stderr.String())
		}
		unchanged, err := os.ReadFile(filepath.Join(state, "statusline", "demo-1.json"))
		if err != nil || !bytes.Equal(unchanged, baseline) {
			t.Fatalf("record changed for %v: %v", args, err)
		}
	}
	for _, invalid := range []string{"{", `{"context_window":{"used_percentage":20}} {}`, `{"context_window":{"used_percentage":120}}`, `{"context_window":{"context_window_size":100}}`} {
		out.Reset()
		stderr.Reset()
		if code := runEntrypoint(context.Background(), []string{"supervise-statusline", "--state-dir", state, "--session", "demo-1"}, strings.NewReader(invalid), &out, &stderr); code != 0 || out.String() != "\n" || stderr.Len() != 0 {
			t.Fatalf("input=%q code=%d out=%q err=%q", invalid, code, out.String(), stderr.String())
		}
		unchanged, err := os.ReadFile(filepath.Join(state, "statusline", "demo-1.json"))
		if err != nil || !bytes.Equal(unchanged, baseline) {
			t.Fatalf("record changed for %q: %v", invalid, err)
		}
	}
	blocked := filepath.Join(home, "blocked")
	if err := os.MkdirAll(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocked, "statusline"), []byte("file"), 0600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	stderr.Reset()
	if code := runEntrypoint(context.Background(), []string{"supervise-statusline", "--state-dir", blocked, "--session", "demo-1"}, bytes.NewReader(input), &out, &stderr); code != 0 || out.String() != "\n" || stderr.Len() != 0 {
		t.Fatalf("write failure code=%d out=%q err=%q", code, out.String(), stderr.String())
	}
	out.Reset()
	stderr.Reset()
	if code := runEntrypoint(context.Background(), []string{"supervise-statusline", "--state-dir", state, "--session", "demo-2"}, strings.NewReader(`{"session_id":"invalid","context_window":{"used_percentage":50},"extra":"secret"}`), &out, &stderr); code != 0 || out.String() != "\n" || stderr.Len() != 0 {
		t.Fatalf("invalid UUID code=%d out=%q err=%q", code, out.String(), stderr.String())
	}
	invalidUUID, err := os.ReadFile(filepath.Join(state, "statusline", "demo-2.json"))
	if err != nil {
		t.Fatal(err)
	}
	got = nil
	if err := json.Unmarshal(invalidUUID, &got); err != nil || len(got) != 4 || got["claude_session_id"] != "" {
		t.Fatalf("invalid UUID record=%v err=%v", got, err)
	}
}

// SCN-sup-01-66: preserve the conversation ID independently of usage.
func TestSuperviseStatusline_SCNSup0166(t *testing.T) {
	const uuid = "123e4567-e89b-12d3-a456-426614174000"
	for _, tc := range []struct {
		name, percentage, sessionID string
		wantUsage                   *int
		unchanged                   bool
	}{
		{name: "decimal", percentage: "42.5", sessionID: uuid},
		{name: "decimal_integer", percentage: "42.0", sessionID: uuid},
		{name: "exponent", percentage: "4.2e1", sessionID: uuid},
		{name: "string", percentage: `"42"`, sessionID: uuid},
		{name: "true", percentage: "true", sessionID: uuid},
		{name: "false", percentage: "false", sessionID: uuid},
		{name: "array", percentage: "[42]", sessionID: uuid},
		{name: "object", percentage: `{"v":42}`, sessionID: uuid},
		{name: "null", percentage: "null", sessionID: uuid, wantUsage: statuslineTestInt(42)},
		{name: "absent", sessionID: uuid, wantUsage: statuslineTestInt(42)},
		{name: "zero", percentage: "0", sessionID: uuid, wantUsage: statuslineTestInt(0)},
		{name: "hundred", percentage: "100", sessionID: uuid, wantUsage: statuslineTestInt(100)},
		{name: "negative", percentage: "-1", sessionID: uuid, unchanged: true},
		{name: "out_of_range", percentage: "150", sessionID: uuid, unchanged: true},
		{name: "no_id", percentage: "42.5", unchanged: true},
		{name: "invalid_id", percentage: "42.5", sessionID: "invalid", unchanged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := t.TempDir()
			statusDir := filepath.Join(state, "statusline")
			if err := os.Mkdir(statusDir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(statusDir, "demo-1.json")
			baseline := []byte(`{"schema_version":1,"observed_at":"2026-10-20T00:00:00Z","used_percentage":70}`)
			if err := os.WriteFile(path, baseline, 0600); err != nil {
				t.Fatal(err)
			}
			usage := ""
			if tc.percentage != "" {
				usage = `"used_percentage":` + tc.percentage + ","
			}
			input := `{"session_id":"` + tc.sessionID + `","context_window":{` + usage + `"context_window_size":1000,"current_usage":{"input_tokens":400,"output_tokens":10,"cache_creation_input_tokens":10,"cache_read_input_tokens":9}},"cwd":"secret"}`
			var out, stderr bytes.Buffer
			before := time.Now().UTC()
			code := runEntrypoint(context.Background(), []string{"supervise-statusline", "--state-dir", state, "--session", "demo-1"}, strings.NewReader(input), &out, &stderr)
			if code != 0 || out.String() != "\n" || stderr.Len() != 0 {
				t.Fatalf("code=%d out=%q stderr=%q", code, out.String(), stderr.String())
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if tc.unchanged {
				if !bytes.Equal(data, baseline) {
					t.Fatalf("record changed: %s", data)
				}
				return
			}
			var got map[string]json.RawMessage
			if err := json.Unmarshal(data, &got); err != nil {
				t.Fatal(err)
			}
			if string(got["schema_version"]) != "1" || string(got["claude_session_id"]) != `"`+uuid+`"` {
				t.Fatalf("record=%s", data)
			}
			var observed time.Time
			if err := json.Unmarshal(got["observed_at"], &observed); err != nil || observed.Before(before) || observed.After(time.Now().UTC()) {
				t.Fatalf("observed_at=%s err=%v", got["observed_at"], err)
			}
			if tc.wantUsage == nil {
				if _, ok := got["used_percentage"]; ok || len(got) != 3 {
					t.Fatalf("unknown usage record=%s", data)
				}
			} else {
				var used int
				if err := json.Unmarshal(got["used_percentage"], &used); err != nil || used != *tc.wantUsage || len(got) != 4 {
					t.Fatalf("fallback/valid usage record=%s err=%v", data, err)
				}
			}
		})
	}
}

func TestSuperviseStatusline_MissingUsageDoesNotWrite(t *testing.T) {
	for _, tc := range []struct {
		name, contextWindow string
	}{
		{name: "absent", contextWindow: `{"context_window_size":0}`},
		{name: "null", contextWindow: `{"used_percentage":null,"context_window_size":0}`},
		{name: "missing_current_usage", contextWindow: `{"context_window_size":1000}`},
		{name: "null_current_usage", contextWindow: `{"used_percentage":null,"context_window_size":1000,"current_usage":null}`},
		{name: "zero_window_with_usage", contextWindow: `{"context_window_size":0,"current_usage":{"input_tokens":400}}`},
	} {
		for _, existing := range []bool{true, false} {
			name := "new"
			if existing {
				name = "existing"
			}
			t.Run(tc.name+"/"+name, func(t *testing.T) {
				state := t.TempDir()
				statusDir := filepath.Join(state, "statusline")
				path := filepath.Join(statusDir, "demo-1.json")
				baseline := []byte(`{"schema_version":1,"observed_at":"2026-10-20T00:00:00Z","used_percentage":70,"claude_session_id":"123e4567-e89b-12d3-a456-426614174000"}`)
				if existing {
					if err := os.Mkdir(statusDir, 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, baseline, 0600); err != nil {
						t.Fatal(err)
					}
				}
				input := `{"session_id":"123e4567-e89b-12d3-a456-426614174000","context_window":` + tc.contextWindow + `}`
				var out, stderr bytes.Buffer
				code := runEntrypoint(context.Background(), []string{"supervise-statusline", "--state-dir", state, "--session", "demo-1"}, strings.NewReader(input), &out, &stderr)
				if code != 0 || out.String() != "\n" || stderr.Len() != 0 {
					t.Fatalf("code=%d out=%q stderr=%q", code, out.String(), stderr.String())
				}
				if !existing {
					if _, err := os.Lstat(statusDir); !os.IsNotExist(err) {
						t.Fatalf("statusline directory created: %v", err)
					}
					return
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(data, baseline) {
					t.Fatalf("record changed: %s", data)
				}
			})
		}
	}
}

func statuslineTestInt(v int) *int { return &v }
