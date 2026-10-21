package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
