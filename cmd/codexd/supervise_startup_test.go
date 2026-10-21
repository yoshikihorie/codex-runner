package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestSupervise_SCNSup0517And0548And0557(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-start-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	configPath := filepath.Join(home, ".claude", "codexd", "supervisor.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte("notify_command = \"none\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	var stderr bytes.Buffer
	go func() {
		done <- runEntrypoint(ctx, []string{"supervise"}, strings.NewReader(""), &bytes.Buffer{}, &stderr)
	}()
	socket := filepath.Join(home, ".claude", "run", "codexd-supervisor.sock")
	deadline := time.After(5 * time.Second)
	for {
		if conn, err := net.Dial("unix", socket); err == nil {
			_ = conn.Close()
			break
		}
		select {
		case code := <-done:
			t.Fatalf("startup code=%d err=%q", code, stderr.String())
		case <-deadline:
			t.Fatal("socket not ready")
		case <-time.After(10 * time.Millisecond):
		}
	}
	eventsPath := filepath.Join(home, ".claude", "codexd", "supervisor", "events.jsonl")
	for {
		if data, err := os.ReadFile(eventsPath); err == nil && bytes.Contains(data, []byte("luna_impl_not_allowed")) {
			break
		}
		select {
		case code := <-done:
			t.Fatalf("startup code=%d err=%q", code, stderr.String())
		case <-deadline:
			t.Fatal("startup events not recorded")
		case <-time.After(10 * time.Millisecond):
		}
	}
	var secondErr bytes.Buffer
	if code := runEntrypoint(context.Background(), []string{"supervise"}, strings.NewReader(""), &bytes.Buffer{}, &secondErr); code != 1 || !strings.Contains(secondErr.String(), "SUP_INSTANCE_LOCK_HELD") {
		t.Fatalf("second code=%d err=%q", code, secondErr.String())
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("shutdown code=%d err=%q", code, stderr.String())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown timeout")
	}
	data, err := os.ReadFile(filepath.Join(home, ".claude", "codexd", "supervisor", "events.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(data, []byte("supervisor_started")) || !bytes.Contains(data, []byte("luna_impl_not_allowed")) || !bytes.Contains(data, []byte("config_missing")) {
		t.Fatalf("events=%q", data)
	}
	if _, err := os.Stat(filepath.Join(home, ".claude", "run", "codexd.lock")); !os.IsNotExist(err) {
		t.Fatalf("daemon lock touched: %v", err)
	}
}

func TestSuperviseStartupNotifyCommandReceivesTwoArguments(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "snotify-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	configPath := filepath.Join(home, ".claude", "codexd", "supervisor.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0700); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(home, "args")
	command := filepath.Join(home, "notify")
	if err := os.WriteFile(command, []byte("#!/bin/sh\nprintf '%s\\n' \"$#\" \"$1\" \"$2\" >> \""+output+"\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte("notify_command = \""+command+"\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() {
		done <- runEntrypoint(ctx, []string{"supervise"}, strings.NewReader(""), &bytes.Buffer{}, &bytes.Buffer{})
	}()
	deadline := time.After(5 * time.Second)
	for {
		if data, err := os.ReadFile(output); err == nil {
			lines := strings.Split(strings.TrimSpace(string(data)), "\n")
			if len(lines) < 3 || lines[0] != "2" || lines[1] != "supervisor_started" || lines[2] != "-" {
				t.Fatalf("args=%q", data)
			}
			break
		}
		select {
		case code := <-done:
			t.Fatalf("startup code=%d", code)
		case <-deadline:
			t.Fatal("notification not invoked")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("shutdown code=%d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown timeout")
	}
}

func TestSuperviseStartup_SCNSup0548ConfigBeforeLock(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sconfig-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	path := filepath.Join(home, ".claude", "codexd", "supervisor.toml")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("unknown_key = true\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	if code := runEntrypoint(context.Background(), []string{"supervise"}, strings.NewReader(""), &bytes.Buffer{}, &stderr); code != 1 || !strings.Contains(stderr.String(), "unknown_key") {
		t.Fatalf("code=%d err=%q", code, stderr.String())
	}
	if _, err := os.Stat(filepath.Join(home, ".claude", "run", supervisorLockName)); !os.IsNotExist(err) {
		t.Fatalf("lock touched: %v", err)
	}
}

func TestSuperviseStartup_SCNSup0548CustomSocketCreatesLockParent(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "scustom-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	customDir := filepath.Join(home, "socket")
	if err := os.MkdirAll(customDir, 0700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(home, ".claude", "codexd", "supervisor.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0700); err != nil {
		t.Fatal(err)
	}
	canonicalHome, err := filepath.EvalSymlinks(home)
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(canonicalHome, "socket", "supervisor.sock")
	if err := os.WriteFile(configPath, []byte("notify_command = \"none\"\nsocket_path = \""+socket+"\"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	var stderr bytes.Buffer
	go func() {
		done <- runEntrypoint(ctx, []string{"supervise"}, strings.NewReader(""), &bytes.Buffer{}, &stderr)
	}()
	deadline := time.After(5 * time.Second)
	for {
		if conn, err := net.Dial("unix", socket); err == nil {
			_ = conn.Close()
			break
		}
		select {
		case code := <-done:
			t.Fatalf("startup code=%d err=%q", code, stderr.String())
		case <-deadline:
			t.Fatal("socket not ready")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("shutdown code=%d err=%q", code, stderr.String())
		}
	case <-deadline:
		t.Fatal("shutdown timeout")
	}
}
