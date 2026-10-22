package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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

// A record must have three newline-terminated fields. A clean EOF before the
// first field differs from a truncated record and from an underlying I/O error.
func readStartupNotifyRecord(reader *bufio.Reader) ([3]string, error) {
	var record [3]string
	for i := range record {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) && (i > 0 || line != "") {
				err = io.ErrUnexpectedEOF
			}
			return record, err
		}
		record[i] = strings.TrimSuffix(line, "\n")
	}
	return record, nil
}

func validateStartupNotifyRecord(record [3]string) error {
	if record != [3]string{"2", "supervisor_started", "-"} {
		return fmt.Errorf("startup arguments=%q", record)
	}
	return nil
}

type startupNotifyReadError struct{}

func (startupNotifyReadError) Read([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestSuperviseStartupNotifyCommandReceivesTwoArguments(t *testing.T) {
	t.Run("record parser", func(t *testing.T) {
		for _, tc := range []struct {
			name, input string
			want        error
		}{
			{"early EOF", "", io.EOF}, {"missing field", "2\nsupervisor_started\n", io.ErrUnexpectedEOF},
			{"unterminated field", "2\nsupervisor_started\n-", io.ErrUnexpectedEOF},
		} {
			t.Run(tc.name, func(t *testing.T) {
				_, err := readStartupNotifyRecord(bufio.NewReader(strings.NewReader(tc.input)))
				if !errors.Is(err, tc.want) {
					t.Fatalf("error=%v, want %v", err, tc.want)
				}
			})
		}
		if _, err := readStartupNotifyRecord(bufio.NewReader(startupNotifyReadError{})); !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("read error=%v", err)
		}
		for _, record := range [][3]string{{"1", "supervisor_started", "-"}, {"2", "other", "-"}, {"2", "supervisor_started", "session"}} {
			if err := validateStartupNotifyRecord(record); err == nil {
				t.Fatalf("accepted %q", record)
			}
		}
	})
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
	held := heldTimingFIFO(t, output)
	reader, err := os.Open(output)
	if err != nil {
		t.Fatal(err)
	}
	command := filepath.Join(home, "notify")
	if err := os.WriteFile(command, []byte("#!/bin/sh\nprintf '%s\\n' \"$#\" \"$1\" \"$2\" > '"+output+"'\n"), 0700); err != nil {
		_ = held.Close()
		_ = reader.Close()
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, []byte("notify_command = \""+command+"\"\n"), 0600); err != nil {
		_ = held.Close()
		_ = reader.Close()
		t.Fatal(err)
	}
	first := make(chan error, 1)
	drained := make(chan error, 1)
	go func() {
		input := bufio.NewReader(reader)
		record, err := readStartupNotifyRecord(input)
		if err == nil {
			err = validateStartupNotifyRecord(record)
		}
		first <- err
		_, err = io.Copy(io.Discard, input)
		drained <- err
	}()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var code int
	go func() {
		defer close(done)
		code = runEntrypoint(ctx, []string{"supervise"}, strings.NewReader(""), io.Discard, io.Discard)
	}()
	defer func() {
		cancel()
		// Keep both the drainer and the holding descriptor alive until all notifications finish.
		awaitTimingTest(t, done, "supervisor shutdown with notification drain")
		_ = held.Close()
		if err := awaitTimingTest(t, drained, "notification reader cleanup"); err != nil {
			t.Errorf("drain: %v", err)
		}
		_ = reader.Close()
	}()
	if err := awaitTimingTest(t, first, "first complete notification"); err != nil {
		t.Fatal(err)
	}
	cancel()
	awaitTimingTest(t, done, "supervisor shutdown")
	if code != 0 {
		t.Fatalf("shutdown code=%d", code)
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
