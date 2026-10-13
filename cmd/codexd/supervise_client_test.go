package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	supervisortransport "github.com/yoshikihorie/codex-runner/internal/supervisor/transport"
)

func TestSupervisorClient_SCNSup0530And0544(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sclient-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	socket := filepath.Join(home, "absent.sock")
	called := false
	old := supervisorClientCall
	supervisorClientCall = func(context.Context, supervisortransport.Client, supervisortransport.Request) (supervisortransport.Response, error) {
		called = true
		return supervisortransport.Response{OK: false, Error: &supervisortransport.ErrorBody{Code: "SUP_SESSION_NOT_FOUND"}}, nil
	}
	t.Cleanup(func() { supervisorClientCall = old })
	for _, tc := range []struct {
		args []string
		want int
		call bool
	}{
		{[]string{"unknown"}, 2, false},
		{[]string{"status", "--socket-path", socket}, 1, true},
		{[]string{"resume", "--socket-path", socket}, 1, false},
	} {
		called = false
		var out, stderr bytes.Buffer
		code := runEntrypoint(context.Background(), append([]string{"supervisor-client"}, tc.args...), strings.NewReader(""), &out, &stderr)
		if code != tc.want || called != tc.call {
			t.Fatalf("args=%v code=%d called=%v out=%q err=%q", tc.args, code, called, out.String(), stderr.String())
		}
		if len(tc.args) > 0 && tc.args[0] == "resume" && !strings.Contains(out.String(), "SUP_RESUME_FORBIDDEN") {
			t.Fatalf("resume out=%q", out.String())
		}
	}
	for _, tc := range []struct {
		response supervisortransport.Response
		err      error
		want     int
	}{{supervisortransport.Response{OK: true}, nil, 0}, {supervisortransport.Response{}, supervisortransport.ErrClientUnavailable, 3}} {
		supervisorClientCall = func(context.Context, supervisortransport.Client, supervisortransport.Request) (supervisortransport.Response, error) {
			return tc.response, tc.err
		}
		var out, stderr bytes.Buffer
		code := runEntrypoint(context.Background(), []string{"supervisor-client", "status", "--socket-path", socket}, strings.NewReader(""), &out, &stderr)
		if code != tc.want {
			t.Fatalf("code=%d want=%d out=%q err=%q", code, tc.want, out.String(), stderr.String())
		}
		if tc.err == supervisortransport.ErrClientUnavailable && !strings.Contains(stderr.String(), "SUP_CLIENT_UNAVAILABLE error.supervisor.clientUnavailable") {
			t.Fatalf("unavailable stderr=%q", stderr.String())
		}
	}
}

func TestSupervisorClient_SCNSup0544CharDeviceIsNotTerminal(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "stty-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	device, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	called := false
	old := supervisorClientCall
	supervisorClientCall = func(context.Context, supervisortransport.Client, supervisortransport.Request) (supervisortransport.Response, error) {
		called = true
		return supervisortransport.Response{OK: true}, nil
	}
	t.Cleanup(func() { supervisorClientCall = old })
	var out, stderr bytes.Buffer
	if code := runEntrypoint(context.Background(), []string{"supervisor-client", "resume", "--socket-path", filepath.Join(home, "absent.sock")}, device, &out, &stderr); code != 1 || called || !strings.Contains(out.String(), "SUP_RESUME_FORBIDDEN") {
		t.Fatalf("code=%d called=%v out=%q err=%q", code, called, out.String(), stderr.String())
	}
}

func TestSupervisorClient_SCNSup0544SupervisedMarkerRefusesBeforeConnect(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "smarker-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	t.Setenv("CODEXD_SUPERVISED", "1")
	device, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	oldTTY, oldCall := supervisorClientIsTerminal, supervisorClientCall
	supervisorClientIsTerminal = func(*os.File) bool { return true }
	called := false
	supervisorClientCall = func(context.Context, supervisortransport.Client, supervisortransport.Request) (supervisortransport.Response, error) {
		called = true
		return supervisortransport.Response{OK: true}, nil
	}
	t.Cleanup(func() { supervisorClientIsTerminal = oldTTY; supervisorClientCall = oldCall })
	var out, stderr bytes.Buffer
	code := runEntrypoint(context.Background(), []string{"supervisor-client", "resume", "--socket-path", filepath.Join(home, "absent.sock")}, device, &out, &stderr)
	if code != 1 || called || !strings.Contains(out.String(), "SUP_RESUME_FORBIDDEN") {
		t.Fatalf("code=%d called=%v out=%q err=%q", code, called, out.String(), stderr.String())
	}
	if _, err := os.Stat(filepath.Join(home, ".claude", "codexd", "supervisor", "events.jsonl")); !os.IsNotExist(err) {
		t.Fatalf("events recorded: %v", err)
	}
}
