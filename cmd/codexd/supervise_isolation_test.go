package main

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	supervisortransport "github.com/yoshikihorie/codex-runner/internal/supervisor/transport"
)

func TestSuperviseIsolation_SCNSup0534(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sisolate-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	previousDaemon := runDaemonMode
	previousCall := supervisorClientCall
	t.Cleanup(func() {
		runDaemonMode = previousDaemon
		supervisorClientCall = previousCall
	})
	daemonCalls := 0
	runDaemonMode = func(context.Context, []string, io.Writer) error {
		daemonCalls++
		return nil
	}
	if code := runEntrypoint(context.Background(), nil, strings.NewReader(""), io.Discard, io.Discard); code != 0 || daemonCalls != 1 {
		t.Fatalf("legacy daemon code=%d calls=%d", code, daemonCalls)
	}
	wantSocket := filepath.Join(home, ".claude", "run", "codexd-supervisor.sock")
	clientCalls := 0
	supervisorClientCall = func(_ context.Context, client supervisortransport.Client, request supervisortransport.Request) (supervisortransport.Response, error) {
		clientCalls++
		if client.SocketPath != wantSocket || request.Verb != "status" {
			t.Fatalf("client socket=%q verb=%q", client.SocketPath, request.Verb)
		}
		return supervisortransport.Response{OK: true}, nil
	}
	var stdout, stderr bytes.Buffer
	if code := runEntrypoint(context.Background(), []string{"supervisor-client", "status"}, strings.NewReader(""), &stdout, &stderr); code != 0 || clientCalls != 1 || daemonCalls != 1 {
		t.Fatalf("supervisor client code=%d calls=%d daemon=%d stderr=%q", code, clientCalls, daemonCalls, stderr.String())
	}
	for _, name := range []string{"codexd.lock", "codexd.sock"} {
		if _, err := os.Stat(filepath.Join(home, ".claude", "run", name)); !os.IsNotExist(err) {
			t.Fatalf("legacy path %s touched: %v", name, err)
		}
	}
}
