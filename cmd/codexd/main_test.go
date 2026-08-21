package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/config"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/transport"
	"github.com/yoshikihorie/codex-runner/internal/transport/client"
)

type terminatorRunnerFake struct {
	killCalls int
}

type clientContextKey struct{}

type failOnReadReader struct {
	t *testing.T
}

func (r failOnReadReader) Read([]byte) (int, error) {
	r.t.Fatal("stdin was read")
	return 0, errors.New("stdin was read")
}

func TestRunEntrypointDispatchesDaemonMode(t *testing.T) {
	previousDaemon := runDaemonMode
	defer func() { runDaemonMode = previousDaemon }()

	for _, tc := range []struct {
		name string
		args []string
		err  error
		want int
	}{
		{name: "no arguments", want: 0},
		{name: "no arguments daemon error", err: errors.New("daemon failed"), want: 1},
		{name: "non-client command", args: []string{"serve"}, want: 0},
		{name: "daemon error", args: []string{"serve"}, err: errors.New("daemon failed"), want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			runDaemonMode = func(context.Context, []string, io.Writer) error {
				calls++
				return tc.err
			}

			var stderr bytes.Buffer
			if got := runEntrypoint(context.Background(), tc.args, failOnReadReader{t: t}, io.Discard, &stderr); got != tc.want {
				t.Fatalf("code=%d, want %d", got, tc.want)
			}
			if calls != 1 {
				t.Fatalf("runDaemonMode calls=%d, want 1", calls)
			}
		})
	}
}

func TestRunEntrypointDispatchesClientRequest(t *testing.T) {
	previousDaemon := runDaemonMode
	previousRequest := newClientRequest
	previousDial := dialAndSend
	defer func() {
		runDaemonMode = previousDaemon
		newClientRequest = previousRequest
		dialAndSend = previousDial
	}()

	runDaemonMode = func(context.Context, []string, io.Writer) error {
		t.Fatal("daemon mode was called")
		return nil
	}
	requestCalls := 0
	newClientRequest = func(verb domain.ProtocolVerb, taskID string, params json.RawMessage) (transport.Request, error) {
		requestCalls++
		if verb != domain.ProtocolVerbSubmit || taskID != "" {
			t.Fatalf("request=(%q, %q), want submit with no task ID", verb, taskID)
		}
		var got map[string]json.RawMessage
		if err := json.Unmarshal(params, &got); err != nil {
			t.Fatal(err)
		}
		if string(got["subcommand"]) != `"review"` || string(got["counter"]) != "9007199254740993" {
			t.Fatalf("params=%s", params)
		}
		return transport.Request{RequestID: "request-id", Verb: string(verb)}, nil
	}
	socketPath := filepath.Join(t.TempDir(), "codexd.sock")
	expectedTimeouts := client.Timeouts{Connect: 7 * time.Second, PingTotal: clientPingTotalTimeoutSeconds * time.Second}
	ctx := context.WithValue(context.Background(), clientContextKey{}, "client context")
	dialCalls := 0
	dialAndSend = func(gotCtx context.Context, gotSocketPath string, timeouts client.Timeouts, req transport.Request, stdout io.Writer) (transport.Response, int, error) {
		dialCalls++
		if gotCtx != ctx || gotSocketPath != socketPath || timeouts != expectedTimeouts || req.RequestID != "request-id" {
			t.Fatalf("client dependencies=(%v, %q, %#v, %#v)", gotCtx == ctx, gotSocketPath, timeouts, req)
		}
		if _, err := stdout.Write([]byte("response\n")); err != nil {
			t.Fatal(err)
		}
		return transport.Response{}, 1, errors.New("protocol rejected")
	}

	var stdout, stderr bytes.Buffer
	code := runEntrypoint(ctx, []string{"client", "submit", "--subcommand", "review", "--socket-path", socketPath, "--connect-timeout-seconds", "7"}, bytes.NewBufferString(`{"counter":9007199254740993}`), &stdout, &stderr)
	if code != 1 || requestCalls != 1 || dialCalls != 1 {
		t.Fatalf("code=%d request calls=%d dial calls=%d, want 1, 1, and 1", code, requestCalls, dialCalls)
	}
	if got := stdout.String(); got != "response\n" {
		t.Fatalf("stdout=%q", got)
	}
	if got := stderr.String(); got == "" {
		t.Fatal("communication error was not written to stderr")
	}
}

func TestRunClientRejectsInvalidInputBeforeSending(t *testing.T) {
	previousRequest := newClientRequest
	previousDial := dialAndSend
	defer func() {
		newClientRequest = previousRequest
		dialAndSend = previousDial
	}()
	newClientRequest = func(domain.ProtocolVerb, string, json.RawMessage) (transport.Request, error) {
		t.Fatal("NewRequest was called")
		return transport.Request{}, nil
	}
	dialAndSend = func(context.Context, string, client.Timeouts, transport.Request, io.Writer) (transport.Response, int, error) {
		t.Fatal("DialAndSend was called")
		return transport.Response{}, 0, nil
	}

	for _, args := range [][]string{
		{"submit", "--subcommand", "review", "--request-file", ""},
		{"submit", "--subcommand", "review", "--request-file", "relative.json"},
		{"submit", "--subcommand", "review"},
		{"ping", "--socket-path", "relative.sock"},
		{"tail", "--task-id", "task", "extra"},
	} {
		var stdout, stderr bytes.Buffer
		input := bytes.NewBufferString("")
		if code := runClient(context.Background(), args, input, &stdout, &stderr); code != 2 {
			t.Fatalf("args=%q code=%d, want 2", args, code)
		}
		if stdout.Len() != 0 || stderr.Len() == 0 {
			t.Fatalf("args=%q stdout=%q stderr=%q", args, stdout.String(), stderr.String())
		}
	}
}

func TestRunClientBuildsNonSubmitRequests(t *testing.T) {
	previousRequest := newClientRequest
	previousDial := dialAndSend
	defer func() {
		newClientRequest = previousRequest
		dialAndSend = previousDial
	}()
	type want struct {
		args   []string
		verb   domain.ProtocolVerb
		taskID string
		params string
	}
	for _, tc := range []want{
		{args: []string{"tail", "--task-id", "task-tail"}, verb: domain.ProtocolVerbTail, taskID: "task-tail"},
		{args: []string{"status", "--task-id", "task-status"}, verb: domain.ProtocolVerbStatus, taskID: "task-status"},
		{args: []string{"cancel", "--task-id", "task-cancel", "--force"}, verb: domain.ProtocolVerbCancel, taskID: "task-cancel", params: `{"force":true}`},
		{args: []string{"ping"}, verb: domain.ProtocolVerbPing},
	} {
		t.Run(string(tc.verb), func(t *testing.T) {
			requestCalls := 0
			newClientRequest = func(verb domain.ProtocolVerb, taskID string, params json.RawMessage) (transport.Request, error) {
				requestCalls++
				if verb != tc.verb || taskID != tc.taskID || string(params) != tc.params {
					t.Fatalf("verb=%q taskID=%q params=%s", verb, taskID, params)
				}
				return transport.Request{RequestID: "request-id", Verb: string(verb)}, nil
			}
			socketPath := filepath.Join(t.TempDir(), "codexd.sock")
			expectedTimeouts := client.Timeouts{Connect: 11 * time.Second, PingTotal: clientPingTotalTimeoutSeconds * time.Second}
			ctx := context.WithValue(context.Background(), clientContextKey{}, string(tc.verb))
			dialCalls := 0
			dialAndSend = func(gotCtx context.Context, gotSocketPath string, timeouts client.Timeouts, req transport.Request, _ io.Writer) (transport.Response, int, error) {
				dialCalls++
				if gotCtx != ctx || gotSocketPath != socketPath || timeouts != expectedTimeouts || req.RequestID != "request-id" {
					t.Fatalf("client dependencies=(%v, %q, %#v, %#v)", gotCtx == ctx, gotSocketPath, timeouts, req)
				}
				return transport.Response{}, 0, nil
			}
			args := append(append([]string{}, tc.args...), "--socket-path", socketPath, "--connect-timeout-seconds", "11")
			if code := runClient(ctx, args, nil, io.Discard, io.Discard); code != 0 {
				t.Fatalf("code=%d, want 0", code)
			}
			if requestCalls != 1 || dialCalls != 1 {
				t.Fatalf("request calls=%d dial calls=%d, want 1 and 1", requestCalls, dialCalls)
			}
		})
	}
}

func TestRunSubmitClientUsesExplicitZeroValueFlags(t *testing.T) {
	previousRequest := newClientRequest
	previousDial := dialAndSend
	defer func() {
		newClientRequest = previousRequest
		dialAndSend = previousDial
	}()
	newClientRequest = func(_ domain.ProtocolVerb, _ string, params json.RawMessage) (transport.Request, error) {
		var got map[string]json.RawMessage
		if err := json.Unmarshal(params, &got); err != nil {
			t.Fatal(err)
		}
		if string(got["model"]) != `""` || string(got["requested_timeout_seconds"]) != "-1" {
			t.Fatalf("params=%s", params)
		}
		return transport.Request{RequestID: "request-id"}, nil
	}
	dialAndSend = func(context.Context, string, client.Timeouts, transport.Request, io.Writer) (transport.Response, int, error) {
		return transport.Response{}, 0, nil
	}
	if code := runClient(context.Background(), []string{"submit", "--subcommand", "review", "--model", "", "--timeout", "-1"}, bytes.NewBufferString(`{"model":"original","requested_timeout_seconds":9}`), io.Discard, io.Discard); code != 0 {
		t.Fatalf("code=%d, want 0", code)
	}
}

func TestRunSubmitClientUsesRequestFileWithoutReadingStdin(t *testing.T) {
	previousRequest := newClientRequest
	previousDial := dialAndSend
	defer func() {
		newClientRequest = previousRequest
		dialAndSend = previousDial
	}()

	file, err := os.CreateTemp("", "codexd-request-*.json")
	if err != nil {
		t.Fatal(err)
	}
	path := file.Name()
	t.Cleanup(func() { _ = os.Remove(path) })
	if _, err := file.WriteString(`{"counter":9007199254740993,"from_file":true}`); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	requestCalls := 0
	newClientRequest = func(verb domain.ProtocolVerb, taskID string, params json.RawMessage) (transport.Request, error) {
		requestCalls++
		if verb != domain.ProtocolVerbSubmit || taskID != "" {
			t.Fatalf("request=(%q, %q), want submit with no task ID", verb, taskID)
		}
		var got map[string]json.RawMessage
		if err := json.Unmarshal(params, &got); err != nil {
			t.Fatal(err)
		}
		if string(got["subcommand"]) != `"review"` || string(got["counter"]) != "9007199254740993" || string(got["from_file"]) != "true" {
			t.Fatalf("params=%s", params)
		}
		return transport.Request{RequestID: "request-id"}, nil
	}
	dialCalls := 0
	dialAndSend = func(context.Context, string, client.Timeouts, transport.Request, io.Writer) (transport.Response, int, error) {
		dialCalls++
		return transport.Response{}, 0, nil
	}

	if code := runClient(context.Background(), []string{"submit", "--subcommand", "review", "--request-file", path}, failOnReadReader{t: t}, io.Discard, io.Discard); code != 0 {
		t.Fatalf("code=%d, want 0", code)
	}
	if requestCalls != 1 || dialCalls != 1 {
		t.Fatalf("request calls=%d dial calls=%d, want 1 and 1", requestCalls, dialCalls)
	}
}

func TestRunSubmitClientRejectsUnreadableRequestFilesBeforeSending(t *testing.T) {
	previousRequest := newClientRequest
	previousDial := dialAndSend
	defer func() {
		newClientRequest = previousRequest
		dialAndSend = previousDial
	}()

	missingPath := filepath.Join(t.TempDir(), "missing.json")
	directoryPath := t.TempDir()
	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "open error", path: missingPath},
		{name: "read error", path: directoryPath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestCalls := 0
			dialCalls := 0
			newClientRequest = func(domain.ProtocolVerb, string, json.RawMessage) (transport.Request, error) {
				requestCalls++
				return transport.Request{}, nil
			}
			dialAndSend = func(context.Context, string, client.Timeouts, transport.Request, io.Writer) (transport.Response, int, error) {
				dialCalls++
				return transport.Response{}, 0, nil
			}

			if code := runClient(context.Background(), []string{"submit", "--subcommand", "review", "--request-file", tc.path}, failOnReadReader{t: t}, io.Discard, io.Discard); code != 2 {
				t.Fatalf("code=%d, want 2", code)
			}
			if requestCalls != 0 || dialCalls != 0 {
				t.Fatalf("request calls=%d dial calls=%d, want 0 and 0", requestCalls, dialCalls)
			}
		})
	}
}

func TestRunSubmitClientPreservesOmittedOptionalFlags(t *testing.T) {
	previousRequest := newClientRequest
	previousDial := dialAndSend
	defer func() {
		newClientRequest = previousRequest
		dialAndSend = previousDial
	}()

	newClientRequest = func(_ domain.ProtocolVerb, _ string, params json.RawMessage) (transport.Request, error) {
		var got map[string]json.RawMessage
		if err := json.Unmarshal(params, &got); err != nil {
			t.Fatal(err)
		}
		if string(got["model"]) != `"original"` || string(got["requested_timeout_seconds"]) != "9" {
			t.Fatalf("params=%s", params)
		}
		return transport.Request{RequestID: "request-id"}, nil
	}
	dialAndSend = func(context.Context, string, client.Timeouts, transport.Request, io.Writer) (transport.Response, int, error) {
		return transport.Response{}, 0, nil
	}

	if code := runClient(context.Background(), []string{"submit", "--subcommand", "review"}, bytes.NewBufferString(`{"model":"original","requested_timeout_seconds":9}`), io.Discard, io.Discard); code != 0 {
		t.Fatalf("code=%d, want 0", code)
	}
}

func TestRunSubmitClientUsesExplicitZeroTimeout(t *testing.T) {
	previousRequest := newClientRequest
	previousDial := dialAndSend
	defer func() {
		newClientRequest = previousRequest
		dialAndSend = previousDial
	}()

	newClientRequest = func(_ domain.ProtocolVerb, _ string, params json.RawMessage) (transport.Request, error) {
		var got map[string]json.RawMessage
		if err := json.Unmarshal(params, &got); err != nil {
			t.Fatal(err)
		}
		if string(got["requested_timeout_seconds"]) != "0" {
			t.Fatalf("params=%s", params)
		}
		return transport.Request{RequestID: "request-id"}, nil
	}
	dialAndSend = func(context.Context, string, client.Timeouts, transport.Request, io.Writer) (transport.Response, int, error) {
		return transport.Response{}, 0, nil
	}

	if code := runClient(context.Background(), []string{"submit", "--subcommand", "review", "--timeout", "0"}, bytes.NewBufferString(`{"requested_timeout_seconds":9}`), io.Discard, io.Discard); code != 0 {
		t.Fatalf("code=%d, want 0", code)
	}
}

func TestRunClientRejectsMalformedSubmitJSONBeforeSending(t *testing.T) {
	previousRequest := newClientRequest
	previousDial := dialAndSend
	defer func() {
		newClientRequest = previousRequest
		dialAndSend = previousDial
	}()

	overstatedInput := `{"payload":"` + strings.Repeat("x", int(clientProtocolLineMaxBytes)) + `"}`
	for _, tc := range []struct {
		name  string
		input string
	}{
		{name: "invalid syntax", input: `{"a":}`},
		{name: "array", input: `[1,2,3]`},
		{name: "string", input: `"just a string"`},
		{name: "number", input: `42`},
		{name: "null", input: `null`},
		{name: "trailing JSON", input: `{"a":1}{"b":2}`},
		{name: "over protocol limit", input: overstatedInput},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestCalls := 0
			dialCalls := 0
			newClientRequest = func(domain.ProtocolVerb, string, json.RawMessage) (transport.Request, error) {
				requestCalls++
				return transport.Request{}, nil
			}
			dialAndSend = func(context.Context, string, client.Timeouts, transport.Request, io.Writer) (transport.Response, int, error) {
				dialCalls++
				return transport.Response{}, 0, nil
			}

			var stdout, stderr bytes.Buffer
			if code := runClient(context.Background(), []string{"submit", "--subcommand", "review"}, strings.NewReader(tc.input), &stdout, &stderr); code != 2 {
				t.Fatalf("code=%d, want 2", code)
			}
			if requestCalls != 0 || dialCalls != 0 {
				t.Fatalf("request calls=%d dial calls=%d, want 0 and 0", requestCalls, dialCalls)
			}
			if stdout.Len() != 0 || stderr.Len() == 0 {
				t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestRunClientRejectsInvalidFlagsBeforeSending(t *testing.T) {
	previousRequest := newClientRequest
	previousDial := dialAndSend
	defer func() {
		newClientRequest = previousRequest
		dialAndSend = previousDial
	}()

	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "submit missing subcommand", args: []string{"submit"}},
		{name: "tail missing task ID", args: []string{"tail"}},
		{name: "status missing task ID", args: []string{"status"}},
		{name: "cancel missing task ID", args: []string{"cancel"}},
		{name: "missing verb", args: nil},
		{name: "unknown verb", args: []string{"foo"}},
		{name: "unknown flag", args: []string{"submit", "--subcommand", "review", "--unknown-flag", "value"}},
		{name: "zero connect timeout", args: []string{"ping", "--connect-timeout-seconds", "0"}},
		{name: "negative connect timeout", args: []string{"ping", "--connect-timeout-seconds", "-1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestCalls := 0
			dialCalls := 0
			newClientRequest = func(domain.ProtocolVerb, string, json.RawMessage) (transport.Request, error) {
				requestCalls++
				return transport.Request{}, nil
			}
			dialAndSend = func(context.Context, string, client.Timeouts, transport.Request, io.Writer) (transport.Response, int, error) {
				dialCalls++
				return transport.Response{}, 0, nil
			}

			if code := runClient(context.Background(), tc.args, failOnReadReader{t: t}, io.Discard, io.Discard); code != 2 {
				t.Fatalf("code=%d, want 2", code)
			}
			if requestCalls != 0 || dialCalls != 0 {
				t.Fatalf("request calls=%d dial calls=%d, want 0 and 0", requestCalls, dialCalls)
			}
		})
	}
}

func TestRunEntrypointPreservesClientExitCodeTwo(t *testing.T) {
	previousRequest := newClientRequest
	previousDial := dialAndSend
	defer func() {
		newClientRequest = previousRequest
		dialAndSend = previousDial
	}()

	newClientRequest = func(domain.ProtocolVerb, string, json.RawMessage) (transport.Request, error) {
		return transport.Request{RequestID: "request-id"}, nil
	}
	dialAndSend = func(context.Context, string, client.Timeouts, transport.Request, io.Writer) (transport.Response, int, error) {
		return transport.Response{}, 2, errors.New("connection failed")
	}

	if code := runEntrypoint(context.Background(), []string{"client", "ping"}, nil, io.Discard, io.Discard); code != 2 {
		t.Fatalf("code=%d, want 2", code)
	}
}

func TestNewEvictLogsUseCaseBuildsConfiguredDaemonPaths(t *testing.T) {
	root := t.TempDir()
	cfg, err := config.LoadExplicit(writeTestConfig(t, root))
	if err != nil {
		t.Fatal(err)
	}
	liveness := execution.NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) {
		return false, nil
	}), execution.DefaultLockPathResolver)
	evictLogs, err := newEvictLogsUseCase(cfg, root, filepath.Join(root, "logs"), liveness, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	if evictLogs == nil {
		t.Fatal("newEvictLogsUseCase returned nil")
	}
}

func writeTestConfig(t *testing.T, root string) string {
	t.Helper()
	binary := filepath.Join(root, "codex")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "config.toml")
	contents := fmt.Sprintf("socket_path = %q\ncodex_binary_path = %q\n", filepath.Join(root, "codexd.sock"), binary)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func (f *terminatorRunnerFake) Launch(context.Context, execution.LaunchParams) (*execution.LaunchedProcess, error) {
	return nil, errors.New("not implemented")
}

func (f *terminatorRunnerFake) SendTerminate(int) error { return nil }

func (f *terminatorRunnerFake) SendKill(int) error {
	f.killCalls++
	return nil
}

func TestPIDTerminatorKillsWhenExitWatcherCannotBeCreated(t *testing.T) {
	runner := &terminatorRunnerFake{}
	terminator := pidTerminator{
		runner: runner,
		watchExit: func(int) (proc.ExitWatcher, error) {
			return nil, errors.New("watcher unavailable")
		},
	}
	if err := terminator.Terminate(123, time.Second); err == nil {
		t.Fatal("Terminate succeeded when exit watcher creation failed")
	}
	if runner.killCalls != 1 {
		t.Fatalf("SendKill calls=%d, want 1", runner.killCalls)
	}
}

func TestDefaultPathLockLocationsUseManagedRunDirectory(t *testing.T) {
	dir, mutexPath, err := defaultPathLockLocations()
	if err != nil {
		t.Fatal(err)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	wantDir := filepath.Join(home, ".claude", "run", "path-locks")
	wantMutexPath := filepath.Join(home, ".claude", "run", "path-locks.lock")
	if dir != wantDir || mutexPath != wantMutexPath {
		t.Fatalf("locations=(%q, %q), want=(%q, %q)", dir, mutexPath, wantDir, wantMutexPath)
	}
}

func TestInstallDefaultLoggerRestoresPreviousLogger(t *testing.T) {
	previous := slog.Default()
	logger := slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil))
	restore := installDefaultLogger(logger)
	if slog.Default() != logger {
		t.Fatal("global logger was not installed")
	}
	restore()
	if slog.Default() != previous {
		t.Fatal("previous global logger was not restored")
	}
}

func TestStartCodexVersionProbeDoesNotBlockCaller(t *testing.T) {
	previousResolver := resolveCodexVersion
	defer func() { resolveCodexVersion = previousResolver }()
	started := make(chan struct{})
	release := make(chan struct{})
	resolveCodexVersion = func(context.Context, string) (*string, error) {
		close(started)
		<-release
		return nil, nil
	}

	start := time.Now()
	startCodexVersionProbe(context.Background(), "codex", slog.Default())
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("startCodexVersionProbe blocked for %s", elapsed)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("version probe did not start asynchronously")
	}
	close(release)
}

func TestEnsureManagedPrivateDirCreatesAndSecuresDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "managed")
	if err := ensureManagedPrivateDir(path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("mode=%#o directory=%v", info.Mode().Perm(), info.IsDir())
	}
}

func TestEnsureManagedPrivateDirRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "link")
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	if err := ensureManagedPrivateDir(path); err == nil {
		t.Fatal("ensureManagedPrivateDir accepted a symlink")
	}
}

func TestEnsureSocketParentDoesNotMutateUnmanagedDirectory(t *testing.T) {
	root := t.TempDir()
	parent := filepath.Join(root, "external")
	if err := os.Mkdir(parent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ensureSocketParent(parent, filepath.Join(root, "managed")); err == nil {
		t.Fatal("ensureSocketParent accepted an insecure unmanaged directory")
	}
	info, err := os.Stat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("unmanaged parent mode changed to %#o", info.Mode().Perm())
	}
}

func TestLoadConfigArguments(t *testing.T) {
	if _, err := parseDaemonConfigArgs([]string{"client"}); err == nil {
		t.Fatal("client argument was accepted")
	}
	if _, err := parseDaemonConfigArgs([]string{"--config", "relative.toml"}); err == nil {
		t.Fatal("relative config was accepted")
	}
	if _, err := parseDaemonConfigArgs([]string{"--unknown"}); err == nil {
		t.Fatal("unknown argument was accepted")
	}
}

func TestOpenDaemonLogIsPrivate(t *testing.T) {
	root := t.TempDir()
	file, err := openDaemonLog(root)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("log mode=%#o", info.Mode().Perm())
	}
	if _, err := fs.Stat(os.DirFS(root), "codexd.log"); err != nil {
		t.Fatal(err)
	}
}

func TestAcquireDaemonInstanceLeaseRejectsHeldLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "codexd.lock")
	first, err := acquireDaemonInstanceLease(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Unlock()

	started := time.Now()
	if _, err := acquireDaemonInstanceLease(path); err == nil {
		t.Fatal("acquireDaemonInstanceLease accepted an already held lock")
	} else if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("acquireDaemonInstanceLease blocked for %s", elapsed)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("lock mode=%#o", info.Mode().Perm())
	}
}

func TestReconcileTerminationGraceMatchesTimeoutKillGrace(t *testing.T) {
	if execution.TimeoutKillGrace != 10*time.Second {
		t.Fatalf("TimeoutKillGrace=%s, want 10s", execution.TimeoutKillGrace)
	}
}

func TestResolveCodexCLIVersionTimesOut(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "hanging-codex")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexec sleep 60\n"), 0o700); err != nil {
		t.Fatal(err)
	}

	started := time.Now()
	version, err := resolveCodexCLIVersion(context.Background(), binary)
	if version != nil {
		t.Fatalf("version=%q, want nil", *version)
	}
	if err == nil {
		t.Fatal("resolveCodexCLIVersion succeeded for a hanging command")
	}
	if elapsed := time.Since(started); elapsed < codexVersionProbeTimeout || elapsed > codexVersionProbeTimeout+2*time.Second {
		t.Fatalf("probe elapsed=%s, want approximately %s", elapsed, codexVersionProbeTimeout)
	}
}

func TestReportMainErrorSuppressesAlreadyReportedError(t *testing.T) {
	var stderr bytes.Buffer
	reportMainError(&stderr, &reportedError{cause: os.ErrInvalid})
	if got := stderr.String(); got != "" {
		t.Fatalf("stderr=%q, want empty", got)
	}

	reportMainError(&stderr, os.ErrInvalid)
	if got := stderr.String(); got == "" {
		t.Fatal("reportMainError did not print an unreported error")
	}
}
