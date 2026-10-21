package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"path/filepath"
	"strings"
	"testing"

	supervisortransport "github.com/yoshikihorie/codex-runner/internal/supervisor/transport"
)

type rejectingReader struct{ calls int }

func (r *rejectingReader) Read([]byte) (int, error) { r.calls++; return 0, io.ErrClosedPipe }

type countedReader struct {
	source io.Reader
	bytes  int
}

func (r *countedReader) Read(p []byte) (int, error) {
	n, err := r.source.Read(p)
	r.bytes += n
	return n, err
}

func TestSupervisorClient_SCNSup0568_SendInputStdinOnly(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "supervisor.sock")
	old := supervisorClientCall
	t.Cleanup(func() { supervisorClientCall = old })
	calls := 0
	var received map[string]string
	supervisorClientCall = func(_ context.Context, _ supervisortransport.Client, request supervisortransport.Request) (supervisortransport.Response, error) {
		calls++
		if request.Verb != "send-input" || json.Unmarshal(request.Params, &received) != nil {
			t.Fatalf("request=%+v", request)
		}
		return supervisortransport.Response{OK: true, Result: json.RawMessage(`{"session_id":"alpha-1","send_id":"alpha-1-s1","confirmed":true}`)}, nil
	}
	for _, args := range [][]string{{"send-input", "--socket-path=" + socket}, {"send-input", "--socket-path=" + socket, "--session=alpha-1", "--text=hello"}, {"send-input", "--socket-path=" + socket, "--session=alpha-1", "--name=sample"}} {
		var out, stderr bytes.Buffer
		if code := runSupervisorClient(context.Background(), args, strings.NewReader("hello"), &out, &stderr); code != 2 || calls != 0 {
			t.Fatalf("args=%v code=%d calls=%d", args, code, calls)
		}
	}
	for _, input := range []string{"", "\n", strings.Repeat("a", 2001), strings.Repeat("界", 2001)} {
		var out, stderr bytes.Buffer
		if code := runSupervisorClient(context.Background(), []string{"send-input", "--socket-path=" + socket, "--session=alpha-1"}, strings.NewReader(input), &out, &stderr); code != 2 || calls != 0 {
			t.Fatalf("input length=%d code=%d calls=%d", len(input), code, calls)
		}
	}
	bounded := &countedReader{source: strings.NewReader(strings.Repeat("a", 100000))}
	var boundedOut, boundedErr bytes.Buffer
	if code := runSupervisorClient(context.Background(), []string{"send-input", "--socket-path=" + socket, "--session=alpha-1"}, bounded, &boundedOut, &boundedErr); code != 2 || bounded.bytes > 2003 || calls != 0 {
		t.Fatalf("bounded input code=%d bytes=%d calls=%d", code, bounded.bytes, calls)
	}
	for _, input := range []string{"hello\n", "hello\r\n", "界\n"} {
		var out, stderr bytes.Buffer
		if code := runSupervisorClient(context.Background(), []string{"send-input", "--socket-path=" + socket, "--session=alpha-1"}, strings.NewReader(input), &out, &stderr); code != 0 || received["text"] != strings.TrimSuffix(strings.TrimSuffix(input, "\n"), "\r") || received["session_id"] != "alpha-1" || len(received) != 2 {
			t.Fatalf("input=%q code=%d received=%v", input, code, received)
		}
	}
}

func TestSupervisorClient_SCNSup0577_SupervisedRefusedBeforeReadOrConnect(t *testing.T) {
	t.Setenv("CODEXD_SUPERVISED", "1")
	reader := &rejectingReader{}
	old := supervisorClientCall
	t.Cleanup(func() { supervisorClientCall = old })
	calls := 0
	supervisorClientCall = func(context.Context, supervisortransport.Client, supervisortransport.Request) (supervisortransport.Response, error) {
		calls++
		return supervisortransport.Response{}, nil
	}
	var out, stderr bytes.Buffer
	code := runSupervisorClient(context.Background(), []string{"send-input", "--socket-path=" + filepath.Join(t.TempDir(), "supervisor.sock"), "--session=alpha-1"}, reader, &out, &stderr)
	if code != 1 || reader.calls != 0 || calls != 0 || !strings.Contains(out.String(), "SUP_OPERATOR_INPUT_FORBIDDEN") {
		t.Fatalf("code=%d reads=%d calls=%d out=%q", code, reader.calls, calls, out.String())
	}
}
