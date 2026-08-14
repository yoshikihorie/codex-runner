package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/transport/schema"
)

var errTailWrite = errors.New("tail write failed")

type failingTailWriter struct {
	err       error
	failAfter int
	writes    int
	bytes     int
}

func (w *failingTailWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes > w.failAfter {
		return 0, w.err
	}
	w.bytes += len(p)
	return len(p), nil
}

func progressTailLine() schema.ProgressLine {
	return schema.ProgressLine{
		LineType:   schema.LineTypeProgress,
		Seq:        4,
		RecordedAt: time.Date(2026, time.August, 14, 12, 34, 56, 0, time.UTC),
		EventType:  "item.completed",
		Raw:        map[string]any{"message": "done"},
		TaskState:  domain.StateRunning,
	}
}

func completeTailLine() schema.CompleteLine {
	return schema.CompleteLine{
		LineType:  schema.LineTypeComplete,
		Reason:    schema.CompleteReasonTaskTerminal,
		TaskState: domain.StateCompleted,
		LastSeq:   4,
	}
}

func decodeTailResponse(t *testing.T, encoded []byte) Response {
	t.Helper()
	var response Response
	if err := json.Unmarshal(encoded, &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func assertTailSuccessEnvelope(t *testing.T, encoded []byte, requestID string) Response {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 4 {
		t.Fatalf("envelope fields = %#v", fields)
	}
	for _, key := range []string{"protocol_version", "request_id", "ok", "result"} {
		if _, ok := fields[key]; !ok {
			t.Fatalf("missing envelope field %q in %#v", key, fields)
		}
	}
	if _, ok := fields["error"]; ok {
		t.Fatalf("error field = %s", fields["error"])
	}
	response := decodeTailResponse(t, encoded)
	if response.ProtocolVersion != ProtocolVersion || response.RequestID != requestID || !response.OK {
		t.Fatalf("response = %#v", response)
	}
	return response
}

func TestProgressWriterWriteProgressUsesSuccessEnvelope(t *testing.T) {
	var output bytes.Buffer
	writer := NewProgressWriter(&output, "request-progress")
	line := progressTailLine()
	if err := writer.WriteProgress(line); err != nil {
		t.Fatal(err)
	}
	response := assertTailSuccessEnvelope(t, output.Bytes(), "request-progress")

	var decoded schema.ProgressLine
	if err := json.Unmarshal(response.Result, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.LineType != line.LineType || decoded.Seq != line.Seq || !decoded.RecordedAt.Equal(line.RecordedAt) || decoded.EventType != line.EventType || decoded.TaskState != line.TaskState {
		t.Fatalf("progress line = %#v", decoded)
	}
	raw, ok := decoded.Raw.(map[string]any)
	if !ok || raw["message"] != "done" {
		t.Fatalf("progress raw = %#v", decoded.Raw)
	}
}

func TestProgressWriterWriteCompletePreservesPayload(t *testing.T) {
	var output bytes.Buffer
	writer := NewProgressWriter(&output, "request-complete")
	line := completeTailLine()
	if err := writer.WriteComplete(line); err != nil {
		t.Fatal(err)
	}
	response := assertTailSuccessEnvelope(t, output.Bytes(), "request-complete")

	var decoded schema.CompleteLine
	if err := json.Unmarshal(response.Result, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded != line {
		t.Fatalf("complete line = %#v, want %#v", decoded, line)
	}
}

func TestProgressWriterWritesSeparateJSONLinesWithSharedRequestID(t *testing.T) {
	var output bytes.Buffer
	writer := NewProgressWriter(&output, "request-shared")
	if err := writer.WriteProgress(progressTailLine()); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteComplete(completeTailLine()); err != nil {
		t.Fatal(err)
	}

	encoded := output.String()
	if !strings.HasSuffix(encoded, "\n") {
		t.Fatalf("output does not end in newline: %q", encoded)
	}
	lines := strings.Split(strings.TrimSuffix(encoded, "\n"), "\n")
	if len(lines) != 2 || lines[0] == "" || lines[1] == "" {
		t.Fatalf("lines = %#v", lines)
	}
	for _, line := range lines {
		response := assertTailSuccessEnvelope(t, []byte(line), "request-shared")
		if response.RequestID != "request-shared" || response.ProtocolVersion != ProtocolVersion || !response.OK {
			t.Fatalf("response = %#v", response)
		}
	}
}

func TestProgressWriterReturnsUnderlyingWriteError(t *testing.T) {
	output := &failingTailWriter{err: errTailWrite, failAfter: 0}
	writer := NewProgressWriter(output, "request-error")
	if err := writer.WriteProgress(progressTailLine()); err != errTailWrite {
		t.Fatalf("WriteProgress error = %v, want %v", err, errTailWrite)
	}
}

func TestProgressWriterReturnsSecondUnderlyingWriteError(t *testing.T) {
	output := &failingTailWriter{err: errTailWrite, failAfter: 1}
	writer := NewProgressWriter(output, "request-second-error")
	if err := writer.WriteProgress(progressTailLine()); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteComplete(completeTailLine()); err != errTailWrite {
		t.Fatalf("WriteComplete error = %v, want %v", err, errTailWrite)
	}
}

func TestProgressWriterReturnsMarshalErrorWithoutWriting(t *testing.T) {
	output := &failingTailWriter{err: errTailWrite, failAfter: 1}
	writer := NewProgressWriter(output, "request-marshal-error")
	line := progressTailLine()
	line.Raw = make(chan struct{})
	if err := writer.WriteProgress(line); err == nil {
		t.Fatal("WriteProgress succeeded with an unmarshalable raw value")
	}
	if output.writes != 0 || output.bytes != 0 {
		t.Fatalf("writer was used: writes=%d bytes=%d", output.writes, output.bytes)
	}
}
