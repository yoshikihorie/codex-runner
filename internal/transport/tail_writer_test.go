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
	if err := writer.WriteProgress(progressTailLine()); !errors.Is(err, errTailWrite) {
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

func TestProgressWriterTruncatesOversizeRawAndPreservesProgressEnvelope(t *testing.T) {
	var output bytes.Buffer
	writer := NewProgressWriter(&output, "request-oversize")
	line := progressTailLine()
	line.Raw = map[string]any{"message": strings.Repeat("x", protocolLineMaxBytes+1)}
	raw, err := json.Marshal(line.Raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteProgress(line); err != nil {
		t.Fatal(err)
	}
	encoded := bytes.TrimSuffix(output.Bytes(), []byte{'\n'})
	if len(encoded) > protocolLineMaxBytes {
		t.Fatalf("encoded progress = %d bytes", len(encoded))
	}
	response := assertTailSuccessEnvelope(t, encoded, "request-oversize")
	var decoded schema.ProgressLine
	if err := json.Unmarshal(response.Result, &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.Truncated || decoded.RawBytes != len(raw) || decoded.Seq != line.Seq || decoded.EventType != line.EventType || decoded.TaskState != line.TaskState {
		t.Fatalf("progress line = %#v", decoded)
	}
	if raw, ok := decoded.Raw.(map[string]any); !ok || len(raw) != 0 {
		t.Fatalf("truncated raw = %#v", decoded.Raw)
	}
}

func TestProgressWriterPreservesRawAndOmitsTruncationMetadataAtOrBelowLimit(t *testing.T) {
	var output bytes.Buffer
	writer := NewProgressWriter(&output, "request-normal").(*progressWriter)
	line := progressTailLine()
	line.Raw = strings.Repeat("x", protocolLineMaxBytes)
	encoded, err := writer.marshalProgress(line)
	if err != nil {
		t.Fatal(err)
	}
	line.Raw = strings.Repeat("x", protocolLineMaxBytes-(len(encoded)-protocolLineMaxBytes))
	encoded, err = writer.marshalProgress(line)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != protocolLineMaxBytes {
		t.Fatalf("encoded progress = %d bytes, want %d", len(encoded), protocolLineMaxBytes)
	}
	if err := writer.WriteProgress(line); err != nil {
		t.Fatal(err)
	}
	if len(bytes.TrimSuffix(output.Bytes(), []byte{'\n'})) != protocolLineMaxBytes {
		t.Fatalf("written progress = %d bytes, want %d", len(bytes.TrimSuffix(output.Bytes(), []byte{'\n'})), protocolLineMaxBytes)
	}
	var result map[string]json.RawMessage
	response := assertTailSuccessEnvelope(t, bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), "request-normal")
	if err := json.Unmarshal(response.Result, &result); err != nil {
		t.Fatal(err)
	}
	if _, ok := result["truncated"]; ok {
		t.Fatalf("unexpected truncated field: %s", response.Result)
	}
	if _, ok := result["raw_bytes"]; ok {
		t.Fatalf("unexpected raw_bytes field: %s", response.Result)
	}
	var raw string
	if err := json.Unmarshal(result["raw"], &raw); err != nil || raw != line.Raw {
		t.Fatalf("raw = %q, err = %v", raw, err)
	}
}

func TestProgressWriterReturnsProtocolLineTooLongWhenEnvelopeExceedsLimitAfterTruncation(t *testing.T) {
	var output bytes.Buffer
	writer := NewProgressWriter(&output, "request-too-long")
	line := progressTailLine()
	line.EventType = strings.Repeat("x", protocolLineMaxBytes)
	line.Raw = map[string]any{"message": strings.Repeat("x", protocolLineMaxBytes+1)}

	if err := writer.WriteProgress(line); !errors.Is(err, errProtocolLineTooLong) {
		t.Fatalf("WriteProgress error = %v, want %v", err, errProtocolLineTooLong)
	}
	if output.Len() != 0 {
		t.Fatalf("output length = %d, want 0", output.Len())
	}
}

func TestProgressWriterTruncatedProgressAllowsFollowingCompleteLine(t *testing.T) {
	var output bytes.Buffer
	writer := NewProgressWriter(&output, "request-following")
	line := progressTailLine()
	line.Raw = map[string]any{"message": strings.Repeat("x", protocolLineMaxBytes+1)}
	if err := writer.WriteProgress(line); err != nil {
		t.Fatal(err)
	}
	if err := writer.WriteComplete(completeTailLine()); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %d", len(lines))
	}
	var complete schema.CompleteLine
	response := assertTailSuccessEnvelope(t, []byte(lines[1]), "request-following")
	if err := json.Unmarshal(response.Result, &complete); err != nil || complete != completeTailLine() {
		t.Fatalf("complete = %#v, err = %v", complete, err)
	}
}
