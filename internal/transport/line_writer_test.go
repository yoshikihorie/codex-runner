package transport

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestWriteResponseLineAppliesJSONBodyBoundaryBeforeNewline(t *testing.T) {
	for _, tc := range []struct {
		name       string
		bodySize   int
		wantErr    bool
		wantOutput int
	}{
		{name: "at maximum", bodySize: protocolLineMaxBytes, wantOutput: protocolLineMaxBytes + 1},
		{name: "above maximum", bodySize: protocolLineMaxBytes + 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			err := writeResponseLine(&output, responseLineOfSize(t, tc.bodySize))
			if tc.wantErr != errors.Is(err, errProtocolLineTooLong) {
				t.Fatalf("error = %v, want protocol line too long = %t", err, tc.wantErr)
			}
			if output.Len() != tc.wantOutput {
				t.Fatalf("output bytes = %d, want %d", output.Len(), tc.wantOutput)
			}
			if tc.wantOutput > 0 && output.Bytes()[output.Len()-1] != '\n' {
				t.Fatal("response frame was not newline terminated")
			}
		})
	}
}

func TestProtocolLineWriterAppliesBoundaryPerCompletedLine(t *testing.T) {
	for _, tc := range []struct {
		name       string
		bodySize   int
		wantErr    bool
		wantOutput int
	}{
		{name: "at maximum", bodySize: protocolLineMaxBytes, wantOutput: protocolLineMaxBytes + 1},
		{name: "above maximum", bodySize: protocolLineMaxBytes + 1, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			writer := newProtocolLineWriter(&output)
			_, err := writer.Write([]byte(strings.Repeat("x", tc.bodySize) + "\n"))
			if tc.wantErr != errors.Is(err, errProtocolLineTooLong) {
				t.Fatalf("error = %v, want protocol line too long = %t", err, tc.wantErr)
			}
			if output.Len() != tc.wantOutput {
				t.Fatalf("output bytes = %d, want %d", output.Len(), tc.wantOutput)
			}
		})
	}
}

func TestProtocolLineWriterPreservesMultipleLineOrder(t *testing.T) {
	var output bytes.Buffer
	writer := newProtocolLineWriter(&output)
	for _, fragment := range []string{"first", "-line\nsecond-line\nthird", "-line\n"} {
		if _, err := writer.Write([]byte(fragment)); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := output.String(), "first-line\nsecond-line\nthird-line\n"; got != want {
		t.Fatalf("output = %q, want %q", got, want)
	}
}

func TestProtocolLineWriterRejectsUnterminatedOversizeLineWithoutOutput(t *testing.T) {
	var output bytes.Buffer
	writer := newProtocolLineWriter(&output)
	if n, err := writer.Write([]byte(strings.Repeat("x", protocolLineMaxBytes))); err != nil || n != protocolLineMaxBytes {
		t.Fatalf("initial write bytes = %d, error = %v", n, err)
	}
	if n, err := writer.Write([]byte("x")); n != 0 || !errors.Is(err, errProtocolLineTooLong) {
		t.Fatalf("overflow write bytes = %d, error = %v", n, err)
	}
	if output.Len() != 0 {
		t.Fatalf("output bytes = %d, want 0", output.Len())
	}
}

func TestProtocolLineWriterWritesBoundaryFrameOnceAndRejectsShortWrite(t *testing.T) {
	underlying := &shortWriteRecorder{}
	writer := newProtocolLineWriter(underlying)
	if _, err := writer.Write([]byte(strings.Repeat("x", protocolLineMaxBytes) + "\n")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("error = %v, want short write", err)
	}
	if underlying.calls != 1 || underlying.requested != protocolLineMaxBytes+1 {
		t.Fatalf("calls = %d, requested = %d", underlying.calls, underlying.requested)
	}
}

type shortWriteRecorder struct {
	calls     int
	requested int
}

func (w *shortWriteRecorder) Write(p []byte) (int, error) {
	w.calls++
	w.requested = len(p)
	return len(p) - 1, nil
}

func responseLineOfSize(t *testing.T, size int) Response {
	t.Helper()
	response := Response{
		ProtocolVersion: ProtocolVersion,
		RequestID:       "line-limit",
		OK:              true,
		Result:          json.RawMessage(`{"padding":""}`),
	}
	empty, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	padding := size - len(empty)
	if padding < 0 {
		t.Fatalf("requested response size %d is smaller than envelope size %d", size, len(empty))
	}
	response.Result = json.RawMessage(`{"padding":"` + strings.Repeat("x", padding) + `"}`)
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != size {
		t.Fatalf("response line length = %d, want %d", len(encoded), size)
	}
	return response
}
