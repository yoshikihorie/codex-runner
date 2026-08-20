package contract

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type exitCodeReaderFake struct {
	existing int
	exists   bool
	err      error
	calls    int
}

func (f *exitCodeReaderFake) ReadExitCode(domain.TaskID) (int, bool, error) {
	f.calls++
	return f.existing, f.exists, f.err
}

type exitCodeWriterFake struct {
	writes int
	code   domain.ExitCode
	err    error
}

func (*exitCodeWriterFake) WritePrompt(domain.TaskID, []byte) error         { return nil }
func (*exitCodeWriterFake) WriteReviewInput(domain.TaskID, []byte) error    { return nil }
func (*exitCodeWriterFake) WriteCombinedPrompt(domain.TaskID, []byte) error { return nil }
func (*exitCodeWriterFake) OpenExecutionLogs(domain.TaskID) (*ExecutionLogs, error) {
	return nil, nil
}
func (f *exitCodeWriterFake) WriteExitCode(_ domain.TaskID, code domain.ExitCode) error {
	f.writes++
	f.code = code
	return f.err
}
func (*exitCodeWriterFake) WritePartialOutput(domain.TaskID, string) error { return nil }
func (*exitCodeWriterFake) WriteRecoveredMarker(domain.TaskID, time.Time) error {
	return nil
}
func (*exitCodeWriterFake) WriteAdoptedMarker(domain.TaskID, time.Time) error { return nil }
func (*exitCodeWriterFake) AppendEvent(domain.TaskID, domain.Event) error     { return nil }
func (*exitCodeWriterFake) AppendRawEvent(domain.TaskID, string, json.RawMessage) error {
	return nil
}

func TestWriteExitCodeIdempotently(t *testing.T) {
	id, err := domain.NewTaskID("impl-20260820-120000-abcd-exitcode")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name       string
		reader     exitCodeReaderFake
		wantWrites int
		wantFatal  bool
	}{
		{"missing writes once", exitCodeReaderFake{}, 1, false},
		{"same value skips write", exitCodeReaderFake{existing: 0, exists: true}, 0, false},
		{"mismatch fails closed", exitCodeReaderFake{existing: 1, exists: true}, 0, true},
		{"read failure fails closed", exitCodeReaderFake{err: errors.New("read")}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := tc.reader
			writer := &exitCodeWriterFake{}
			writeErr, fatalErr := WriteExitCodeIdempotently(&reader, writer, id, domain.NewExitCode(0))
			if writer.writes != tc.wantWrites || writeErr != nil || (fatalErr != nil) != tc.wantFatal {
				t.Fatalf("writes=%d writeErr=%v fatalErr=%v", writer.writes, writeErr, fatalErr)
			}
			if tc.name == "mismatch fails closed" {
				if !errors.Is(fatalErr, domain.ErrContractWriteFailed) {
					t.Fatalf("fatalErr=%v", fatalErr)
				}
				existing, attempted, ok := ExitCodeMismatch(fatalErr)
				if !ok || existing != 1 || attempted != 0 {
					t.Fatalf("mismatch=(%d,%d,%t)", existing, attempted, ok)
				}
			}
		})
	}
}
