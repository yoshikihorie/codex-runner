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

type exitCodeMismatchExpectation struct {
	existing  int
	attempted int
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
	writeFailure := errors.New("write")
	for _, tc := range []struct {
		name                    string
		reader                  exitCodeReaderFake
		writerErr               error
		exitCode                int
		wantWrites              int
		wantWriteErr            bool
		wantFatal               bool
		wantContractWriteFailed bool
		wantMismatch            *exitCodeMismatchExpectation
	}{
		{"missing writes once", exitCodeReaderFake{}, nil, 0, 1, false, false, false, nil},
		{"same value skips write", exitCodeReaderFake{existing: 0, exists: true}, nil, 0, 0, false, false, false, nil},
		{"mismatch fails closed", exitCodeReaderFake{existing: 1, exists: true}, nil, 0, 0, false, true, true, &exitCodeMismatchExpectation{existing: 1, attempted: 0}},
		{"read failure fails closed", exitCodeReaderFake{err: errors.New("read")}, nil, 0, 0, false, true, true, nil},
		{"write failure is returned separately", exitCodeReaderFake{}, writeFailure, 0, 1, true, false, false, nil},
		{"requested nonzero value is written", exitCodeReaderFake{}, nil, 137, 1, false, false, false, nil},
		{"same nonzero value skips write", exitCodeReaderFake{existing: 137, exists: true}, nil, 137, 0, false, false, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := tc.reader
			writer := &exitCodeWriterFake{err: tc.writerErr}
			writeErr, fatalErr := WriteExitCodeIdempotently(&reader, writer, id, domain.NewExitCode(tc.exitCode))
			if writer.writes != tc.wantWrites || (writeErr != nil) != tc.wantWriteErr || (fatalErr != nil) != tc.wantFatal {
				t.Fatalf("writes=%d writeErr=%v fatalErr=%v", writer.writes, writeErr, fatalErr)
			}
			if tc.writerErr != nil && !errors.Is(writeErr, tc.writerErr) {
				t.Fatalf("writeErr=%v, want=%v", writeErr, tc.writerErr)
			}
			if reader.calls != 1 {
				t.Fatalf("reader calls=%d, want=1", reader.calls)
			}
			if writer.writes > 0 && writer.code.Raw() != tc.exitCode {
				t.Fatalf("written code=%d, want=%d", writer.code.Raw(), tc.exitCode)
			}
			if errors.Is(fatalErr, domain.ErrContractWriteFailed) != tc.wantContractWriteFailed {
				t.Fatalf("fatalErr=%v", fatalErr)
			}
			existing, attempted, ok := ExitCodeMismatch(fatalErr)
			if tc.wantMismatch == nil {
				if ok {
					t.Fatalf("mismatch=(%d,%d,%t), want none", existing, attempted, ok)
				}
			} else if !ok || existing != tc.wantMismatch.existing || attempted != tc.wantMismatch.attempted {
				t.Fatalf("mismatch=(%d,%d,%t), want=(%d,%d,true)", existing, attempted, ok, tc.wantMismatch.existing, tc.wantMismatch.attempted)
			}
		})
	}
}
