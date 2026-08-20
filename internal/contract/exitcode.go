package contract

import (
	"errors"
	"fmt"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// ExitCodeReader is the smallest read boundary needed to preserve the frozen
// exit-code contract.
type ExitCodeReader interface {
	ReadExitCode(domain.TaskID) (int, bool, error)
}

type exitCodeMismatchError struct {
	existing  int
	attempted int
}

func (e *exitCodeMismatchError) Error() string {
	return fmt.Sprintf("exit-code mismatch: existing=%d attempted=%d", e.existing, e.attempted)
}

// CheckExitCode determines whether a terminal operation may write exit-code.
// A conflicting or unreadable existing value is fatal and must stop terminal
// persistence before any contract mutation occurs.
func CheckExitCode(reader ExitCodeReader, taskID domain.TaskID, exitCode domain.ExitCode) (shouldWrite bool, fatalErr error) {
	existing, exists, err := reader.ReadExitCode(taskID)
	if err != nil {
		return false, fmt.Errorf("%w: read exit-code: %w", domain.ErrContractWriteFailed, err)
	}
	if exists && existing != exitCode.Raw() {
		return false, fmt.Errorf("%w: %w", domain.ErrContractWriteFailed, &exitCodeMismatchError{existing: existing, attempted: exitCode.Raw()})
	}
	return !exists, nil
}

// WriteExitCodeIdempotently writes a missing exit code once, accepts the same
// existing value, and rejects a conflicting existing value without replacing it.
func WriteExitCodeIdempotently(reader ExitCodeReader, writer ContractWriter, taskID domain.TaskID, exitCode domain.ExitCode) (writeErr error, fatalErr error) {
	shouldWrite, fatalErr := CheckExitCode(reader, taskID, exitCode)
	if fatalErr != nil {
		return nil, fatalErr
	}
	if shouldWrite {
		if err := writer.WriteExitCode(taskID, exitCode); err != nil {
			return err, nil
		}
	}
	return nil, nil
}

// ExitCodeMismatch extracts a conflicting persisted and attempted exit code.
func ExitCodeMismatch(err error) (existing, attempted int, ok bool) {
	var mismatch *exitCodeMismatchError
	if !errors.As(err, &mismatch) {
		return 0, 0, false
	}
	return mismatch.existing, mismatch.attempted, true
}
