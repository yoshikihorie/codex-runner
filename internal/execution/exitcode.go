package execution

import (
	"errors"
	"fmt"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type exitCodeMismatchError struct {
	existing  int
	attempted int
}

func (e *exitCodeMismatchError) Error() string {
	return fmt.Sprintf("exit-code mismatch: existing=%d attempted=%d", e.existing, e.attempted)
}

// writeExitCodeIdempotently preserves the frozen exit-code contract for terminal operations.
func writeExitCodeIdempotently(reader store.ContractReader, writer contract.ContractWriter, taskID domain.TaskID, exitCode domain.ExitCode) (writeErr error, fatalErr error) {
	existing, exists, err := reader.ReadExitCode(taskID)
	if err != nil {
		return nil, fmt.Errorf("read exit-code: %w", err)
	}
	if exists && existing != exitCode.Raw() {
		return nil, fmt.Errorf("%w: %w", domain.ErrContractWriteFailed, &exitCodeMismatchError{existing: existing, attempted: exitCode.Raw()})
	}
	if !exists {
		if err := writer.WriteExitCode(taskID, exitCode); err != nil {
			return err, nil
		}
	}
	return nil, nil
}

// WriteExitCodeIdempotently exposes the frozen-contract implementation to
// terminal use cases outside the execution package.
func WriteExitCodeIdempotently(reader store.ContractReader, writer contract.ContractWriter, taskID domain.TaskID, exitCode domain.ExitCode) (writeErr error, fatalErr error) {
	return writeExitCodeIdempotently(reader, writer, taskID, exitCode)
}

func exitCodeMismatch(err error) (existing, attempted int, ok bool) {
	var mismatch *exitCodeMismatchError
	if !errors.As(err, &mismatch) {
		return 0, 0, false
	}
	return mismatch.existing, mismatch.attempted, true
}
