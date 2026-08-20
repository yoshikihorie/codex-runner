package execution

import (
	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

// writeExitCodeIdempotently preserves the frozen exit-code contract for terminal operations.
func writeExitCodeIdempotently(reader store.ContractReader, writer contract.ContractWriter, taskID domain.TaskID, exitCode domain.ExitCode) (writeErr error, fatalErr error) {
	return contract.WriteExitCodeIdempotently(reader, writer, taskID, exitCode)
}

// WriteExitCodeIdempotently exposes the frozen-contract implementation to
// terminal use cases outside the execution package.
func WriteExitCodeIdempotently(reader store.ContractReader, writer contract.ContractWriter, taskID domain.TaskID, exitCode domain.ExitCode) (writeErr error, fatalErr error) {
	return contract.WriteExitCodeIdempotently(reader, writer, taskID, exitCode)
}

func exitCodeMismatch(err error) (existing, attempted int, ok bool) {
	return contract.ExitCodeMismatch(err)
}
