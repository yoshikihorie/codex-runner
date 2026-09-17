package recovery

import (
	"context"
	"errors"
	"log/slog"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const (
	machineCodeRecoverySessionUnavailable = "RECOVERY_SESSION_UNAVAILABLE"
	messageKeyRecoverySessionUnavailable  = "error.recovery.sessionUnavailable"
	machineCodeRecoveryResumeLaunchFailed = "RECOVERY_RESUME_LAUNCH_FAILED"
	messageKeyRecoveryResumeLaunchFailed  = "error.recovery.resumeLaunchFailed"
	machineCodeRecoveryOutputReadFailed   = "RECOVERY_OUTPUT_READ_FAILED"
	messageKeyRecoveryOutputReadFailed    = "error.recovery.outputReadFailed"
	machineCodeTaskInvalidTransition      = "TASK_INVALID_TRANSITION"
	messageKeyTaskInvalidTransition       = "error.task.invalidTransition"
	machineCodeContractWriteFailed        = "CONTRACT_WRITE_FAILED"
	messageKeyContractWriteFailed         = "error.contract.writeFailed"
)

var (
	errRecoverySessionUnavailable = errors.New("recovery session unavailable")
	errRecoveryResumeLaunchFailed = errors.New("recovery resume launch failed")
	errRecoveryOutputReadFailed   = errors.New("recovery output read failed")
)

func isRecoverySessionUnavailable(err error) bool {
	return errors.Is(err, errRecoverySessionUnavailable) || errors.Is(err, context.DeadlineExceeded)
}

func isRecoveryResumeLaunchFailed(err error) bool {
	return errors.Is(err, errRecoveryResumeLaunchFailed)
}

func isRecoveryOutputReadFailed(err error) bool {
	return errors.Is(err, errRecoveryOutputReadFailed)
}

func logRecoveryError(ctx context.Context, logger *slog.Logger, level slog.Level, message, code, messageKey string, taskID domain.TaskID, operation, stage string, err error) {
	logger.Log(ctx, level, message,
		"code", code,
		"message_key", messageKey,
		"task_id", taskID.String(),
		"operation", operation,
		"stage", stage,
		"error", err,
	)
}

func logRecoveryInfo(ctx context.Context, logger *slog.Logger, message string, taskID domain.TaskID, operation, stage string) {
	logger.Log(ctx, slog.LevelInfo, message,
		"task_id", taskID.String(),
		"operation", operation,
		"stage", stage,
	)
}
