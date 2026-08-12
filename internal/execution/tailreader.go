package execution

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// stdoutTailPollInterval is STDOUT_TAIL_POLL_INTERVAL_MS from validation-rules.md.
const stdoutTailPollInterval = 250 * time.Millisecond

type tailPollSource interface {
	Sleep(context.Context, time.Duration) error
}
type realTailPollSource struct{}

func (realTailPollSource) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// StdoutTailReader turns a growing stdout file into a reader which finishes only
// after liveness confirms that the child has died and the file is drained.
type StdoutTailReader struct {
	ctx                 context.Context
	file                *os.File
	taskID              domain.TaskID
	liveness            *CheckLivenessUseCase
	pollInterval        time.Duration
	poller              tailPollSource
	logger              *slog.Logger
	deadConfirmed       bool
	livenessErrorActive bool
}

func NewStdoutTailReader(ctx context.Context, file *os.File, taskID domain.TaskID, liveness *CheckLivenessUseCase, pollInterval time.Duration, loggers ...*slog.Logger) *StdoutTailReader {
	if ctx == nil || file == nil || liveness == nil {
		panic("stdout tail reader requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("stdout tail reader accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	if pollInterval <= 0 {
		pollInterval = stdoutTailPollInterval
	}
	return &StdoutTailReader{ctx: ctx, file: file, taskID: taskID, liveness: liveness, pollInterval: pollInterval, poller: realTailPollSource{}, logger: logger}
}

func (r *StdoutTailReader) Read(p []byte) (int, error) {
	for {
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		n, err := r.file.Read(p)
		if n > 0 || !errors.Is(err, io.EOF) {
			return n, err
		}
		if r.deadConfirmed {
			return 0, io.EOF
		}
		dead, liveErr := r.liveness.Execute(r.ctx, r.taskID)
		if liveErr == nil {
			r.livenessErrorActive = false
		}
		if liveErr == nil && dead {
			r.deadConfirmed = true
			continue
		}
		if liveErr != nil {
			r.logLivenessError(liveErr)
		}
		if err := r.poller.Sleep(r.ctx, r.pollInterval); err != nil {
			return 0, err
		}
	}
}

func (r *StdoutTailReader) logLivenessError(err error) {
	// error-codes.md limits this classification to task.lock I/O; cancellation
	// and a missing lock are ordinary retry conditions.
	if errors.Is(err, domain.ErrTaskNotFound) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	if r.livenessErrorActive {
		return
	}
	r.livenessErrorActive = true
	r.logger.Warn("liveness lock I/O error", "code", "LIVENESS_LOCK_IO_ERROR", "task_id", r.taskID.String(), "error", err)
}

var _ io.Reader = (*StdoutTailReader)(nil)
