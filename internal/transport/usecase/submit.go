package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

const taskIDGenerationMaxAttempts = 10

type SubmitTaskStore interface {
	Reserve(domain.TaskID) error
	Release(domain.TaskID) error
}
type SubmitPathLockAcquirer interface {
	Acquire(domain.TaskID, []string) ([]domain.NormalizedPath, error)
}
type SubmitPathLockReleaser interface {
	Release(context.Context, domain.TaskID) error
}
type TaskAdmitter interface {
	Admit(execution.TaskAdmissionInput) (execution.TaskAdmissionResult, error)
	CompensateRejectedStart(domain.TaskID) error
}
type TaskOptionResolver interface {
	ResolveModel(domain.Subcommand, *string) (string, bool)
	ResolveReasoningEffort(domain.Subcommand, *string) (*string, bool)
}

type SubmitTaskInput struct {
	Subcommand              string
	RawSlug                 string
	Prompt                  string
	RequestedTimeoutSeconds *int
	RawPaths                []string
	Model                   *string
	ReasoningEffort         *string
	RawWorkingDir           string
	RequestedAt             time.Time
}
type SubmitTaskOutput struct {
	TaskID        domain.TaskID
	State         domain.TaskState
	QueuePosition *int
	Events        []domain.Event
}

type SubmitTaskUseCase struct {
	tasks            SubmitTaskStore
	pathLocks        SubmitPathLockAcquirer
	pathLockReleaser SubmitPathLockReleaser
	admitter         TaskAdmitter
	queueMaxDepth    int
	starter          execution.TaskLifecycleStarter
	options          TaskOptionResolver
	clock            domain.Clock
	logger           *slog.Logger
	random           io.Reader
}

func NewSubmitTaskUseCase(tasks SubmitTaskStore, pathLocks SubmitPathLockAcquirer, pathLockReleaser SubmitPathLockReleaser, admitter TaskAdmitter, queueMaxDepth int, starter execution.TaskLifecycleStarter, options TaskOptionResolver, clock domain.Clock, logger *slog.Logger) *SubmitTaskUseCase {
	if tasks == nil || admitter == nil || starter == nil || options == nil || clock == nil {
		panic("submit use case requires non-nil dependencies")
	}
	if (pathLocks == nil) != (pathLockReleaser == nil) {
		panic("submit use case requires paired path lock dependencies")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &SubmitTaskUseCase{tasks: tasks, pathLocks: pathLocks, pathLockReleaser: pathLockReleaser, admitter: admitter, queueMaxDepth: queueMaxDepth, starter: starter, options: options, clock: clock, logger: logger, random: productionTaskIDReader()}
}

type submitWireInput struct {
	Subcommand              string   `json:"subcommand"`
	Slug                    string   `json:"slug"`
	Prompt                  string   `json:"prompt"`
	RequestedTimeoutSeconds *int     `json:"requested_timeout_seconds"`
	Paths                   []string `json:"paths"`
	Model                   *string  `json:"model"`
	ReasoningEffort         *string  `json:"reasoning_effort"`
	WorkingDir              string   `json:"working_dir"`
}

type submitError struct {
	code, message string
	detail        map[string]any
	cause         error
}

func (e *submitError) Error() string { return e.code }
func (e *submitError) Unwrap() error { return e.cause }

func submitFailure(code, message string, detail map[string]any) error {
	return &submitError{code: code, message: message, detail: detail}
}

func (uc *SubmitTaskUseCase) Handle(req transport.Request) transport.Response {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(req.Params, &object); err != nil || object == nil {
		return submitErrorResponse(req.RequestID, submitFailure("SUBMIT_PARAMS_MALFORMED", "error.submit.paramsMalformed", nil))
	}
	var wire submitWireInput
	if err := json.Unmarshal(req.Params, &wire); err != nil {
		return submitErrorResponse(req.RequestID, submitFailure("SUBMIT_PARAMS_MALFORMED", "error.submit.paramsMalformed", nil))
	}
	out, err := uc.Execute(context.Background(), SubmitTaskInput{Subcommand: wire.Subcommand, RawSlug: wire.Slug, Prompt: wire.Prompt, RequestedTimeoutSeconds: wire.RequestedTimeoutSeconds, RawPaths: wire.Paths, Model: wire.Model, ReasoningEffort: wire.ReasoningEffort, RawWorkingDir: wire.WorkingDir, RequestedAt: uc.clock.Now()})
	if err != nil {
		return submitErrorResponse(req.RequestID, uc.mapError(err))
	}
	body, marshalErr := json.Marshal(struct {
		TaskID        string           `json:"task_id"`
		State         domain.TaskState `json:"state"`
		QueuePosition *int             `json:"queue_position"`
	}{out.TaskID.String(), domain.StateQueued, out.QueuePosition})
	if marshalErr != nil {
		panic(fmt.Errorf("marshal submit response: %w", marshalErr))
	}
	return transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: req.RequestID, OK: true, Result: body}
}

func (uc *SubmitTaskUseCase) Execute(ctx context.Context, in SubmitTaskInput) (SubmitTaskOutput, error) {
	slug, err := domain.NewSlug(in.RawSlug)
	if err != nil {
		return SubmitTaskOutput{}, submitFailure("SLUG_INVALID_FORMAT", "error.slug.invalidFormat", map[string]any{"slug": in.RawSlug})
	}
	timeout, err := domain.ResolveTimeout(in.RequestedTimeoutSeconds)
	if err != nil {
		return SubmitTaskOutput{}, submitFailure("TIMEOUT_BELOW_MINIMUM", "error.timeout.belowMinimum", map[string]any{"requested_seconds": dereferenceInt(in.RequestedTimeoutSeconds), "min_seconds": timeoutMinimum()})
	}
	if strings.TrimSpace(in.Prompt) == "" {
		return SubmitTaskOutput{}, submitFailure("PROMPT_EMPTY", "error.prompt.empty", nil)
	}
	subcommand := domain.Subcommand(in.Subcommand)
	if !domain.IsSubmittable(subcommand) {
		return SubmitTaskOutput{}, submitFailure("SUBCOMMAND_NOT_SUBMITTABLE", "error.subcommand.notSubmittable", map[string]any{"subcommand": in.Subcommand})
	}
	model, ok := uc.options.ResolveModel(subcommand, in.Model)
	if !ok {
		return SubmitTaskOutput{}, submitFailure("MODEL_NOT_ALLOWED", "error.model.notAllowed", map[string]any{"model": dereferenceString(in.Model)})
	}
	effort, ok := uc.options.ResolveReasoningEffort(subcommand, in.ReasoningEffort)
	if !ok {
		return SubmitTaskOutput{}, submitFailure("REASONING_EFFORT_NOT_ALLOWED", "error.reasoningEffort.notAllowed", map[string]any{"reasoning_effort": dereferenceString(in.ReasoningEffort)})
	}
	if in.RawWorkingDir == "" || !filepath.IsAbs(in.RawWorkingDir) {
		return SubmitTaskOutput{}, submitFailure("WORKING_DIR_NOT_ABSOLUTE", "error.workingDir.notAbsolute", nil)
	}
	workingDir := filepath.Clean(in.RawWorkingDir)
	if subcommand == domain.SubcommandImpl {
		for _, path := range in.RawPaths {
			if !filepath.IsAbs(path) {
				return SubmitTaskOutput{}, submitFailure("PATHS_NOT_ABSOLUTE", "error.paths.notAbsolute", nil)
			}
		}
	}
	id, err := uc.reserveTaskID(subcommand, slug, in.RequestedAt)
	if err != nil {
		return SubmitTaskOutput{}, uc.mapError(err)
	}
	normalizedPaths := []domain.NormalizedPath(nil)
	acquired := false
	if subcommand == domain.SubcommandImpl {
		if uc.pathLocks == nil {
			uc.releaseReservation(id)
			return SubmitTaskOutput{}, fmt.Errorf("submit path lock acquirer is required for impl")
		}
		normalizedPaths, err = uc.pathLocks.Acquire(id, in.RawPaths)
		if err != nil {
			uc.releaseReservation(id)
			return SubmitTaskOutput{}, uc.mapPathLockError(err, id)
		}
		acquired = true
	}
	sandbox := "read-only"
	if subcommand == domain.SubcommandImpl {
		sandbox = "workspace-write"
	}
	result, err := uc.admitter.Admit(execution.TaskAdmissionInput{TaskID: id, Subcommand: subcommand, Slug: slug, RequestedTimeout: in.RequestedTimeoutSeconds, RequestedAt: in.RequestedAt, PromptText: in.Prompt, NormalizedPaths: normalizedPaths, ResolvedTimeout: timeout, Model: model, ReasoningEffort: effort, SandboxMode: sandbox, SourceWorkingDir: workingDir})
	if err != nil {
		if acquired {
			if cleanupErr := uc.pathLockReleaser.Release(context.WithoutCancel(ctx), id); cleanupErr != nil {
				uc.logger.Error("release path lock after admission failure", "task_id", id.String(), "error", execution.ErrorTypeName(cleanupErr))
			}
		}
		uc.releaseReservation(id)
		return SubmitTaskOutput{}, err
	}
	if result.LaunchPayload != nil {
		if !uc.starter.Start(*result.LaunchPayload) {
			if cleanupErr := uc.admitter.CompensateRejectedStart(id); cleanupErr != nil {
				uc.logger.Error("compensate rejected lifecycle start", "task_id", id.String(), "error", execution.ErrorTypeName(cleanupErr))
			}
			if acquired {
				if cleanupErr := uc.pathLockReleaser.Release(context.WithoutCancel(ctx), id); cleanupErr != nil {
					uc.logger.Error("release path lock after rejected lifecycle start", "task_id", id.String(), "error", execution.ErrorTypeName(cleanupErr))
				}
			}
			uc.releaseReservation(id)
			return SubmitTaskOutput{}, &submitError{code: "TASK_DIR_CREATE_FAILED", message: "error.taskDir.createFailed", cause: context.Canceled}
		}
	}
	return SubmitTaskOutput{TaskID: id, State: domain.StateQueued, QueuePosition: result.QueuePosition, Events: result.Events}, nil
}

type taskReservationError struct {
	TaskID domain.TaskID
	Err    error
}

func (e *taskReservationError) Error() string { return e.Err.Error() }
func (e *taskReservationError) Unwrap() error { return e.Err }

func (uc *SubmitTaskUseCase) reserveTaskID(subcommand domain.Subcommand, slug domain.Slug, at time.Time) (domain.TaskID, error) {
	var last domain.TaskID
	for attempt := 0; attempt < taskIDGenerationMaxAttempts; attempt++ {
		id, err := newTaskID(subcommand, slug, at, uc.random)
		if err != nil {
			return domain.TaskID{}, err
		}
		last = id
		err = uc.tasks.Reserve(id)
		if err == nil {
			return id, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return id, &taskReservationError{TaskID: id, Err: fmt.Errorf("reserve task directory: %w", err)}
		}
	}
	return last, &taskReservationError{TaskID: last, Err: fmt.Errorf("reserve task directory: %w", os.ErrExist)}
}
func (uc *SubmitTaskUseCase) releaseReservation(id domain.TaskID) {
	if err := uc.tasks.Release(id); err != nil {
		uc.logger.Error("release task reservation", "task_id", id.String(), "error", execution.ErrorTypeName(err))
	}
}
func (uc *SubmitTaskUseCase) mapPathLockError(err error, taskID domain.TaskID) error {
	if errors.Is(err, domain.ErrPathLockConflict) {
		var conflict *execution.PathLockConflictError
		if errors.As(err, &conflict) {
			return submitFailure("PATH_LOCK_CONFLICT", "error.pathLock.conflict", map[string]any{"path": conflict.Path.String(), "owner_task_id": conflict.TaskID.String()})
		}
	}
	if errors.Is(err, domain.ErrPathLockInfraFailure) {
		return submitFailure("PATH_LOCK_IO_ERROR", "error.pathLock.ioError", map[string]any{"task_id": taskID.String()})
	}
	var liveness *execution.LivenessCheckError
	if errors.As(err, &liveness) {
		return submitFailure("LIVENESS_LOCK_IO_ERROR", "error.liveness.lockIoError", map[string]any{"task_id": liveness.TaskID.String()})
	}
	return err
}
func (uc *SubmitTaskUseCase) mapError(err error) error {
	if _, ok := err.(*submitError); ok {
		return err
	}
	if errors.Is(err, errTaskIDRandomRead) {
		return submitFailure("TASK_ID_RANDOM_READ_FAILED", "error.taskId.randomReadFailed", nil)
	}
	if errors.Is(err, domain.ErrQueueFull) {
		return submitFailure("QUEUE_FULL", "error.queue.full", map[string]any{"queue_max_depth": uc.queueMaxDepth})
	}
	var reservation *taskReservationError
	if errors.As(err, &reservation) {
		return submitFailure("TASK_DIR_CREATE_FAILED", "error.taskDir.createFailed", map[string]any{"task_id": reservation.TaskID.String()})
	}
	return submitFailure("TASK_DIR_CREATE_FAILED", "error.taskDir.createFailed", nil)
}
func submitErrorResponse(requestID string, err error) transport.Response {
	value, ok := err.(*submitError)
	if !ok {
		value = &submitError{code: "TASK_DIR_CREATE_FAILED", message: "error.taskDir.createFailed"}
	}
	return transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: requestID, OK: false, Error: &transport.ErrorBody{Code: value.code, MessageKey: value.message, Detail: value.detail}}
}
func dereferenceInt(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}
func dereferenceString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
func timeoutMinimum() int { value, _ := domain.ResolveTimeout(nil); return value.ResolvedSeconds() }
