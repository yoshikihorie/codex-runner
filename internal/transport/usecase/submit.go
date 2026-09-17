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
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

const (
	taskIDGenerationMaxAttempts = 10
	// Canonical source: validation-rules.md OUTPUT_SCHEMA_MAX_BYTES.
	outputSchemaMaxBytes = int64(1_000_000)

	// Canonical identifiers registered in the published error-code and message catalogs.
	admissionUnavailableCode       = "ADMISSION_UNAVAILABLE"
	admissionUnavailableMessageKey = "error.admission.unavailable"
	admissionFailedCode            = "ADMISSION_FAILED"
	admissionFailedMessageKey      = "error.admission.failed"
)

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
type taskPlacementRootProvider interface {
	TaskPlacementRoot() string
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
	RawWorktreeMode         *string
	OutputSchemaPath        OptionalString
	SandboxMode             OptionalString
	RequestedAt             time.Time
}
type SubmitTaskOutput struct {
	TaskID        domain.TaskID
	State         domain.TaskState
	QueuePosition *int
	Events        []domain.Event
}

type SubmitTaskUseCase struct {
	tasks             SubmitTaskStore
	pathLocks         SubmitPathLockAcquirer
	pathLockReleaser  SubmitPathLockReleaser
	admitter          TaskAdmitter
	queueMaxDepth     int
	starter           execution.TaskLifecycleStarter
	options           TaskOptionResolver
	clock             domain.Clock
	logger            *slog.Logger
	random            io.Reader
	outputSchemaOpen  func(string, int, os.FileMode) (*os.File, error)
	outputSchemaCopy  func(io.Writer, io.Reader) (int64, error)
	outputSchemaClose func(*os.File) error
}

func NewSubmitTaskUseCase(tasks SubmitTaskStore, pathLocks SubmitPathLockAcquirer, pathLockReleaser SubmitPathLockReleaser, admitter TaskAdmitter, queueMaxDepth int, starter execution.TaskLifecycleStarter, options TaskOptionResolver, clock domain.Clock, logger *slog.Logger) *SubmitTaskUseCase {
	if tasks == nil || admitter == nil || starter == nil || options == nil || clock == nil {
		panic("submit use case requires non-nil dependencies")
	}
	if pathLocks == nil || pathLockReleaser == nil {
		panic("submit use case requires path lock dependencies")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &SubmitTaskUseCase{tasks: tasks, pathLocks: pathLocks, pathLockReleaser: pathLockReleaser, admitter: admitter, queueMaxDepth: queueMaxDepth, starter: starter, options: options, clock: clock, logger: logger, random: productionTaskIDReader(), outputSchemaOpen: os.OpenFile, outputSchemaCopy: io.Copy, outputSchemaClose: func(file *os.File) error { return file.Close() }}
}

type submitWireInput struct {
	Subcommand              string         `json:"subcommand"`
	Slug                    string         `json:"slug"`
	Prompt                  string         `json:"prompt"`
	RequestedTimeoutSeconds *int           `json:"requested_timeout_seconds"`
	Paths                   []string       `json:"paths"`
	Model                   *string        `json:"model"`
	ReasoningEffort         *string        `json:"reasoning_effort"`
	WorkingDir              string         `json:"working_dir"`
	WorktreeMode            *string        `json:"worktree_mode"`
	OutputSchemaPath        OptionalString `json:"output_schema_path"`
	SandboxMode             OptionalString `json:"sandbox_mode"`
}

// OptionalString retains whether an optional string was omitted, null, or a string.
type OptionalString struct {
	Present bool
	Null    bool
	Value   string
}

func (value *OptionalString) UnmarshalJSON(data []byte) error {
	*value = OptionalString{Present: true}
	if string(data) == "null" {
		value.Null = true
		return nil
	}
	var decoded string
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	value.Value = decoded
	return nil
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
	out, err := uc.Execute(context.Background(), SubmitTaskInput{Subcommand: wire.Subcommand, RawSlug: wire.Slug, Prompt: wire.Prompt, RequestedTimeoutSeconds: wire.RequestedTimeoutSeconds, RawPaths: wire.Paths, Model: wire.Model, ReasoningEffort: wire.ReasoningEffort, RawWorkingDir: wire.WorkingDir, RawWorktreeMode: wire.WorktreeMode, OutputSchemaPath: wire.OutputSchemaPath, SandboxMode: wire.SandboxMode, RequestedAt: uc.clock.Now()})
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
	if in.OutputSchemaPath.Null || in.SandboxMode.Null {
		return SubmitTaskOutput{}, submitFailure("SUBMIT_PARAMS_MALFORMED", "error.submit.paramsMalformed", nil)
	}
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
	sourceFile, err := validateOutputSchemaPath(subcommand, in.OutputSchemaPath)
	if err != nil {
		return SubmitTaskOutput{}, err
	}
	if sourceFile != nil {
		defer sourceFile.Close()
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
	worktreeMode := domain.WorktreeModeAuto
	if subcommand == domain.SubcommandImpl {
		resolved, resolveErr := resolveWorktreeMode(in.RawWorktreeMode)
		if resolveErr != nil {
			return SubmitTaskOutput{}, submitFailure("WORKTREE_MODE_NOT_ALLOWED", "error.worktreeMode.notAllowed", map[string]any{"worktree_mode": dereferenceString(in.RawWorktreeMode)})
		}
		worktreeMode = resolved
	}
	sandbox, err := resolveSandboxMode(subcommand, in.SandboxMode)
	if err != nil {
		return SubmitTaskOutput{}, err
	}
	id, err := uc.reserveTaskID(subcommand, slug, in.RequestedAt)
	if err != nil {
		return SubmitTaskOutput{}, uc.mapError(err)
	}
	// Snapshot precedes PathLock acquisition so every post-reservation failure
	// shares the same recursive reservation rollback path.
	outputSchemaPath, err := uc.snapshotOutputSchema(id, sourceFile)
	if err != nil {
		uc.releaseReservation(id, nil)
		return SubmitTaskOutput{}, uc.mapError(&taskReservationError{TaskID: id, Err: err})
	}
	normalizedPaths := []domain.NormalizedPath(nil)
	acquired := false
	if subcommand == domain.SubcommandImpl {
		normalizedPaths, acquired, err = uc.acquirePathLocksAfterSnapshot(id, in.RawPaths, outputSchemaPath)
		if err != nil {
			return SubmitTaskOutput{}, uc.mapPathLockError(err, id)
		}
	}
	result, err := uc.admitter.Admit(execution.TaskAdmissionInput{TaskID: id, Subcommand: subcommand, Slug: slug, RequestedTimeout: in.RequestedTimeoutSeconds, RequestedAt: in.RequestedAt, PromptText: in.Prompt, NormalizedPaths: normalizedPaths, ResolvedTimeout: timeout, Model: model, ReasoningEffort: effort, SandboxMode: sandbox, SourceWorkingDir: workingDir, WorktreeMode: worktreeMode, OutputSchemaPath: outputSchemaPath})
	if err != nil {
		mappedErr := uc.mapError(err)
		if _, classified := mappedErr.(*submitError); !classified {
			uc.logger.Error("unclassified task admission failure", "task_id", id.String(), "error", execution.ErrorTypeName(err))
		}
		if acquired {
			if cleanupErr := uc.pathLockReleaser.Release(context.WithoutCancel(ctx), id); cleanupErr != nil {
				uc.logger.Error("release path lock after admission failure", "task_id", id.String(), "error", execution.ErrorTypeName(cleanupErr))
			}
		}
		uc.releaseReservation(id, outputSchemaPath)
		return SubmitTaskOutput{}, uc.mapAdmissionError(mappedErr, id)
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
			uc.releaseReservation(id, outputSchemaPath)
			return SubmitTaskOutput{}, &submitError{
				code:    admissionUnavailableCode,
				message: admissionUnavailableMessageKey,
				detail:  map[string]any{"task_id": id.String()},
				cause:   context.Canceled,
			}
		}
	}
	return SubmitTaskOutput{TaskID: id, State: domain.StateQueued, QueuePosition: result.QueuePosition, Events: result.Events}, nil
}

func validateOutputSchemaPath(subcommand domain.Subcommand, path OptionalString) (*os.File, error) {
	if !path.Present {
		return nil, nil
	}
	if !domain.SupportsOutputSchema(subcommand) {
		return nil, submitFailure("OUTPUT_SCHEMA_SUBCOMMAND_NOT_ALLOWED", "error.outputSchema.subcommandNotAllowed", nil)
	}
	if path.Value == "" || !filepath.IsAbs(path.Value) {
		return nil, submitFailure("OUTPUT_SCHEMA_NOT_ABSOLUTE", "error.outputSchema.notAbsolute", nil)
	}
	file, err := os.OpenFile(path.Value, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, submitFailure("OUTPUT_SCHEMA_NOT_FOUND", "error.outputSchema.notFound", nil)
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, submitFailure("OUTPUT_SCHEMA_NOT_FOUND", "error.outputSchema.notFound", nil)
	}
	if info.Size() > outputSchemaMaxBytes {
		_ = file.Close()
		return nil, submitFailure("OUTPUT_SCHEMA_TOO_LARGE", "error.outputSchema.tooLarge", map[string]any{"max_bytes": outputSchemaMaxBytes})
	}
	return file, nil
}

func resolveSandboxMode(subcommand domain.Subcommand, requested OptionalString) (string, error) {
	if !requested.Present {
		if subcommand == domain.SubcommandImpl {
			return "workspace-write", nil
		}
		return "read-only", nil
	}
	if requested.Value != "read-only" && requested.Value != "workspace-write" {
		return "", submitFailure("SANDBOX_MODE_NOT_ALLOWED", "error.sandboxMode.notAllowed", map[string]any{"sandbox_mode": requested.Value})
	}
	if subcommand == domain.SubcommandPlan || (requested.Value == "read-only" && subcommand != domain.SubcommandImpl) {
		return requested.Value, nil
	}
	return "", submitFailure("SANDBOX_MODE_NOT_ALLOWED", "error.sandboxMode.notAllowed", map[string]any{"sandbox_mode": requested.Value})
}

func (uc *SubmitTaskUseCase) snapshotOutputSchema(id domain.TaskID, sourceFile *os.File) (*string, error) {
	if sourceFile == nil {
		return nil, nil
	}
	rootProvider, ok := uc.options.(taskPlacementRootProvider)
	if !ok {
		return nil, submitFailure("OUTPUT_SCHEMA_NOT_FOUND", "error.outputSchema.notFound", nil)
	}
	root, err := domain.NewNormalizedPath(rootProvider.TaskPlacementRoot())
	if err != nil {
		return nil, submitFailure("OUTPUT_SCHEMA_NOT_FOUND", "error.outputSchema.notFound", nil)
	}
	snapshot, err := store.OutputSchemaPath(root.String(), id)
	if err != nil {
		return nil, fmt.Errorf("derive output schema snapshot path: %w", err)
	}
	destination, err := uc.outputSchemaOpen(snapshot, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create output schema snapshot: %w", err)
	}
	if _, err := sourceFile.Seek(0, io.SeekStart); err != nil {
		_ = uc.outputSchemaClose(destination)
		_ = os.Remove(snapshot)
		return nil, fmt.Errorf("seek output schema source: %w", err)
	}
	written, err := uc.outputSchemaCopy(destination, io.LimitReader(sourceFile, outputSchemaMaxBytes+1))
	if err != nil {
		_ = uc.outputSchemaClose(destination)
		_ = os.Remove(snapshot)
		return nil, fmt.Errorf("copy output schema snapshot: %w", err)
	}
	if written > outputSchemaMaxBytes {
		_ = uc.outputSchemaClose(destination)
		_ = os.Remove(snapshot)
		return nil, fmt.Errorf("output schema exceeds maximum size during snapshot")
	}
	if err := uc.outputSchemaClose(destination); err != nil {
		_ = os.Remove(snapshot)
		return nil, fmt.Errorf("close output schema snapshot: %w", err)
	}
	return &snapshot, nil
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
		// Subcommand and slug have already been validated, so NewTaskID can only
		// fail here when the random suffix cannot be read and is classified above.
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
func (uc *SubmitTaskUseCase) releaseReservation(id domain.TaskID, outputSchemaPath *string) {
	if outputSchemaPath != nil {
		if err := os.Remove(*outputSchemaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			uc.logger.Error("remove output schema snapshot before reservation release", "task_id", id.String(), "error", execution.ErrorTypeName(err))
		}
	}
	if err := uc.tasks.Release(id); err != nil {
		uc.logger.Error("release task reservation", "task_id", id.String(), "error", execution.ErrorTypeName(err))
	}
}
func (uc *SubmitTaskUseCase) acquirePathLocksAfterSnapshot(id domain.TaskID, paths []string, outputSchemaPath *string) ([]domain.NormalizedPath, bool, error) {
	normalized, err := uc.pathLocks.Acquire(id, paths)
	if err != nil {
		uc.releaseReservation(id, outputSchemaPath)
		return nil, false, err
	}
	return normalized, true, nil
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
	// domain.Acquire currently returns only ErrPathLockConflict or nil, so this
	// branch is unreachable through the production acquirer until that changes.
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
	return err
}

func (uc *SubmitTaskUseCase) mapAdmissionError(err error, taskID domain.TaskID) error {
	if _, ok := err.(*submitError); ok {
		return err
	}
	return submitFailure(admissionFailedCode, admissionFailedMessageKey, map[string]any{"task_id": taskID.String()})
}

func submitErrorResponse(requestID string, err error) transport.Response {
	value, ok := err.(*submitError)
	if !ok {
		panic(fmt.Errorf("submit response received unclassified error: %w", err))
	}
	return transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: requestID, OK: false, Error: &transport.ErrorBody{Code: value.code, MessageKey: value.message, Detail: value.detail}}
}
func resolveWorktreeMode(raw *string) (domain.WorktreeMode, error) {
	if raw == nil {
		return domain.WorktreeModeAuto, nil
	}
	return domain.ParseWorktreeMode(*raw)
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
