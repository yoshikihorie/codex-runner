package recovery

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
)

const (
	// Canonical source: validation-rules.md RESUME_RECOVERY_TIMEOUT_SECONDS.
	resumeRecoveryTimeout = 300 * time.Second
	// Canonical source: output-contract.md task placement root.
	recoveryTaskPlacementRoot = "/tmp/codex-tasks"
)

type TaskStore interface {
	Load(domain.TaskID) (domain.TaskSnapshot, error)
	Save(domain.TaskID, domain.TaskSnapshot) error
}
type TaskMutex interface {
	Lock(domain.TaskID)
	Unlock(domain.TaskID)
}
type Clock = domain.Clock

type Recoverer interface {
	Resume(context.Context, domain.TaskID, *domain.SessionRef, domain.RecoveryOrigin) (RecoveryResult, error)
}

type ResumeLaunchParams struct {
	TaskID                domain.TaskID
	CodexBinaryPath       string
	SessionID             string
	OutputLastMessagePath string
}
type ResumeLauncher interface {
	LaunchAndWait(context.Context, ResumeLaunchParams) error
}
type RecoveryResult struct {
	Succeeded          bool
	ExitCode           domain.ExitCode
	PartialOutputSaved bool
}
type RecoveryAttempt struct {
	TaskID            domain.TaskID
	Origin            domain.RecoveryOrigin
	SessionRef        domain.SessionRef
	StartedAt         time.Time
	CodexBinaryPath   string
	TaskPlacementRoot string
}

func failureExitCodeFor(origin domain.RecoveryOrigin) domain.ExitCode {
	if origin == domain.RecoveryOriginTimeout {
		return domain.NewExitCode(6)
	}
	return domain.NewExitCode(1)
}

func (r *RecoveryAttempt) Attempt(ctx context.Context, launcher ResumeLauncher, reader ContractReader) (RecoveryResult, error) {
	ctx, cancel := context.WithTimeout(ctx, resumeRecoveryTimeout)
	defer cancel()
	taskPlacementRoot := r.TaskPlacementRoot
	if taskPlacementRoot == "" {
		// Fallback for callers that construct RecoveryAttempt without wiring
		// TaskPlacementRoot explicitly (e.g. existing unit tests).
		taskPlacementRoot = recoveryTaskPlacementRoot
	}
	params := ResumeLaunchParams{TaskID: r.TaskID, CodexBinaryPath: r.CodexBinaryPath, SessionID: r.SessionRef.SessionID(), OutputLastMessagePath: filepath.Join(taskPlacementRoot, r.TaskID.String(), "last-message.md")}
	if err := launcher.LaunchAndWait(ctx, params); err != nil {
		return RecoveryResult{ExitCode: failureExitCodeFor(r.Origin)}, err
	}
	present, err := reader.ReadLastMessage(r.TaskID)
	if err != nil {
		return RecoveryResult{ExitCode: failureExitCodeFor(r.Origin)}, err
	}
	if !present {
		return RecoveryResult{ExitCode: failureExitCodeFor(r.Origin)}, nil
	}
	return RecoveryResult{Succeeded: true, ExitCode: domain.NewExitCode(0)}, nil
}

type resumeRecoverer struct {
	launcher          ResumeLauncher
	reader            ContractReader
	codexBinaryPath   string
	taskPlacementRoot string
	clock             Clock
}

func NewResumeRecoverer(launcher ResumeLauncher, reader ContractReader, codexBinaryPath string, taskPlacementRoot string, clock Clock) Recoverer {
	if isNilAdoptionDependency(launcher) || isNilAdoptionDependency(reader) || isNilAdoptionDependency(clock) {
		panic("resume recoverer requires non-nil dependencies")
	}
	return &resumeRecoverer{launcher: launcher, reader: reader, codexBinaryPath: codexBinaryPath, taskPlacementRoot: taskPlacementRoot, clock: clock}
}
func (r *resumeRecoverer) Resume(ctx context.Context, taskID domain.TaskID, sessionRef *domain.SessionRef, origin domain.RecoveryOrigin) (RecoveryResult, error) {
	if sessionRef == nil {
		return RecoveryResult{}, nil
	}
	return (&RecoveryAttempt{TaskID: taskID, Origin: origin, SessionRef: *sessionRef, StartedAt: r.clock.Now(), CodexBinaryPath: r.codexBinaryPath, TaskPlacementRoot: r.taskPlacementRoot}).Attempt(ctx, r.launcher, r.reader)
}

type recoveryContractWriter interface {
	ContractWriter
	WriteRecoveredMarker(domain.TaskID, time.Time) error
	AppendEvent(domain.TaskID, domain.Event) error
	WriteExitCode(domain.TaskID, domain.ExitCode) error
}

type RecoverViaResumeUseCase struct {
	tasks           TaskStore
	contract        recoveryContractWriter
	recoverer       Recoverer
	partial         *SavePartialOutputUseCase
	slotReleaser    SlotReleaser
	metricsRecorder MetricsRecorder
	stalledTracker  stalledTimeTracker
	taskMu          TaskMutex
	clock           Clock
	logger          *slog.Logger
}

func NewRecoverViaResumeUseCase(tasks TaskStore, contract recoveryContractWriter, recoverer Recoverer, partial *SavePartialOutputUseCase, slotReleaser SlotReleaser, metricsRecorder MetricsRecorder, stalledTracker stalledTimeTracker, taskMu TaskMutex, clock Clock, loggers ...*slog.Logger) *RecoverViaResumeUseCase {
	if isNilAdoptionDependency(tasks) || isNilAdoptionDependency(contract) || isNilAdoptionDependency(recoverer) || isNilAdoptionDependency(partial) || isNilAdoptionDependency(slotReleaser) || isNilAdoptionDependency(metricsRecorder) || isNilAdoptionDependency(stalledTracker) || isNilAdoptionDependency(taskMu) || isNilAdoptionDependency(clock) {
		panic("recover via resume use case requires non-nil dependencies")
	}
	logger := slog.Default()
	if len(loggers) > 0 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &RecoverViaResumeUseCase{tasks: tasks, contract: contract, recoverer: recoverer, partial: partial, slotReleaser: slotReleaser, metricsRecorder: metricsRecorder, stalledTracker: stalledTracker, taskMu: taskMu, clock: clock, logger: logger}
}

func (uc *RecoverViaResumeUseCase) Execute(ctx context.Context, in RecoverViaResumeInput) (RecoverViaResumeOutput, error) {
	if err := ctx.Err(); err != nil {
		return RecoverViaResumeOutput{}, err
	}
	if in.TaskID.String() == "" {
		return RecoverViaResumeOutput{}, fmt.Errorf("recovery: task id is required")
	}
	if in.Origin != domain.RecoveryOriginTimeout && in.Origin != domain.RecoveryOriginOrphan {
		return RecoverViaResumeOutput{}, fmt.Errorf("recovery: invalid origin")
	}
	if in.SessionRef != nil && in.SessionRef.Ephemeral() {
		return RecoverViaResumeOutput{}, domain.ErrSessionNotResumable
	}
	origin, err := uc.begin(in)
	if err != nil {
		return RecoverViaResumeOutput{}, err
	}
	result := RecoveryResult{ExitCode: failureExitCodeFor(origin)}
	if in.SessionRef != nil {
		resumeResult, resumeErr := uc.recoverer.Resume(ctx, in.TaskID, in.SessionRef, origin)
		if resumeErr != nil {
			uc.logger.Warn("resume recovery failed", "task_id", in.TaskID.String(), "error", resumeErr)
		} else {
			result = resumeResult
		}
	}
	completedAt := uc.clock.Now()
	cleanupCtx := context.WithoutCancel(ctx)
	output, terminal := uc.finish(cleanupCtx, in, origin, result, completedAt)
	if !terminal {
		return output, fmt.Errorf("recovery: terminal transition was not persisted")
	}
	stalledTotal := uc.stalledTracker.TakeTotal(in.TaskID)
	uc.metricsRecorder.Execute(cleanupCtx, metrics.RecordTaskMetricsInput{TaskID: in.TaskID, FinalState: output.FinalState, Estimated: true, OccurredAt: completedAt, StalledTotalMs: stalledTotal})
	uc.slotReleaser.ReleaseAndAdvance(cleanupCtx, in.TaskID, uc.clock.Now())
	return output, nil
}

func (uc *RecoverViaResumeUseCase) begin(in RecoverViaResumeInput) (domain.RecoveryOrigin, error) {
	uc.taskMu.Lock(in.TaskID)
	defer uc.taskMu.Unlock(in.TaskID)
	snapshot, err := uc.tasks.Load(in.TaskID)
	if err != nil {
		return "", err
	}
	task, err := snapshot.Restore()
	if err != nil {
		return "", err
	}
	events, err := task.BeginRecovery(in.SessionRef, in.OccurredAt)
	if err != nil {
		return "", err
	}
	attempted := events[0].(domain.RecoveryAttempted)
	updated, err := snapshot.WithTask(task, in.OccurredAt)
	if err != nil {
		return "", err
	}
	if err := uc.tasks.Save(in.TaskID, updated); err != nil {
		return "", err
	}
	writer := uc.contract
	if err := writer.AppendEvent(in.TaskID, attempted); err != nil {
		uc.logger.Warn("append recovery attempted event failed", "task_id", in.TaskID.String(), "error", err)
	}
	return attempted.Origin, nil
}

func (uc *RecoverViaResumeUseCase) finish(ctx context.Context, in RecoverViaResumeInput, origin domain.RecoveryOrigin, result RecoveryResult, at time.Time) (RecoverViaResumeOutput, bool) {
	uc.taskMu.Lock(in.TaskID)
	defer uc.taskMu.Unlock(in.TaskID)
	snapshot, err := uc.tasks.Load(in.TaskID)
	if err != nil {
		return RecoverViaResumeOutput{}, false
	}
	task, err := snapshot.Restore()
	if err != nil {
		return RecoverViaResumeOutput{}, false
	}
	writer := uc.contract
	partialSaved := result.PartialOutputSaved
	var events []domain.Event
	if result.Succeeded {
		events, err = task.CompleteRecovery(result.ExitCode, at)
		if err != nil {
			return RecoverViaResumeOutput{}, false
		}
		if err := writer.WriteRecoveredMarker(in.TaskID, at); err != nil {
			uc.logger.Warn("write recovered marker failed", "task_id", in.TaskID.String(), "error", err)
		}
	} else {
		if origin == domain.RecoveryOriginTimeout {
			partialResult, partialErr := uc.partial.Execute(ctx, SavePartialOutputInput{TaskID: in.TaskID, OccurredAt: at})
			if partialErr != nil {
				uc.logger.Warn("save partial output failed", "task_id", in.TaskID.String(), "error", partialErr)
			}
			partialSaved = partialResult.Saved
		}
		events, err = task.FailRecovery(partialSaved, at)
		if err != nil {
			return RecoverViaResumeOutput{}, false
		}
	}
	updated, snapshotErr := snapshot.WithTask(task, at)
	if snapshotErr != nil {
		uc.logger.Error("build recovery terminal snapshot failed", "task_id", in.TaskID.String(), "error", snapshotErr)
		return RecoverViaResumeOutput{}, false
	}
	output := RecoverViaResumeOutput{Succeeded: result.Succeeded, ExitCode: result.ExitCode, PartialOutputSaved: partialSaved, FinalState: task.State()}
	if err := writer.WriteExitCode(in.TaskID, result.ExitCode); err != nil {
		uc.logger.Warn("write recovery exit code failed", "task_id", in.TaskID.String(), "error", err)
	}
	if err := uc.tasks.Save(in.TaskID, updated); err != nil {
		uc.logger.Warn("save recovery terminal state failed", "task_id", in.TaskID.String(), "error", err)
	}
	for _, event := range events {
		if err := writer.AppendEvent(in.TaskID, event); err != nil {
			uc.logger.Warn("append recovery event failed", "task_id", in.TaskID.String(), "event_type", event.Type(), "error", err)
		}
	}
	return output, true
}
