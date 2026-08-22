package recovery

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
)

const (
	adoptionOutcomeResumedMonitoring     = "resumed-monitoring"
	adoptionOutcomeOrphanRecovered       = "orphan-recovered"
	adoptionOutcomeOrphanRecoveryStarted = "orphan-recovery-started"
	adoptionOutcomeError                 = "error"
	adoptionOutcomeReconciled            = "reconciled"
	adoptionOutcomeDeferred              = "deferred"
)

// LivenessChecker reports whether a task's process is no longer alive.
type LivenessChecker interface {
	Execute(context.Context, domain.TaskID) (bool, error)
}

// AdoptionTaskStore is the task persistence boundary used during restart adoption.
type AdoptionTaskStore interface {
	Load(domain.TaskID) (domain.TaskSnapshot, error)
	Save(domain.TaskID, domain.TaskSnapshot) error
	ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error)
}

type adoptionContractReader interface {
	ContractReader
	contract.ExitCodeReader
}

// OrphanFinalizer is satisfied by execution.FinalizeTaskUseCase.
type OrphanFinalizer interface {
	Finalize(taskID domain.TaskID, rawExitCode int, estimated bool, adoptedAfterRestart bool, occurredAt time.Time) error
}

// TerminationEnsurer is satisfied by execution.TerminationEnsurer.
type TerminationEnsurer interface {
	Confirm(ctx context.Context, taskID domain.TaskID) (dead bool, err error)
	SendAndConfirm(ctx context.Context, taskID domain.TaskID, authority ProcessSignalAuthority, grace time.Duration) TerminationAttemptResult
}

type TerminationAttemptResult struct {
	Dead         bool
	TerminateErr error
	ConfirmErr   error
}

// KillConfirmer is satisfied by execution.ConfirmTaskKilledUseCase.
type KillConfirmer interface {
	ConfirmKilled(ctx context.Context, taskID domain.TaskID, rawExitCode int, estimated bool, occurredAt time.Time) error
}

// PathLockReleaser is satisfied by execution.ReleasePathLockUseCase.
type PathLockReleaser interface {
	Release(ctx context.Context, taskID domain.TaskID) error
}

type stalledTimeTracker interface {
	LeaveStalled(domain.TaskID, time.Time) int
	TakeTotal(domain.TaskID) int
}

type orphanResumeDispatcher interface {
	dispatch(func())
}

type asynchronousOrphanResumeDispatcher struct{}

func (asynchronousOrphanResumeDispatcher) dispatch(f func()) {
	go f()
}

type orphanResumeDispatcherOption struct {
	dispatcher orphanResumeDispatcher
}

type AdoptRunningTasksOutput struct {
	Outcomes      []AdoptionOutcome
	ElapsedMillis int64
}

type AdoptionOutcome struct {
	TaskID  domain.TaskID
	Outcome string
}

// AdoptRunningTasksUseCase re-establishes monitoring for tasks found in
// non-terminal states after a daemon restart.
type AdoptRunningTasksUseCase struct {
	tasks           AdoptionTaskStore
	liveness        LivenessChecker
	reader          adoptionContractReader
	contract        contract.ContractWriter
	finalizer       OrphanFinalizer
	resume          *RecoverViaResumeUseCase
	slots           SlotReleaser
	resetter        SlotResetter
	termination     TerminationEnsurer
	killed          KillConfirmer
	pathLocks       PathLockReleaser
	pending         *PendingReconciliationSet
	taskMu          TaskMutex
	clock           domain.Clock
	stalledTracker  stalledTimeTracker
	metricsRecorder MetricsRecorder
	resumeDispatch  orphanResumeDispatcher
	grace           time.Duration
	logger          *slog.Logger
}

func NewAdoptRunningTasksUseCase(tasks AdoptionTaskStore, liveness LivenessChecker, reader adoptionContractReader, writer contract.ContractWriter, finalizer OrphanFinalizer, resume *RecoverViaResumeUseCase, slots SlotReleaser, resetter SlotResetter, termination TerminationEnsurer, killed KillConfirmer, pathLocks PathLockReleaser, pending *PendingReconciliationSet, taskMu TaskMutex, clock domain.Clock, stalledTracker stalledTimeTracker, metricsRecorder MetricsRecorder, options ...any) *AdoptRunningTasksUseCase {
	if isNilAdoptionDependency(tasks) || isNilAdoptionDependency(liveness) || isNilAdoptionDependency(reader) || isNilAdoptionDependency(writer) || isNilAdoptionDependency(finalizer) || isNilAdoptionDependency(resume) || isNilAdoptionDependency(slots) || isNilAdoptionDependency(resetter) || isNilAdoptionDependency(termination) || isNilAdoptionDependency(killed) || isNilAdoptionDependency(pathLocks) || isNilAdoptionDependency(pending) || isNilAdoptionDependency(taskMu) || isNilAdoptionDependency(clock) || isNilAdoptionDependency(stalledTracker) || isNilAdoptionDependency(metricsRecorder) {
		panic("adopt running tasks use case requires non-nil dependencies")
	}
	logger := slog.Default()
	grace := defaultRecoveryTerminationGrace
	resumeDispatch := orphanResumeDispatcher(asynchronousOrphanResumeDispatcher{})
	for _, option := range options {
		switch value := option.(type) {
		case time.Duration:
			if value <= 0 {
				panic("adopt running tasks use case requires a positive grace")
			}
			grace = value
		case *slog.Logger:
			if value != nil {
				logger = value
			}
		case orphanResumeDispatcherOption:
			if isNilAdoptionDependency(value.dispatcher) {
				panic("adopt running tasks use case requires a non-nil orphan resume dispatcher")
			}
			resumeDispatch = value.dispatcher
		default:
			panic("adopt running tasks use case received unsupported option")
		}
	}
	return &AdoptRunningTasksUseCase{tasks: tasks, liveness: liveness, reader: reader, contract: writer, finalizer: finalizer, resume: resume, slots: slots, resetter: resetter, termination: termination, killed: killed, pathLocks: pathLocks, pending: pending, taskMu: taskMu, clock: clock, stalledTracker: stalledTracker, metricsRecorder: metricsRecorder, resumeDispatch: resumeDispatch, grace: grace, logger: logger}
}

func (uc *AdoptRunningTasksUseCase) Execute(ctx context.Context) (AdoptRunningTasksOutput, error) {
	adoptionObservedAt := uc.clock.Now()
	startedAt := adoptionObservedAt
	if ctx.Err() != nil {
		return uc.cancelledAdoptionOutput(startedAt), nil
	}
	snapshots, err := uc.tasks.ListByStates(adoptionStates())
	if err != nil {
		return AdoptRunningTasksOutput{}, err
	}
	if ctx.Err() != nil {
		return uc.cancelledAdoptionOutput(startedAt), nil
	}

	reservations := make(map[domain.TaskID]domain.Subcommand, len(snapshots))
	for _, snapshot := range snapshots {
		reservations[snapshot.TaskID] = snapshot.Subcommand
	}
	if ctx.Err() != nil {
		return uc.cancelledAdoptionOutput(startedAt), nil
	}
	uc.resetter.Reset(reservations)
	if ctx.Err() != nil {
		return uc.cancelledAdoptionOutput(startedAt), nil
	}

	out := AdoptRunningTasksOutput{Outcomes: make([]AdoptionOutcome, 0, len(snapshots))}
	for _, snapshot := range snapshots {
		if ctx.Err() != nil {
			break
		}
		outcome, completed := uc.adoptOne(ctx, snapshot.TaskID, snapshot, adoptionObservedAt)
		if !completed {
			break
		}
		out.Outcomes = append(out.Outcomes, AdoptionOutcome{TaskID: snapshot.TaskID, Outcome: outcome})
	}
	out.ElapsedMillis = uc.clock.Now().Sub(startedAt).Milliseconds()
	return out, nil
}

func (uc *AdoptRunningTasksUseCase) cancelledAdoptionOutput(startedAt time.Time) AdoptRunningTasksOutput {
	return AdoptRunningTasksOutput{Outcomes: []AdoptionOutcome{}, ElapsedMillis: uc.clock.Now().Sub(startedAt).Milliseconds()}
}

func (uc *AdoptRunningTasksUseCase) adoptOne(ctx context.Context, taskID domain.TaskID, listedSnapshot domain.TaskSnapshot, adoptionObservedAt time.Time) (outcome string, completed bool) {
	uc.taskMu.Lock(taskID)
	locked := true
	defer func() {
		if locked {
			uc.taskMu.Unlock(taskID)
		}
	}()
	if ctx.Err() != nil {
		return "", false
	}
	snapshot, err := uc.tasks.Load(taskID)
	if ctx.Err() != nil {
		return "", false
	}
	if err != nil {
		uc.logFailure("load task for adoption failed", taskID, err)
		if !errors.Is(err, domain.ErrTaskNotFound) {
			if ctx.Err() != nil {
				return "", false
			}
			uc.registerPending(taskID, listedSnapshot)
			if ctx.Err() != nil {
				return "", false
			}
		}
		return adoptionOutcomeError, true
	}
	if !isAdoptionState(snapshot.State) {
		return adoptionOutcomeReconciled, true
	}

	switch snapshot.State {
	case domain.StateRecovering:
		return uc.adoptRecoveringLocked(ctx, taskID, snapshot, adoptionObservedAt, &locked)
	case domain.StateTimeout:
		return uc.adoptTimeoutLocked(ctx, taskID, snapshot, &locked)
	case domain.StateOrphaned:
		uc.taskMu.Unlock(taskID)
		locked = false
		if ctx.Err() != nil {
			return "", false
		}
		return uc.recoverOrphan(ctx, taskID, snapshot.SessionRef, adoptionObservedAt)
	case domain.StateCancelling:
		return uc.adoptCancellingLocked(ctx, taskID, snapshot, &locked)
	}

	if ctx.Err() != nil {
		return "", false
	}
	dead, err := uc.liveness.Execute(ctx, taskID)
	if ctx.Err() != nil {
		return "", false
	}
	if err != nil {
		uc.logFailure("check task liveness for adoption failed", taskID, err)
		return adoptionOutcomeError, true
	}
	task, err := snapshot.Restore()
	if err != nil {
		uc.logFailure("restore task for adoption failed", taskID, err)
		return adoptionOutcomeError, true
	}
	events, err := task.Adopt(dead, adoptionObservedAt)
	if err != nil {
		uc.logFailure("adopt task failed", taskID, err)
		return adoptionOutcomeError, true
	}
	updated, err := snapshot.WithTask(task, adoptionObservedAt)
	if err != nil {
		uc.logFailure("build adopted task snapshot failed", taskID, err)
		return adoptionOutcomeError, true
	}
	if ctx.Err() != nil {
		return "", false
	}
	if err := uc.tasks.Save(taskID, updated); err != nil {
		if ctx.Err() != nil {
			return "", false
		}
		uc.logFailure("save adopted task failed", taskID, err)
		return adoptionOutcomeError, true
	}
	if ctx.Err() != nil {
		return "", false
	}
	if snapshot.State == domain.StateStalled {
		uc.stalledTracker.LeaveStalled(taskID, adoptionObservedAt)
	}
	if ctx.Err() != nil {
		return "", false
	}
	if err := uc.contract.WriteAdoptedMarker(taskID, adoptionObservedAt); err != nil {
		if ctx.Err() != nil {
			return "", false
		}
		uc.logFailure("write adopted marker failed", taskID, err)
	}
	if !uc.appendAdoptionEvents(ctx, taskID, events) {
		return "", false
	}
	uc.taskMu.Unlock(taskID)
	locked = false

	if !dead {
		return adoptionOutcomeResumedMonitoring, true
	}
	if ctx.Err() != nil {
		return "", false
	}
	return uc.recoverOrphan(ctx, taskID, snapshot.SessionRef, adoptionObservedAt)
}

func isNilAdoptionDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func (uc *AdoptRunningTasksUseCase) adoptRecoveringLocked(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, occurredAt time.Time, locked *bool) (string, bool) {
	if ctx.Err() != nil {
		return "", false
	}
	dead, err := uc.liveness.Execute(ctx, taskID)
	if ctx.Err() != nil {
		return "", false
	}
	if err != nil {
		uc.logFailure("check recovering task liveness for adoption failed", taskID, err)
		uc.taskMu.Unlock(taskID)
		*locked = false
		if ctx.Err() != nil {
			return "", false
		}
		uc.registerPending(taskID, snapshot)
		if ctx.Err() != nil {
			return "", false
		}
		return adoptionOutcomeError, true
	}
	if !dead {
		uc.taskMu.Unlock(taskID)
		*locked = false
		if ctx.Err() != nil {
			return "", false
		}
		uc.registerPending(taskID, snapshot)
		if ctx.Err() != nil {
			return "", false
		}
		return adoptionOutcomeDeferred, true
	}
	if ctx.Err() != nil {
		return "", false
	}
	present, err := uc.reader.ReadLastMessage(taskID)
	if ctx.Err() != nil {
		return "", false
	}
	if err != nil {
		uc.logFailure("read last message for recovering task failed", taskID, err)
		uc.taskMu.Unlock(taskID)
		*locked = false
		if ctx.Err() != nil {
			return "", false
		}
		uc.registerPending(taskID, snapshot)
		if ctx.Err() != nil {
			return "", false
		}
		return adoptionOutcomeError, true
	}
	if err, completed := resolveRecoveringLockedWithContext(ctx, uc.tasks, uc.reader, uc.contract, uc.logger, taskID, snapshot, present, occurredAt); !completed {
		return "", false
	} else if err != nil {
		uc.logFailure("resolve recovering task failed", taskID, err)
		uc.taskMu.Unlock(taskID)
		*locked = false
		if ctx.Err() != nil {
			return "", false
		}
		uc.registerPendingConfirmOnly(taskID)
		if ctx.Err() != nil {
			return "", false
		}
		if ctx.Err() != nil {
			return "", false
		}
		uc.reconcilePendingAfterConfirmedDeath(taskID)
		if ctx.Err() != nil {
			return "", false
		}
		return adoptionOutcomeError, true
	}
	uc.taskMu.Unlock(taskID)
	*locked = false
	if ctx.Err() != nil {
		return "", false
	}
	finalState := recoveringFinalState(present, snapshot)
	stalledTotal := uc.stalledTracker.TakeTotal(taskID)
	if ctx.Err() != nil {
		return "", false
	}
	uc.metricsRecorder.Execute(ctx, metrics.RecordTaskMetricsInput{TaskID: taskID, FinalState: finalState, Estimated: true, OccurredAt: occurredAt, StalledTotalMs: stalledTotal})
	if ctx.Err() != nil {
		return "", false
	}
	uc.slots.ReleaseAndAdvance(ctx, taskID, uc.clock.Now())
	if ctx.Err() != nil {
		return "", false
	}
	if present {
		return adoptionOutcomeOrphanRecovered, true
	}
	return adoptionOutcomeError, true
}

func resolveRecoveringLockedWithContext(ctx context.Context, tasks AdoptionTaskStore, reader contract.ExitCodeReader, writer contract.ContractWriter, logger *slog.Logger, taskID domain.TaskID, snapshot domain.TaskSnapshot, present bool, occurredAt time.Time) (error, bool) {
	task, err := snapshot.Restore()
	if err != nil {
		return err, true
	}
	exitCode := domain.NewExitCode(1)
	var events []domain.Event
	if present {
		exitCode = domain.NewExitCode(0)
		events, err = task.CompleteRecovery(exitCode, occurredAt)
	} else {
		events, err = task.FailRecovery(false, occurredAt)
	}
	if err != nil {
		return err, true
	}
	updated, err := snapshot.WithTask(task, occurredAt)
	if err != nil {
		return err, true
	}
	updated.AdoptedAfterRestart = true
	if ctx.Err() != nil {
		return nil, false
	}
	shouldWriteExitCode, err := contract.CheckExitCode(reader, taskID, exitCode)
	if ctx.Err() != nil {
		return nil, false
	}
	if err != nil {
		return err, true
	}
	if ctx.Err() != nil {
		return nil, false
	}
	if err := writer.WriteAdoptedMarker(taskID, occurredAt); err != nil {
		if ctx.Err() != nil {
			return nil, false
		}
		logger.Error("write adopted marker failed", "task_id", taskID.String(), "error", err)
	}
	if ctx.Err() != nil {
		return nil, false
	}
	if present {
		if err := writer.WriteRecoveredMarker(taskID, occurredAt); err != nil {
			if ctx.Err() != nil {
				return nil, false
			}
			logger.Error("write recovered marker failed", "task_id", taskID.String(), "error", err)
		}
	}
	if ctx.Err() != nil {
		return nil, false
	}
	if shouldWriteExitCode {
		if err := writer.WriteExitCode(taskID, exitCode); err != nil {
			if ctx.Err() != nil {
				return nil, false
			}
			logger.Error("write recovery exit code failed", "task_id", taskID.String(), "error", err)
		}
	}
	if ctx.Err() != nil {
		return nil, false
	}
	if err := tasks.Save(taskID, updated); err != nil {
		logger.Error("save recovered task failed", "task_id", taskID.String(), "error", err)
		return err, true
	}
	if ctx.Err() != nil {
		return nil, false
	}
	for _, event := range events {
		if ctx.Err() != nil {
			return nil, false
		}
		if err := writer.AppendEvent(taskID, event); err != nil {
			if ctx.Err() != nil {
				return nil, false
			}
			logger.Error("append adoption event failed", "task_id", taskID.String(), "error", err)
		}
		if ctx.Err() != nil {
			return nil, false
		}
	}
	return nil, true
}

// resolveRecoveringLocked persists recovery completion while the caller holds taskMu.
func resolveRecoveringLocked(tasks AdoptionTaskStore, reader contract.ExitCodeReader, writer contract.ContractWriter, logger *slog.Logger, taskID domain.TaskID, snapshot domain.TaskSnapshot, present bool, occurredAt time.Time) error {
	task, err := snapshot.Restore()
	if err != nil {
		return err
	}
	var (
		events   []domain.Event
		exitCode domain.ExitCode
	)
	if present {
		exitCode = domain.NewExitCode(0)
		events, err = task.CompleteRecovery(exitCode, occurredAt)
		if err != nil {
			return err
		}
	} else {
		exitCode = domain.NewExitCode(1)
		events, err = task.FailRecovery(false, occurredAt)
		if err != nil {
			return err
		}
	}
	updated, err := snapshot.WithTask(task, occurredAt)
	if err != nil {
		return err
	}
	updated.AdoptedAfterRestart = true
	shouldWriteExitCode, err := contract.CheckExitCode(reader, taskID, exitCode)
	if err != nil {
		return err
	}
	if err := writer.WriteAdoptedMarker(taskID, occurredAt); err != nil {
		logger.Error("write adopted marker failed", "task_id", taskID.String(), "error", err)
	}
	if present {
		if err := writer.WriteRecoveredMarker(taskID, occurredAt); err != nil {
			logger.Error("write recovered marker failed", "task_id", taskID.String(), "error", err)
		}
	}
	if shouldWriteExitCode {
		if err := writer.WriteExitCode(taskID, exitCode); err != nil {
			logger.Error("write recovery exit code failed", "task_id", taskID.String(), "error", err)
		}
	}
	if err := tasks.Save(taskID, updated); err != nil {
		logger.Error("save recovered task failed", "task_id", taskID.String(), "error", err)
		return err
	}
	for _, event := range events {
		if err := writer.AppendEvent(taskID, event); err != nil {
			logger.Error("append adoption event failed", "task_id", taskID.String(), "error", err)
		}
	}
	return nil
}

func recoveringFinalState(present bool, snapshot domain.TaskSnapshot) domain.TaskState {
	if present {
		return domain.StateRecovered
	}
	if snapshot.RecoveryOrigin != nil && *snapshot.RecoveryOrigin == domain.RecoveryOriginTimeout {
		return domain.StateTimeoutLost
	}
	return domain.StateLost
}

func (uc *AdoptRunningTasksUseCase) adoptTimeoutLocked(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, locked *bool) (string, bool) {
	if ctx.Err() != nil {
		return "", false
	}
	dead, err := uc.liveness.Execute(ctx, taskID)
	if ctx.Err() != nil {
		return "", false
	}
	if err != nil {
		uc.logFailure("check timeout task liveness for adoption failed", taskID, err)
		uc.taskMu.Unlock(taskID)
		*locked = false
		if ctx.Err() != nil {
			return "", false
		}
		uc.registerPending(taskID, snapshot)
		if ctx.Err() != nil {
			return "", false
		}
		return adoptionOutcomeError, true
	}
	if dead {
		uc.taskMu.Unlock(taskID)
		*locked = false
		if err, completed := uc.startTimeoutRecovery(ctx, taskID, snapshot); !completed {
			return "", false
		} else if err != nil {
			uc.reconcilePendingAfterConfirmedDeath(taskID)
			return adoptionOutcomeError, true
		}
		return adoptionOutcomeOrphanRecoveryStarted, true
	}
	if snapshot.PID == nil {
		uc.taskMu.Unlock(taskID)
		*locked = false
		if ctx.Err() != nil {
			return "", false
		}
		uc.registerPending(taskID, snapshot)
		if ctx.Err() != nil {
			return "", false
		}
		return adoptionOutcomeDeferred, true
	}
	uc.taskMu.Unlock(taskID)
	*locked = false
	if ctx.Err() != nil {
		return "", false
	}
	if !uc.registerPending(taskID, snapshot) {
		return adoptionOutcomeError, true
	}
	if ctx.Err() != nil {
		return "", false
	}
	go uc.confirmTimeoutTermination(context.WithoutCancel(ctx), taskID, snapshot)
	return adoptionOutcomeDeferred, true
}

func (uc *AdoptRunningTasksUseCase) confirmTimeoutTermination(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot) {
	authority, ok := processSignalAuthority(snapshot)
	if !ok {
		uc.registerPending(taskID, snapshot)
		return
	}
	claim, outcome := uc.pending.ClaimForSend(taskID, authority)
	if outcome != ClaimAcquired {
		return
	}
	result := uc.termination.SendAndConfirm(ctx, taskID, authority, uc.grace)
	if !result.Dead {
		uc.handleUnconfirmedTermination(taskID, claim, result)
		return
	}
	latest, valid := currentClaimAuthority(uc.tasks, uc.taskMu, claim)
	if !valid {
		uc.pending.InvalidateSend(claim)
		return
	}
	terminationConfirmedAt := uc.clock.Now()
	if err := uc.releaseTimeoutPathLock(ctx, taskID, latest); err != nil {
		uc.logFailure("start timeout recovery after termination failed", taskID, err)
		uc.pending.InvalidateSend(claim)
		return
	}
	recoveryDispatchAt := uc.clock.Now()
	go uc.resumeRecovery(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: taskID, SessionRef: latest.SessionRef, Origin: domain.RecoveryOriginTimeout, OccurredAt: recoveryDispatchAt}, claim)
	_ = terminationConfirmedAt
}

func (uc *AdoptRunningTasksUseCase) startTimeoutRecovery(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot) (error, bool) {
	if ctx.Err() != nil {
		return nil, false
	}
	if err := uc.releaseTimeoutPathLock(ctx, taskID, snapshot); err != nil {
		return err, true
	}
	if ctx.Err() != nil {
		return nil, false
	}
	recoveryDispatchAt := uc.clock.Now()
	if ctx.Err() != nil {
		return nil, false
	}
	go uc.resumeRecovery(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: taskID, SessionRef: snapshot.SessionRef, Origin: domain.RecoveryOriginTimeout, OccurredAt: recoveryDispatchAt}, SendClaim{})
	return nil, true
}

func (uc *AdoptRunningTasksUseCase) releaseTimeoutPathLock(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot) error {
	if snapshot.Subcommand == domain.SubcommandImpl {
		if err := uc.pathLocks.Release(ctx, taskID); err != nil {
			uc.logFailure("release path lock before timeout recovery failed", taskID, err)
			return err
		}
	}
	return nil
}

func (uc *AdoptRunningTasksUseCase) adoptCancellingLocked(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, locked *bool) (string, bool) {
	occurredAt := uc.clock.Now()
	if ctx.Err() != nil {
		return "", false
	}
	dead, err := uc.liveness.Execute(ctx, taskID)
	if ctx.Err() != nil {
		return "", false
	}
	if errors.Is(err, domain.ErrTaskNotFound) || (err == nil && dead) {
		uc.taskMu.Unlock(taskID)
		*locked = false
		if ctx.Err() != nil {
			return "", false
		}
		if err := uc.killed.ConfirmKilled(ctx, taskID, cancelledExitCode, true, occurredAt); err != nil {
			if ctx.Err() != nil {
				return "", false
			}
			uc.logFailure("confirm cancelling task killed failed", taskID, err)
			uc.reconcilePendingAfterConfirmedDeath(taskID)
			return adoptionOutcomeError, true
		}
		return adoptionOutcomeReconciled, true
	}
	if err != nil {
		uc.logFailure("check cancelling task liveness for adoption failed", taskID, err)
		uc.taskMu.Unlock(taskID)
		*locked = false
		if ctx.Err() != nil {
			return "", false
		}
		uc.registerPending(taskID, snapshot)
		if ctx.Err() != nil {
			return "", false
		}
		return adoptionOutcomeError, true
	}
	if snapshot.PID == nil {
		uc.taskMu.Unlock(taskID)
		*locked = false
		if ctx.Err() != nil {
			return "", false
		}
		uc.registerPending(taskID, snapshot)
		if ctx.Err() != nil {
			return "", false
		}
		return adoptionOutcomeDeferred, true
	}
	uc.taskMu.Unlock(taskID)
	*locked = false
	if ctx.Err() != nil {
		return "", false
	}
	if !uc.registerPending(taskID, snapshot) {
		return adoptionOutcomeError, true
	}
	if ctx.Err() != nil {
		return "", false
	}
	go uc.confirmCancellationTermination(context.WithoutCancel(ctx), taskID, snapshot)
	return adoptionOutcomeDeferred, true
}

func (uc *AdoptRunningTasksUseCase) confirmCancellationTermination(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot) {
	authority, ok := processSignalAuthority(snapshot)
	if !ok {
		uc.registerPending(taskID, snapshot)
		return
	}
	claim, outcome := uc.pending.ClaimForSend(taskID, authority)
	if outcome != ClaimAcquired {
		return
	}
	result := uc.termination.SendAndConfirm(ctx, taskID, authority, uc.grace)
	if !result.Dead {
		uc.handleUnconfirmedTermination(taskID, claim, result)
		return
	}
	_, valid := currentClaimAuthority(uc.tasks, uc.taskMu, claim)
	if !valid {
		uc.pending.InvalidateSend(claim)
		return
	}
	terminationConfirmedAt := uc.clock.Now()
	if err := uc.killed.ConfirmKilled(ctx, taskID, cancelledExitCode, true, terminationConfirmedAt); err != nil {
		uc.logFailure("confirm cancelling task killed after termination failed", taskID, err)
		uc.pending.InvalidateSend(claim)
		return
	}
	uc.pending.RemoveClaim(claim)
}

func (uc *AdoptRunningTasksUseCase) recoverOrphan(ctx context.Context, taskID domain.TaskID, sessionRef *domain.SessionRef, occurredAt time.Time) (string, bool) {
	if ctx.Err() != nil {
		return "", false
	}
	present, err := uc.reader.ReadLastMessage(taskID)
	if ctx.Err() != nil {
		return "", false
	}
	if err != nil {
		uc.logFailure("read last message for adopted orphan failed", taskID, err)
		if ctx.Err() != nil {
			return "", false
		}
		uc.registerPendingConfirmOnly(taskID)
		return adoptionOutcomeError, true
	}
	if present {
		if ctx.Err() != nil {
			return "", false
		}
		if err := uc.finalizer.Finalize(taskID, 0, true, true, occurredAt); err != nil {
			if ctx.Err() != nil {
				return "", false
			}
			uc.logFailure("finalize adopted orphan failed", taskID, err)
			uc.reconcilePendingAfterConfirmedDeath(taskID)
			return adoptionOutcomeError, true
		}
		return adoptionOutcomeOrphanRecovered, true
	}
	if uc.resume == nil {
		uc.logFailure("resume recovery for adopted orphan is unavailable", taskID, nil)
		return adoptionOutcomeError, true
	}
	recoveryDispatchAt := uc.clock.Now()
	if ctx.Err() != nil {
		return "", false
	}
	uc.resumeDispatch.dispatch(func() {
		uc.resumeRecovery(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: taskID, SessionRef: sessionRef, Origin: domain.RecoveryOriginOrphan, OccurredAt: recoveryDispatchAt}, SendClaim{})
	})
	return adoptionOutcomeOrphanRecoveryStarted, true
}

func (uc *AdoptRunningTasksUseCase) resumeRecovery(ctx context.Context, in RecoverViaResumeInput, claim SendClaim) {
	if _, err := uc.resume.Execute(ctx, in); err != nil {
		uc.logFailure("resume recovery for adopted task failed", in.TaskID, err)
		if claim.Token != 0 {
			uc.pending.InvalidateSend(claim)
		} else {
			uc.reconcilePendingAfterConfirmedDeath(in.TaskID)
		}
		return
	}
	if claim.Token != 0 {
		uc.pending.RemoveClaim(claim)
	} else {
		uc.pending.Remove(in.TaskID)
	}
}

func processSignalAuthority(snapshot domain.TaskSnapshot) (ProcessSignalAuthority, bool) {
	if snapshot.PID == nil || snapshot.ProcessStartedAt == nil || (snapshot.State != domain.StateTimeout && snapshot.State != domain.StateCancelling) {
		return ProcessSignalAuthority{}, false
	}
	return ProcessSignalAuthority{TaskID: snapshot.TaskID, PID: *snapshot.PID, ProcessStartedAt: *snapshot.ProcessStartedAt, ExpectedState: snapshot.State}, true
}

func pendingRegistration(snapshot domain.TaskSnapshot) (PendingSendDisposition, *ProcessSignalAuthority) {
	if snapshot.State == domain.StateTimeout || snapshot.State == domain.StateCancelling {
		if authority, ok := processSignalAuthority(snapshot); ok {
			return PendingSendUnsent, &authority
		}
	}
	return PendingSendConfirmOnly, nil
}

func currentClaimAuthority(tasks AdoptionTaskStore, taskMu TaskMutex, claim SendClaim) (domain.TaskSnapshot, bool) {
	snapshot, valid, _ := currentClaimAuthorityWithError(tasks, taskMu, claim)
	return snapshot, valid
}

func currentClaimAuthorityWithError(tasks AdoptionTaskStore, taskMu TaskMutex, claim SendClaim) (domain.TaskSnapshot, bool, error) {
	taskMu.Lock(claim.TaskID)
	snapshot, err := tasks.Load(claim.TaskID)
	taskMu.Unlock(claim.TaskID)
	if err != nil {
		return domain.TaskSnapshot{}, false, err
	}
	return snapshot, matchesProcessSignalAuthority(snapshot, claim.Authority), nil
}

func releaseOrInvalidateSendAfterTerminationError(tasks AdoptionTaskStore, pending *PendingReconciliationSet, taskMu TaskMutex, logger *slog.Logger, claim SendClaim) {
	_, valid, err := currentClaimAuthorityWithError(tasks, taskMu, claim)
	if err != nil && !errors.Is(err, domain.ErrTaskNotFound) {
		logger.Error("load task after termination send failure", "task_id", claim.TaskID.String(), "error", err)
		pending.ReleaseSend(claim)
		return
	}
	if valid {
		pending.ReleaseSend(claim)
		return
	}
	pending.InvalidateSend(claim)
}

func (uc *AdoptRunningTasksUseCase) handleUnconfirmedTermination(taskID domain.TaskID, claim SendClaim, result TerminationAttemptResult) {
	uc.logger.Warn("adopted task termination remains unconfirmed", "task_id", taskID.String(), "terminate_error", result.TerminateErr, "confirm_error", result.ConfirmErr)
	switch {
	case errors.Is(result.TerminateErr, ErrProcessSignalAuthorityInvalid):
		uc.pending.InvalidateSend(claim)
	case result.TerminateErr == nil:
		uc.pending.CompleteSend(claim)
	default:
		releaseOrInvalidateSendAfterTerminationError(uc.tasks, uc.pending, uc.taskMu, uc.logger, claim)
	}
}

func (uc *AdoptRunningTasksUseCase) registerPending(taskID domain.TaskID, snapshot domain.TaskSnapshot) bool {
	disposition, authority := pendingRegistration(snapshot)
	if err := uc.pending.Register(taskID, disposition, authority); err != nil {
		uc.logFailure("register pending reconciliation failed", taskID, err)
		return false
	}
	return true
}

func (uc *AdoptRunningTasksUseCase) registerPendingConfirmOnly(taskID domain.TaskID) {
	if err := uc.pending.Register(taskID, PendingSendConfirmOnly, nil); err != nil {
		uc.logFailure("register pending reconciliation failed", taskID, err)
	}
}

func (uc *AdoptRunningTasksUseCase) reconcilePendingAfterSnapshotFailure(taskID domain.TaskID) {
	reconcilePendingAfterFailure(uc.tasks, uc.pending, uc.taskMu, uc.logger, taskID, pendingFailureFromSnapshot)
}

func (uc *AdoptRunningTasksUseCase) reconcilePendingAfterConfirmedDeath(taskID domain.TaskID) {
	reconcilePendingAfterFailure(uc.tasks, uc.pending, uc.taskMu, uc.logger, taskID, pendingFailureAfterConfirmedDeath)
}

func (uc *AdoptRunningTasksUseCase) appendAdoptionEvents(ctx context.Context, taskID domain.TaskID, events []domain.Event) bool {
	for _, event := range events {
		if ctx.Err() != nil {
			return false
		}
		if err := uc.contract.AppendEvent(taskID, event); err != nil {
			if ctx.Err() != nil {
				return false
			}
			uc.logFailure("append adoption event failed", taskID, err)
		}
		if ctx.Err() != nil {
			return false
		}
	}
	return true
}

func (uc *AdoptRunningTasksUseCase) logFailure(message string, taskID domain.TaskID, err error) {
	uc.logger.Error(message, "task_id", taskID.String(), "error", err)
}

func adoptionStates() []domain.TaskState {
	return []domain.TaskState{
		domain.StateStarting,
		domain.StateRunning,
		domain.StateStalled,
		domain.StateRecovering,
		domain.StateTimeout,
		domain.StateOrphaned,
		domain.StateCancelling,
	}
}

func isAdoptionState(state domain.TaskState) bool {
	for _, candidate := range adoptionStates() {
		if state == candidate {
			return true
		}
	}
	return false
}
