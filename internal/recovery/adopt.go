package recovery

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
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

// OrphanFinalizer is satisfied by execution.FinalizeTaskUseCase.
type OrphanFinalizer interface {
	Finalize(taskID domain.TaskID, rawExitCode int, estimated bool, adoptedAfterRestart bool, occurredAt time.Time) error
}

// TerminationEnsurer is satisfied by execution.TerminationEnsurer.
type TerminationEnsurer interface {
	Confirm(ctx context.Context, taskID domain.TaskID) (dead bool, err error)
	SendAndConfirm(ctx context.Context, taskID domain.TaskID, pid int, grace time.Duration) (dead bool, err error)
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
	tasks          AdoptionTaskStore
	liveness       LivenessChecker
	reader         ContractReader
	contract       contract.ContractWriter
	finalizer      OrphanFinalizer
	resume         *RecoverViaResumeUseCase
	slots          SlotReleaser
	resetter       SlotResetter
	termination    TerminationEnsurer
	killed         KillConfirmer
	pathLocks      PathLockReleaser
	pending        *PendingReconciliationSet
	taskMu         TaskMutex
	clock          domain.Clock
	stalledTracker stalledTimeTracker
	grace          time.Duration
	logger         *slog.Logger
}

func NewAdoptRunningTasksUseCase(tasks AdoptionTaskStore, liveness LivenessChecker, reader ContractReader, writer contract.ContractWriter, finalizer OrphanFinalizer, resume *RecoverViaResumeUseCase, slots SlotReleaser, resetter SlotResetter, termination TerminationEnsurer, killed KillConfirmer, pathLocks PathLockReleaser, pending *PendingReconciliationSet, taskMu TaskMutex, clock domain.Clock, stalledTracker stalledTimeTracker, options ...any) *AdoptRunningTasksUseCase {
	if isNilAdoptionDependency(tasks) || isNilAdoptionDependency(liveness) || isNilAdoptionDependency(reader) || isNilAdoptionDependency(writer) || isNilAdoptionDependency(finalizer) || isNilAdoptionDependency(resume) || isNilAdoptionDependency(slots) || isNilAdoptionDependency(resetter) || isNilAdoptionDependency(termination) || isNilAdoptionDependency(killed) || isNilAdoptionDependency(pathLocks) || isNilAdoptionDependency(pending) || isNilAdoptionDependency(taskMu) || isNilAdoptionDependency(clock) || isNilAdoptionDependency(stalledTracker) {
		panic("adopt running tasks use case requires non-nil dependencies")
	}
	logger := slog.Default()
	grace := defaultRecoveryTerminationGrace
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
		default:
			panic("adopt running tasks use case received unsupported option")
		}
	}
	return &AdoptRunningTasksUseCase{tasks: tasks, liveness: liveness, reader: reader, contract: writer, finalizer: finalizer, resume: resume, slots: slots, resetter: resetter, termination: termination, killed: killed, pathLocks: pathLocks, pending: pending, taskMu: taskMu, clock: clock, stalledTracker: stalledTracker, grace: grace, logger: logger}
}

func (uc *AdoptRunningTasksUseCase) Execute(ctx context.Context) (AdoptRunningTasksOutput, error) {
	startedAt := uc.clock.Now()
	snapshots, err := uc.tasks.ListByStates(adoptionStates())
	if err != nil {
		return AdoptRunningTasksOutput{}, err
	}

	taskIDs := make([]domain.TaskID, 0, len(snapshots))
	for _, snapshot := range snapshots {
		taskIDs = append(taskIDs, snapshot.TaskID)
	}
	uc.resetter.Reset(taskIDs)

	out := AdoptRunningTasksOutput{Outcomes: make([]AdoptionOutcome, 0, len(taskIDs))}
	for _, taskID := range taskIDs {
		out.Outcomes = append(out.Outcomes, AdoptionOutcome{TaskID: taskID, Outcome: uc.adoptOne(ctx, taskID)})
	}
	out.ElapsedMillis = uc.clock.Now().Sub(startedAt).Milliseconds()
	return out, nil
}

func (uc *AdoptRunningTasksUseCase) adoptOne(ctx context.Context, taskID domain.TaskID) string {
	uc.taskMu.Lock(taskID)
	snapshot, err := uc.tasks.Load(taskID)
	if err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logFailure("load task for adoption failed", taskID, err)
		return adoptionOutcomeError
	}
	if !isAdoptionState(snapshot.State) {
		uc.taskMu.Unlock(taskID)
		return adoptionOutcomeReconciled
	}

	switch snapshot.State {
	case domain.StateRecovering:
		return uc.adoptRecovering(ctx, taskID, snapshot)
	case domain.StateTimeout:
		return uc.adoptTimeout(ctx, taskID, snapshot)
	case domain.StateOrphaned:
		uc.taskMu.Unlock(taskID)
		return uc.recoverOrphan(ctx, taskID, snapshot.SessionRef, uc.clock.Now())
	case domain.StateCancelling:
		return uc.adoptCancelling(ctx, taskID, snapshot)
	}

	occurredAt := uc.clock.Now()
	dead, err := uc.liveness.Execute(ctx, taskID)
	if err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logFailure("check task liveness for adoption failed", taskID, err)
		return adoptionOutcomeError
	}
	task, err := snapshot.Restore()
	if err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logFailure("restore task for adoption failed", taskID, err)
		return adoptionOutcomeError
	}
	events, err := task.Adopt(dead, occurredAt)
	if err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logFailure("adopt task failed", taskID, err)
		return adoptionOutcomeError
	}
	updated, err := snapshot.WithTask(task, occurredAt)
	if err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logFailure("build adopted task snapshot failed", taskID, err)
		return adoptionOutcomeError
	}
	if err := uc.tasks.Save(taskID, updated); err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logFailure("save adopted task failed", taskID, err)
		return adoptionOutcomeError
	}
	if snapshot.State == domain.StateStalled {
		uc.stalledTracker.LeaveStalled(taskID, occurredAt)
	}
	if err := uc.contract.WriteAdoptedMarker(taskID, occurredAt); err != nil {
		uc.logFailure("write adopted marker failed", taskID, err)
	}
	uc.appendAdoptionEvents(taskID, events)
	uc.taskMu.Unlock(taskID)

	if !dead {
		return adoptionOutcomeResumedMonitoring
	}
	return uc.recoverOrphan(ctx, taskID, snapshot.SessionRef, occurredAt)
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

func (uc *AdoptRunningTasksUseCase) adoptRecovering(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot) string {
	occurredAt := uc.clock.Now()
	dead, err := uc.liveness.Execute(ctx, taskID)
	if err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logFailure("check recovering task liveness for adoption failed", taskID, err)
		uc.pending.Add(taskID, false)
		return adoptionOutcomeError
	}
	if !dead {
		uc.taskMu.Unlock(taskID)
		uc.pending.Add(taskID, true)
		return adoptionOutcomeDeferred
	}
	present, err := uc.reader.ReadLastMessage(taskID)
	if err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logFailure("read last message for recovering task failed", taskID, err)
		uc.pending.Add(taskID, false)
		return adoptionOutcomeError
	}
	if err := resolveRecoveringLocked(uc.tasks, uc.contract, uc.logger, taskID, snapshot, present, occurredAt); err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logFailure("resolve recovering task failed", taskID, err)
		uc.pending.Add(taskID, false)
		return adoptionOutcomeError
	}
	uc.taskMu.Unlock(taskID)
	uc.slots.ReleaseAndAdvance(ctx, taskID, uc.clock.Now())
	if present {
		return adoptionOutcomeOrphanRecovered
	}
	return adoptionOutcomeError
}

// resolveRecoveringLocked persists recovery completion while the caller holds taskMu.
func resolveRecoveringLocked(tasks AdoptionTaskStore, writer contract.ContractWriter, logger *slog.Logger, taskID domain.TaskID, snapshot domain.TaskSnapshot, present bool, occurredAt time.Time) error {
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
	if err := writer.WriteAdoptedMarker(taskID, occurredAt); err != nil {
		logger.Error("write adopted marker failed", "task_id", taskID.String(), "error", err)
	}
	if present {
		if err := writer.WriteRecoveredMarker(taskID, occurredAt); err != nil {
			logger.Error("write recovered marker failed", "task_id", taskID.String(), "error", err)
		}
	}
	if err := writer.WriteExitCode(taskID, exitCode); err != nil {
		logger.Error("write recovery exit code failed", "task_id", taskID.String(), "error", err)
	}
	if err := tasks.Save(taskID, updated); err != nil {
		logger.Error("save recovered task failed", "task_id", taskID.String(), "error", err)
	}
	for _, event := range events {
		if err := writer.AppendEvent(taskID, event); err != nil {
			logger.Error("append adoption event failed", "task_id", taskID.String(), "error", err)
		}
	}
	return nil
}

func (uc *AdoptRunningTasksUseCase) adoptTimeout(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot) string {
	occurredAt := uc.clock.Now()
	dead, err := uc.liveness.Execute(ctx, taskID)
	if err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logFailure("check timeout task liveness for adoption failed", taskID, err)
		uc.pending.Add(taskID, false)
		return adoptionOutcomeError
	}
	if dead {
		uc.taskMu.Unlock(taskID)
		if err := uc.startTimeoutRecovery(ctx, taskID, snapshot, occurredAt); err != nil {
			return adoptionOutcomeError
		}
		return adoptionOutcomeOrphanRecoveryStarted
	}
	if snapshot.PID == nil {
		uc.taskMu.Unlock(taskID)
		uc.pending.Add(taskID, true)
		return adoptionOutcomeDeferred
	}
	pid := *snapshot.PID
	uc.taskMu.Unlock(taskID)
	go uc.confirmTimeoutTermination(context.WithoutCancel(ctx), taskID, snapshot, pid, occurredAt)
	return adoptionOutcomeDeferred
}

func (uc *AdoptRunningTasksUseCase) confirmTimeoutTermination(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, pid int, occurredAt time.Time) {
	dead, err := uc.termination.SendAndConfirm(ctx, taskID, pid, uc.grace)
	if err != nil {
		uc.logFailure("confirm timeout task termination failed", taskID, err)
	}
	if err != nil || !dead {
		uc.pending.Add(taskID, true)
		return
	}
	if err := uc.startTimeoutRecovery(ctx, taskID, snapshot, occurredAt); err != nil {
		uc.logFailure("start timeout recovery after termination failed", taskID, err)
	}
}

func (uc *AdoptRunningTasksUseCase) startTimeoutRecovery(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, occurredAt time.Time) error {
	if snapshot.Subcommand == domain.SubcommandImpl {
		if err := uc.pathLocks.Release(ctx, taskID); err != nil {
			uc.logFailure("release path lock before timeout recovery failed", taskID, err)
			return err
		}
	}
	go uc.resumeRecovery(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: taskID, SessionRef: snapshot.SessionRef, Origin: domain.RecoveryOriginTimeout, OccurredAt: occurredAt})
	return nil
}

func (uc *AdoptRunningTasksUseCase) adoptCancelling(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot) string {
	occurredAt := uc.clock.Now()
	dead, err := uc.liveness.Execute(ctx, taskID)
	if errors.Is(err, domain.ErrTaskNotFound) || (err == nil && dead) {
		uc.taskMu.Unlock(taskID)
		if err := uc.killed.ConfirmKilled(ctx, taskID, cancelledExitCode, true, occurredAt); err != nil {
			uc.logFailure("confirm cancelling task killed failed", taskID, err)
			return adoptionOutcomeError
		}
		return adoptionOutcomeReconciled
	}
	if err != nil {
		uc.taskMu.Unlock(taskID)
		uc.logFailure("check cancelling task liveness for adoption failed", taskID, err)
		uc.pending.Add(taskID, false)
		return adoptionOutcomeError
	}
	if snapshot.PID == nil {
		uc.taskMu.Unlock(taskID)
		uc.pending.Add(taskID, true)
		return adoptionOutcomeDeferred
	}
	pid := *snapshot.PID
	uc.taskMu.Unlock(taskID)
	go uc.confirmCancellationTermination(context.WithoutCancel(ctx), taskID, pid, occurredAt)
	return adoptionOutcomeDeferred
}

func (uc *AdoptRunningTasksUseCase) confirmCancellationTermination(ctx context.Context, taskID domain.TaskID, pid int, occurredAt time.Time) {
	dead, err := uc.termination.SendAndConfirm(ctx, taskID, pid, uc.grace)
	if err != nil {
		uc.logFailure("confirm cancelling task termination failed", taskID, err)
	}
	if err != nil || !dead {
		uc.pending.Add(taskID, true)
		return
	}
	if err := uc.killed.ConfirmKilled(ctx, taskID, cancelledExitCode, true, occurredAt); err != nil {
		uc.logFailure("confirm cancelling task killed after termination failed", taskID, err)
	}
}

func (uc *AdoptRunningTasksUseCase) recoverOrphan(ctx context.Context, taskID domain.TaskID, sessionRef *domain.SessionRef, occurredAt time.Time) string {
	present, err := uc.reader.ReadLastMessage(taskID)
	if err == nil && present {
		if err := uc.finalizer.Finalize(taskID, 0, true, true, occurredAt); err != nil {
			uc.logFailure("finalize adopted orphan failed", taskID, err)
			return adoptionOutcomeError
		}
		return adoptionOutcomeOrphanRecovered
	}
	if err != nil {
		uc.logFailure("read last message for adopted orphan failed", taskID, err)
	}
	if uc.resume == nil {
		uc.logFailure("resume recovery for adopted orphan is unavailable", taskID, nil)
		return adoptionOutcomeError
	}
	go uc.resumeRecovery(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: taskID, SessionRef: sessionRef, Origin: domain.RecoveryOriginOrphan, OccurredAt: occurredAt})
	return adoptionOutcomeOrphanRecoveryStarted
}

func (uc *AdoptRunningTasksUseCase) resumeRecovery(ctx context.Context, in RecoverViaResumeInput) {
	if _, err := uc.resume.Execute(ctx, in); err != nil {
		uc.logFailure("resume recovery for adopted task failed", in.TaskID, err)
	}
}

func (uc *AdoptRunningTasksUseCase) appendAdoptionEvents(taskID domain.TaskID, events []domain.Event) {
	for _, event := range events {
		if err := uc.contract.AppendEvent(taskID, event); err != nil {
			uc.logFailure("append adoption event failed", taskID, err)
		}
	}
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
