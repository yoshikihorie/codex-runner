package recovery

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
)

const (
	cancelledExitCode = 130

	// defaultRecoveryTerminationGrace has the same canonical value as
	// internal/execution.TimeoutKillGrace and TIMEOUT_KILL_GRACE_SECONDS in
	// validation-rules.md. It is duplicated in recovery to avoid an import cycle.
	defaultRecoveryTerminationGrace = 10 * time.Second
)

// ReconcilePendingUseCase periodically re-evaluates tasks whose adoption could
// not synchronously establish a terminal state.
type ReconcilePendingUseCase struct {
	pending          *PendingReconciliationSet
	tasks            AdoptionTaskStore
	liveness         LivenessChecker
	reader           reconcileContractReader
	writer           contract.ContractWriter
	finalizer        OrphanFinalizer
	termination      TerminationEnsurer
	killed           KillConfirmer
	pathLocks        PathLockReleaser
	resume           *RecoverViaResumeUseCase
	ownership        RecoveryOwnershipRegistry
	slots            SlotReleaser
	taskMu           TaskMutex
	clock            domain.Clock
	stalledTracker   stalledTimeTracker
	metricsRecorder  MetricsRecorder
	interval         time.Duration
	terminationGrace time.Duration
	logger           *slog.Logger
}

type reconcileContractReader interface {
	ContractReader
	ReadExitCode(domain.TaskID) (int, bool, error)
}

func NewReconcilePendingUseCase(pending *PendingReconciliationSet, tasks AdoptionTaskStore, liveness LivenessChecker, reader reconcileContractReader, writer contract.ContractWriter, finalizer OrphanFinalizer, termination TerminationEnsurer, killed KillConfirmer, pathLocks PathLockReleaser, resume *RecoverViaResumeUseCase, slots SlotReleaser, taskMu TaskMutex, clock domain.Clock, stalledTracker stalledTimeTracker, metricsRecorder MetricsRecorder, interval time.Duration, terminationGrace time.Duration, logger *slog.Logger) *ReconcilePendingUseCase {
	if isNilAdoptionDependency(pending) || isNilAdoptionDependency(tasks) || isNilAdoptionDependency(liveness) || isNilAdoptionDependency(reader) || isNilAdoptionDependency(writer) || isNilAdoptionDependency(finalizer) || isNilAdoptionDependency(termination) || isNilAdoptionDependency(killed) || isNilAdoptionDependency(pathLocks) || isNilAdoptionDependency(resume) || isNilAdoptionDependency(slots) || isNilAdoptionDependency(taskMu) || isNilAdoptionDependency(clock) || isNilAdoptionDependency(stalledTracker) || isNilAdoptionDependency(metricsRecorder) {
		panic("reconcile pending use case requires non-nil dependencies")
	}
	if interval <= 0 {
		panic("reconcile pending use case requires a positive interval")
	}
	if terminationGrace <= 0 {
		panic("reconcile pending use case requires a positive termination grace")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &ReconcilePendingUseCase{pending: pending, tasks: tasks, liveness: liveness, reader: reader, writer: writer, finalizer: finalizer, termination: termination, killed: killed, pathLocks: pathLocks, resume: resume, ownership: NewRecoveryOwnershipRegistry(), slots: slots, taskMu: taskMu, clock: clock, stalledTracker: stalledTracker, metricsRecorder: metricsRecorder, interval: interval, terminationGrace: terminationGrace, logger: logger}
}

// WithRecoveryOwnership wires the use case to the daemon-shared registry.
// Omitting it leaves each use case with an independent registry and disables
// cross-use-case recovery protection.
func (uc *ReconcilePendingUseCase) WithRecoveryOwnership(ownership RecoveryOwnershipRegistry) *ReconcilePendingUseCase {
	if isNilAdoptionDependency(ownership) {
		panic("reconcile pending use case requires a non-nil recovery ownership registry")
	}
	uc.ownership = ownership
	return uc
}

// Run processes a snapshot of the pending set on every interval until ctx ends.
func (uc *ReconcilePendingUseCase) Run(ctx context.Context) {
	ticker := time.NewTicker(uc.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			uc.reconcileTick(ctx)
		}
	}
}

func (uc *ReconcilePendingUseCase) reconcileTick(ctx context.Context) {
	for _, entry := range uc.pending.List() {
		if ctx.Err() != nil {
			return
		}
		uc.reconcileOne(ctx, entry)
	}
}

func (uc *ReconcilePendingUseCase) reconcileOne(ctx context.Context, entry PendingEntry) {
	taskID := entry.taskID
	if ctx.Err() != nil {
		return
	}
	uc.taskMu.Lock(taskID)
	snapshot, err := uc.tasks.Load(taskID)
	uc.taskMu.Unlock(taskID)
	if err != nil {
		uc.logFailure("load pending task failed", taskID, err)
		return
	}
	if ctx.Err() != nil {
		return
	}
	switch snapshot.State {
	case domain.StateRecovering:
		// Acquire, rather than IsOwned, decides exclusive recovery ownership.
		release, acquired := uc.ownership.Acquire(taskID)
		if !acquired {
			return
		}
		defer release()
		uc.reconcileRecovering(ctx, taskID)
	case domain.StateOrphaned:
		uc.reconcileOrphaned(ctx, taskID, snapshot)
	case domain.StateTimeout:
		uc.reconcileTermination(ctx, taskID, snapshot, true, entry.authority)
	case domain.StateCancelling:
		uc.reconcileTermination(ctx, taskID, snapshot, false, entry.authority)
	default:
		if isReconciliationTerminalState(snapshot.State) {
			uc.pending.Remove(taskID)
		} else {
			uc.logFailure("pending task has unsupported state", taskID, nil)
		}
	}
}

func (uc *ReconcilePendingUseCase) reconcileRecovering(ctx context.Context, taskID domain.TaskID) {
	if ctx.Err() != nil {
		return
	}
	dead, err := uc.liveness.Execute(ctx, taskID)
	if err != nil {
		uc.logFailure("check pending recovering task liveness failed", taskID, err)
		return
	}
	if !dead || ctx.Err() != nil {
		return
	}
	present, err := uc.reader.ReadLastMessage(taskID)
	if err != nil {
		uc.logFailure("read last message for pending recovering task failed", taskID, err)
		uc.registerConfirmOnly(taskID)
		return
	}
	if ctx.Err() != nil {
		return
	}
	occurredAt := uc.clock.Now()
	uc.taskMu.Lock(taskID)
	snapshot, err := uc.tasks.Load(taskID)
	if err == nil && snapshot.State == domain.StateRecovering {
		err = resolveRecoveringLocked(uc.tasks, uc.reader, uc.writer, uc.logger, taskID, snapshot, present, occurredAt)
	}
	uc.taskMu.Unlock(taskID)
	if err != nil {
		uc.logFailure("resolve pending recovering task failed", taskID, err)
		uc.reconcilePendingAfterConfirmedDeath(taskID)
		return
	}
	if snapshot.State != domain.StateRecovering {
		if isReconciliationTerminalState(snapshot.State) {
			uc.pending.Remove(taskID)
		}
		return
	}
	if ctx.Err() != nil {
		return
	}
	finalState := recoveringFinalState(present, snapshot)
	stalledTotal := uc.stalledTracker.TakeTotal(taskID)
	uc.metricsRecorder.Execute(ctx, metrics.RecordTaskMetricsInput{TaskID: taskID, FinalState: finalState, Estimated: true, OccurredAt: occurredAt, StalledTotalMs: stalledTotal})
	uc.slots.ReleaseAndAdvance(ctx, taskID, uc.clock.Now())
	uc.pending.Remove(taskID)
}

func (uc *ReconcilePendingUseCase) reconcileOrphaned(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot) {
	if ctx.Err() != nil {
		return
	}
	present, err := uc.reader.ReadLastMessage(taskID)
	if err != nil {
		uc.logFailure("read last message for pending orphaned task failed", taskID, err)
		uc.registerConfirmOnly(taskID)
		return
	}
	if ctx.Err() != nil {
		return
	}
	if present {
		if err := uc.finalizer.Finalize(taskID, 0, true, true, uc.clock.Now()); err != nil {
			uc.logFailure("finalize pending orphaned task failed", taskID, err)
			uc.reconcilePendingAfterConfirmedDeath(taskID)
			return
		}
		uc.pending.Remove(taskID)
		return
	}
	go uc.resumeRecovery(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: taskID, SessionRef: snapshot.SessionRef, Origin: domain.RecoveryOriginOrphan, OccurredAt: uc.clock.Now()}, SendClaim{})
}

func (uc *ReconcilePendingUseCase) reconcileTermination(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, timeout bool, authority ProcessSignalAuthority) {
	if ctx.Err() != nil {
		return
	}
	if validProcessSignalAuthority(authority) {
		claim, outcome := uc.pending.ClaimForSend(taskID, authority)
		switch outcome {
		case ClaimNotFound, ClaimAlreadyClaimed:
			return
		case ClaimAcquired:
			if ctx.Err() != nil {
				uc.pending.ReleaseSend(claim)
				return
			}
			go uc.sendAndReconcile(context.WithoutCancel(ctx), claim, snapshot, timeout)
			return
		case ClaimSent, ClaimConfirmOnly:
			// Confirm-only paths intentionally do not own a send claim.
		}
	}
	if ctx.Err() != nil {
		return
	}
	confirmedDead, err := uc.termination.Confirm(ctx, taskID)
	if err != nil {
		uc.logFailure("confirm pending task termination failed", taskID, err)
		uc.reconcilePendingAfterSnapshotFailure(taskID)
		return
	}
	if confirmedDead {
		uc.completeTerminated(ctx, taskID, snapshot, timeout, uc.clock.Now())
	}
}

func (uc *ReconcilePendingUseCase) sendAndReconcile(ctx context.Context, claim SendClaim, snapshot domain.TaskSnapshot, timeout bool) {
	result := uc.termination.SendAndConfirm(ctx, claim.TaskID, claim.Authority, uc.terminationGrace)
	if !result.Dead {
		if errors.Is(result.TerminateErr, ErrProcessSignalAuthorityInvalid) {
			uc.pending.InvalidateSend(claim)
			return
		}
		if result.TerminateErr == nil {
			uc.pending.CompleteSend(claim)
			return
		}
		releaseOrInvalidateSendAfterTerminationError(uc.tasks, uc.pending, uc.taskMu, uc.logger, claim)
		return
	}
	latest, valid := currentClaimAuthority(uc.tasks, uc.taskMu, claim)
	if !valid {
		uc.pending.InvalidateSend(claim)
		return
	}
	uc.completeTerminated(ctx, claim.TaskID, latest, timeout, uc.clock.Now(), claim)
}

func (uc *ReconcilePendingUseCase) completeTerminated(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, timeout bool, terminationConfirmedAt time.Time, claims ...SendClaim) {
	var claim SendClaim
	if len(claims) > 0 {
		claim = claims[0]
	}
	if ctx.Err() != nil {
		return
	}
	if timeout {
		if snapshot.Subcommand == domain.SubcommandImpl {
			if err := uc.pathLocks.Release(ctx, taskID); err != nil {
				uc.logFailure("release path lock before pending timeout recovery failed", taskID, err)
				if claim.Token != 0 {
					uc.pending.InvalidateSend(claim)
				} else {
					uc.reconcilePendingAfterConfirmedDeath(taskID)
				}
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		recoveryDispatchAt := uc.clock.Now()
		go uc.resumeRecovery(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: taskID, SessionRef: snapshot.SessionRef, Origin: domain.RecoveryOriginTimeout, OccurredAt: recoveryDispatchAt}, claim)
		return
	}
	rawExitCode := cancelledExitCode
	if existing, exists, err := uc.reader.ReadExitCode(taskID); err == nil && exists {
		rawExitCode = existing
	}
	if err := uc.killed.ConfirmKilled(ctx, taskID, rawExitCode, true, terminationConfirmedAt); err != nil {
		uc.logFailure("confirm pending cancelling task killed failed", taskID, err)
		if claim.Token != 0 {
			uc.pending.InvalidateSend(claim)
		} else {
			uc.reconcilePendingAfterConfirmedDeath(taskID)
		}
		return
	}
	if claim.Token != 0 {
		uc.pending.RemoveClaim(claim)
	} else {
		uc.pending.Remove(taskID)
	}
}

func (uc *ReconcilePendingUseCase) resumeRecovery(ctx context.Context, in RecoverViaResumeInput, claim SendClaim) {
	if _, err := uc.resume.Execute(ctx, in); err != nil {
		uc.logFailure("resume recovery for pending task failed", in.TaskID, err)
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

func (uc *ReconcilePendingUseCase) registerConfirmOnly(taskID domain.TaskID) {
	if err := uc.pending.Register(taskID, PendingSendConfirmOnly, nil); err != nil {
		uc.logFailure("register pending reconciliation failed", taskID, err)
	}
}

func (uc *ReconcilePendingUseCase) reconcilePendingAfterSnapshotFailure(taskID domain.TaskID) {
	reconcilePendingAfterFailure(uc.tasks, uc.pending, uc.taskMu, uc.logger, taskID, pendingFailureFromSnapshot)
}

func (uc *ReconcilePendingUseCase) reconcilePendingAfterConfirmedDeath(taskID domain.TaskID) {
	reconcilePendingAfterFailure(uc.tasks, uc.pending, uc.taskMu, uc.logger, taskID, pendingFailureAfterConfirmedDeath)
}

type pendingFailureDisposition uint8

const (
	pendingFailureFromSnapshot pendingFailureDisposition = iota
	pendingFailureAfterConfirmedDeath
)

func reconcilePendingAfterFailure(tasks AdoptionTaskStore, pending *PendingReconciliationSet, taskMu TaskMutex, logger *slog.Logger, taskID domain.TaskID, failureDisposition pendingFailureDisposition) {
	taskMu.Lock(taskID)
	snapshot, err := tasks.Load(taskID)
	taskMu.Unlock(taskID)
	if err != nil {
		logger.Error("reload task after recovery failure failed", "task_id", taskID.String(), "error", err)
		return
	}
	if isReconciliationTerminalState(snapshot.State) {
		pending.Remove(taskID)
		return
	}
	switch snapshot.State {
	case domain.StateTimeout, domain.StateCancelling, domain.StateRecovering, domain.StateOrphaned:
		disposition, authority := pendingFailureRegistration(snapshot, failureDisposition)
		if failureDisposition == pendingFailureAfterConfirmedDeath {
			// Register deliberately preserves a completed send, so replace that entry
			// before recording the stronger confirmed-death disposition.
			pending.Remove(taskID)
		}
		if err := pending.Register(taskID, disposition, authority); err != nil {
			logger.Error("register task after recovery failure failed", "task_id", taskID.String(), "error", err)
		}
	default:
		logger.Error("task after recovery failure has unsupported state", "task_id", taskID.String())
	}
}

func pendingFailureRegistration(snapshot domain.TaskSnapshot, failureDisposition pendingFailureDisposition) (PendingSendDisposition, *ProcessSignalAuthority) {
	if failureDisposition == pendingFailureAfterConfirmedDeath {
		return PendingSendConfirmOnly, nil
	}
	return pendingRegistration(snapshot)
}

func (uc *ReconcilePendingUseCase) logFailure(message string, taskID domain.TaskID, err error) {
	uc.logger.Error(message, "task_id", taskID.String(), "error", err)
}

func isReconciliationTerminalState(state domain.TaskState) bool {
	switch state {
	case domain.StateCompleted, domain.StateFailed, domain.StateRecovered, domain.StateTimeoutLost, domain.StateKilled, domain.StateLost:
		return true
	}
	return false
}
