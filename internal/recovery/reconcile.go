package recovery

import (
	"context"
	"log/slog"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
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
	reader           ContractReader
	writer           contract.ContractWriter
	finalizer        OrphanFinalizer
	termination      TerminationEnsurer
	killed           KillConfirmer
	pathLocks        PathLockReleaser
	resume           *RecoverViaResumeUseCase
	slots            SlotReleaser
	taskMu           TaskMutex
	clock            domain.Clock
	interval         time.Duration
	terminationGrace time.Duration
	logger           *slog.Logger
}

func NewReconcilePendingUseCase(pending *PendingReconciliationSet, tasks AdoptionTaskStore, liveness LivenessChecker, reader ContractReader, writer contract.ContractWriter, finalizer OrphanFinalizer, termination TerminationEnsurer, killed KillConfirmer, pathLocks PathLockReleaser, resume *RecoverViaResumeUseCase, slots SlotReleaser, taskMu TaskMutex, clock domain.Clock, interval time.Duration, terminationGrace time.Duration, logger *slog.Logger) *ReconcilePendingUseCase {
	if pending == nil || tasks == nil || liveness == nil || reader == nil || writer == nil || finalizer == nil || termination == nil || killed == nil || pathLocks == nil || resume == nil || slots == nil || taskMu == nil || clock == nil {
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
	return &ReconcilePendingUseCase{pending: pending, tasks: tasks, liveness: liveness, reader: reader, writer: writer, finalizer: finalizer, termination: termination, killed: killed, pathLocks: pathLocks, resume: resume, slots: slots, taskMu: taskMu, clock: clock, interval: interval, terminationGrace: terminationGrace, logger: logger}
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
		uc.reconcileOne(ctx, entry.taskID)
	}
}

func (uc *ReconcilePendingUseCase) reconcileOne(ctx context.Context, taskID domain.TaskID) {
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
		uc.reconcileRecovering(ctx, taskID)
	case domain.StateOrphaned:
		uc.reconcileOrphaned(ctx, taskID, snapshot)
	case domain.StateTimeout:
		uc.reconcileTermination(ctx, taskID, snapshot, true)
	case domain.StateCancelling:
		uc.reconcileTermination(ctx, taskID, snapshot, false)
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
	uc.taskMu.Lock(taskID)
	snapshot, err := uc.tasks.Load(taskID)
	if err == nil && snapshot.State == domain.StateRecovering {
		err = resolveRecoveringLocked(uc.tasks, uc.writer, uc.logger, taskID, snapshot, present, uc.clock.Now())
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
	go uc.resumeRecovery(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: taskID, SessionRef: snapshot.SessionRef, Origin: domain.RecoveryOriginOrphan, OccurredAt: uc.clock.Now()})
}

func (uc *ReconcilePendingUseCase) reconcileTermination(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, timeout bool) {
	if ctx.Err() != nil {
		return
	}
	authority, hasAuthority := processSignalAuthority(snapshot)
	if hasAuthority {
		claim, found := uc.pending.ClaimForSend(taskID, authority)
		if !found {
			return
		}
		if claim.Token != 0 {
			if ctx.Err() != nil {
				uc.pending.ReleaseSend(claim)
				return
			}
			go uc.sendAndReconcile(context.WithoutCancel(ctx), claim, snapshot, timeout)
			return
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
	dead, err := uc.termination.SendAndConfirm(ctx, claim.TaskID, claim.Authority.PID, uc.terminationGrace)
	if err != nil {
		uc.logFailure("confirm pending task termination after signal failed", claim.TaskID, err)
		uc.pending.ReleaseSend(claim)
		return
	}
	if !dead {
		uc.pending.ReleaseSend(claim)
		return
	}
	latest, valid := currentClaimAuthority(uc.tasks, uc.taskMu, claim)
	if !valid {
		uc.pending.InvalidateSend(claim)
		return
	}
	uc.pending.CompleteSend(claim)
	uc.completeTerminated(ctx, claim.TaskID, latest, timeout, uc.clock.Now())
}

func (uc *ReconcilePendingUseCase) completeTerminated(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, timeout bool, terminationConfirmedAt time.Time) {
	if ctx.Err() != nil {
		return
	}
	if timeout {
		if snapshot.Subcommand == domain.SubcommandImpl {
			if err := uc.pathLocks.Release(ctx, taskID); err != nil {
				uc.logFailure("release path lock before pending timeout recovery failed", taskID, err)
				uc.reconcilePendingAfterConfirmedDeath(taskID)
				return
			}
		}
		if ctx.Err() != nil {
			return
		}
		recoveryDispatchAt := uc.clock.Now()
		uc.pending.Remove(taskID)
		go uc.resumeRecovery(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: taskID, SessionRef: snapshot.SessionRef, Origin: domain.RecoveryOriginTimeout, OccurredAt: recoveryDispatchAt})
		return
	}
	if err := uc.killed.ConfirmKilled(ctx, taskID, cancelledExitCode, true, terminationConfirmedAt); err != nil {
		uc.logFailure("confirm pending cancelling task killed failed", taskID, err)
		uc.reconcilePendingAfterConfirmedDeath(taskID)
		return
	}
	uc.pending.Remove(taskID)
}

func (uc *ReconcilePendingUseCase) resumeRecovery(ctx context.Context, in RecoverViaResumeInput) {
	if _, err := uc.resume.Execute(ctx, in); err != nil {
		uc.logFailure("resume recovery for pending task failed", in.TaskID, err)
		uc.reconcilePendingAfterConfirmedDeath(in.TaskID)
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
