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

func NewReconcilePendingUseCase(pending *PendingReconciliationSet, tasks AdoptionTaskStore, liveness LivenessChecker, reader ContractReader, writer contract.ContractWriter, termination TerminationEnsurer, killed KillConfirmer, pathLocks PathLockReleaser, resume *RecoverViaResumeUseCase, slots SlotReleaser, taskMu TaskMutex, clock domain.Clock, interval time.Duration, terminationGrace time.Duration, logger *slog.Logger) *ReconcilePendingUseCase {
	if pending == nil || tasks == nil || liveness == nil || reader == nil || writer == nil || termination == nil || killed == nil || pathLocks == nil || resume == nil || slots == nil || taskMu == nil || clock == nil {
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
	return &ReconcilePendingUseCase{pending: pending, tasks: tasks, liveness: liveness, reader: reader, writer: writer, termination: termination, killed: killed, pathLocks: pathLocks, resume: resume, slots: slots, taskMu: taskMu, clock: clock, interval: interval, terminationGrace: terminationGrace, logger: logger}
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
			for _, entry := range uc.pending.List() {
				uc.reconcileOne(ctx, entry.taskID)
			}
		}
	}
}

func (uc *ReconcilePendingUseCase) reconcileOne(ctx context.Context, taskID domain.TaskID) {
	snapshot, err := uc.tasks.Load(taskID)
	if err != nil {
		uc.logFailure("load pending task failed", taskID, err)
		return
	}

	switch snapshot.State {
	case domain.StateRecovering:
		uc.reconcileRecovering(ctx, taskID)
	case domain.StateTimeout:
		uc.reconcileTermination(ctx, taskID, snapshot, true)
	case domain.StateCancelling:
		uc.reconcileTermination(ctx, taskID, snapshot, false)
	case domain.StateCompleted, domain.StateFailed, domain.StateRecovered, domain.StateTimeoutLost, domain.StateKilled, domain.StateLost:
		uc.pending.Remove(taskID)
	}
}

func (uc *ReconcilePendingUseCase) reconcileRecovering(ctx context.Context, taskID domain.TaskID) {
	dead, err := uc.liveness.Execute(ctx, taskID)
	if err != nil {
		uc.logFailure("check pending recovering task liveness failed", taskID, err)
		return
	}
	if !dead {
		return
	}
	present, err := uc.reader.ReadLastMessage(taskID)
	if err != nil {
		uc.logFailure("read last message for pending recovering task failed", taskID, err)
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
		return
	}
	if snapshot.State != domain.StateRecovering {
		if isReconciliationTerminalState(snapshot.State) {
			uc.pending.Remove(taskID)
		}
		return
	}
	uc.slots.ReleaseAndAdvance(ctx, taskID, uc.clock.Now())
	uc.pending.Remove(taskID)
}

func (uc *ReconcilePendingUseCase) reconcileTermination(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, timeout bool) {
	claimed, found := uc.pending.ClaimForSend(taskID)
	if !found {
		return
	}
	if claimed && snapshot.PID != nil {
		go uc.sendAndReconcile(context.WithoutCancel(ctx), taskID, snapshot, timeout)
		return
	}

	confirmedDead := false
	if !claimed || snapshot.PID == nil {
		var err error
		confirmedDead, err = uc.termination.Confirm(ctx, taskID)
		if err != nil {
			uc.logFailure("confirm pending task termination failed", taskID, err)
			return
		}
	}
	if confirmedDead {
		uc.completeTerminated(ctx, taskID, snapshot, timeout)
	}
}

func (uc *ReconcilePendingUseCase) sendAndReconcile(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, timeout bool) {
	dead, err := uc.termination.SendAndConfirm(ctx, taskID, *snapshot.PID, uc.terminationGrace)
	if err != nil {
		uc.logFailure("confirm pending task termination after signal failed", taskID, err)
		return
	}
	if dead {
		uc.completeTerminated(ctx, taskID, snapshot, timeout)
	}
}

func (uc *ReconcilePendingUseCase) completeTerminated(ctx context.Context, taskID domain.TaskID, snapshot domain.TaskSnapshot, timeout bool) {
	if timeout {
		if snapshot.Subcommand == domain.SubcommandImpl {
			if err := uc.pathLocks.Release(ctx, taskID); err != nil {
				uc.logFailure("release path lock before pending timeout recovery failed", taskID, err)
				return
			}
		}
		go uc.resumeRecovery(context.WithoutCancel(ctx), RecoverViaResumeInput{TaskID: taskID, SessionRef: snapshot.SessionRef, Origin: domain.RecoveryOriginTimeout, OccurredAt: uc.clock.Now()})
		return
	}

	if err := uc.killed.ConfirmKilled(ctx, taskID, cancelledExitCode, true, uc.clock.Now()); err != nil {
		uc.logFailure("confirm pending cancelling task killed failed", taskID, err)
		return
	}
	uc.pending.Remove(taskID)
}

func (uc *ReconcilePendingUseCase) resumeRecovery(ctx context.Context, in RecoverViaResumeInput) {
	if _, err := uc.resume.Execute(ctx, in); err != nil {
		uc.logFailure("resume recovery for pending timeout task failed", in.TaskID, err)
	}
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
