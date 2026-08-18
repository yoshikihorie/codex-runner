package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

type CancelTaskStore interface {
	Load(domain.TaskID) (domain.TaskSnapshot, error)
	Save(domain.TaskID, domain.TaskSnapshot) error
	IsReserved(domain.TaskID) (bool, error)
}
type CancelTaskQueue interface {
	Remove(domain.TaskID, time.Time) (execution.TaskLaunchPayload, int, bool, []domain.Event)
	Restore(execution.TaskLaunchPayload, int, time.Time) []domain.Event
}
type CancelEventAppender interface {
	AppendEvent(domain.TaskID, domain.Event) error
}
type CancelProcessTerminator interface {
	SendTerminate(int) error
	SendKill(int) error
}
type cancelTerminationEnsurer interface {
	Confirm(context.Context, domain.TaskID) (bool, error)
	SendAndConfirm(context.Context, domain.TaskID, recovery.ProcessSignalAuthority, time.Duration) recovery.TerminationAttemptResult
}
type CancelTaskMutex interface {
	Lock(domain.TaskID)
	Unlock(domain.TaskID)
}
type stalledTimeTracker interface {
	LeaveStalled(domain.TaskID, time.Time) int
}
type cancelLifecycleOwnership interface {
	Current(domain.TaskID) (domain.LifecycleGeneration, bool)
}

type CancelTaskInput struct {
	TaskID     domain.TaskID
	Force      bool
	OccurredAt time.Time
}
type CancelTaskOutput struct {
	State                domain.TaskState
	Events               []domain.Event
	TerminationTriggered bool
}

var errCancelStateChanged = errors.New("cancel state changed")

type cancelStateError struct {
	state domain.TaskState
	cause error
}

func (e cancelStateError) Error() string { return e.cause.Error() }

func (e cancelStateError) Unwrap() error { return e.cause }

type CancelTaskUseCase struct {
	tasks            CancelTaskStore
	queue            CancelTaskQueue
	queueMu          *sync.Mutex
	taskMu           CancelTaskMutex
	events           CancelEventAppender
	terminator       CancelProcessTerminator
	termination      cancelTerminationEnsurer
	pendingRegistrar recovery.PendingRegistrar
	timeoutDisarmer  execution.TimeoutDisarmer
	confirmer        *execution.ConfirmTaskKilledUseCase
	stalledTracker   stalledTimeTracker
	ownership        cancelLifecycleOwnership
	clock            domain.Clock
	logger           *slog.Logger
}

func NewCancelTaskUseCase(tasks CancelTaskStore, queue CancelTaskQueue, queueMu *sync.Mutex, taskMu CancelTaskMutex, events CancelEventAppender, terminator CancelProcessTerminator, termination cancelTerminationEnsurer, pendingRegistrar recovery.PendingRegistrar, disarmer execution.TimeoutDisarmer, confirmer *execution.ConfirmTaskKilledUseCase, stalledTracker stalledTimeTracker, ownership cancelLifecycleOwnership, clock domain.Clock, options ...any) *CancelTaskUseCase {
	if isNilStatusUseCaseDependency(tasks) || isNilStatusUseCaseDependency(queue) || isNilStatusUseCaseDependency(queueMu) || isNilStatusUseCaseDependency(taskMu) || isNilStatusUseCaseDependency(events) || isNilStatusUseCaseDependency(terminator) || isNilStatusUseCaseDependency(termination) || isNilStatusUseCaseDependency(pendingRegistrar) || isNilStatusUseCaseDependency(disarmer) || isNilStatusUseCaseDependency(confirmer) || isNilStatusUseCaseDependency(stalledTracker) || isNilStatusUseCaseDependency(ownership) || isNilStatusUseCaseDependency(clock) {
		panic("cancel task use case requires non-nil dependencies")
	}
	logger := slog.Default()
	for _, option := range options {
		switch value := option.(type) {
		case *slog.Logger:
			if value != nil {
				logger = value
			}
		default:
			panic("cancel task use case received unsupported option")
		}
	}
	return &CancelTaskUseCase{tasks: tasks, queue: queue, queueMu: queueMu, taskMu: taskMu, events: events, terminator: terminator, termination: termination, pendingRegistrar: pendingRegistrar, timeoutDisarmer: disarmer, confirmer: confirmer, stalledTracker: stalledTracker, ownership: ownership, clock: clock, logger: logger}
}

func (uc *CancelTaskUseCase) Execute(ctx context.Context, in CancelTaskInput) (CancelTaskOutput, error) {
	uc.queueMu.Lock()
	queueLocked := true
	defer func() {
		if queueLocked {
			uc.queueMu.Unlock()
		}
	}()
	payload, index, removed, _ := uc.queue.Remove(in.TaskID, in.OccurredAt)
	if removed {
		out, err := uc.cancelQueued(in, payload, index)
		if err != nil {
			return CancelTaskOutput{}, err
		}
		uc.queueMu.Unlock()
		queueLocked = false
		_, err = uc.confirmer.Execute(ctx, execution.ConfirmTaskKilledInput{TaskID: in.TaskID, RawExitCode: 130, Estimated: true, OccurredAt: in.OccurredAt})
		return out, err
	}
	uc.queueMu.Unlock()
	queueLocked = false
	reserved, err := uc.tasks.IsReserved(in.TaskID)
	if err != nil {
		return CancelTaskOutput{}, contractWriteError(err)
	}
	if !reserved {
		return CancelTaskOutput{}, domain.ErrTaskNotFound
	}
	return uc.cancelPersisted(ctx, in)
}

func (uc *CancelTaskUseCase) cancelQueued(in CancelTaskInput, payload execution.TaskLaunchPayload, index int) (out CancelTaskOutput, err error) {
	committed := false
	defer func() {
		if !committed {
			uc.queue.Restore(payload, index, in.OccurredAt)
		}
	}()
	if payload.Task == nil || payload.Task.State() != domain.StateQueued {
		panic("removed queue payload must be queued")
	}
	candidate := *payload.Task
	events, err := candidate.RequestCancel(in.Force, in.OccurredAt)
	if err != nil {
		return CancelTaskOutput{}, err
	}
	snapshot, err := domain.NewTaskSnapshotFromAdmission(&candidate, payload.ResolvedTimeout, payload.Model, payload.ReasoningEffort, domain.ExecutionRouteDaemon, in.OccurredAt)
	if err != nil {
		return CancelTaskOutput{}, err
	}
	if err := uc.tasks.Save(in.TaskID, snapshot); err != nil {
		return CancelTaskOutput{}, contractWriteError(err)
	}
	committed = true
	if err := uc.events.AppendEvent(in.TaskID, events[0]); err != nil {
		uc.logger.Warn("append cancel event failed (retained cancelling state)", "task_id", in.TaskID.String(), "error", err)
	}
	out = CancelTaskOutput{State: domain.StateCancelling, Events: events}
	return out, nil
}

func (uc *CancelTaskUseCase) cancelPersisted(ctx context.Context, in CancelTaskInput) (out CancelTaskOutput, err error) {
	uc.taskMu.Lock(in.TaskID)
	locked := true
	defer func() {
		if locked {
			uc.taskMu.Unlock(in.TaskID)
		}
	}()
	var previous domain.TaskState
	var pid *int
	var processStartedAt *time.Time
	var generation *domain.LifecycleGeneration
	snapshot, err := uc.tasks.Load(in.TaskID)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			reserved, checkErr := uc.tasks.IsReserved(in.TaskID)
			if checkErr != nil {
				return out, contractWriteError(checkErr)
			}
			if reserved {
				return out, errCancelStateChanged
			}
			return out, domain.ErrTaskNotFound
		}
		return out, contractWriteError(err)
	}
	previous = snapshot.State
	if cancelNeedsGeneration(snapshot) {
		current, owned := uc.ownership.Current(in.TaskID)
		if !owned {
			return CancelTaskOutput{State: domain.StateCancelling}, errCancelStateChanged
		}
		generation = &current
	}
	if snapshot.PID != nil {
		value := *snapshot.PID
		pid = &value
	}
	if snapshot.ProcessStartedAt != nil {
		value := *snapshot.ProcessStartedAt
		processStartedAt = &value
	}
	if previous == domain.StateStarting && pid != nil {
		if generation == nil || processStartedAt == nil {
			return CancelTaskOutput{State: domain.StateCancelling}, errCancelStateChanged
		}
		authority := recovery.ProcessSignalAuthority{TaskID: in.TaskID, PID: *pid, ProcessStartedAt: *processStartedAt, ExpectedState: domain.StateCancelling, LifecycleGeneration: generation}
		uc.taskMu.Unlock(in.TaskID)
		locked = false
		claim, outcome := uc.pendingRegistrar.ClaimInitialSend(in.TaskID, authority)
		return uc.finishStartingClaim(ctx, in, previous, pid, processStartedAt, *generation, authority, claim, outcome)
	}
	task, err := snapshot.Restore()
	if err != nil {
		return out, err
	}
	events, err := task.RequestCancel(in.Force, in.OccurredAt)
	if err != nil {
		return out, cancelStateError{state: previous, cause: err}
	}
	updated, err := snapshot.WithTask(task, in.OccurredAt)
	if err != nil {
		return out, err
	}
	if err := uc.tasks.Save(in.TaskID, updated); err != nil {
		return out, contractWriteError(err)
	}
	if previous == domain.StateStalled {
		uc.stalledTracker.LeaveStalled(in.TaskID, in.OccurredAt)
	}
	if err := uc.events.AppendEvent(in.TaskID, events[0]); err != nil {
		uc.logger.Warn("append cancel event failed (retained cancelling state)", "task_id", in.TaskID.String(), "error", err)
	}
	out = CancelTaskOutput{State: domain.StateCancelling, Events: events}
	uc.taskMu.Unlock(in.TaskID)
	locked = false
	if previous == domain.StateOrphaned {
		_, err = uc.confirmer.Execute(ctx, execution.ConfirmTaskKilledInput{TaskID: in.TaskID, RawExitCode: 130, Estimated: true, OccurredAt: in.OccurredAt})
		return out, err
	}
	pidlessAdopted := pid == nil &&
		(previous == domain.StateAdopted ||
			(snapshot.AdoptedAfterRestart &&
				(previous == domain.StateRunning || previous == domain.StateStalled)))
	if pidlessAdopted {
		if err := uc.pendingRegistrar.Register(in.TaskID, recovery.PendingSendConfirmOnly, nil); err != nil {
			return out, contractWriteError(err)
		}
		uc.timeoutDisarmer.Disarm(in.TaskID)
		return out, nil
	}
	if pid != nil && ((previous == domain.StateStarting) || previous == domain.StateRunning || previous == domain.StateStalled || previous == domain.StateAdopted) {
		if generation == nil && !snapshot.AdoptedAfterRestart {
			return out, nil
		}
		out.TerminationTriggered = true
		if previous != domain.StateStarting && processStartedAt != nil {
			authority := recovery.ProcessSignalAuthority{TaskID: in.TaskID, PID: *pid, ProcessStartedAt: *processStartedAt, ExpectedState: domain.StateCancelling, LifecycleGeneration: generation}
			result := uc.termination.SendAndConfirm(ctx, in.TaskID, authority, execution.TimeoutKillGrace)
			if result.TerminateErr != nil {
				uc.logger.Warn("terminate cancelled task", "task_id", in.TaskID.String(), "error", result.TerminateErr)
			}
			if result.ConfirmErr != nil {
				uc.logger.Warn("confirm cancelled task termination", "task_id", in.TaskID.String(), "error", result.ConfirmErr)
			}
			if result.Dead {
				_, confirmErr := uc.confirmer.Execute(ctx, execution.ConfirmTaskKilledInput{TaskID: in.TaskID, RawExitCode: 130, Estimated: true, OccurredAt: uc.clock.Now()})
				if confirmErr == nil {
					return out, nil
				}
				if registerErr := uc.pendingRegistrar.Register(in.TaskID, recovery.PendingSendConfirmOnly, nil); registerErr != nil {
					return out, errors.Join(confirmErr, contractWriteError(registerErr))
				}
				uc.timeoutDisarmer.Disarm(in.TaskID)
				return out, confirmErr
			}
			var disposition recovery.PendingSendDisposition
			var pendingAuthority *recovery.ProcessSignalAuthority
			if result.TerminateErr == nil {
				disposition = recovery.PendingSendSent
			} else if errors.Is(result.TerminateErr, recovery.ErrProcessSignalAuthorityInvalid) {
				disposition = recovery.PendingSendConfirmOnly
			} else {
				disposition, pendingAuthority = cancelPendingRegistration(in.TaskID, pid, processStartedAt, generation)
			}
			if registerErr := uc.pendingRegistrar.Register(in.TaskID, disposition, pendingAuthority); registerErr != nil {
				registerErr = contractWriteError(registerErr)
				if result.TerminateErr != nil {
					return out, errors.Join(result.TerminateErr, registerErr)
				}
				return out, registerErr
			}
			uc.timeoutDisarmer.Disarm(in.TaskID)
			return out, nil
		} else {
			terminateErr := uc.terminator.SendTerminate(*pid)
			if terminateErr != nil {
				uc.logger.Warn("terminate cancelled task", "task_id", in.TaskID.String(), "error", terminateErr)
				disposition, authority := cancelPendingRegistration(in.TaskID, pid, processStartedAt, generation)
				if registerErr := uc.pendingRegistrar.Register(in.TaskID, disposition, authority); registerErr != nil {
					return out, errors.Join(terminateErr, contractWriteError(registerErr))
				}
			}
			if previous == domain.StateStarting {
				return out, nil
			}
		}
	}
	return out, nil
}

func cancelNeedsGeneration(snapshot domain.TaskSnapshot) bool {
	if snapshot.AdoptedAfterRestart {
		return false
	}
	return snapshot.State == domain.StateStarting || snapshot.State == domain.StateRunning || snapshot.State == domain.StateStalled || snapshot.State == domain.StateAdopted
}

func (uc *CancelTaskUseCase) finishStartingClaim(ctx context.Context, in CancelTaskInput, previous domain.TaskState, pid *int, processStartedAt *time.Time, generation domain.LifecycleGeneration, authority recovery.ProcessSignalAuthority, claim recovery.SendClaim, outcome recovery.ClaimOutcome) (CancelTaskOutput, error) {
	switch outcome {
	case recovery.ClaimAcquired:
		uc.taskMu.Lock(in.TaskID)
		snapshot, err := uc.tasks.Load(in.TaskID)
		if err != nil {
			uc.taskMu.Unlock(in.TaskID)
			uc.pendingRegistrar.RemoveClaim(claim)
			return CancelTaskOutput{}, contractWriteError(err)
		}
		current, owned := uc.ownership.Current(in.TaskID)
		matchingCancellation := snapshot.State == domain.StateCancelling && sameCancelProcess(snapshot, pid, processStartedAt) && owned && current == generation
		if snapshot.State != previous && !matchingCancellation || !sameCancelProcess(snapshot, pid, processStartedAt) || !owned || current != generation {
			uc.taskMu.Unlock(in.TaskID)
			uc.pendingRegistrar.RemoveClaim(claim)
			return CancelTaskOutput{State: domain.StateCancelling}, nil
		}
		out := CancelTaskOutput{State: domain.StateCancelling}
		if snapshot.State == previous {
			task, restoreErr := snapshot.Restore()
			if restoreErr != nil {
				uc.taskMu.Unlock(in.TaskID)
				uc.pendingRegistrar.RemoveClaim(claim)
				return CancelTaskOutput{}, restoreErr
			}
			events, requestErr := task.RequestCancel(in.Force, in.OccurredAt)
			if requestErr != nil {
				uc.taskMu.Unlock(in.TaskID)
				uc.pendingRegistrar.RemoveClaim(claim)
				return CancelTaskOutput{}, cancelStateError{state: previous, cause: requestErr}
			}
			updated, updateErr := snapshot.WithTask(task, in.OccurredAt)
			if updateErr != nil {
				uc.taskMu.Unlock(in.TaskID)
				uc.pendingRegistrar.RemoveClaim(claim)
				return CancelTaskOutput{}, updateErr
			}
			if saveErr := uc.tasks.Save(in.TaskID, updated); saveErr != nil {
				uc.taskMu.Unlock(in.TaskID)
				uc.pendingRegistrar.RemoveClaim(claim)
				return CancelTaskOutput{}, contractWriteError(saveErr)
			}
			out.Events = events
			if err := uc.events.AppendEvent(in.TaskID, events[0]); err != nil {
				uc.logger.Warn("append cancel event failed (retained cancelling state)", "task_id", in.TaskID.String(), "error", err)
			}
		}
		uc.taskMu.Unlock(in.TaskID)
		return uc.sendClaimedStarting(ctx, in, authority, claim, out)
	case recovery.ClaimSent:
		dead, err := uc.termination.Confirm(ctx, in.TaskID)
		if err != nil {
			uc.logger.Warn("confirm cancelled task termination", "task_id", in.TaskID.String(), "error", err)
		}
		if dead {
			_, err = uc.confirmer.Execute(ctx, execution.ConfirmTaskKilledInput{TaskID: in.TaskID, RawExitCode: 130, Estimated: true, OccurredAt: uc.clock.Now()})
		}
		return CancelTaskOutput{State: domain.StateCancelling}, err
	case recovery.ClaimAlreadyClaimed, recovery.ClaimConfirmOnly, recovery.ClaimNotFound:
		return CancelTaskOutput{State: domain.StateCancelling}, nil
	default:
		return CancelTaskOutput{State: domain.StateCancelling}, nil
	}
}

func sameCancelProcess(snapshot domain.TaskSnapshot, pid *int, processStartedAt *time.Time) bool {
	return pid != nil && processStartedAt != nil && snapshot.PID != nil && snapshot.ProcessStartedAt != nil && *snapshot.PID == *pid && snapshot.ProcessStartedAt.Equal(*processStartedAt)
}

func (uc *CancelTaskUseCase) sendClaimedStarting(ctx context.Context, in CancelTaskInput, authority recovery.ProcessSignalAuthority, claim recovery.SendClaim, out CancelTaskOutput) (CancelTaskOutput, error) {
	out.TerminationTriggered = true
	if err := uc.terminator.SendTerminate(authority.PID); err == nil {
		uc.pendingRegistrar.CompleteSend(claim)
	} else if errors.Is(err, recovery.ErrProcessSignalAuthorityInvalid) {
		uc.pendingRegistrar.InvalidateSend(claim)
	} else {
		uc.pendingRegistrar.ReleaseSend(claim)
		uc.logger.Warn("terminate cancelled task", "task_id", in.TaskID.String(), "error", err)
	}
	return out, nil
}

func contractWriteError(cause error) error {
	return errors.Join(domain.ErrContractWriteFailed, cause)
}

func cancelPendingRegistration(taskID domain.TaskID, pid *int, processStartedAt *time.Time, generation *domain.LifecycleGeneration) (recovery.PendingSendDisposition, *recovery.ProcessSignalAuthority) {
	if pid == nil || *pid <= 0 || processStartedAt == nil || processStartedAt.IsZero() {
		return recovery.PendingSendConfirmOnly, nil
	}
	var generationCopy *domain.LifecycleGeneration
	if generation != nil {
		value := *generation
		generationCopy = &value
	}
	authority := recovery.ProcessSignalAuthority{TaskID: taskID, PID: *pid, ProcessStartedAt: *processStartedAt, ExpectedState: domain.StateCancelling, LifecycleGeneration: generationCopy}
	return recovery.PendingSendUnsent, &authority
}

func (uc *CancelTaskUseCase) Handle(req transport.Request) transport.Response {
	id, err := domain.NewTaskID(req.TaskID)
	if err != nil {
		return cancelErrorResponse(req.RequestID, "TASK_ID_INVALID_FORMAT", "error.task.idInvalidFormat", nil)
	}
	force := false
	if len(req.Params) != 0 {
		decoder := json.NewDecoder(bytes.NewReader(req.Params))
		var raw map[string]json.RawMessage
		if err := decoder.Decode(&raw); err != nil || raw == nil {
			return cancelErrorResponse(req.RequestID, "CANCEL_PARAMS_MALFORMED", "error.cancel.paramsMalformed", nil)
		}
		if decoder.Decode(&struct{}{}) != io.EOF {
			return cancelErrorResponse(req.RequestID, "CANCEL_PARAMS_MALFORMED", "error.cancel.paramsMalformed", nil)
		}
		for key := range raw {
			if key != "force" {
				return cancelErrorResponse(req.RequestID, "CANCEL_PARAMS_MALFORMED", "error.cancel.paramsMalformed", nil)
			}
		}
		if forceRaw, ok := raw["force"]; ok {
			if bytes.Equal(bytes.TrimSpace(forceRaw), []byte("null")) || json.Unmarshal(forceRaw, &force) != nil {
				return cancelErrorResponse(req.RequestID, "CANCEL_PARAMS_MALFORMED", "error.cancel.paramsMalformed", nil)
			}
		}
	}
	out, err := uc.Execute(context.Background(), CancelTaskInput{TaskID: id, Force: force, OccurredAt: uc.clock.Now()})
	if err != nil {
		return uc.cancelMappedError(req.RequestID, id, err)
	}
	body, err := json.Marshal(struct {
		TaskID     string           `json:"task_id"`
		State      domain.TaskState `json:"state"`
		MessageKey string           `json:"message_key"`
	}{id.String(), out.State, "status.task.cancelling"})
	if err != nil {
		panic(err)
	}
	return transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: req.RequestID, OK: true, Result: body}
}

func (uc *CancelTaskUseCase) cancelMappedError(requestID string, id domain.TaskID, err error) transport.Response {
	detail := map[string]any{"task_id": id.String()}
	switch {
	case errors.Is(err, domain.ErrTaskNotFound):
		return cancelErrorResponse(requestID, "TASK_NOT_FOUND", "error.task.notFound", detail)
	case errors.Is(err, domain.ErrTaskAlreadyTerminal):
		var stateErr cancelStateError
		if errors.As(err, &stateErr) {
			detail["state"] = stateErr.state
		}
		return cancelErrorResponse(requestID, "TASK_ALREADY_TERMINAL", "error.task.alreadyTerminal", detail)
	case errors.Is(err, domain.ErrInvalidStateTransition):
		return cancelErrorResponse(requestID, "TASK_INVALID_TRANSITION", "error.task.invalidTransition", detail)
	case errors.Is(err, errCancelStateChanged):
		return cancelErrorResponse(requestID, "CANCEL_STATE_CHANGED", "error.cancel.stateChanged", detail)
	default:
		return cancelErrorResponse(requestID, "CONTRACT_WRITE_FAILED", "error.contract.writeFailed", detail)
	}
}
func cancelErrorResponse(requestID, code, message string, detail map[string]any) transport.Response {
	return transport.Response{ProtocolVersion: transport.ProtocolVersion, RequestID: requestID, OK: false, Error: &transport.ErrorBody{Code: code, MessageKey: message, Detail: detail}}
}
