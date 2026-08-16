package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
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
	Terminate(int, time.Duration) error
}
type CancelTaskMutex interface {
	Lock(domain.TaskID)
	Unlock(domain.TaskID)
}
type stalledTimeTracker interface {
	LeaveStalled(domain.TaskID, time.Time) int
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

type CancelTaskUseCase struct {
	tasks           CancelTaskStore
	queue           CancelTaskQueue
	queueMu         *sync.Mutex
	taskMu          CancelTaskMutex
	events          CancelEventAppender
	terminator      CancelProcessTerminator
	timeoutDisarmer execution.TimeoutDisarmer
	confirmer       *execution.ConfirmTaskKilledUseCase
	stalledTracker  stalledTimeTracker
	clock           domain.Clock
	logger          *slog.Logger
}

func NewCancelTaskUseCase(tasks CancelTaskStore, queue CancelTaskQueue, queueMu *sync.Mutex, taskMu CancelTaskMutex, events CancelEventAppender, terminator CancelProcessTerminator, disarmer execution.TimeoutDisarmer, confirmer *execution.ConfirmTaskKilledUseCase, stalledTracker stalledTimeTracker, clock domain.Clock, loggers ...*slog.Logger) *CancelTaskUseCase {
	if isNilStatusUseCaseDependency(tasks) || isNilStatusUseCaseDependency(queue) || isNilStatusUseCaseDependency(queueMu) || isNilStatusUseCaseDependency(taskMu) || isNilStatusUseCaseDependency(events) || isNilStatusUseCaseDependency(terminator) || isNilStatusUseCaseDependency(disarmer) || isNilStatusUseCaseDependency(confirmer) || isNilStatusUseCaseDependency(stalledTracker) || isNilStatusUseCaseDependency(clock) {
		panic("cancel task use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("cancel task use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &CancelTaskUseCase{tasks: tasks, queue: queue, queueMu: queueMu, taskMu: taskMu, events: events, terminator: terminator, timeoutDisarmer: disarmer, confirmer: confirmer, stalledTracker: stalledTracker, clock: clock, logger: logger}
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
		return CancelTaskOutput{}, fmt.Errorf("%w: reservation", domain.ErrContractWriteFailed)
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
		return CancelTaskOutput{}, fmt.Errorf("%w: task.json", domain.ErrContractWriteFailed)
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
	snapshot, err := uc.tasks.Load(in.TaskID)
	if err != nil {
		if errors.Is(err, domain.ErrTaskNotFound) {
			reserved, checkErr := uc.tasks.IsReserved(in.TaskID)
			if checkErr != nil {
				return out, fmt.Errorf("%w: reservation", domain.ErrContractWriteFailed)
			}
			if reserved {
				return out, errCancelStateChanged
			}
		}
		return out, err
	}
	previous = snapshot.State
	if snapshot.PID != nil {
		value := *snapshot.PID
		pid = &value
	}
	task, err := snapshot.Restore()
	if err != nil {
		return out, err
	}
	events, err := task.RequestCancel(in.Force, in.OccurredAt)
	if err != nil {
		return out, err
	}
	updated, err := snapshot.WithTask(task, in.OccurredAt)
	if err != nil {
		return out, err
	}
	if err := uc.tasks.Save(in.TaskID, updated); err != nil {
		return out, fmt.Errorf("%w: task.json", domain.ErrContractWriteFailed)
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
	if previous == domain.StateOrphaned || (previous == domain.StateAdopted && pid == nil) {
		if previous == domain.StateAdopted {
			out.TerminationTriggered = true
		}
		_, err = uc.confirmer.Execute(ctx, execution.ConfirmTaskKilledInput{TaskID: in.TaskID, RawExitCode: 130, Estimated: true, OccurredAt: in.OccurredAt})
		return out, err
	}
	if pid != nil && ((previous == domain.StateStarting) || previous == domain.StateRunning || previous == domain.StateStalled || previous == domain.StateAdopted) {
		out.TerminationTriggered = true
		if terminateErr := uc.terminator.Terminate(*pid, execution.TimeoutKillGrace); terminateErr != nil {
			uc.logger.Warn("terminate cancelled task", "task_id", in.TaskID.String(), "error", terminateErr)
		}
		if previous != domain.StateStarting {
			uc.timeoutDisarmer.Disarm(in.TaskID)
		}
	}
	return out, nil
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
