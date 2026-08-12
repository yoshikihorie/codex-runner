package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

const (
	taskInvalidTransitionCode = "TASK_INVALID_TRANSITION"
	taskStateReadFailedCode   = "TASK_STATE_READ_FAILED"
	contractWriteFailedCode   = "CONTRACT_WRITE_FAILED"
)

type taskLocker interface {
	Lock(domain.TaskID)
	Unlock(domain.TaskID)
}

var _ taskLocker = (*store.TaskMutex)(nil)

type MonitorTaskEventsUseCase struct {
	monitor  execution.EventMonitor
	tasks    store.TaskStore
	taskMu   taskLocker
	contract contract.ContractWriter
	clock    domain.Clock
	logger   *slog.Logger
}

func NewMonitorTaskEventsUseCase(monitor execution.EventMonitor, tasks store.TaskStore, taskMu taskLocker, contractWriter contract.ContractWriter, clock domain.Clock, loggers ...*slog.Logger) *MonitorTaskEventsUseCase {
	if isNilValue(monitor) || isNilValue(tasks) || isNilValue(taskMu) || isNilValue(contractWriter) || isNilValue(clock) {
		panic("monitor task events use case requires non-nil dependencies")
	}
	if len(loggers) > 1 {
		panic("monitor task events use case accepts at most one logger")
	}
	logger := slog.Default()
	if len(loggers) == 1 && loggers[0] != nil {
		logger = loggers[0]
	}
	return &MonitorTaskEventsUseCase{monitor: monitor, tasks: tasks, taskMu: taskMu, contract: contractWriter, clock: clock, logger: logger}
}

func (u *MonitorTaskEventsUseCase) Run(ctx context.Context, taskID domain.TaskID, stdout io.Reader) error {
	if isNilValue(ctx) || isNilValue(stdout) {
		return errors.New("monitor task events use case requires non-nil context and stdout")
	}
	return u.monitor.Observe(ctx, stdout,
		func(typ string, raw json.RawMessage) { u.observeKnown(taskID, typ, raw) },
		func(typ string, raw json.RawMessage) { u.appendRaw(taskID, typ, raw) },
	)
}

func (u *MonitorTaskEventsUseCase) observeKnown(taskID domain.TaskID, typ string, raw json.RawMessage) {
	u.taskMu.Lock(taskID)
	defer u.taskMu.Unlock(taskID)
	snapshot, err := u.tasks.Load(taskID)
	if err != nil {
		u.logStateRead(taskID, "load", err)
		return
	}
	task, err := snapshot.Restore()
	if err != nil {
		u.logStateRead(taskID, "restore", err)
		return
	}
	now := u.clock.Now()
	events, err := task.ObserveEvent(typ, now)
	if err != nil {
		if errors.Is(err, domain.ErrInvalidStateTransition) {
			logInvalidTransition(u.logger, taskID, "observe-event", task.State(), err)
		}
		u.appendRaw(taskID, typ, raw)
		return
	}
	updated, err := snapshot.WithTask(task, now)
	if err != nil {
		u.logger.Warn("snapshot validation failed after task update", "task_id", taskID.String(), "operation", "with-task", "error", err.Error())
		u.appendRaw(taskID, typ, raw)
		return
	}
	saved := u.tasks.Save(taskID, updated)
	if saved != nil {
		u.logContract(taskID, "save-task", "", saved)
	}
	u.appendRaw(taskID, typ, raw)
	if saved == nil {
		for _, event := range events {
			u.appendEvent(taskID, event)
		}
	}
}

func isNilValue(value any) bool {
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

func (u *MonitorTaskEventsUseCase) appendRaw(taskID domain.TaskID, typ string, raw json.RawMessage) {
	if err := u.contract.AppendRawEvent(taskID, typ, raw); err != nil {
		u.logContract(taskID, "append-raw-event", typ, err)
	}
}
func (u *MonitorTaskEventsUseCase) appendEvent(taskID domain.TaskID, event domain.Event) {
	if err := u.contract.AppendEvent(taskID, event); err != nil {
		u.logContract(taskID, "append-event", event.Type(), err)
	}
}
func (u *MonitorTaskEventsUseCase) logStateRead(taskID domain.TaskID, operation string, err error) {
	u.logger.Warn("task state read failed", "code", taskStateReadFailedCode, "task_id", taskID.String(), "operation", operation, "error", err)
}
func (u *MonitorTaskEventsUseCase) logContract(taskID domain.TaskID, operation, eventType string, err error) {
	u.logger.Warn("contract write failed", "code", contractWriteFailedCode, "task_id", taskID.String(), "operation", operation, "event_type", eventType, "error", err)
}
func logInvalidTransition(logger *slog.Logger, taskID domain.TaskID, operation string, state domain.TaskState, err error) {
	logger.Warn("task invalid transition", "code", taskInvalidTransitionCode, "task_id", taskID.String(), "operation", operation, "state", string(state), "error", err)
}
