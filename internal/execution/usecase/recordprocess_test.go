package usecase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type recordProcessTaskStoreFake struct {
	store.TaskStore
	snapshot         domain.TaskSnapshot
	loadErr, saveErr error
	loads, saves     int
	trace            *[]string
}

func (f *recordProcessTaskStoreFake) Load(_ domain.TaskID) (domain.TaskSnapshot, error) {
	f.loads++
	*f.trace = append(*f.trace, "load")
	return f.snapshot, f.loadErr
}
func (f *recordProcessTaskStoreFake) Save(_ domain.TaskID, snapshot domain.TaskSnapshot) error {
	f.saves++
	f.snapshot = snapshot
	*f.trace = append(*f.trace, "save")
	return f.saveErr
}

type recordProcessContractWriterFake struct {
	contract.ContractWriter
	appendErr error
	events    []domain.Event
	trace     *[]string
}

func (f *recordProcessContractWriterFake) AppendEvent(_ domain.TaskID, event domain.Event) error {
	f.events = append(f.events, event)
	*f.trace = append(*f.trace, "event")
	return f.appendErr
}

func recordProcessTask(t *testing.T) (*domain.Task, domain.TaskSnapshot) {
	t.Helper()
	task := recordStartingTask(t)
	now := time.Date(2026, time.August, 10, 12, 0, 0, 0, time.UTC)
	if _, err := task.Start(recordStartingTimeout(t, nil), "gpt-5", now); err != nil {
		t.Fatal(err)
	}
	snapshot, err := domain.NewInitialTaskSnapshot(domain.ExecutionRouteDaemon, nil).WithTask(task, now)
	if err != nil {
		t.Fatal(err)
	}
	return task, snapshot
}

func TestRecordTaskProcessUseCase_WritesPIDThenAppendsTaskStartedEvent(t *testing.T) {
	trace := []string{}
	task, snapshot := recordProcessTask(t)
	tasks := &recordProcessTaskStoreFake{snapshot: snapshot, trace: &trace}
	writer := &recordProcessContractWriterFake{trace: &trace}
	started := time.Date(2026, time.August, 10, 12, 1, 0, 0, time.UTC)
	if err := NewRecordTaskProcessUseCase(tasks, writer).Execute(context.Background(), task, &domain.ProcessHandle{PID: 42, ProcessStartedAt: started}, started); err != nil {
		t.Fatal(err)
	}
	if tasks.snapshot.PID == nil || *tasks.snapshot.PID != 42 || tasks.snapshot.ProcessStartedAt == nil || !tasks.snapshot.ProcessStartedAt.Equal(started) || tasks.snapshot.State != domain.StateStarting || len(writer.events) != 1 || writer.events[0].Type() != "TaskStarted" || fmtTrace(trace) != "[load save event]" {
		t.Fatalf("trace=%v snapshot=%#v events=%v", trace, tasks.snapshot, writer.events)
	}
}
func TestRecordTaskProcessUseCase_EventAppendFailureDoesNotFailExecute(t *testing.T) {
	trace := []string{}
	task, snapshot := recordProcessTask(t)
	tasks := &recordProcessTaskStoreFake{snapshot: snapshot, trace: &trace}
	writer := &recordProcessContractWriterFake{trace: &trace, appendErr: errors.New("append")}
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	if err := NewRecordTaskProcessUseCase(tasks, writer, logger).Execute(context.Background(), task, &domain.ProcessHandle{PID: 42, ProcessStartedAt: time.Now()}, time.Now()); err != nil || !bytes.Contains(logs.Bytes(), []byte("append event failed")) {
		t.Fatalf("err=%v logs=%s", err, logs.String())
	}
}
func TestRecordTaskProcessUseCase_TaskJSONWriteFailureReturnsContractWriteFailed(t *testing.T) {
	trace := []string{}
	task, snapshot := recordProcessTask(t)
	tasks := &recordProcessTaskStoreFake{snapshot: snapshot, trace: &trace, saveErr: errors.New("save")}
	writer := &recordProcessContractWriterFake{trace: &trace}
	err := NewRecordTaskProcessUseCase(tasks, writer).Execute(context.Background(), task, &domain.ProcessHandle{PID: 42, ProcessStartedAt: time.Now()}, time.Now())
	if !errors.Is(err, domain.ErrContractWriteFailed) {
		t.Fatalf("err=%v", err)
	}
}
func TestRecordTaskProcessUseCase_NonStartingTaskRejected(t *testing.T) {
	trace := []string{}
	task, snapshot := recordProcessTask(t)
	snapshot.State = domain.StateFailed
	snapshot.PID, snapshot.ProcessStartedAt = nil, nil
	tasks := &recordProcessTaskStoreFake{snapshot: snapshot, trace: &trace}
	writer := &recordProcessContractWriterFake{trace: &trace}
	err := NewRecordTaskProcessUseCase(tasks, writer).Execute(context.Background(), task, &domain.ProcessHandle{PID: 42, ProcessStartedAt: time.Now()}, time.Now())
	if !errors.Is(err, domain.ErrInvalidStateTransition) || tasks.saves != 0 {
		t.Fatalf("err=%v saves=%d", err, tasks.saves)
	}
}

func TestRecordTaskProcessUseCase_StaleLoadedSnapshotRejectsOverwrite(t *testing.T) {
	trace := []string{}
	task, snapshot := recordProcessTask(t)
	snapshot.State = domain.StateFailed
	snapshot.PID, snapshot.ProcessStartedAt = nil, nil
	tasks := &recordProcessTaskStoreFake{snapshot: snapshot, trace: &trace}
	writer := &recordProcessContractWriterFake{trace: &trace}
	err := NewRecordTaskProcessUseCase(tasks, writer).Execute(context.Background(), task, &domain.ProcessHandle{PID: 42, ProcessStartedAt: time.Now()}, time.Now())
	if !errors.Is(err, domain.ErrInvalidStateTransition) || tasks.saves != 0 || len(writer.events) != 0 {
		t.Fatalf("err=%v saves=%d events=%d", err, tasks.saves, len(writer.events))
	}
}
func TestRecordTaskProcessUseCase_TaskLoadFailureIsNotWrappedAsContractWriteFailed(t *testing.T) {
	trace := []string{}
	task, _ := recordProcessTask(t)
	tasks := &recordProcessTaskStoreFake{trace: &trace, loadErr: domain.ErrTaskNotFound}
	writer := &recordProcessContractWriterFake{trace: &trace}
	err := NewRecordTaskProcessUseCase(tasks, writer).Execute(context.Background(), task, &domain.ProcessHandle{PID: 42, ProcessStartedAt: time.Now()}, time.Now())
	if !errors.Is(err, domain.ErrTaskNotFound) || errors.Is(err, domain.ErrContractWriteFailed) {
		t.Fatalf("err=%v", err)
	}
}
func TestRecordTaskProcessUseCase_WithTaskFailureIsNotWrappedAsContractWriteFailed(t *testing.T) {
	trace := []string{}
	task, snapshot := recordProcessTask(t)
	snapshot.Route = domain.ExecutionRouteLegacy
	tasks := &recordProcessTaskStoreFake{snapshot: snapshot, trace: &trace}
	writer := &recordProcessContractWriterFake{trace: &trace}
	err := NewRecordTaskProcessUseCase(tasks, writer).Execute(context.Background(), task, &domain.ProcessHandle{PID: 42, ProcessStartedAt: time.Now()}, time.Now())
	if err == nil || errors.Is(err, domain.ErrContractWriteFailed) || tasks.saves != 0 {
		t.Fatalf("err=%v saves=%d", err, tasks.saves)
	}
}
func TestRecordTaskProcessUseCase_RejectsInvalidInput(t *testing.T) {
	for _, tc := range []struct {
		name   string
		task   *domain.Task
		handle *domain.ProcessHandle
		now    time.Time
	}{{"nil task", nil, &domain.ProcessHandle{PID: 42, ProcessStartedAt: time.Now()}, time.Now()}, {"nil handle", recordStartingTask(t), nil, time.Now()}, {"zero now", recordStartingTask(t), &domain.ProcessHandle{PID: 42, ProcessStartedAt: time.Now()}, time.Time{}}} {
		t.Run(tc.name, func(t *testing.T) {
			trace := []string{}
			tasks := &recordProcessTaskStoreFake{trace: &trace}
			writer := &recordProcessContractWriterFake{trace: &trace}
			if NewRecordTaskProcessUseCase(tasks, writer).Execute(context.Background(), tc.task, tc.handle, tc.now) == nil || tasks.loads != 0 || tasks.saves != 0 || len(writer.events) != 0 {
				t.Fatal("invalid input was accepted")
			}
		})
	}
}

func TestRecordTaskProcessUseCase_RejectsNilTask(t *testing.T) {
	assertRecordProcessInvalidInput(t, nil, &domain.ProcessHandle{PID: 42, ProcessStartedAt: time.Now()}, time.Now())
}

func TestRecordTaskProcessUseCase_RejectsNilHandle(t *testing.T) {
	assertRecordProcessInvalidInput(t, recordStartingTask(t), nil, time.Now())
}

func TestRecordTaskProcessUseCase_RejectsZeroNow(t *testing.T) {
	assertRecordProcessInvalidInput(t, recordStartingTask(t), &domain.ProcessHandle{PID: 42, ProcessStartedAt: time.Now()}, time.Time{})
}

func assertRecordProcessInvalidInput(t *testing.T, task *domain.Task, handle *domain.ProcessHandle, now time.Time) {
	t.Helper()
	trace := []string{}
	tasks := &recordProcessTaskStoreFake{trace: &trace}
	writer := &recordProcessContractWriterFake{trace: &trace}
	if NewRecordTaskProcessUseCase(tasks, writer).Execute(context.Background(), task, handle, now) == nil || tasks.loads != 0 || tasks.saves != 0 || len(writer.events) != 0 {
		t.Fatal("invalid input was accepted")
	}
}
func fmtTrace(trace []string) string { return fmt.Sprint(trace) }
