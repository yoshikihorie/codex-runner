package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/recovery"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type killedStoreFake struct {
	snapshot domain.TaskSnapshot
	saveErr  error
	saves    int
	trace    *[]string
}

func (f *killedStoreFake) Load(domain.TaskID) (domain.TaskSnapshot, error) { return f.snapshot, nil }
func (f *killedStoreFake) Save(_ domain.TaskID, snapshot domain.TaskSnapshot) error {
	f.saves++
	if f.trace != nil {
		*f.trace = append(*f.trace, "task.json")
	}
	if f.saveErr == nil {
		f.snapshot = snapshot
	}
	return f.saveErr
}
func (*killedStoreFake) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	return nil, nil
}
func (*killedStoreFake) Reserve(domain.TaskID) error            { return nil }
func (*killedStoreFake) Release(domain.TaskID) error            { return nil }
func (*killedStoreFake) IsReserved(domain.TaskID) (bool, error) { return true, nil }

type killedReaderFake struct {
	code   int
	exists bool
	err    error
}

func (*killedReaderFake) ReadStderrLog(domain.TaskID) ([]byte, error)          { return nil, nil }
func (*killedReaderFake) ReadLastMessage(domain.TaskID) (bool, error)          { return false, nil }
func (*killedReaderFake) ReadPromptContent(domain.TaskID) ([]byte, error)      { return nil, nil }
func (*killedReaderFake) ReadLastMessageContent(domain.TaskID) ([]byte, error) { return nil, nil }
func (*killedReaderFake) ReadPartialOutputContent(domain.TaskID) ([]byte, error) {
	return nil, nil
}
func (f *killedReaderFake) ReadExitCode(domain.TaskID) (int, bool, error) {
	return f.code, f.exists, f.err
}

type killedWriterFake struct {
	contract.ContractWriter
	exitErr, appendErr error
	calls              []string
	exits              []domain.ExitCode
	events             []domain.Event
	trace              *[]string
}

func (f *killedWriterFake) WriteExitCode(_ domain.TaskID, code domain.ExitCode) error {
	f.calls = append(f.calls, "exit-code")
	if f.trace != nil {
		*f.trace = append(*f.trace, "exit-code")
	}
	f.exits = append(f.exits, code)
	return f.exitErr
}
func (f *killedWriterFake) AppendEvent(_ domain.TaskID, event domain.Event) error {
	f.calls = append(f.calls, "event")
	if f.trace != nil {
		*f.trace = append(*f.trace, "event")
	}
	f.events = append(f.events, event)
	return f.appendErr
}

type killedDisarmerFake struct{ calls int }

func (f *killedDisarmerFake) Disarm(domain.TaskID) { f.calls++ }

type killedSlotFake struct {
	calls         int
	beforeRelease func()
}

func (f *killedSlotFake) ReleaseAndAdvance(context.Context, domain.TaskID, time.Time) {
	if f.beforeRelease != nil {
		f.beforeRelease()
	}
	f.calls++
}

var _ recovery.SlotReleaser = (*killedSlotFake)(nil)

type killedPathStoreFake struct {
	deleteErr error
	calls     int
}

func (*killedPathStoreFake) List() ([]PathLockSnapshot, error)                 { return nil, nil }
func (*killedPathStoreFake) Save(domain.TaskID, []domain.NormalizedPath) error { return nil }
func (f *killedPathStoreFake) Delete(domain.TaskID) error                      { f.calls++; return f.deleteErr }

func killedSnapshot(t *testing.T) domain.TaskSnapshot {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260811-120000-a1b2-killed")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 11, 12, 0, 0, 0, time.UTC)
	return domain.TaskSnapshot{TaskID: id, Subcommand: domain.SubcommandImpl, ResolvedTimeoutSeconds: 1800, Model: "gpt-5", RequestedAt: at, Route: domain.ExecutionRouteDaemon, State: domain.StateCancelling, StateUpdatedAt: at, SchemaVersion: 1}
}
func killedFixture(t *testing.T) (*killedStoreFake, *killedReaderFake, *killedWriterFake, *killedDisarmerFake, *killedSlotFake, *killedPathStoreFake, *ConfirmTaskKilledUseCase) {
	t.Helper()
	snapshot := killedSnapshot(t)
	tasks := &killedStoreFake{snapshot: snapshot}
	reader := &killedReaderFake{}
	writer := &killedWriterFake{}
	disarmer := &killedDisarmerFake{}
	slots := &killedSlotFake{}
	paths := &killedPathStoreFake{}
	uc := NewConfirmTaskKilledUseCase(tasks, writer, reader, store.NewTaskMutex(), disarmer, NewReleasePathLockUseCase(paths), slots, domain.ClockFunc(func() time.Time { return snapshot.StateUpdatedAt }))
	return tasks, reader, writer, disarmer, slots, paths, uc
}

func TestConfirmTaskKilledTypesAreAvailable(t *testing.T) {
	var output ConfirmTaskKilledOutput
	if output.Events != nil {
		t.Fatal("zero output must not create events")
	}
}

func TestConfirmTaskKilledExecute_ReturnsOneTaskKilledEvent(t *testing.T) {
	tasks, _, writer, disarmer, slots, _, uc := killedFixture(t)
	at := tasks.snapshot.StateUpdatedAt.Add(time.Second)
	out, err := uc.Execute(context.Background(), ConfirmTaskKilledInput{TaskID: tasks.snapshot.TaskID, RawExitCode: 130, Estimated: true, OccurredAt: at})
	if err != nil || len(out.Events) != 1 || disarmer.calls != 1 || slots.calls != 1 {
		t.Fatalf("out=%#v err=%v", out, err)
	}
	event, ok := out.Events[0].(domain.TaskKilled)
	if !ok || event.ExitCode.Raw() != 130 || !event.Estimated || !event.OccurredAt.Equal(at) || tasks.snapshot.State != domain.StateKilled || len(writer.exits) != 1 {
		t.Fatalf("event=%#v snapshot=%#v", out.Events[0], tasks.snapshot)
	}
}

func TestConfirmTaskKilledExecute_EventAppendFailureStillReturnsTaskKilledEvent(t *testing.T) {
	tasks, _, writer, _, _, _, uc := killedFixture(t)
	writer.appendErr = errors.New("append")
	out, err := uc.Execute(context.Background(), ConfirmTaskKilledInput{TaskID: tasks.snapshot.TaskID, RawExitCode: 130, OccurredAt: tasks.snapshot.StateUpdatedAt})
	if err != nil || len(out.Events) != 1 || out.Events[0].Type() != "TaskKilled" {
		t.Fatalf("out=%#v err=%v", out, err)
	}
}

func TestConfirmTaskKilledExecuteLocked_ExitCodeRulesAndConfirmedResult(t *testing.T) {
	for _, tc := range []struct {
		name                              string
		reader                            killedReaderFake
		writeErr                          error
		wantWrite, wantConfirmed, wantErr bool
	}{
		{"missing writes once", killedReaderFake{}, nil, true, true, false},
		{"same code idempotent", killedReaderFake{code: 130, exists: true}, nil, false, true, false},
		{"different code fails closed", killedReaderFake{code: 1, exists: true}, nil, false, true, true},
		{"read failure fatal", killedReaderFake{err: errors.New("read")}, nil, false, true, true},
		{"write failure preserves confirmed", killedReaderFake{}, errors.New("write"), true, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tasks, reader, writer, _, _, _, uc := killedFixture(t)
			*reader = tc.reader
			writer.exitErr = tc.writeErr
			result, err := uc.ExecuteLocked(context.Background(), ConfirmTaskKilledInput{TaskID: tasks.snapshot.TaskID, RawExitCode: 130, OccurredAt: tasks.snapshot.StateUpdatedAt})
			if result.Confirmed != tc.wantConfirmed || len(writer.exits) != map[bool]int{true: 1, false: 0}[tc.wantWrite] || (err != nil) != tc.wantErr {
				t.Fatalf("result=%#v err=%v writes=%v", result, err, writer.exits)
			}
		})
	}
}

func TestConfirmTaskKilledExecute_DoubleConfirmationDoesNotOverwriteKilled(t *testing.T) {
	tasks, _, writer, _, _, _, uc := killedFixture(t)
	in := ConfirmTaskKilledInput{TaskID: tasks.snapshot.TaskID, RawExitCode: 130, OccurredAt: tasks.snapshot.StateUpdatedAt}
	if _, err := uc.Execute(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.Execute(context.Background(), in); !errors.Is(err, domain.ErrInvalidStateTransition) || len(writer.exits) != 1 {
		t.Fatalf("err=%v writes=%v", err, writer.exits)
	}
}

func TestConfirmTaskKilledExecute_SaveFailureRetainsConfirmedResultAndReleases(t *testing.T) {
	tasks, _, _, disarmer, slots, paths, uc := killedFixture(t)
	tasks.saveErr = errors.New("save")
	out, err := uc.Execute(context.Background(), ConfirmTaskKilledInput{TaskID: tasks.snapshot.TaskID, RawExitCode: 130, Estimated: true, OccurredAt: tasks.snapshot.StateUpdatedAt})
	if !errors.Is(err, domain.ErrContractWriteFailed) || len(out.Events) != 1 || tasks.saves != 1 || tasks.snapshot.State != domain.StateCancelling || disarmer.calls != 1 || paths.calls != 1 || slots.calls != 1 {
		t.Fatalf("out=%#v err=%v saves=%d snapshot=%#v disarms=%d paths=%d slots=%d", out, err, tasks.saves, tasks.snapshot, disarmer.calls, paths.calls, slots.calls)
	}
}

func TestConfirmTaskKilledExecute_PersistsExitCodeBeforeTaskAndEvent(t *testing.T) {
	tasks, _, writer, _, _, _, uc := killedFixture(t)
	trace := []string{}
	tasks.trace, writer.trace = &trace, &trace
	_, err := uc.Execute(context.Background(), ConfirmTaskKilledInput{TaskID: tasks.snapshot.TaskID, RawExitCode: 130, OccurredAt: tasks.snapshot.StateUpdatedAt})
	if err != nil || len(trace) != 3 || trace[0] != "exit-code" || trace[1] != "task.json" || trace[2] != "event" {
		t.Fatalf("err=%v persistence order=%v", err, trace)
	}
}

func TestConfirmTaskKilledExecute_PathReleaseFailureStillReleasesSlot(t *testing.T) {
	tasks, _, _, disarmer, slots, paths, uc := killedFixture(t)
	paths.deleteErr = errors.New("delete path lock")
	_, err := uc.Execute(context.Background(), ConfirmTaskKilledInput{TaskID: tasks.snapshot.TaskID, RawExitCode: 130, OccurredAt: tasks.snapshot.StateUpdatedAt})
	if err != nil || tasks.snapshot.State != domain.StateKilled || disarmer.calls != 1 || paths.calls != 1 || slots.calls != 1 {
		t.Fatalf("err=%v snapshot=%#v disarms=%d paths=%d slots=%d", err, tasks.snapshot, disarmer.calls, paths.calls, slots.calls)
	}
}

func TestConfirmTaskKilledExecute_SaveFailureUnlocksBeforeReleasingSlot(t *testing.T) {
	snapshot := killedSnapshot(t)
	tasks := &killedStoreFake{snapshot: snapshot, saveErr: errors.New("save")}
	reader := &killedReaderFake{}
	writer := &killedWriterFake{}
	disarmer := &killedDisarmerFake{}
	paths := &killedPathStoreFake{}
	taskMu := store.NewTaskMutex()
	slots := &killedSlotFake{}
	slots.beforeRelease = func() {
		taskMu.Lock(snapshot.TaskID)
		taskMu.Unlock(snapshot.TaskID)
	}
	uc := NewConfirmTaskKilledUseCase(tasks, writer, reader, taskMu, disarmer, NewReleasePathLockUseCase(paths), slots, domain.ClockFunc(func() time.Time { return snapshot.StateUpdatedAt }))

	completed := make(chan error, 1)
	go func() {
		_, err := uc.Execute(context.Background(), ConfirmTaskKilledInput{TaskID: snapshot.TaskID, RawExitCode: 130, OccurredAt: snapshot.StateUpdatedAt})
		completed <- err
	}()
	select {
	case err := <-completed:
		if !errors.Is(err, domain.ErrContractWriteFailed) || slots.calls != 1 {
			t.Fatalf("err=%v slots=%d", err, slots.calls)
		}
	case <-time.After(time.Second):
		t.Fatal("confirmation did not unlock task mutex before releasing the slot")
	}
}
