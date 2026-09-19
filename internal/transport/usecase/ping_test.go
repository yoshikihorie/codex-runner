package usecase

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

func newPingUseCaseForTest(t *testing.T, failedTaskSnapshots int) *PingUseCase {
	t.Helper()
	useCase, err := NewPingUseCase(failedTaskSnapshots)
	if err != nil {
		t.Fatal(err)
	}
	return useCase
}

func TestPingUseCaseExecute(t *testing.T) {
	useCase, err := NewPingUseCase(0)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	for _, ctx := range []context.Context{context.Background(), cancelled} {
		got, err := useCase.Execute(ctx)
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		want := PingResult{ProtocolVersion: transport.ProtocolVersion, FailedTaskSnapshots: 0}
		if got != want {
			t.Fatalf("Execute() = %#v, want %#v", got, want)
		}
	}
}

func TestNewPingUseCaseValidatesFailedTaskSnapshots(t *testing.T) {
	if useCase, err := NewPingUseCase(-1); err == nil || useCase != nil {
		t.Fatalf("NewPingUseCase(-1) = %#v, %v", useCase, err)
	}
	for _, count := range []int{0, 2} {
		useCase, err := NewPingUseCase(count)
		if err != nil {
			t.Fatalf("NewPingUseCase(%d): %v", count, err)
		}
		result, err := useCase.Execute(context.Background())
		if err != nil || result.FailedTaskSnapshots != count {
			t.Fatalf("Execute() = %#v, %v; want failed snapshots %d", result, err, count)
		}
	}
}

func TestPingUseCaseDoesNotReferenceQueue(t *testing.T) {
	got, err := newPingUseCaseForTest(t, 0).Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got.ProtocolVersion != transport.ProtocolVersion {
		t.Fatalf("Execute() = %#v", got)
	}
}

func TestPingUseCaseHandleIsIdempotent(t *testing.T) {
	useCase, err := NewPingUseCase(2)
	if err != nil {
		t.Fatal(err)
	}
	req := transport.Request{RequestID: "r-7", Verb: "ping"}
	var first PingResult

	for i := 0; i < 10; i++ {
		resp := useCase.Handle(req)
		var got PingResult
		if err := json.Unmarshal(resp.Result, &got); err != nil {
			t.Fatalf("response %d result: %v", i, err)
		}
		if i == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("response %d = %#v, want %#v", i, got, first)
		}
	}
}

func TestPingUseCaseKeepsStartupFailureCountAfterSnapshotRepair_SCNDaemon0142(t *testing.T) {
	root := t.TempDir()
	id, err := domain.NewTaskID("impl-20260919-120000-a1b2-scn42-ping")
	if err != nil {
		t.Fatal(err)
	}
	taskDir := filepath.Join(root, id.String())
	if err := os.Mkdir(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	snapshotPath := filepath.Join(taskDir, "task.json")
	if err := os.WriteFile(snapshotPath, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	taskStore, err := store.NewFileTaskStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if corrupted := taskStore.CorruptedTaskIDs(); len(corrupted) != 1 || corrupted[0] != id {
		t.Fatalf("CorruptedTaskIDs() = %v, want [%s]", corrupted, id)
	}
	useCase := newPingUseCaseForTest(t, len(taskStore.CorruptedTaskIDs()))
	before, err := useCase.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if before.FailedTaskSnapshots != 1 {
		t.Fatalf("startup failed_task_snapshots = %d, want 1", before.FailedTaskSnapshots)
	}

	at := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	repaired := domain.TaskSnapshot{
		TaskID: id, Subcommand: domain.SubcommandImpl, ResolvedTimeoutSeconds: 1920,
		Model: "gpt-5", SandboxMode: "workspace-write", RequestedAt: at,
		Route: domain.ExecutionRouteDaemon, State: domain.StateStarting, StateUpdatedAt: at,
		SchemaVersion: 2,
	}
	if err := repaired.Validate(); err != nil {
		t.Fatalf("repaired snapshot is invalid: %v", err)
	}
	body, err := json.Marshal(repaired)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapshotPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := taskStore.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State != domain.StateStarting {
		t.Fatalf("Load() state = %q, want starting", loaded.State)
	}

	after, err := useCase.Execute(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if after.FailedTaskSnapshots != 1 {
		t.Fatalf("repaired failed_task_snapshots = %d, want startup value 1", after.FailedTaskSnapshots)
	}
}

func TestPingResultJSON(t *testing.T) {
	body, err := json.Marshal(PingResult{ProtocolVersion: transport.ProtocolVersion, FailedTaskSnapshots: 0})
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"protocol_version":"1","failed_task_snapshots":0}` {
		t.Fatalf("Marshal(PingResult) = %s", body)
	}
}

func TestPingHandleSuccess(t *testing.T) {
	useCase, err := NewPingUseCase(2)
	if err != nil {
		t.Fatal(err)
	}
	resp := useCase.Handle(transport.Request{RequestID: "r-1", Verb: "ping"})
	var result PingResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatal(err)
	}

	if !resp.OK || resp.RequestID != "r-1" || resp.ProtocolVersion != transport.ProtocolVersion {
		t.Fatalf("response = %#v", resp)
	}
	if result.ProtocolVersion != transport.ProtocolVersion || result.ProtocolVersion != resp.ProtocolVersion {
		t.Fatalf("result = %#v, response protocol version = %q", result, resp.ProtocolVersion)
	}
	if result.FailedTaskSnapshots != 2 {
		t.Fatalf("result = %#v, want failed snapshots 2", result)
	}
}

func TestPingHandleIgnoresTaskIDAndParams(t *testing.T) {
	useCase := newPingUseCaseForTest(t, 0)
	withExtras := useCase.Handle(transport.Request{
		RequestID: "r-2",
		Verb:      "ping",
		TaskID:    "impl-example",
		Params:    json.RawMessage(`{"x":1}`),
	})
	withoutExtras := useCase.Handle(transport.Request{RequestID: "r-2", Verb: "ping"})

	if !withExtras.OK || !reflect.DeepEqual(withExtras, withoutExtras) {
		t.Fatalf("response with extras = %#v, without extras = %#v", withExtras, withoutExtras)
	}
}

func TestPingHandleAcceptsUnknownRequestProtocolVersion(t *testing.T) {
	resp := newPingUseCaseForTest(t, 0).Handle(transport.Request{
		ProtocolVersion: "999",
		RequestID:       "r-4",
		Verb:            "ping",
	})
	if !resp.OK || resp.ProtocolVersion != transport.ProtocolVersion {
		t.Fatalf("response = %#v", resp)
	}
}

func TestPingHandleEchoesEmptyRequestID(t *testing.T) {
	resp := newPingUseCaseForTest(t, 0).Handle(transport.Request{Verb: "ping"})
	if resp.RequestID != "" || !resp.OK {
		t.Fatalf("response = %#v", resp)
	}
}
