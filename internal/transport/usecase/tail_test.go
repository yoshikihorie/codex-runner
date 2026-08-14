package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
	"github.com/yoshikihorie/codex-runner/internal/transport/schema"
)

type tailProviderFake struct {
	snapshot domain.TaskSnapshot
	err      error
	calls    int
	order    *[]string
}

func (f *tailProviderFake) Snapshot(domain.TaskID) (domain.TaskSnapshot, error) {
	f.calls++
	*f.order = append(*f.order, "snapshot")
	return f.snapshot, f.err
}

func (*tailProviderFake) QueuePosition(domain.TaskID) (int, bool, error) { return 0, false, nil }

type tailEventsFake struct {
	records []store.EventRecord
	err     error
	calls   []int
	order   *[]string
}

func (f *tailEventsFake) ReadFrom(_ domain.TaskID, from int) ([]store.EventRecord, error) {
	f.calls = append(f.calls, from)
	*f.order = append(*f.order, "read")
	if f.err != nil {
		return nil, f.err
	}
	var out []store.EventRecord
	for _, record := range f.records {
		if record.Seq >= from {
			out = append(out, record)
		}
	}
	return out, nil
}

type tailNotifierFake struct {
	calls        int
	unsubscribes int
	order        *[]string
}

func (f *tailNotifierFake) Subscribe(domain.TaskID) (<-chan struct{}, func()) {
	f.calls++
	*f.order = append(*f.order, "subscribe")
	return make(chan struct{}), func() { f.unsubscribes++ }
}

type tailWriterFake struct {
	progress []schema.ProgressLine
	complete []schema.CompleteLine
	err      error
}

func (f *tailWriterFake) WriteProgress(line schema.ProgressLine) error {
	if f.err != nil {
		return f.err
	}
	f.progress = append(f.progress, line)
	return nil
}

func (f *tailWriterFake) WriteComplete(line schema.CompleteLine) error {
	if f.err != nil {
		return f.err
	}
	f.complete = append(f.complete, line)
	return nil
}

func tailTestID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260814-120000-a1b2-tail")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func tailSnapshot(t *testing.T, state domain.TaskState) domain.TaskSnapshot {
	t.Helper()
	return domain.TaskSnapshot{TaskID: tailTestID(t), State: state}
}

func newTailUseCase(state domain.TaskState) (*TailTaskUseCase, *tailProviderFake, *tailEventsFake, *tailNotifierFake) {
	order := []string{}
	provider := &tailProviderFake{snapshot: domain.TaskSnapshot{State: state}, order: &order}
	events := &tailEventsFake{order: &order}
	notifier := &tailNotifierFake{order: &order}
	return NewTailTaskUseCase(provider, events, notifier), provider, events, notifier
}

func decodeTailError(t *testing.T, output *bytes.Buffer) transport.Response {
	t.Helper()
	if bytes.Count(output.Bytes(), []byte("\n")) != 1 {
		t.Fatalf("output must contain exactly one newline-terminated line: %q", output.String())
	}
	var response transport.Response
	if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &response); err != nil {
		t.Fatal(err)
	}
	return response
}

func TestTailTaskHandleRejectsInvalidTaskIDBeforeDependencies(t *testing.T) {
	for _, taskID := range []string{"", "invalid", "impl-20260814-120000-a1b2-😀"} {
		t.Run(taskID, func(t *testing.T) {
			uc, provider, events, notifier := newTailUseCase(domain.StateRunning)
			var output bytes.Buffer
			err := uc.Handle(context.Background(), transport.Request{TaskID: taskID, RequestID: "request-invalid"}, &output)
			if err != nil {
				t.Fatal(err)
			}
			response := decodeTailError(t, &output)
			if response.ProtocolVersion != transport.ProtocolVersion || response.RequestID != "request-invalid" || response.OK || response.Error == nil || response.Error.Code != "TASK_ID_INVALID_FORMAT" || response.Error.MessageKey != "error.task.idInvalidFormat" || response.Error.Detail["task_id"] != taskID {
				t.Fatalf("response = %#v", response)
			}
			if provider.calls != 0 || len(events.calls) != 0 || notifier.calls != 0 {
				t.Fatalf("dependencies called: provider=%d events=%v notifier=%d", provider.calls, events.calls, notifier.calls)
			}
		})
	}
}

func TestTailTaskHandleValidatesFromSeqBeforeDependencies(t *testing.T) {
	invalid := []struct {
		params      json.RawMessage
		wantFromSeq any
	}{
		{params: []byte(`{"from_seq":0}`), wantFromSeq: float64(0)},
		{params: []byte(`{"from_seq":-1}`), wantFromSeq: float64(-1)},
		{params: []byte(`{"from_seq":"1"}`), wantFromSeq: "1"},
		{params: []byte(`{"from_seq":1.5}`), wantFromSeq: 1.5},
		{params: []byte(`{"from_seq":true}`), wantFromSeq: true},
		{params: []byte(`null`), wantFromSeq: nil},
		{params: []byte(`[]`), wantFromSeq: nil},
		{params: []byte(`{"from_seq":null}`), wantFromSeq: nil},
		{params: []byte(`{`), wantFromSeq: nil},
	}
	for _, tt := range invalid {
		t.Run(string(tt.params), func(t *testing.T) {
			uc, provider, events, notifier := newTailUseCase(domain.StateRunning)
			var output bytes.Buffer
			err := uc.Handle(context.Background(), transport.Request{TaskID: tailTestID(t).String(), Params: tt.params, RequestID: "request-from-seq"}, &output)
			if err != nil {
				t.Fatal(err)
			}
			response := decodeTailError(t, &output)
			if response.OK || response.Error == nil || response.Error.Code != "TAIL_FROM_SEQ_INVALID" || response.Error.MessageKey != "error.tail.fromSeqInvalid" || !reflect.DeepEqual(response.Error.Detail, map[string]any{"from_seq": tt.wantFromSeq}) {
				t.Fatalf("response = %#v", response)
			}
			if provider.calls != 0 || len(events.calls) != 0 || notifier.calls != 0 {
				t.Fatalf("dependencies called: provider=%d events=%v notifier=%d", provider.calls, events.calls, notifier.calls)
			}
		})
	}
}

func TestTailTaskHandleDefaultsAndPassesFromSeq(t *testing.T) {
	for _, params := range []json.RawMessage{nil, []byte(`{}`), []byte(`{"from_seq":8,"ignored":true}`)} {
		t.Run(string(params), func(t *testing.T) {
			uc, _, events, _ := newTailUseCase(domain.StateCompleted)
			var output bytes.Buffer
			if err := uc.Handle(context.Background(), transport.Request{TaskID: tailTestID(t).String(), Params: params, RequestID: "request-default"}, &output); err != nil {
				t.Fatal(err)
			}
			want := 1
			if bytes.Contains(params, []byte("8")) {
				want = 8
			}
			if !reflect.DeepEqual(events.calls, []int{want}) {
				t.Fatalf("ReadFrom calls = %v, want [%d]", events.calls, want)
			}
		})
	}
}

func TestTailTaskHandleMapsNotFoundToSingleResponse(t *testing.T) {
	uc, provider, events, notifier := newTailUseCase(domain.StateRunning)
	provider.err = domain.ErrTaskNotFound
	var output bytes.Buffer
	if err := uc.Handle(context.Background(), transport.Request{TaskID: tailTestID(t).String(), RequestID: "request-not-found"}, &output); err != nil {
		t.Fatal(err)
	}
	response := decodeTailError(t, &output)
	if response.OK || response.Error == nil || response.Error.Code != "TASK_NOT_FOUND" || response.Error.MessageKey != "error.task.notFound" || response.Error.Detail["task_id"] != tailTestID(t).String() {
		t.Fatalf("response = %#v", response)
	}
	if provider.calls != 1 || len(events.calls) != 0 || notifier.calls != 0 {
		t.Fatalf("unexpected dependency calls")
	}
}

func TestTailTaskExecuteSnapshotsThenSubscribesThenReplaysNonTerminal(t *testing.T) {
	states := []domain.TaskState{domain.StateQueued, domain.StateStarting, domain.StateRunning}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			order := []string{}
			provider := &tailProviderFake{snapshot: tailSnapshot(t, state), order: &order}
			events := &tailEventsFake{order: &order}
			notifier := &tailNotifierFake{order: &order}
			uc := NewTailTaskUseCase(provider, events, notifier)
			writer := &tailWriterFake{}
			if err := uc.Execute(context.Background(), schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 1}, writer); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(order, []string{"snapshot", "subscribe", "read"}) || notifier.unsubscribes != 1 {
				t.Fatalf("order=%v unsubscribes=%d", order, notifier.unsubscribes)
			}
		})
	}
}

func TestTailTaskExecutePropagatesNonNotFoundSnapshotError(t *testing.T) {
	uc, provider, events, notifier := newTailUseCase(domain.StateRunning)
	want := errors.New("snapshot read failed")
	provider.err = want
	err := uc.Execute(context.Background(), schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 1}, &tailWriterFake{})
	if !errors.Is(err, want) || len(events.calls) != 0 || notifier.calls != 0 {
		t.Fatalf("err=%v events=%v notifier=%d", err, events.calls, notifier.calls)
	}
}

func TestTailTaskExecuteUnsubscribesWhenReplayFails(t *testing.T) {
	uc, _, events, notifier := newTailUseCase(domain.StateRunning)
	want := errors.New("event read failed")
	events.err = want

	err := uc.Execute(context.Background(), schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 1}, &tailWriterFake{})
	if !errors.Is(err, want) || notifier.calls != 1 || notifier.unsubscribes != 1 {
		t.Fatalf("err=%v notifier calls=%d unsubscribes=%d", err, notifier.calls, notifier.unsubscribes)
	}
}

func TestTailTaskExecuteTerminalReplaysThenCompletesWithoutSubscription(t *testing.T) {
	terminals := []domain.TaskState{domain.StateCompleted, domain.StateFailed, domain.StateRecovered, domain.StateTimeoutLost, domain.StateKilled, domain.StateLost}
	for _, state := range terminals {
		t.Run(string(state), func(t *testing.T) {
			order := []string{}
			provider := &tailProviderFake{snapshot: tailSnapshot(t, state), order: &order}
			events := &tailEventsFake{order: &order, records: []store.EventRecord{{Seq: 2}, {Seq: 3}}}
			notifier := &tailNotifierFake{order: &order}
			writer := &tailWriterFake{}
			uc := NewTailTaskUseCase(provider, events, notifier)
			if err := uc.Execute(context.Background(), schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 2}, writer); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(order, []string{"snapshot", "read"}) || notifier.calls != 0 || len(writer.complete) != 1 || writer.complete[0].Reason != schema.CompleteReasonTaskTerminal || writer.complete[0].TaskState != state || writer.complete[0].LastSeq != 3 {
				t.Fatalf("order=%v notifier=%d progress=%#v complete=%#v", order, notifier.calls, writer.progress, writer.complete)
			}
		})
	}
}

func TestTailSessionReplayKeepsNextSeqSeparateFromLastDeliveredSeq(t *testing.T) {
	order := []string{}
	events := &tailEventsFake{order: &order, records: []store.EventRecord{{Seq: 1}, {Seq: 5}}}
	writer := &tailWriterFake{}
	session := &tailSession{taskID: tailTestID(t), taskState: domain.StateRunning, nextSeq: 8}
	if err := replayTailHistory(events, session, writer); err != nil {
		t.Fatal(err)
	}
	if session.nextSeq != 8 || session.lastDeliveredSeq != 0 {
		t.Fatalf("after empty replay: %#v", session)
	}
	events.records = append(events.records, store.EventRecord{Seq: 6}, store.EventRecord{Seq: 7}, store.EventRecord{Seq: 8})
	if err := replayTailHistory(events, session, writer); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events.calls, []int{8, 8}) || len(writer.progress) != 1 || writer.progress[0].Seq != 8 || session.nextSeq != 9 || session.lastDeliveredSeq != 8 {
		t.Fatalf("calls=%v progress=%#v session=%#v", events.calls, writer.progress, session)
	}
}

func TestTailTaskExecutePreservesUnknownEventFields(t *testing.T) {
	recordedAt := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	raw := map[string]any{"nested": []any{"value", float64(2)}}
	order := []string{}
	provider := &tailProviderFake{snapshot: tailSnapshot(t, domain.StateCompleted), order: &order}
	events := &tailEventsFake{order: &order, records: []store.EventRecord{{Seq: 7, RecordedAt: recordedAt, EventType: "unknown", Raw: raw}}}
	notifier := &tailNotifierFake{order: &order}
	writer := &tailWriterFake{}
	uc := NewTailTaskUseCase(provider, events, notifier)
	if err := uc.Execute(context.Background(), schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 1}, writer); err != nil {
		t.Fatal(err)
	}
	if len(writer.progress) != 1 {
		t.Fatalf("progress=%#v", writer.progress)
	}
	got := writer.progress[0]
	if got.LineType != schema.LineTypeProgress || got.Seq != 7 || !got.RecordedAt.Equal(recordedAt) || got.EventType != "unknown" || !reflect.DeepEqual(got.Raw, raw) || got.TaskState != domain.StateCompleted {
		t.Fatalf("progress=%#v", got)
	}
}
