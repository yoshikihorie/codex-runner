package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"github.com/yoshikihorie/codex-runner/internal/transport"
	"github.com/yoshikihorie/codex-runner/internal/transport/schema"
)

var tailOrderMu sync.Mutex

type tailProviderFake struct {
	snapshot domain.TaskSnapshot
	err      error
	calls    int
	order    *[]string
}

func (f *tailProviderFake) Snapshot(domain.TaskID) (domain.TaskSnapshot, error) {
	f.calls++
	tailOrderMu.Lock()
	*f.order = append(*f.order, "snapshot")
	tailOrderMu.Unlock()
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
	tailOrderMu.Lock()
	*f.order = append(*f.order, "read")
	tailOrderMu.Unlock()
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

// tailManualTimers keeps timer control deterministic: no specification timeout is
// waited for in real time.
type tailManualTimers struct {
	mu     sync.Mutex
	timers []*tailManualTimer
}

type tailManualTimer struct {
	duration time.Duration
	ch       chan time.Time
	stopped  bool
	stops    int
}

func (f *tailManualTimers) new(duration time.Duration) (<-chan time.Time, func()) {
	f.mu.Lock()
	timer := &tailManualTimer{duration: duration, ch: make(chan time.Time, 1)}
	f.timers = append(f.timers, timer)
	f.mu.Unlock()
	return timer.ch, func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		timer.stopped = true
		timer.stops++
	}
}

func (f *tailManualTimers) fire(t *testing.T, index int) {
	t.Helper()
	f.mu.Lock()
	timer := f.timers[index]
	stopped := timer.stopped
	f.mu.Unlock()
	if stopped {
		return
	}
	timer.ch <- time.Now()
}

func (f *tailManualTimers) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.timers)
}

func (f *tailManualTimers) timer(index int) *tailManualTimer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.timers[index]
}

func waitTail(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func (f *tailNotifierFake) Subscribe(domain.TaskID) (<-chan struct{}, func()) {
	f.calls++
	tailOrderMu.Lock()
	*f.order = append(*f.order, "subscribe")
	tailOrderMu.Unlock()
	return make(chan struct{}), func() { f.unsubscribes++ }
}

type tailWriterFake struct {
	mu       sync.Mutex
	progress []schema.ProgressLine
	complete []schema.CompleteLine
	err      error
}

func (f *tailWriterFake) WriteProgress(line schema.ProgressLine) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.progress = append(f.progress, line)
	return nil
}

func (f *tailWriterFake) WriteComplete(line schema.CompleteLine) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.complete = append(f.complete, line)
	return nil
}

func (f *tailWriterFake) progressLines() []schema.ProgressLine {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]schema.ProgressLine(nil), f.progress...)
}

func (f *tailWriterFake) completeLines() []schema.CompleteLine {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]schema.CompleteLine(nil), f.complete...)
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
			ctx, cancel := context.WithCancel(context.Background())
			errCh := make(chan error, 1)
			go func() { errCh <- uc.Execute(ctx, schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 1}, writer) }()
			deadline := time.Now().Add(time.Second)
			for tailOrderLen(&order) < 4 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			cancel()
			if err := <-errCh; !errors.Is(err, context.Canceled) {
				t.Fatalf("err=%v", err)
			}
			if got := tailOrderCopy(&order); !reflect.DeepEqual(got, []string{"snapshot", "subscribe", "snapshot", "read"}) || notifier.unsubscribes != 1 {
				t.Fatalf("order=%v unsubscribes=%d", got, notifier.unsubscribes)
			}
		})
	}
}

func tailOrderLen(order *[]string) int {
	tailOrderMu.Lock()
	defer tailOrderMu.Unlock()
	return len(*order)
}

func tailOrderCopy(order *[]string) []string {
	tailOrderMu.Lock()
	defer tailOrderMu.Unlock()
	return append([]string(nil), (*order)...)
}

func TestTailTaskExecuteIdleTimerRechecksTerminalBeforeCompleting(t *testing.T) {
	order := []string{}
	provider := &tailProviderFake{snapshot: tailSnapshot(t, domain.StateRunning), order: &order}
	events := &tailEventsFake{order: &order}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	writer := &tailWriterFake{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- uc.Execute(ctx, schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 1}, writer) }()
	for timers.count() != 1 {
		time.Sleep(time.Millisecond)
	}
	provider.snapshot = tailSnapshot(t, domain.StateCompleted)
	timers.fire(t, 0)
	for timers.count() != 2 {
		time.Sleep(time.Millisecond)
	}
	if complete := writer.completeLines(); len(complete) != 0 || timers.timer(0).stops == 0 {
		t.Fatalf("complete=%#v idle=%#v", complete, timers.timer(0))
	}
	timers.fire(t, 1)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if complete := writer.completeLines(); len(complete) != 1 || complete[0].Reason != schema.CompleteReasonTaskTerminal {
		t.Fatalf("complete=%#v", complete)
	}
}

func TestTailTaskExecuteIdleTimerDeliversPersistedEventBeforeTimingOut(t *testing.T) {
	order := []string{}
	provider := &tailProviderFake{snapshot: tailSnapshot(t, domain.StateRunning), order: &order}
	events := &tailEventsFake{order: &order}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	writer := &tailWriterFake{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- uc.Execute(ctx, schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 1}, writer) }()
	for timers.count() != 1 {
		time.Sleep(time.Millisecond)
	}
	events.records = append(events.records, store.EventRecord{Seq: 1})
	timers.fire(t, 0)
	for len(writer.progressLines()) != 1 || timers.count() != 2 {
		time.Sleep(time.Millisecond)
	}
	progress := writer.progressLines()
	complete := writer.completeLines()
	if progress[0].Seq != 1 || len(complete) != 0 || timers.timer(0).stops == 0 {
		t.Fatalf("progress=%#v complete=%#v old=%#v", progress, complete, timers.timer(0))
	}
	// The running session is deliberately cancelled instead of waiting for the
	// replacement idle timer; cancellation must not emit a completion line.
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
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
			progress := writer.progressLines()
			complete := writer.completeLines()
			if !reflect.DeepEqual(order, []string{"snapshot", "read"}) || notifier.calls != 0 || len(complete) != 1 || complete[0].Reason != schema.CompleteReasonTaskTerminal || complete[0].TaskState != state || complete[0].LastSeq != 3 {
				t.Fatalf("order=%v notifier=%d progress=%#v complete=%#v", order, notifier.calls, progress, complete)
			}
		})
	}
}

func TestTailSessionReplayKeepsNextSeqSeparateFromLastDeliveredSeq(t *testing.T) {
	order := []string{}
	events := &tailEventsFake{order: &order, records: []store.EventRecord{{Seq: 1}, {Seq: 5}}}
	writer := &tailWriterFake{}
	session := &tailSession{taskID: tailTestID(t), taskState: domain.StateRunning, nextSeq: 8}
	if err := replayTailHistory(context.Background(), events, session, writer); err != nil {
		t.Fatal(err)
	}
	if session.nextSeq != 8 || session.lastDeliveredSeq != 0 {
		t.Fatalf("after empty replay: %#v", session)
	}
	events.records = append(events.records, store.EventRecord{Seq: 6}, store.EventRecord{Seq: 7}, store.EventRecord{Seq: 8})
	if err := replayTailHistory(context.Background(), events, session, writer); err != nil {
		t.Fatal(err)
	}
	progress := writer.progressLines()
	if !reflect.DeepEqual(events.calls, []int{8, 8}) || len(progress) != 1 || progress[0].Seq != 8 || session.nextSeq != 9 || session.lastDeliveredSeq != 8 {
		t.Fatalf("calls=%v progress=%#v session=%#v", events.calls, progress, session)
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
	progress := writer.progressLines()
	if len(progress) != 1 {
		t.Fatalf("progress=%#v", progress)
	}
	got := progress[0]
	if got.LineType != schema.LineTypeProgress || got.Seq != 7 || !got.RecordedAt.Equal(recordedAt) || got.EventType != "unknown" || !reflect.DeepEqual(got.Raw, raw) || got.TaskState != domain.StateCompleted {
		t.Fatalf("progress=%#v", got)
	}
}
