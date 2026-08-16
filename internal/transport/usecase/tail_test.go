package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
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
	mu        sync.Mutex
	snapshot  domain.TaskSnapshot
	snapshots []domain.TaskSnapshot
	err       error
	calls     int
	order     *[]string
}

func (f *tailProviderFake) Snapshot(domain.TaskID) (domain.TaskSnapshot, error) {
	f.mu.Lock()
	f.calls++
	snapshot := f.snapshot
	if len(f.snapshots) > 0 {
		index := f.calls - 1
		if index >= len(f.snapshots) {
			index = len(f.snapshots) - 1
		}
		snapshot = f.snapshots[index]
	}
	err := f.err
	f.mu.Unlock()
	tailOrderMu.Lock()
	*f.order = append(*f.order, "snapshot")
	tailOrderMu.Unlock()
	return snapshot, err
}

func (f *tailProviderFake) setSnapshots(snapshots ...domain.TaskSnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshots = append([]domain.TaskSnapshot(nil), snapshots...)
}

func (*tailProviderFake) QueuePosition(domain.TaskID) (int, bool, error) { return 0, false, nil }

type tailEventsFake struct {
	mu      sync.Mutex
	records []store.EventRecord
	err     error
	calls   []int
	order   *[]string
}

func (f *tailEventsFake) ReadFrom(_ domain.TaskID, from int) ([]store.EventRecord, error) {
	f.mu.Lock()
	f.calls = append(f.calls, from)
	err := f.err
	records := append([]store.EventRecord(nil), f.records...)
	f.mu.Unlock()
	tailOrderMu.Lock()
	*f.order = append(*f.order, "read")
	tailOrderMu.Unlock()
	if err != nil {
		return nil, err
	}
	var out []store.EventRecord
	for _, record := range records {
		if record.Seq >= from {
			out = append(out, record)
		}
	}
	return out, nil
}

func (f *tailEventsFake) add(records ...store.EventRecord) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, records...)
}

type tailNotifierFake struct {
	mu           sync.Mutex
	calls        int
	unsubscribes int
	order        *[]string
	nextID       int
	changes      map[int]chan struct{}
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
	timer := *f.timers[index]
	return &timer
}

func (f *tailManualTimers) sameTimer(first, second int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.timers[first] == f.timers[second]
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
	f.mu.Lock()
	f.calls++
	if f.changes == nil {
		f.changes = make(map[int]chan struct{})
	}
	id := f.nextID
	f.nextID++
	changes := make(chan struct{}, 1)
	f.changes[id] = changes
	f.mu.Unlock()
	tailOrderMu.Lock()
	*f.order = append(*f.order, "subscribe")
	tailOrderMu.Unlock()
	var once sync.Once
	return changes, func() {
		once.Do(func() {
			f.mu.Lock()
			defer f.mu.Unlock()
			delete(f.changes, id)
			f.unsubscribes++
		})
	}
}

func (f *tailNotifierFake) wake() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, changes := range f.changes {
		select {
		case changes <- struct{}{}:
		default:
		}
	}
}

func (f *tailNotifierFake) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *tailNotifierFake) unsubscribeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unsubscribes
}

func (f *tailNotifierFake) activeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.changes)
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
	for _, taskID := range []string{"", "invalid", "impl-20260814-120000-a1b2-😀", "unknown-20260101-000000-ffff-slug", "😀-20260101-000000-ffff-slug"} {
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
			if provider.calls != 0 || len(events.calls) != 0 || notifier.callCount() != 0 {
				t.Fatalf("dependencies called: provider=%d events=%v notifier=%d", provider.calls, events.calls, notifier.callCount())
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
			if provider.calls != 0 || len(events.calls) != 0 || notifier.callCount() != 0 {
				t.Fatalf("dependencies called: provider=%d events=%v notifier=%d", provider.calls, events.calls, notifier.callCount())
			}
		})
	}
}

func TestTailTaskHandleDefaultsAndPassesFromSeq(t *testing.T) {
	for _, params := range []json.RawMessage{nil, []byte(`{}`), []byte(`{"from_seq":8,"ignored":true}`)} {
		t.Run(string(params), func(t *testing.T) {
			uc, _, events, _ := newTailUseCase(domain.StateCompleted)
			timers := &tailManualTimers{}
			uc.timerFactory = timers.new
			var output bytes.Buffer
			errCh := make(chan error, 1)
			go func() {
				errCh <- uc.Handle(context.Background(), transport.Request{TaskID: tailTestID(t).String(), Params: params, RequestID: "request-default"}, &output)
			}()
			for timers.count() != 2 {
				time.Sleep(time.Millisecond)
			}
			timers.fire(t, 1)
			if err := <-errCh; err != nil {
				t.Fatal(err)
			}
			want := 1
			if bytes.Contains(params, []byte("8")) {
				want = 8
			}
			if !reflect.DeepEqual(events.calls, []int{want, want}) {
				t.Fatalf("ReadFrom calls = %v, want [%d %d]", events.calls, want, want)
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
	if provider.calls != 1 || len(events.calls) != 0 || notifier.callCount() != 0 {
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
			for tailOrderLen(&order) < 3 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			cancel()
			if err := <-errCh; !errors.Is(err, context.Canceled) {
				t.Fatalf("err=%v", err)
			}
			if got := tailOrderCopy(&order); !reflect.DeepEqual(got, []string{"snapshot", "subscribe", "read"}) || notifier.unsubscribeCount() != 1 {
				t.Fatalf("order=%v unsubscribes=%d", got, notifier.unsubscribeCount())
			}
		})
	}
}

func waitTailTimers(t *testing.T, timers *tailManualTimers, want int, errCh <-chan error) {
	t.Helper()
	deadline := time.After(time.Second)
	for timers.count() < want {
		select {
		case err := <-errCh:
			t.Fatalf("Execute ended before %d timers: %v", want, err)
		case <-deadline:
			t.Fatalf("timers=%d, want at least %d", timers.count(), want)
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestTailTaskExecuteDelayedTerminalEventWaitsForDrainRetry(t *testing.T) {
	order := []string{}
	provider := &tailProviderFake{order: &order}
	provider.setSnapshots(tailSnapshot(t, domain.StateRunning), tailSnapshot(t, domain.StateCompleted))
	events := &tailEventsFake{order: &order}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	writer := &tailWriterFake{}
	errCh := make(chan error, 1)
	go func() {
		errCh <- uc.Execute(context.Background(), schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 1}, writer)
	}()

	waitTailCondition(t, errCh, "waiting for tail subscription", func() bool { return notifier.activeCount() == 1 }, func() string {
		return fmt.Sprintf("subscriptions=%d order=%v", notifier.activeCount(), tailOrderCopy(&order))
	})
	notifier.wake()
	waitTailTimers(t, timers, 3, errCh)
	if got := tailOrderCopy(&order); !reflect.DeepEqual(got[:3], []string{"snapshot", "subscribe", "read"}) {
		t.Fatalf("initial order=%v", got)
	}
	if got := []time.Duration{timers.timer(0).duration, timers.timer(1).duration, timers.timer(2).duration}; !reflect.DeepEqual(got, []time.Duration{tailIdleTimeout, tailTerminalDrainMaxWait, tailTerminalDrainRetryInterval}) {
		t.Fatalf("timer durations=%v", got)
	}
	select {
	case err := <-errCh:
		t.Fatalf("Execute ended before retry: %v", err)
	default:
	}
	if complete := writer.completeLines(); len(complete) != 0 {
		t.Fatalf("complete=%#v", complete)
	}

	events.add(store.EventRecord{Seq: 1, EventType: "terminal"})
	timers.fire(t, 2)
	waitTailTimers(t, timers, 4, errCh)
	if progress := writer.progressLines(); len(progress) != 1 || progress[0].Seq != 1 {
		t.Fatalf("progress=%#v", progress)
	}
	timers.fire(t, 3)
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	complete := writer.completeLines()
	if len(complete) != 1 || complete[0].LastSeq != 1 {
		t.Fatalf("complete=%#v", complete)
	}
}

func TestTailTaskExecuteLiveEventsUsePreviousSnapshotBeforeRefreshing(t *testing.T) {
	order := []string{}
	provider := &tailProviderFake{order: &order}
	provider.setSnapshots(tailSnapshot(t, domain.StateRunning), tailSnapshot(t, domain.StateStalled), tailSnapshot(t, domain.StateStalled))
	events := &tailEventsFake{order: &order}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	writer := &tailWriterFake{}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- uc.Execute(ctx, schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 1}, writer) }()
	waitTailTimers(t, timers, 1, errCh)

	events.add(store.EventRecord{Seq: 1})
	notifier.wake()
	deadline := time.After(time.Second)
	for len(writer.progressLines()) < 1 || tailOrderLen(&order) < 6 {
		select {
		case err := <-errCh:
			t.Fatalf("Execute ended: %v", err)
		case <-deadline:
			t.Fatalf("order=%v progress=%#v", tailOrderCopy(&order), writer.progressLines())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if got := tailOrderCopy(&order); !reflect.DeepEqual(got[:6], []string{"snapshot", "subscribe", "read", "read", "read", "snapshot"}) {
		t.Fatalf("first iteration order=%v", got)
	}
	if progress := writer.progressLines(); progress[0].TaskState != domain.StateRunning {
		t.Fatalf("first state=%s", progress[0].TaskState)
	}

	events.add(store.EventRecord{Seq: 2})
	notifier.wake()
	deadline = time.After(time.Second)
	for len(writer.progressLines()) < 2 || tailOrderLen(&order) < 9 {
		select {
		case err := <-errCh:
			t.Fatalf("Execute ended: %v", err)
		case <-deadline:
			t.Fatalf("order=%v progress=%#v", tailOrderCopy(&order), writer.progressLines())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if progress := writer.progressLines(); progress[1].TaskState != domain.StateStalled {
		t.Fatalf("second state=%s", progress[1].TaskState)
	}
	if got := tailOrderCopy(&order); !reflect.DeepEqual(got[6:9], []string{"read", "read", "snapshot"}) {
		t.Fatalf("second iteration order=%v", got)
	}
	cancel()
	if err := <-errCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if notifier.unsubscribeCount() != 1 || timers.timer(1).stops == 0 {
		t.Fatalf("unsubscribes=%d replacement idle=%#v", notifier.unsubscribeCount(), timers.timer(1))
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
	provider.setSnapshots(tailSnapshot(t, domain.StateCompleted))
	timers.fire(t, 0)
	for timers.count() != 3 {
		time.Sleep(time.Millisecond)
	}
	if complete := writer.completeLines(); len(complete) != 0 || timers.timer(0).stops == 0 {
		t.Fatalf("complete=%#v idle=%#v", complete, timers.timer(0))
	}
	timers.fire(t, 2)
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
	events.add(store.EventRecord{Seq: 1})
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
	if !errors.Is(err, want) || len(events.calls) != 0 || notifier.callCount() != 0 {
		t.Fatalf("err=%v events=%v notifier=%d", err, events.calls, notifier.callCount())
	}
}

func TestTailTaskExecuteUnsubscribesWhenReplayFails(t *testing.T) {
	uc, _, events, notifier := newTailUseCase(domain.StateRunning)
	want := errors.New("event read failed")
	events.err = want

	err := uc.Execute(context.Background(), schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 1}, &tailWriterFake{})
	if !errors.Is(err, want) || notifier.callCount() != 1 || notifier.unsubscribeCount() != 1 {
		t.Fatalf("err=%v notifier calls=%d unsubscribes=%d", err, notifier.callCount(), notifier.unsubscribeCount())
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
			timers := &tailManualTimers{}
			uc.timerFactory = timers.new
			errCh := make(chan error, 1)
			go func() {
				errCh <- uc.Execute(context.Background(), schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 2}, writer)
			}()
			for timers.count() != 2 {
				time.Sleep(time.Millisecond)
			}
			if complete := writer.completeLines(); len(complete) != 0 || timers.timer(0).duration != tailTerminalDrainMaxWait || timers.timer(1).duration != tailTerminalDrainRetryInterval {
				t.Fatalf("complete=%#v timers=%#v", complete, timers.timers)
			}
			timers.fire(t, 1)
			if err := <-errCh; err != nil {
				t.Fatal(err)
			}
			progress := writer.progressLines()
			complete := writer.completeLines()
			if !reflect.DeepEqual(order, []string{"snapshot", "read", "read", "snapshot", "read"}) || notifier.callCount() != 0 || len(complete) != 1 || complete[0].Reason != schema.CompleteReasonTaskTerminal || complete[0].TaskState != state || complete[0].LastSeq != 3 || timers.timer(0).stops == 0 || timers.timer(1).stops == 0 {
				t.Fatalf("order=%v notifier=%d progress=%#v complete=%#v", order, notifier.callCount(), progress, complete)
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
	events.add(store.EventRecord{Seq: 6}, store.EventRecord{Seq: 7}, store.EventRecord{Seq: 8})
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
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	errCh := make(chan error, 1)
	go func() {
		errCh <- uc.Execute(context.Background(), schema.TailTaskInput{TaskID: tailTestID(t), FromSeq: 1}, writer)
	}()
	for timers.count() != 2 {
		time.Sleep(time.Millisecond)
	}
	timers.fire(t, 1)
	if err := <-errCh; err != nil {
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

type tailDecodedSuccessLine struct {
	ProtocolVersion string
	RequestID       string
	OK              bool
	Progress        *schema.ProgressLine
	Complete        *schema.CompleteLine
}

func decodeTailSuccessLines(t *testing.T, output *bytes.Buffer) []tailDecodedSuccessLine {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(output.Bytes()))
	var lines []tailDecodedSuccessLine
	for {
		var response transport.Response
		if err := decoder.Decode(&response); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatal(err)
		}
		if !response.OK {
			t.Fatalf("response is not successful: %#v", response)
		}
		var result struct {
			LineType string `json:"line_type"`
		}
		if err := json.Unmarshal(response.Result, &result); err != nil {
			t.Fatal(err)
		}
		line := tailDecodedSuccessLine{ProtocolVersion: response.ProtocolVersion, RequestID: response.RequestID, OK: response.OK}
		switch result.LineType {
		case schema.LineTypeProgress:
			line.Progress = &schema.ProgressLine{}
			if err := json.Unmarshal(response.Result, line.Progress); err != nil {
				t.Fatal(err)
			}
		case schema.LineTypeComplete:
			line.Complete = &schema.CompleteLine{}
			if err := json.Unmarshal(response.Result, line.Complete); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatalf("unknown result line type %q", result.LineType)
		}
		lines = append(lines, line)
	}
	return lines
}

func startTailGoroutine(t *testing.T, run func(context.Context) error) (context.CancelFunc, <-chan error, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		errCh <- run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("tail goroutine did not exit after cancellation")
		}
	})
	return cancel, errCh, done
}

func waitTailCondition(t *testing.T, errCh <-chan error, message string, condition func() bool, diagnostics func() string) {
	t.Helper()
	deadline := time.After(time.Second)
	for !condition() {
		select {
		case err := <-errCh:
			t.Fatalf("%s: tail ended early: %v; %s", message, err, diagnostics())
		case <-deadline:
			t.Fatalf("%s: %s", message, diagnostics())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func waitTailOrderAfter(t *testing.T, errCh <-chan error, order *[]string, before int, suffix ...string) {
	t.Helper()
	waitTailCondition(t, errCh, "waiting for tail order suffix", func() bool {
		got := tailOrderCopy(order)
		return len(got) >= before+len(suffix) && reflect.DeepEqual(got[len(got)-len(suffix):], suffix)
	}, func() string {
		return "order=" + strings.Join(tailOrderCopy(order), ",")
	})
}

func waitTailTimer(t *testing.T, errCh <-chan error, timers *tailManualTimers, count int, duration time.Duration, writer *tailWriterFake, order *[]string) {
	t.Helper()
	waitTailCondition(t, errCh, "waiting for tail timer", func() bool {
		return timers.count() >= count && timers.timer(count-1).duration == duration
	}, func() string {
		return fmt.Sprintf("timers=%d progress=%#v complete=%#v order=%v", timers.count(), writer.progressLines(), writer.completeLines(), tailOrderCopy(order))
	})
}

func waitTailExit(t *testing.T, errCh <-chan error, done <-chan struct{}, want error) {
	t.Helper()
	select {
	case err := <-errCh:
		if !errors.Is(err, want) {
			t.Fatalf("tail error = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("tail did not exit within deadline")
	}
	waitTail(t, done, "tail goroutine did not close done")
}

func tailSequence(lines []schema.ProgressLine) []int {
	sequences := make([]int, len(lines))
	for i, line := range lines {
		sequences[i] = line.Seq
	}
	return sequences
}

func TestTailTaskExecuteAllowsConcurrentSessionsWithIndependentWakeAndIdleTimers(t *testing.T) {
	order := []string{}
	id := tailTestID(t)
	provider := &tailProviderFake{snapshot: tailSnapshot(t, domain.StateRunning), order: &order}
	events := &tailEventsFake{order: &order, records: []store.EventRecord{{Seq: 1}, {Seq: 2}, {Seq: 3}}}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new

	writerA := &tailWriterFake{}
	writerB := &tailWriterFake{}
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	errA := make(chan error, 1)
	errB := make(chan error, 1)
	doneA := make(chan struct{})
	doneB := make(chan struct{})
	go func() {
		defer close(doneA)
		errA <- uc.Execute(ctxA, schema.TailTaskInput{TaskID: id, FromSeq: 1}, writerA)
	}()
	go func() {
		defer close(doneB)
		errB <- uc.Execute(ctxB, schema.TailTaskInput{TaskID: id, FromSeq: 3}, writerB)
	}()
	t.Cleanup(func() {
		cancelA()
		cancelB()
		waitTail(t, doneA, "tail A goroutine did not exit after cancellation")
		waitTail(t, doneB, "tail B goroutine did not exit after cancellation")
	})

	waitTailCondition(t, errA, "waiting for both tail subscriptions", func() bool {
		return notifier.activeCount() == 2
	}, func() string {
		return fmt.Sprintf("active subscriptions=%d", notifier.activeCount())
	})
	waitTailCondition(t, errA, "waiting for initial A replay", func() bool {
		return reflect.DeepEqual(tailSequence(writerA.progressLines()), []int{1, 2, 3})
	}, func() string {
		return fmt.Sprintf("A progress=%#v", writerA.progressLines())
	})
	waitTailCondition(t, errB, "waiting for initial B replay", func() bool {
		return reflect.DeepEqual(tailSequence(writerB.progressLines()), []int{3})
	}, func() string {
		return fmt.Sprintf("B progress=%#v", writerB.progressLines())
	})
	waitTailCondition(t, errA, "waiting for independent initial idle timers", func() bool {
		return timers.count() == 2
	}, func() string {
		return fmt.Sprintf("timers=%d", timers.count())
	})
	if timers.sameTimer(0, 1) {
		t.Fatal("initial tail sessions shared an idle timer")
	}

	events.add(store.EventRecord{Seq: 4})
	notifier.wake()
	waitTailCondition(t, errA, "waiting for A broadcast progress", func() bool {
		return reflect.DeepEqual(tailSequence(writerA.progressLines()), []int{1, 2, 3, 4})
	}, func() string {
		return fmt.Sprintf("A progress=%#v", writerA.progressLines())
	})
	waitTailCondition(t, errB, "waiting for B broadcast progress", func() bool {
		return reflect.DeepEqual(tailSequence(writerB.progressLines()), []int{3, 4})
	}, func() string {
		return fmt.Sprintf("B progress=%#v", writerB.progressLines())
	})
	waitTailCondition(t, errA, "waiting for replaced idle timers", func() bool {
		return timers.count() == 4
	}, func() string {
		return fmt.Sprintf("timers=%d", timers.count())
	})
	if timers.timer(0).stops == 0 || timers.timer(1).stops == 0 {
		t.Fatalf("initial timers were not stopped: A=%#v B=%#v", timers.timer(0), timers.timer(1))
	}

	cancelA()
	waitTailExit(t, errA, doneA, context.Canceled)
	waitTailCondition(t, errB, "waiting for A unsubscribe", func() bool {
		return notifier.activeCount() == 1
	}, func() string {
		return fmt.Sprintf("active subscriptions=%d", notifier.activeCount())
	})
	progressA := writerA.progressLines()

	events.add(store.EventRecord{Seq: 5})
	notifier.wake()
	waitTailCondition(t, errB, "waiting for B progress after A disconnect", func() bool {
		return reflect.DeepEqual(tailSequence(writerB.progressLines()), []int{3, 4, 5})
	}, func() string {
		return fmt.Sprintf("B progress=%#v", writerB.progressLines())
	})
	if got := writerA.progressLines(); !reflect.DeepEqual(got, progressA) {
		t.Fatalf("A received progress after cancellation: got=%#v want=%#v", got, progressA)
	}
	waitTailCondition(t, errB, "waiting for B replacement idle timer", func() bool {
		return timers.count() == 5
	}, func() string {
		return fmt.Sprintf("timers=%d", timers.count())
	})
	timers.fire(t, 4)
	waitTailExit(t, errB, doneB, nil)
	completeB := writerB.completeLines()
	if len(completeB) != 1 || completeB[0].Reason != schema.CompleteReasonIdleTimeout || completeB[0].LastSeq != 5 {
		t.Fatalf("B completion=%#v", completeB)
	}
	if completeA := writerA.completeLines(); len(completeA) != 0 {
		t.Fatalf("A completion after cancellation=%#v", completeA)
	}
	if notifier.activeCount() != 0 || notifier.unsubscribeCount() != 2 {
		t.Fatalf("active=%d unsubscribes=%d", notifier.activeCount(), notifier.unsubscribeCount())
	}
	if got := tailSequence(writerA.progressLines()); !reflect.DeepEqual(got, []int{1, 2, 3, 4}) {
		t.Fatalf("A sequences=%v", got)
	}
	if got := tailSequence(writerB.progressLines()); !reflect.DeepEqual(got, []int{3, 4, 5}) {
		t.Fatalf("B sequences=%v", got)
	}
}

func TestTailTaskHandleReplaysLiveEventThenCompletes(t *testing.T) {
	order := []string{}
	provider := &tailProviderFake{snapshot: tailSnapshot(t, domain.StateRunning), order: &order}
	events := &tailEventsFake{order: &order, records: []store.EventRecord{{Seq: 1}, {Seq: 2}, {Seq: 3}, {Seq: 4}, {Seq: 5}}}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	var output bytes.Buffer
	id := tailTestID(t)
	cancel, errCh, done := startTailGoroutine(t, func(ctx context.Context) error {
		return uc.Handle(ctx, transport.Request{TaskID: id.String(), RequestID: "request-live"}, &output)
	})
	defer cancel()
	waitTailTimer(t, errCh, timers, 1, tailIdleTimeout, &tailWriterFake{}, &order)
	events.add(store.EventRecord{Seq: 6})
	firstWakeOrder := tailOrderLen(&order)
	notifier.wake()
	waitTailOrderAfter(t, errCh, &order, firstWakeOrder, "read", "read", "snapshot")
	provider.setSnapshots(tailSnapshot(t, domain.StateCompleted))
	secondWakeOrder := tailOrderLen(&order)
	notifier.wake()
	waitTailOrderAfter(t, errCh, &order, secondWakeOrder, "read", "snapshot")
	waitTailTimer(t, errCh, timers, 4, tailTerminalDrainRetryInterval, &tailWriterFake{}, &order)
	select {
	case err := <-errCh:
		t.Fatalf("Handle ended before terminal drain retry: %v", err)
	default:
	}
	timers.fire(t, 3)
	waitTailExit(t, errCh, done, nil)

	lines := decodeTailSuccessLines(t, &output)
	var progress []schema.ProgressLine
	var complete []schema.CompleteLine
	for _, line := range lines {
		if line.ProtocolVersion != transport.ProtocolVersion || line.RequestID != "request-live" || !line.OK {
			t.Fatalf("response envelope = %#v", line)
		}
		if line.Progress != nil {
			progress = append(progress, *line.Progress)
		}
		if line.Complete != nil {
			complete = append(complete, *line.Complete)
		}
	}
	if got := tailSequence(progress); !reflect.DeepEqual(got, []int{1, 2, 3, 4, 5, 6}) {
		t.Fatalf("progress sequences=%v", got)
	}
	if len(complete) != 1 || complete[0].Reason != schema.CompleteReasonTaskTerminal || complete[0].TaskState != domain.StateCompleted || complete[0].LastSeq != 6 {
		t.Fatalf("complete=%#v", complete)
	}
	if !reflect.DeepEqual(events.calls, []int{1, 6, 7, 7, 7}) || notifier.callCount() != 1 || notifier.unsubscribeCount() != 1 {
		t.Fatalf("event calls=%v notifier=%d/%d", events.calls, notifier.callCount(), notifier.unsubscribeCount())
	}
}

func TestTailTaskHandleFailedTaskReplaysDefaultSequenceThenCompletesWithoutSubscription(t *testing.T) {
	order := []string{}
	provider := &tailProviderFake{snapshot: tailSnapshot(t, domain.StateFailed), order: &order}
	records := make([]store.EventRecord, 9)
	for i := range records {
		records[i].Seq = i + 1
	}
	events := &tailEventsFake{order: &order, records: records}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	var output bytes.Buffer
	id := tailTestID(t)
	cancel, errCh, done := startTailGoroutine(t, func(ctx context.Context) error {
		return uc.Handle(ctx, transport.Request{TaskID: id.String(), RequestID: "request-failed"}, &output)
	})
	defer cancel()
	waitTailTimer(t, errCh, timers, 1, tailTerminalDrainMaxWait, &tailWriterFake{}, &order)
	waitTailTimer(t, errCh, timers, 2, tailTerminalDrainRetryInterval, &tailWriterFake{}, &order)
	select {
	case err := <-errCh:
		t.Fatalf("Handle ended before terminal drain retry: %v", err)
	default:
	}
	timers.fire(t, 1)
	waitTailExit(t, errCh, done, nil)

	lines := decodeTailSuccessLines(t, &output)
	var progress []schema.ProgressLine
	var complete []schema.CompleteLine
	for _, line := range lines {
		if line.ProtocolVersion != transport.ProtocolVersion || line.RequestID != "request-failed" || !line.OK {
			t.Fatalf("response envelope = %#v", line)
		}
		if line.Progress != nil {
			progress = append(progress, *line.Progress)
		}
		if line.Complete != nil {
			complete = append(complete, *line.Complete)
		}
	}
	if got := tailSequence(progress); !reflect.DeepEqual(got, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatalf("progress sequences=%v", got)
	}
	if len(complete) != 1 || complete[0].Reason != schema.CompleteReasonTaskTerminal || complete[0].TaskState != domain.StateFailed || complete[0].LastSeq != 9 {
		t.Fatalf("complete=%#v", complete)
	}
	if !reflect.DeepEqual(events.calls, []int{1, 10, 10}) || notifier.callCount() != 0 {
		t.Fatalf("event calls=%v notifier calls=%d", events.calls, notifier.callCount())
	}
}

func TestTailTaskHandleIdleTimeoutKeepsRunningState(t *testing.T) {
	order := []string{}
	provider := &tailProviderFake{snapshot: tailSnapshot(t, domain.StateRunning), order: &order}
	events := &tailEventsFake{order: &order}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	var output bytes.Buffer
	id := tailTestID(t)
	cancel, errCh, done := startTailGoroutine(t, func(ctx context.Context) error {
		return uc.Handle(ctx, transport.Request{TaskID: id.String(), RequestID: "request-idle"}, &output)
	})
	defer cancel()
	waitTailTimer(t, errCh, timers, 1, tailIdleTimeout, &tailWriterFake{}, &order)
	timers.fire(t, 0)
	waitTailExit(t, errCh, done, nil)
	lines := decodeTailSuccessLines(t, &output)
	if len(lines) != 1 || !lines[0].OK || lines[0].Complete == nil || lines[0].Complete.Reason != schema.CompleteReasonIdleTimeout || lines[0].Complete.TaskState != domain.StateRunning || lines[0].Complete.LastSeq != 0 {
		t.Fatalf("lines=%#v", lines)
	}
	if provider.calls != 2 || notifier.callCount() != 1 || notifier.unsubscribeCount() != 1 {
		t.Fatalf("provider=%d notifier=%d/%d", provider.calls, notifier.callCount(), notifier.unsubscribeCount())
	}
}

func TestTailTaskExecuteReconnectsFromNextSequenceWithoutDuplicateOrGap(t *testing.T) {
	order := []string{}
	provider := &tailProviderFake{snapshot: tailSnapshot(t, domain.StateRunning), order: &order}
	events := &tailEventsFake{order: &order, records: []store.EventRecord{{Seq: 1}, {Seq: 2}, {Seq: 3}, {Seq: 4}, {Seq: 5}, {Seq: 6}, {Seq: 7}}}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	id := tailTestID(t)

	first := &tailWriterFake{}
	cancelFirst, firstErrCh, firstDone := startTailGoroutine(t, func(ctx context.Context) error {
		return uc.Execute(ctx, schema.TailTaskInput{TaskID: id, FromSeq: 1}, first)
	})
	waitTailCondition(t, firstErrCh, "waiting for first replay", func() bool { return len(first.progressLines()) == 7 && timers.count() >= 1 }, func() string {
		return fmt.Sprintf("progress=%#v timers=%d order=%v", first.progressLines(), timers.count(), tailOrderCopy(&order))
	})
	cancelFirst()
	waitTailExit(t, firstErrCh, firstDone, context.Canceled)

	events.add(store.EventRecord{Seq: 8}, store.EventRecord{Seq: 9}, store.EventRecord{Seq: 10})
	second := &tailWriterFake{}
	cancelSecond, secondErrCh, secondDone := startTailGoroutine(t, func(ctx context.Context) error {
		return uc.Execute(ctx, schema.TailTaskInput{TaskID: id, FromSeq: 8}, second)
	})
	waitTailCondition(t, secondErrCh, "waiting for second replay", func() bool { return len(second.progressLines()) == 3 && timers.count() >= 2 }, func() string {
		return fmt.Sprintf("progress=%#v timers=%d order=%v", second.progressLines(), timers.count(), tailOrderCopy(&order))
	})
	cancelSecond()
	waitTailExit(t, secondErrCh, secondDone, context.Canceled)

	if got := tailSequence(first.progressLines()); !reflect.DeepEqual(got, []int{1, 2, 3, 4, 5, 6, 7}) {
		t.Fatalf("first sequences=%v", got)
	}
	if got := tailSequence(second.progressLines()); !reflect.DeepEqual(got, []int{8, 9, 10}) {
		t.Fatalf("second sequences=%v", got)
	}
	if notifier.unsubscribeCount() != 2 || provider.calls != 2 {
		t.Fatalf("unsubscribes=%d provider calls=%d", notifier.unsubscribeCount(), provider.calls)
	}
}

func TestTailTaskExecuteReplaysKnownAndUnknownEventTypesInSequence(t *testing.T) {
	order := []string{}
	types := []string{"thread.started", "turn.started", "item.started", "item.completed", "turn.completed", "turn.failed", "unknown"}
	records := make([]store.EventRecord, len(types))
	for i, eventType := range types {
		records[i] = store.EventRecord{Seq: i + 1, EventType: eventType}
	}
	provider := &tailProviderFake{snapshot: tailSnapshot(t, domain.StateRunning), order: &order}
	events := &tailEventsFake{order: &order, records: records}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	writer := &tailWriterFake{}
	id := tailTestID(t)
	cancel, errCh, done := startTailGoroutine(t, func(ctx context.Context) error {
		return uc.Execute(ctx, schema.TailTaskInput{TaskID: id, FromSeq: 1}, writer)
	})
	waitTailCondition(t, errCh, "waiting for event replay", func() bool { return len(writer.progressLines()) == len(types) && timers.count() >= 1 }, func() string {
		return fmt.Sprintf("progress=%#v timers=%d order=%v", writer.progressLines(), timers.count(), tailOrderCopy(&order))
	})
	cancel()
	waitTailExit(t, errCh, done, context.Canceled)
	progress := writer.progressLines()
	if got := tailSequence(progress); !reflect.DeepEqual(got, []int{1, 2, 3, 4, 5, 6, 7}) {
		t.Fatalf("sequences=%v", got)
	}
	gotTypes := make([]string, len(progress))
	for i, line := range progress {
		gotTypes[i] = line.EventType
	}
	if !reflect.DeepEqual(gotTypes, types) {
		t.Fatalf("event types=%v", gotTypes)
	}
}

func TestTailTaskExecuteFollowsCancellingThenKilled(t *testing.T) {
	order := []string{}
	provider := &tailProviderFake{order: &order}
	provider.setSnapshots(tailSnapshot(t, domain.StateRunning), tailSnapshot(t, domain.StateCancelling), tailSnapshot(t, domain.StateKilled), tailSnapshot(t, domain.StateKilled))
	events := &tailEventsFake{order: &order}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	writer := &tailWriterFake{}
	id := tailTestID(t)
	_, errCh, done := startTailGoroutine(t, func(ctx context.Context) error {
		return uc.Execute(ctx, schema.TailTaskInput{TaskID: id, FromSeq: 1}, writer)
	})
	waitTailTimer(t, errCh, timers, 1, tailIdleTimeout, writer, &order)
	firstWakeOrder := tailOrderLen(&order)
	notifier.wake()
	waitTailOrderAfter(t, errCh, &order, firstWakeOrder, "read", "snapshot")
	if complete := writer.completeLines(); len(complete) != 0 {
		t.Fatalf("complete after cancelling=%#v", complete)
	}
	secondWakeOrder := tailOrderLen(&order)
	notifier.wake()
	waitTailOrderAfter(t, errCh, &order, secondWakeOrder, "read", "snapshot")
	waitTailTimer(t, errCh, timers, 3, tailTerminalDrainRetryInterval, writer, &order)
	select {
	case err := <-errCh:
		t.Fatalf("Execute ended before terminal drain retry: %v", err)
	default:
	}
	timers.fire(t, 2)
	waitTailExit(t, errCh, done, nil)
	complete := writer.completeLines()
	if len(complete) != 1 || complete[0].Reason != schema.CompleteReasonTaskTerminal || complete[0].TaskState != domain.StateKilled {
		t.Fatalf("complete=%#v", complete)
	}
}

func TestTailTaskExecuteQueuedToRunningDeliversFirstEvent(t *testing.T) {
	order := []string{}
	provider := &tailProviderFake{order: &order}
	provider.setSnapshots(tailSnapshot(t, domain.StateQueued), tailSnapshot(t, domain.StateStarting), tailSnapshot(t, domain.StateRunning), tailSnapshot(t, domain.StateRunning))
	events := &tailEventsFake{order: &order}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	writer := &tailWriterFake{}
	id := tailTestID(t)
	cancel, errCh, done := startTailGoroutine(t, func(ctx context.Context) error {
		return uc.Execute(ctx, schema.TailTaskInput{TaskID: id, FromSeq: 1}, writer)
	})
	waitTailTimer(t, errCh, timers, 1, tailIdleTimeout, writer, &order)
	firstWakeOrder := tailOrderLen(&order)
	notifier.wake()
	waitTailOrderAfter(t, errCh, &order, firstWakeOrder, "read", "snapshot")
	if progress := writer.progressLines(); len(progress) != 0 {
		t.Fatalf("progress after starting=%#v", progress)
	}
	secondWakeOrder := tailOrderLen(&order)
	notifier.wake()
	waitTailOrderAfter(t, errCh, &order, secondWakeOrder, "read", "snapshot")
	if progress := writer.progressLines(); len(progress) != 0 {
		t.Fatalf("progress after running=%#v", progress)
	}
	events.add(store.EventRecord{Seq: 1})
	thirdWakeOrder := tailOrderLen(&order)
	notifier.wake()
	waitTailOrderAfter(t, errCh, &order, thirdWakeOrder, "read", "read", "snapshot")
	waitTailCondition(t, errCh, "waiting for first progress", func() bool { return len(writer.progressLines()) == 1 }, func() string {
		return fmt.Sprintf("progress=%#v order=%v", writer.progressLines(), tailOrderCopy(&order))
	})
	cancel()
	waitTailExit(t, errCh, done, context.Canceled)
	progress := writer.progressLines()
	if len(progress) != 1 || progress[0].Seq != 1 || progress[0].TaskState != domain.StateRunning {
		t.Fatalf("progress=%#v", progress)
	}
}

func TestTailTaskExecuteStateOnlyStallWakeAppliesToNextProgress(t *testing.T) {
	order := []string{}
	provider := &tailProviderFake{order: &order}
	provider.setSnapshots(tailSnapshot(t, domain.StateRunning), tailSnapshot(t, domain.StateStalled), tailSnapshot(t, domain.StateStalled))
	events := &tailEventsFake{order: &order}
	notifier := &tailNotifierFake{order: &order}
	uc := NewTailTaskUseCase(provider, events, notifier)
	timers := &tailManualTimers{}
	uc.timerFactory = timers.new
	writer := &tailWriterFake{}
	id := tailTestID(t)
	cancel, errCh, done := startTailGoroutine(t, func(ctx context.Context) error {
		return uc.Execute(ctx, schema.TailTaskInput{TaskID: id, FromSeq: 1}, writer)
	})
	waitTailTimer(t, errCh, timers, 1, tailIdleTimeout, writer, &order)
	firstWakeOrder := tailOrderLen(&order)
	notifier.wake()
	waitTailOrderAfter(t, errCh, &order, firstWakeOrder, "read", "snapshot")
	if progress := writer.progressLines(); len(progress) != 0 {
		t.Fatalf("progress after stalled state=%#v", progress)
	}
	events.add(store.EventRecord{Seq: 1})
	secondWakeOrder := tailOrderLen(&order)
	notifier.wake()
	waitTailOrderAfter(t, errCh, &order, secondWakeOrder, "read", "read", "snapshot")
	waitTailCondition(t, errCh, "waiting for stalled progress", func() bool { return len(writer.progressLines()) == 1 }, func() string {
		return fmt.Sprintf("progress=%#v order=%v", writer.progressLines(), tailOrderCopy(&order))
	})
	cancel()
	waitTailExit(t, errCh, done, context.Canceled)
	progress := writer.progressLines()
	if len(progress) != 1 || progress[0].Seq != 1 || progress[0].TaskState != domain.StateStalled {
		t.Fatalf("progress=%#v", progress)
	}
}
