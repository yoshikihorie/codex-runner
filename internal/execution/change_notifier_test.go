package execution

import (
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

var (
	_ TaskChangeNotifier      = (*taskChangeBroadcaster)(nil)
	_ contract.ContractWriter = (*notifyingContractWriter)(nil)
	_ store.TaskStore         = (*notifyingTaskStore)(nil)
)

// Subscribe 前の event 回収は notifier 単体の責務ではない。Batch 3A の
// TailTaskUseCase が EventReader.ReadFrom を先行・再実行して検証する。

func changeNotifierID(t *testing.T, suffix string) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260814-120000-a1b2-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func changeNotifierSnapshot(id domain.TaskID) domain.TaskSnapshot {
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	return domain.TaskSnapshot{TaskID: id, Subcommand: domain.SubcommandImpl, ResolvedTimeoutSeconds: 1800, Model: "gpt-5", RequestedAt: now, Route: domain.ExecutionRouteDaemon, State: domain.StateQueued, StateUpdatedAt: now, SchemaVersion: 1}
}

func requireWakeup(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("wakeup was not delivered")
	}
}

func requireNoWakeup(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("unexpected wakeup")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestNotifyingContractWriterSuccessNotifiesAllSubscribers(t *testing.T) {
	id := changeNotifierID(t, "writer")
	notifier := NewTaskChangeNotifier()
	first, stopFirst := notifier.Subscribe(id)
	defer stopFirst()
	second, stopSecond := notifier.Subscribe(id)
	defer stopSecond()
	delegate := &contractWriterFake{}
	writer := NewNotifyingContractWriter(delegate, notifier)
	event := domain.TaskQueued{TaskID: id}
	if err := writer.AppendEvent(id, event); err != nil {
		t.Fatal(err)
	}
	if got := delegate.appendEventCalls.Load(); got != 1 {
		t.Fatalf("AppendEvent calls = %d, want 1", got)
	}
	requireWakeup(t, first)
	requireWakeup(t, second)
}

func TestNotifyingContractWriterAppendRawEventNotifiesWithoutFilteringType(t *testing.T) {
	id := changeNotifierID(t, "raw")
	notifier := NewTaskChangeNotifier()
	ch, stop := notifier.Subscribe(id)
	defer stop()
	delegate := &contractWriterFake{}
	writer := NewNotifyingContractWriter(delegate, notifier)
	raw := json.RawMessage(`{"unknown":true}`)
	if err := writer.AppendRawEvent(id, "unregistered.event", raw); err != nil {
		t.Fatal(err)
	}
	rawID, rawType, delegatedRaw := delegate.rawArgs()
	if rawID != id || rawType != "unregistered.event" || string(delegatedRaw) != string(raw) || delegate.appendRawCalls.Load() != 1 {
		t.Fatalf("AppendRawEvent was not delegated unchanged: %#v", delegate)
	}
	requireWakeup(t, ch)
}

func TestNotifyingTaskStoreSaveSuccessNotifies(t *testing.T) {
	id := changeNotifierID(t, "store")
	snapshot := changeNotifierSnapshot(id)
	notifier := NewTaskChangeNotifier()
	ch, stop := notifier.Subscribe(id)
	defer stop()
	delegate := &taskStoreFake{}
	tasks := NewNotifyingTaskStore(delegate, notifier)
	if err := tasks.Save(id, snapshot); err != nil {
		t.Fatal(err)
	}
	savedID, savedSnapshot := delegate.savedArgs()
	if savedID != id || savedSnapshot != snapshot || delegate.saveCalls.Load() != 1 {
		t.Fatalf("Save was not delegated unchanged: %#v", delegate)
	}
	requireWakeup(t, ch)
}

func TestNotifyingWritersDoNotNotifyOnDelegateError(t *testing.T) {
	id := changeNotifierID(t, "errors")
	errSentinel := errors.New("delegate failure")
	for name, run := range map[string]func(*taskChangeBroadcaster) error{
		"AppendEvent": func(notifier *taskChangeBroadcaster) error {
			return NewNotifyingContractWriter(&contractWriterFake{appendEventErr: errSentinel}, notifier).AppendEvent(id, domain.TaskQueued{TaskID: id})
		},
		"AppendRawEvent": func(notifier *taskChangeBroadcaster) error {
			return NewNotifyingContractWriter(&contractWriterFake{appendRawErr: errSentinel}, notifier).AppendRawEvent(id, "unknown", json.RawMessage(`{}`))
		},
		"Save": func(notifier *taskChangeBroadcaster) error {
			return NewNotifyingTaskStore(&taskStoreFake{saveErr: errSentinel}, notifier).Save(id, changeNotifierSnapshot(id))
		},
	} {
		t.Run(name, func(t *testing.T) {
			notifier := NewTaskChangeNotifier()
			ch, stop := notifier.Subscribe(id)
			defer stop()
			if err := run(notifier); err != errSentinel {
				t.Fatalf("error = %v, want sentinel", err)
			}
			requireNoWakeup(t, ch)
		})
	}
}

func TestNotifyingDecoratorsPromoteNonTargetMethods(t *testing.T) {
	id := changeNotifierID(t, "forward")
	errSentinel := errors.New("unchanged")
	logs := &contract.ExecutionLogs{}
	writerDelegate := &contractWriterFake{otherErr: errSentinel, logs: logs}
	writer := NewNotifyingContractWriter(writerDelegate, NewTaskChangeNotifier())
	if err := writer.WritePrompt(id, []byte("prompt")); err != errSentinel {
		t.Fatal(err)
	}
	if err := writer.WriteReviewInput(id, []byte("input")); err != errSentinel {
		t.Fatal(err)
	}
	if err := writer.WriteCombinedPrompt(id, []byte("combined")); err != errSentinel {
		t.Fatal(err)
	}
	if got, err := writer.OpenExecutionLogs(id); got != logs || err != errSentinel {
		t.Fatalf("logs=%p err=%v", got, err)
	}
	if err := writer.WriteExitCode(id, domain.NewExitCode(7)); err != errSentinel {
		t.Fatal(err)
	}
	if err := writer.WritePartialOutput(id, "partial"); err != errSentinel {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)
	if err := writer.WriteRecoveredMarker(id, at); err != errSentinel {
		t.Fatal(err)
	}
	if err := writer.WriteAdoptedMarker(id, at); err != errSentinel {
		t.Fatal(err)
	}
	if writerDelegate.otherCalls.Load() != 8 {
		t.Fatalf("writer calls = %d, want 8", writerDelegate.otherCalls.Load())
	}

	snapshot := changeNotifierSnapshot(id)
	storeDelegate := &taskStoreFake{otherErr: errSentinel, loaded: snapshot, listed: []domain.TaskSnapshot{snapshot}, reserved: true}
	tasks := NewNotifyingTaskStore(storeDelegate, NewTaskChangeNotifier())
	if got, err := tasks.Load(id); got != snapshot || err != errSentinel {
		t.Fatalf("Load = %#v, %v", got, err)
	}
	if got, err := tasks.ListByStates([]domain.TaskState{domain.StateQueued}); len(got) != 1 || got[0] != snapshot || err != errSentinel {
		t.Fatalf("ListByStates = %#v, %v", got, err)
	}
	if err := tasks.Reserve(id); err != errSentinel {
		t.Fatal(err)
	}
	if err := tasks.Release(id); err != errSentinel {
		t.Fatal(err)
	}
	if got, err := tasks.IsReserved(id); !got || err != errSentinel {
		t.Fatalf("IsReserved = %t, %v", got, err)
	}
	if storeDelegate.otherCalls.Load() != 5 {
		t.Fatalf("store calls = %d, want 5", storeDelegate.otherCalls.Load())
	}
}

func TestTaskChangeBroadcasterTaskIsolationAndIdempotentUnsubscribe(t *testing.T) {
	first := changeNotifierID(t, "first")
	second := changeNotifierID(t, "second")
	notifier := NewTaskChangeNotifier()
	firstCh, stopFirst := notifier.Subscribe(first)
	secondCh, stopSecond := notifier.Subscribe(second)
	defer stopSecond()
	notifier.notify(first)
	requireWakeup(t, firstCh)
	requireNoWakeup(t, secondCh)
	stopFirst()
	stopFirst()
	notifier.notify(first)
	requireNoWakeup(t, firstCh)
	notifier.notify(second)
	requireWakeup(t, secondCh)
}

func TestTaskChangeBroadcasterCoalescesUnreadWakeupsWithoutBlocking(t *testing.T) {
	id := changeNotifierID(t, "coalesce")
	notifier := NewTaskChangeNotifier()
	ch, stop := notifier.Subscribe(id)
	defer stop()
	writer := NewNotifyingContractWriter(&contractWriterFake{}, notifier)
	tasks := NewNotifyingTaskStore(&taskStoreFake{}, notifier)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for n := 0; n < 64; n++ {
			_ = writer.AppendEvent(id, domain.TaskQueued{TaskID: id})
			_ = writer.AppendRawEvent(id, "unknown", json.RawMessage(`{}`))
			_ = tasks.Save(id, changeNotifierSnapshot(id))
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("writers blocked on unread wakeup")
	}
	requireWakeup(t, ch)
	requireNoWakeup(t, ch)
}

func TestTaskChangeBroadcasterConcurrentSubscribeUnsubscribeAndNotify(t *testing.T) {
	id := changeNotifierID(t, "concurrent")
	notifier := NewTaskChangeNotifier()
	writer := NewNotifyingContractWriter(&contractWriterFake{}, notifier)
	tasks := NewNotifyingTaskStore(&taskStoreFake{}, notifier)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for n := 0; n < 48; n++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; _, stop := notifier.Subscribe(id); stop(); stop() }()
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = writer.AppendEvent(id, domain.TaskQueued{TaskID: id})
			_ = writer.AppendRawEvent(id, "unknown", json.RawMessage(`{}`))
			_ = tasks.Save(id, changeNotifierSnapshot(id))
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent operations did not complete")
	}
}

type contractWriterFake struct {
	mu                                           sync.Mutex
	appendEventErr, appendRawErr, otherErr       error
	appendEventCalls, appendRawCalls, otherCalls atomic.Int64
	rawID                                        domain.TaskID
	rawType                                      string
	raw                                          json.RawMessage
	logs                                         *contract.ExecutionLogs
}

func (f *contractWriterFake) WritePrompt(domain.TaskID, []byte) error {
	f.otherCalls.Add(1)
	return f.otherErr
}
func (f *contractWriterFake) WriteReviewInput(domain.TaskID, []byte) error {
	f.otherCalls.Add(1)
	return f.otherErr
}
func (f *contractWriterFake) WriteCombinedPrompt(domain.TaskID, []byte) error {
	f.otherCalls.Add(1)
	return f.otherErr
}
func (f *contractWriterFake) OpenExecutionLogs(domain.TaskID) (*contract.ExecutionLogs, error) {
	f.otherCalls.Add(1)
	return f.logs, f.otherErr
}
func (f *contractWriterFake) WriteExitCode(domain.TaskID, domain.ExitCode) error {
	f.otherCalls.Add(1)
	return f.otherErr
}
func (f *contractWriterFake) WritePartialOutput(domain.TaskID, string) error {
	f.otherCalls.Add(1)
	return f.otherErr
}
func (f *contractWriterFake) WriteRecoveredMarker(domain.TaskID, time.Time) error {
	f.otherCalls.Add(1)
	return f.otherErr
}
func (f *contractWriterFake) WriteAdoptedMarker(domain.TaskID, time.Time) error {
	f.otherCalls.Add(1)
	return f.otherErr
}
func (f *contractWriterFake) AppendEvent(domain.TaskID, domain.Event) error {
	f.appendEventCalls.Add(1)
	return f.appendEventErr
}
func (f *contractWriterFake) AppendRawEvent(id domain.TaskID, kind string, raw json.RawMessage) error {
	f.appendRawCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rawID, f.rawType, f.raw = id, kind, raw
	return f.appendRawErr
}
func (f *contractWriterFake) rawArgs() (domain.TaskID, string, json.RawMessage) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rawID, f.rawType, f.raw
}

type taskStoreFake struct {
	mu                    sync.Mutex
	saveErr, otherErr     error
	saveCalls, otherCalls atomic.Int64
	saveID                domain.TaskID
	saved, loaded         domain.TaskSnapshot
	listed                []domain.TaskSnapshot
	reserved              bool
}

func (f *taskStoreFake) Load(domain.TaskID) (domain.TaskSnapshot, error) {
	f.otherCalls.Add(1)
	return f.loaded, f.otherErr
}
func (f *taskStoreFake) Save(id domain.TaskID, snapshot domain.TaskSnapshot) error {
	f.saveCalls.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saveID, f.saved = id, snapshot
	return f.saveErr
}
func (f *taskStoreFake) savedArgs() (domain.TaskID, domain.TaskSnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saveID, f.saved
}
func (f *taskStoreFake) ListByStates([]domain.TaskState) ([]domain.TaskSnapshot, error) {
	f.otherCalls.Add(1)
	return f.listed, f.otherErr
}
func (f *taskStoreFake) Reserve(domain.TaskID) error { f.otherCalls.Add(1); return f.otherErr }
func (f *taskStoreFake) Release(domain.TaskID) error { f.otherCalls.Add(1); return f.otherErr }
func (f *taskStoreFake) IsReserved(domain.TaskID) (bool, error) {
	f.otherCalls.Add(1)
	return f.reserved, f.otherErr
}
