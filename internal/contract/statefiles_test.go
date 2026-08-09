package contract

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

func stateID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260806-120000-a1b2-state")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func stateWriter(t *testing.T) (*fileContractWriter, string, domain.TaskID, time.Time) {
	t.Helper()
	root, id := t.TempDir(), stateID(t)
	if err := os.Mkdir(filepath.Join(root, id.String()), 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	return NewFileContractWriter(root, domain.ClockFunc(func() time.Time { return now })), root, id, now
}
func readEventRecords(t *testing.T, root string, id domain.TaskID) []store.EventRecord {
	t.Helper()
	p, err := store.EventsJSONLPath(root, id)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []store.EventRecord
	s := bufio.NewScanner(f)
	for s.Scan() {
		var v store.EventRecord
		if err := json.Unmarshal(s.Bytes(), &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, v)
	}
	if err := s.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
func TestAppendEventAssignsSequenceTimeAndRaw(t *testing.T) {
	w, root, id, now := stateWriter(t)
	event := domain.TaskQueued{TaskID: id, Subcommand: domain.SubcommandImpl, OccurredAt: now}
	if err := w.AppendEvent(id, event); err != nil {
		t.Fatal(err)
	}
	raw := json.RawMessage(`{"message":"raw"}`)
	if err := w.AppendRawEvent(id, "raw", raw); err != nil {
		t.Fatal(err)
	}
	got := readEventRecords(t, root, id)
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 2 || !got[0].RecordedAt.Equal(now) || got[0].EventType != event.Type() {
		t.Fatalf("records=%#v", got)
	}
	if _, ok := got[0].Raw.(map[string]any); !ok {
		t.Fatalf("event raw type=%T", got[0].Raw)
	}
	if rawValue, ok := got[1].Raw.(map[string]any); !ok || rawValue["message"] != "raw" {
		t.Fatalf("raw=%#v", got[1].Raw)
	}
}
func TestAppendEventConcurrentCallsProduceMonotonicSeq(t *testing.T) {
	w, root, id, _ := stateWriter(t)
	const count = 24
	var wg sync.WaitGroup
	for n := 0; n < count; n++ {
		n := n
		wg.Add(1)
		go func() {
			defer wg.Done()
			if n%2 == 0 {
				if err := w.AppendEvent(id, domain.TaskQueued{TaskID: id, Subcommand: domain.SubcommandImpl}); err != nil {
					t.Error(err)
				}
			} else if err := w.AppendRawEvent(id, "raw", json.RawMessage(`{"n":1}`)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	got := readEventRecords(t, root, id)
	if len(got) != count {
		t.Fatalf("records=%d", len(got))
	}
	seq := make([]int, len(got))
	for i, v := range got {
		seq[i] = v.Seq
	}
	sort.Ints(seq)
	for i, v := range seq {
		if v != i+1 {
			t.Fatalf("sequences=%v", seq)
		}
	}
}
func TestAppendEventResumesSeqAfterRestart(t *testing.T) {
	w, root, id, _ := stateWriter(t)
	if err := w.AppendRawEvent(id, "first", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	restarted := NewFileContractWriter(root, nil)
	if err := restarted.AppendRawEvent(id, "second", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	got := readEventRecords(t, root, id)
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 2 {
		t.Fatalf("records=%#v", got)
	}
}
func TestAppendEventTruncatesIncompleteEventOnResume(t *testing.T) {
	w, root, id, _ := stateWriter(t)
	if err := w.AppendRawEvent(id, "first", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	p, err := store.EventsJSONLPath(root, id)
	if err != nil {
		t.Fatal(err)
	}
	partial := []byte(`{"seq":2,"event_type"`)
	f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(partial); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := NewFileContractWriter(root, nil)
	if err := restarted.AppendRawEvent(id, "second", json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}

	contents, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(contents, partial) {
		t.Fatalf("events.jsonl still contains incomplete event: %q", contents)
	}
	got := readEventRecords(t, root, id)
	if len(got) != 2 || got[0].Seq != 1 || got[1].Seq != 2 {
		t.Fatalf("records=%#v", got)
	}
}
func TestAppendEventFailureWrapsContractWriteError(t *testing.T) {
	w, root, id, _ := stateWriter(t)
	p, err := store.EventsJSONLPath(root, id)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, p); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendRawEvent(id, "raw", json.RawMessage(`{}`)); !errors.Is(err, domain.ErrContractWriteFailed) {
		t.Fatalf("AppendRawEvent error=%v", err)
	}
}
