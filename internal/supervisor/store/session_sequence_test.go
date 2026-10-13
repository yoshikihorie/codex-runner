package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSessionSequenceConcurrentAndRestart(t *testing.T) {
	root := t.TempDir()
	seq := NewSessionSequence(root)
	const count = 32
	ids := make(chan string, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := seq.Next("alpha")
			if err != nil {
				t.Error(err)
				return
			}
			ids <- id.String()
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Errorf("duplicate %s", id)
		}
		seen[id] = true
	}
	if len(seen) != count {
		t.Fatalf("got %d ids", len(seen))
	}
	id, err := NewSessionSequence(root).Next("alpha")
	if err != nil || id.String() != "alpha-33" {
		t.Fatalf("restart id=%q err=%v", id, err)
	}
}

func TestSessionSequenceStartedAtPreservesCounter(t *testing.T) {
	root := t.TempDir()
	seq := NewSessionSequence(root)
	if _, err := seq.Next("alpha"); err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC().Truncate(time.Second)
	if err := seq.MarkStarted(started); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		SessionSeq map[string]uint64 `json:"session_seq"`
		Started    time.Time         `json:"supervisor_started_at"`
	}
	if err := json.Unmarshal(data, &got); err != nil || got.SessionSeq["alpha"] != 2 || !got.Started.Equal(started) {
		t.Fatalf("state=%+v err=%v", got, err)
	}
}

func TestSessionSequenceSlugLimit(t *testing.T) {
	seq := NewSessionSequence(t.TempDir())
	if _, err := seq.Next("abcdefghijklmnopqrstuvwxyzabcdefghijklmno"); err == nil {
		t.Fatal("expected slug limit error")
	}
}

func TestSessionSequenceSkipsExistingAndQuarantinedRecordsWithoutState(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sessions"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"alpha-1.json", "alpha-2.json.corrupt-old"} {
		if err := os.WriteFile(filepath.Join(root, "sessions", name), []byte("occupied"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	id, err := NewSessionSequence(root).Next("alpha")
	if err != nil || id.String() != "alpha-3" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}
