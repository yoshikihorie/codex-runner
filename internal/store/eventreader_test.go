package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func eventReaderID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260806-120000-a1b2-events")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func writeEvents(t *testing.T, root string, id domain.TaskID, lines ...[]byte) {
	t.Helper()
	dir := filepath.Join(root, id.String())
	if err := os.Mkdir(dir, taskDirPerm); err != nil {
		t.Fatal(err)
	}
	p, _ := EventsJSONLPath(root, id)
	var b []byte
	for _, line := range lines {
		b = append(b, line...)
	}
	if err := os.WriteFile(p, b, taskFilePerm); err != nil {
		t.Fatal(err)
	}
}
func eventLine(t *testing.T, seq int, raw any) []byte {
	t.Helper()
	b, err := json.Marshal(EventRecord{Seq: seq, RecordedAt: time.Date(2026, 8, 6, 12, 0, seq, 0, time.UTC), EventType: "event", Raw: raw})
	if err != nil {
		t.Fatal(err)
	}
	return append(b, '\n')
}
func TestEventReaderReadsAllAndFiltersBySequence(t *testing.T) {
	root, id := t.TempDir(), eventReaderID(t)
	writeEvents(t, root, id, eventLine(t, 1, "a"), eventLine(t, 2, "b"), eventLine(t, 3, "c"))
	r := NewFileEventReader(root)
	all, err := r.ReadFrom(id, 0)
	if err != nil || len(all) != 3 {
		t.Fatalf("all=%#v err=%v", all, err)
	}
	got, err := r.ReadFrom(id, 2)
	if err != nil || len(got) != 2 || got[0].Seq != 2 || got[1].Seq != 3 {
		t.Fatalf("filtered=%#v err=%v", got, err)
	}
}
func TestEventReaderIgnoresIncompleteFinalLine(t *testing.T) {
	root, id := t.TempDir(), eventReaderID(t)
	writeEvents(t, root, id, eventLine(t, 1, "ok"), []byte(`{"seq":2`))
	got, err := NewFileEventReader(root).ReadFrom(id, 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("got=%#v err=%v", got, err)
	}
}
func TestEventReaderFailsFastOnMalformedCompleteLine(t *testing.T) {
	root, id := t.TempDir(), eventReaderID(t)
	writeEvents(t, root, id, eventLine(t, 1, "ok"), []byte("{bad}\n"), eventLine(t, 3, "late"))
	if _, err := NewFileEventReader(root).ReadFrom(id, 0); err == nil {
		t.Fatal("malformed line accepted")
	}
}
func TestEventReaderMissingFileReturnsEmptySlice(t *testing.T) {
	got, err := NewFileEventReader(t.TempDir()).ReadFrom(eventReaderID(t), 0)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("got=%#v err=%v", got, err)
	}
}
func TestEventReaderReadsLineOverBufferLimit(t *testing.T) {
	root, id := t.TempDir(), eventReaderID(t)
	raw := string(make([]byte, 70*1024))
	writeEvents(t, root, id, eventLine(t, 1, raw))
	got, err := NewFileEventReader(root).ReadFrom(id, 0)
	if err != nil || len(got) != 1 {
		t.Fatalf("got=%d err=%v", len(got), err)
	}
}
