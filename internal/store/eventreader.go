package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"io"
	"os"
	"syscall"
	"time"
)

type EventRecord struct {
	Seq        int       `json:"seq"`
	RecordedAt time.Time `json:"recorded_at"`
	EventType  string    `json:"event_type"`
	Raw        any       `json:"raw"`
}
type FileEventReader struct{ root string }

type EventReader interface {
	ReadFrom(domain.TaskID, int) ([]EventRecord, error)
}

var _ EventReader = (*FileEventReader)(nil)

func NewFileEventReader(root string) *FileEventReader { return &FileEventReader{root} }
func (r *FileEventReader) ReadFrom(id domain.TaskID, from int) ([]EventRecord, error) {
	p, e := newTaskPaths(r.root, id)
	if e != nil {
		return nil, e
	}
	out := []EventRecord{}
	d, e := openTaskDir(p.dir())
	if e != nil {
		return nil, e
	}
	if d == nil {
		return out, nil
	}
	d.Close()
	f, e := os.OpenFile(p.eventsJSONL(), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if os.IsNotExist(e) {
		return out, nil
	}
	if e != nil {
		return nil, e
	}
	defer f.Close()
	br := bufio.NewReader(f)
	for {
		line, e := br.ReadBytes('\n')
		if len(line) > 0 && e == nil {
			var v EventRecord
			if x := json.Unmarshal(bytes.TrimSuffix(line, []byte{'\n'}), &v); x != nil {
				return nil, x
			}
			if v.Seq >= from {
				out = append(out, v)
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
	}
	return out, nil
}
