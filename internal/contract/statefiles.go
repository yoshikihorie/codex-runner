package contract

import (
	"bufio"
	"bytes"
	"encoding/json"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"io"
	"os"
	"sync"
	"syscall"
	"time"
)

type eventState struct{ states sync.Map }
type taskEventSeqState struct {
	mu          sync.Mutex
	next        int
	initialized bool
}

func (w *fileContractWriter) AppendEvent(id domain.TaskID, e domain.Event) error {
	return w.appendEventRecord(id, e.Type(), e)
}
func (w *fileContractWriter) AppendRawEvent(id domain.TaskID, typ string, raw json.RawMessage) error {
	return w.appendEventRecord(id, typ, raw)
}
func (w *fileContractWriter) appendEventRecord(id domain.TaskID, typ string, raw any) error {
	if e := w.verifyTaskDir(id); e != nil {
		return e
	}
	p, e := store.EventsJSONLPath(w.root, id)
	if e != nil {
		return e
	}
	v, _ := w.events.states.LoadOrStore(id.String(), &taskEventSeqState{})
	s := v.(*taskEventSeqState)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.initialized {
		n, completeEndOffset, e := lastSeq(p)
		if e != nil {
			return contractWriteError(e)
		}
		info, e := os.Stat(p)
		if e != nil && !os.IsNotExist(e) {
			return contractWriteError(e)
		}
		if e == nil && completeEndOffset < info.Size() {
			if e := os.Truncate(p, completeEndOffset); e != nil {
				return contractWriteError(e)
			}
		}
		s.next = n + 1
		s.initialized = true
	}
	now := time.Now()
	if w.clock != nil {
		now = w.clock.Now()
	}
	b, e := json.Marshal(store.EventRecord{Seq: s.next, RecordedAt: now, EventType: typ, Raw: raw})
	if e != nil {
		return contractWriteError(e)
	}
	b = append(b, '\n')
	f, e := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if e != nil {
		return contractWriteError(e)
	}
	_, e = f.Write(b)
	x := f.Close()
	if e == nil {
		e = x
	}
	if e != nil {
		return contractWriteError(e)
	}
	s.next++
	return nil
}
func lastSeq(path string) (seq int, completeEndOffset int64, err error) {
	f, e := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if os.IsNotExist(e) {
		return 0, 0, nil
	}
	if e != nil {
		return 0, 0, e
	}
	defer f.Close()
	r := bufio.NewReader(f)
	for {
		b, e := r.ReadBytes('\n')
		if len(b) > 0 && e == nil {
			var v store.EventRecord
			if x := json.Unmarshal(bytes.TrimSuffix(b, []byte{'\n'}), &v); x != nil {
				return 0, 0, x
			}
			seq = v.Seq
			completeEndOffset += int64(len(b))
		}
		if e == io.EOF {
			return seq, completeEndOffset, nil
		}
		if e != nil {
			return 0, 0, e
		}
	}
}
