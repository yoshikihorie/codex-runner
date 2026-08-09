package contract

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

type ContractWriter interface {
	WritePrompt(domain.TaskID, []byte) error
	WriteReviewInput(domain.TaskID, []byte) error
	WriteCombinedPrompt(domain.TaskID, []byte) error
	OpenExecutionLogs(domain.TaskID) (*ExecutionLogs, error)
	WriteExitCode(domain.TaskID, domain.ExitCode) error
	WritePartialOutput(domain.TaskID, string) error
	WriteRecoveredMarker(domain.TaskID, time.Time) error
	WriteAdoptedMarker(domain.TaskID, time.Time) error
	AppendEvent(domain.TaskID, domain.Event) error
	AppendRawEvent(domain.TaskID, string, json.RawMessage) error
}
type ExecutionLogs struct {
	Stdout *os.File
	Stderr *os.File
}

func (l *ExecutionLogs) Close() error {
	var es []error
	if l.Stdout != nil {
		es = append(es, l.Stdout.Close())
	}
	if l.Stderr != nil {
		es = append(es, l.Stderr.Close())
	}
	return errors.Join(es...)
}

type fileContractWriter struct {
	root   string
	clock  domain.Clock
	events eventState
}

func NewFileContractWriter(root string, clock domain.Clock) *fileContractWriter {
	return &fileContractWriter{root: root, clock: clock}
}
func (w *fileContractWriter) dir(id domain.TaskID) (string, error) {
	p, e := store.EventsJSONLPath(w.root, id)
	if e != nil {
		return "", e
	}
	return filepath.Dir(p), nil
}
func contractWriteError(err error) error {
	return fmt.Errorf("%w: %v", domain.ErrContractWriteFailed, err)
}
func (w *fileContractWriter) verifyTaskDir(id domain.TaskID) error {
	p, e := w.dir(id)
	if e != nil {
		return e
	}
	f, e := os.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if e != nil {
		return contractWriteError(e)
	}
	defer f.Close()
	i, e := f.Stat()
	if e != nil {
		return contractWriteError(e)
	}
	if !i.IsDir() {
		return contractWriteError(fmt.Errorf("task dir path is not a directory: %s", p))
	}
	return nil
}
func (w *fileContractWriter) once(id domain.TaskID, path func(string, domain.TaskID) (string, error), b []byte) error {
	if e := w.verifyTaskDir(id); e != nil {
		return e
	}
	p, e := path(w.root, id)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if e != nil {
		return contractWriteError(e)
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	if e == nil {
		e = f.Chmod(0o600)
	}
	if x := f.Close(); e == nil {
		e = x
	}
	if e == nil {
		e = os.Link(tmp, p)
	}
	if e != nil {
		return contractWriteError(e)
	}
	return nil
}
func (w *fileContractWriter) WritePrompt(id domain.TaskID, b []byte) error {
	return w.once(id, store.PromptMDPath, b)
}
func (w *fileContractWriter) WriteReviewInput(id domain.TaskID, b []byte) error {
	return w.once(id, store.InputTXTPath, b)
}
func (w *fileContractWriter) WriteCombinedPrompt(id domain.TaskID, b []byte) error {
	return w.once(id, store.CombinedPromptMDPath, b)
}
func (w *fileContractWriter) OpenExecutionLogs(id domain.TaskID) (*ExecutionLogs, error) {
	if e := w.verifyTaskDir(id); e != nil {
		return nil, e
	}
	a, e := store.StdoutLogPath(w.root, id)
	if e != nil {
		return nil, e
	}
	b, e := store.StderrLogPath(w.root, id)
	if e != nil {
		return nil, e
	}
	o, e := os.OpenFile(a, os.O_APPEND|os.O_CREATE|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if e != nil {
		return nil, contractWriteError(e)
	}
	s, e := os.OpenFile(b, os.O_APPEND|os.O_CREATE|os.O_WRONLY|syscall.O_NOFOLLOW, 0o600)
	if e != nil {
		o.Close()
		return nil, contractWriteError(e)
	}
	return &ExecutionLogs{o, s}, nil
}
func (w *fileContractWriter) atomic(id domain.TaskID, path func(string, domain.TaskID) (string, error), b []byte) error {
	if e := w.verifyTaskDir(id); e != nil {
		return e
	}
	p, e := path(w.root, id)
	if e != nil {
		return e
	}
	if e = store.WriteAtomic(p, b, 0o600); e != nil {
		return contractWriteError(e)
	}
	return nil
}
func (w *fileContractWriter) WriteExitCode(id domain.TaskID, c domain.ExitCode) error {
	return w.atomic(id, store.ExitCodePath, []byte(fmt.Sprintf("%d\n", c.Raw())))
}
func (w *fileContractWriter) WritePartialOutput(id domain.TaskID, c string) error {
	return w.atomic(id, store.PartialOutputMDPath, []byte(c))
}
func (w *fileContractWriter) WriteRecoveredMarker(id domain.TaskID, at time.Time) error {
	return w.atomic(id, store.RecoveredAfterTimeoutPath, []byte(at.Format("2006-01-02T15:04:05-0700")+"\n"))
}
func (w *fileContractWriter) WriteAdoptedMarker(id domain.TaskID, at time.Time) error {
	return w.atomic(id, store.AdoptedAfterRestartPath, []byte(at.Format("2006-01-02T15:04:05-0700")+"\n"))
}
