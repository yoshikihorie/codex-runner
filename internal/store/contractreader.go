package store

import (
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
)

type FileContractReader struct{ root string }

type ContractReader interface {
	ReadStderrLog(domain.TaskID) ([]byte, error)
	ReadLastMessage(domain.TaskID) (bool, error)
	ReadPromptContent(domain.TaskID) ([]byte, error)
	ReadLastMessageContent(domain.TaskID) ([]byte, error)
	ReadExitCode(domain.TaskID) (int, bool, error)
}

var _ ContractReader = (*FileContractReader)(nil)

func NewFileContractReader(root string) *FileContractReader { return &FileContractReader{root} }
func (r *FileContractReader) read(id domain.TaskID, get func(taskPaths) string) ([]byte, error) {
	p, e := newTaskPaths(r.root, id)
	if e != nil {
		return nil, e
	}
	d, e := openTaskDir(p.dir())
	if e != nil {
		return nil, e
	}
	if d == nil {
		return nil, nil
	}
	d.Close()
	f, e := os.OpenFile(get(p), os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if os.IsNotExist(e) {
		return nil, nil
	}
	if e != nil {
		return nil, e
	}
	defer f.Close()
	return io.ReadAll(f)
}
func (r *FileContractReader) ReadStderrLog(id domain.TaskID) ([]byte, error) {
	return r.read(id, taskPaths.stderrLog)
}
func (r *FileContractReader) ReadPromptContent(id domain.TaskID) ([]byte, error) {
	return r.read(id, taskPaths.promptMD)
}
func (r *FileContractReader) ReadLastMessageContent(id domain.TaskID) ([]byte, error) {
	return r.read(id, taskPaths.lastMessageMD)
}
func (r *FileContractReader) ReadLastMessage(id domain.TaskID) (bool, error) {
	b, e := r.ReadLastMessageContent(id)
	return len(strings.TrimSpace(string(b))) > 0, e
}
func (r *FileContractReader) ReadExitCode(id domain.TaskID) (int, bool, error) {
	b, e := r.read(id, taskPaths.exitCode)
	if e != nil {
		return 0, false, e
	}
	if b == nil {
		return 0, false, nil
	}
	v, e := strconv.Atoi(strings.TrimSpace(string(b)))
	return v, true, e
}
