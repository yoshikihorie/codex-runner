package store

import (
	"errors"
	"fmt"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"os"
	"path/filepath"
	"syscall"
)

const (
	taskDirPerm  = 0o700
	taskFilePerm = 0o600
)

var errZeroTaskID = errors.New("task id is zero value")

type taskPaths struct{ root string }

func defaultTasksRoot() string { return "/tmp/codex-tasks" }
func newTaskPaths(root string, id domain.TaskID) (taskPaths, error) {
	if id.String() == "" {
		return taskPaths{}, errZeroTaskID
	}
	return taskPaths{filepath.Join(root, id.String())}, nil
}
func (p taskPaths) dir() string              { return p.root }
func (p taskPaths) taskJSON() string         { return filepath.Join(p.root, "task.json") }
func (p taskPaths) eventsJSONL() string      { return filepath.Join(p.root, "events.jsonl") }
func (p taskPaths) promptMD() string         { return filepath.Join(p.root, "prompt.md") }
func (p taskPaths) inputTXT() string         { return filepath.Join(p.root, "input.txt") }
func (p taskPaths) combinedPromptMD() string { return filepath.Join(p.root, "combined-prompt.md") }
func (p taskPaths) lastMessageMD() string    { return filepath.Join(p.root, "last-message.md") }
func (p taskPaths) exitCode() string         { return filepath.Join(p.root, "exit-code") }
func (p taskPaths) stdoutLog() string        { return filepath.Join(p.root, "stdout.log") }
func (p taskPaths) stderrLog() string        { return filepath.Join(p.root, "stderr.log") }
func (p taskPaths) partialOutputMD() string  { return filepath.Join(p.root, "partial-output.md") }
func (p taskPaths) recoveredAfterTimeout() string {
	return filepath.Join(p.root, "recovered-after-timeout")
}
func (p taskPaths) adoptedAfterRestart() string {
	return filepath.Join(p.root, "adopted-after-restart")
}
func openTaskDir(path string) (*os.File, error) {
	f, e := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if os.IsNotExist(e) {
		return nil, nil
	}
	if e != nil {
		return nil, fmt.Errorf("refusing to open task dir at %s: %w", path, e)
	}
	i, e := f.Stat()
	if e != nil {
		f.Close()
		return nil, e
	}
	if !i.IsDir() {
		f.Close()
		return nil, fmt.Errorf("task dir path is not a directory: %s", path)
	}
	return f, nil
}
func taskPath(r string, id domain.TaskID, g func(taskPaths) string) (string, error) {
	p, e := newTaskPaths(r, id)
	if e != nil {
		return "", e
	}
	return g(p), nil
}
func EventsJSONLPath(r string, id domain.TaskID) (string, error) {
	return taskPath(r, id, taskPaths.eventsJSONL)
}
func PromptMDPath(r string, id domain.TaskID) (string, error) {
	return taskPath(r, id, taskPaths.promptMD)
}
func InputTXTPath(r string, id domain.TaskID) (string, error) {
	return taskPath(r, id, taskPaths.inputTXT)
}
func CombinedPromptMDPath(r string, id domain.TaskID) (string, error) {
	return taskPath(r, id, taskPaths.combinedPromptMD)
}
func ExitCodePath(r string, id domain.TaskID) (string, error) {
	return taskPath(r, id, taskPaths.exitCode)
}
func StdoutLogPath(r string, id domain.TaskID) (string, error) {
	return taskPath(r, id, taskPaths.stdoutLog)
}
func StderrLogPath(r string, id domain.TaskID) (string, error) {
	return taskPath(r, id, taskPaths.stderrLog)
}
func PartialOutputMDPath(r string, id domain.TaskID) (string, error) {
	return taskPath(r, id, taskPaths.partialOutputMD)
}
func RecoveredAfterTimeoutPath(r string, id domain.TaskID) (string, error) {
	return taskPath(r, id, taskPaths.recoveredAfterTimeout)
}
func AdoptedAfterRestartPath(r string, id domain.TaskID) (string, error) {
	return taskPath(r, id, taskPaths.adoptedAfterRestart)
}
