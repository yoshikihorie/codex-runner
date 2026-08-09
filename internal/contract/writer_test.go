package contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

func writerID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260806-120000-a1b2-writer")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func writerRoot(t *testing.T) (string, domain.TaskID) {
	t.Helper()
	root, id := t.TempDir(), writerID(t)
	if err := os.Mkdir(filepath.Join(root, id.String()), 0o700); err != nil {
		t.Fatal(err)
	}
	return root, id
}
func readWriterFile(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("content=%q want=%q", got, want)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%o err=%v", info.Mode().Perm(), err)
	}
}
func TestContractWriterWritesAllContractFiles(t *testing.T) {
	root, id := writerRoot(t)
	w := NewFileContractWriter(root, domain.ClockFunc(func() time.Time { return time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC) }))
	for _, tc := range []struct {
		name  string
		write func() error
		path  func(string, domain.TaskID) (string, error)
		want  []byte
	}{
		{"prompt", func() error { return w.WritePrompt(id, []byte("prompt")) }, store.PromptMDPath, []byte("prompt")},
		{"input", func() error { return w.WriteReviewInput(id, []byte("input")) }, store.InputTXTPath, []byte("input")},
		{"combined", func() error { return w.WriteCombinedPrompt(id, []byte("combined")) }, store.CombinedPromptMDPath, []byte("combined")},
		{"exit", func() error { return w.WriteExitCode(id, domain.NewExitCode(124)) }, store.ExitCodePath, []byte("124\n")},
		{"partial", func() error { return w.WritePartialOutput(id, "partial") }, store.PartialOutputMDPath, []byte("partial")},
		{"recovered", func() error { return w.WriteRecoveredMarker(id, time.Date(2026, 8, 6, 12, 0, 1, 0, time.UTC)) }, store.RecoveredAfterTimeoutPath, []byte("2026-08-06T12:00:01+0000\n")},
		{"adopted", func() error { return w.WriteAdoptedMarker(id, time.Date(2026, 8, 6, 12, 0, 2, 0, time.UTC)) }, store.AdoptedAfterRestartPath, []byte("2026-08-06T12:00:02+0000\n")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.write(); err != nil {
				t.Fatal(err)
			}
			p, err := tc.path(root, id)
			if err != nil {
				t.Fatal(err)
			}
			readWriterFile(t, p, tc.want)
		})
	}
	logs, err := w.OpenExecutionLogs(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = logs.Stdout.Write([]byte("out")); err != nil {
		t.Fatal(err)
	}
	if _, err = logs.Stderr.Write([]byte("err")); err != nil {
		t.Fatal(err)
	}
	if err = logs.Close(); err != nil {
		t.Fatal(err)
	}
	stdout, _ := store.StdoutLogPath(root, id)
	stderr, _ := store.StderrLogPath(root, id)
	readWriterFile(t, stdout, []byte("out"))
	readWriterFile(t, stderr, []byte("err"))
	if err := w.AppendRawEvent(id, "raw", json.RawMessage(`{"key":"value"}`)); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendEvent(id, domain.TaskQueued{TaskID: id, Subcommand: domain.SubcommandImpl}); err != nil {
		t.Fatal(err)
	}
	events, _ := store.EventsJSONLPath(root, id)
	data, err := os.ReadFile(events)
	if err != nil || !bytes.Contains(data, []byte(`"event_type":"raw"`)) || !bytes.Contains(data, []byte(`"event_type":"TaskQueued"`)) {
		t.Fatalf("events=%q err=%v", data, err)
	}
}
func TestOpenExecutionLogsOpensBothHandles(t *testing.T) {
	root, id := writerRoot(t)
	logs, err := NewFileContractWriter(root, nil).OpenExecutionLogs(id)
	if err != nil || logs == nil || logs.Stdout == nil || logs.Stderr == nil {
		t.Fatalf("logs=%#v err=%v", logs, err)
	}
	if err := logs.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestOpenExecutionLogsCleansUpOnPartialFailure(t *testing.T) {
	root, id := writerRoot(t)
	stderr, _ := store.StderrLogPath(root, id)
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, stderr); err != nil {
		t.Fatal(err)
	}
	if _, err := NewFileContractWriter(root, nil).OpenExecutionLogs(id); err == nil {
		t.Fatal("partial log opening succeeded")
	}
	// stdout.log is an O_APPEND stream-type file, so the all-or-nothing
	// contract (TASK 811-814) only requires closing its handle when the
	// second open fails; deleting the file itself is out of scope.
	stdout, _ := store.StdoutLogPath(root, id)
	if _, err := os.Stat(stdout); err != nil {
		t.Fatalf("stdout.log should still exist after partial failure: %v", err)
	}
}
func TestContractWriterOnceFilesRejectClobber(t *testing.T) {
	root, id := writerRoot(t)
	w := NewFileContractWriter(root, nil)
	for _, tc := range []struct {
		name  string
		write func() error
	}{
		{"prompt", func() error { return w.WritePrompt(id, []byte("a")) }},
		{"input", func() error { return w.WriteReviewInput(id, []byte("a")) }},
		{"combined", func() error { return w.WriteCombinedPrompt(id, []byte("a")) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.write(); err != nil {
				t.Fatal(err)
			}
			if err := tc.write(); err == nil {
				t.Fatal("second write overwrote once file")
			}
		})
	}
}
func TestContractWriterRejectsSymlinkedContractPath(t *testing.T) {
	root, id := writerRoot(t)
	w := NewFileContractWriter(root, nil)
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, _ := store.PromptMDPath(root, id)
	if err := os.Symlink(target, p); err != nil {
		t.Fatal(err)
	}
	err := w.WritePrompt(id, []byte("attacker"))
	if err == nil || !errors.Is(err, domain.ErrContractWriteFailed) {
		t.Fatalf("symlink write=%v", err)
	}
	readWriterFile(t, target, []byte("unchanged"))
}
func TestContractWriterRejectsSymlinkedTaskDir(t *testing.T) {
	root, id := writerRoot(t)
	p := filepath.Join(root, id.String())
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, p); err != nil {
		t.Fatal(err)
	}
	w := NewFileContractWriter(root, nil)
	for name, write := range map[string]func() error{
		"once":   func() error { return w.WritePrompt(id, []byte("prompt")) },
		"logs":   func() error { _, err := w.OpenExecutionLogs(id); return err },
		"atomic": func() error { return w.WritePartialOutput(id, "partial") },
		"event":  func() error { return w.AppendRawEvent(id, "raw", json.RawMessage(`{}`)) },
	} {
		if err := write(); !errors.Is(err, domain.ErrContractWriteFailed) {
			t.Errorf("%s error=%v", name, err)
		}
	}
}
func TestExecutionLogsCloseNil(t *testing.T) {
	if (&ExecutionLogs{}).Close() != nil {
		t.Fatal("unexpected error")
	}
}
