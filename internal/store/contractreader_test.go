package store

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func readerTaskID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260806-120000-a1b2-reader")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func readerDir(t *testing.T) (string, domain.TaskID) {
	t.Helper()
	root, id := t.TempDir(), readerTaskID(t)
	if err := os.Mkdir(filepath.Join(root, id.String()), taskDirPerm); err != nil {
		t.Fatal(err)
	}
	return root, id
}
func TestReadStderrLogExistsAndMissing(t *testing.T) {
	root, id := readerDir(t)
	r := NewFileContractReader(root)
	p, _ := StderrLogPath(root, id)
	if err := os.WriteFile(p, []byte("stderr"), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadStderrLog(id)
	if err != nil || !bytes.Equal(got, []byte("stderr")) {
		t.Fatalf("got=%q err=%v", got, err)
	}
	missing, err := domain.NewTaskID("impl-20260806-120000-a1b2-missing")
	if err != nil {
		t.Fatal(err)
	}
	got, err = r.ReadStderrLog(missing)
	if err != nil || got != nil {
		t.Fatalf("missing=%q err=%v", got, err)
	}
}
func TestContractReaderRejectsSymlinkedFile(t *testing.T) {
	root, id := readerDir(t)
	r := NewFileContractReader(root)
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("secret"), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	p, _ := StderrLogPath(root, id)
	if err := os.Symlink(target, p); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ReadStderrLog(id); err == nil {
		t.Fatal("ReadStderrLog followed symlink")
	}
}
func TestReadLastMessagePresenceAndContent(t *testing.T) {
	root, id := readerDir(t)
	r := NewFileContractReader(root)
	p := filepath.Join(root, id.String(), "last-message.md")
	present, err := r.ReadLastMessage(id)
	if err != nil || present {
		t.Fatalf("missing = %t, %v", present, err)
	}
	if err := os.WriteFile(p, nil, taskFilePerm); err != nil {
		t.Fatal(err)
	}
	present, err = r.ReadLastMessage(id)
	if err != nil || present {
		t.Fatalf("empty = %t, %v", present, err)
	}
	if err := os.WriteFile(p, []byte(" \t\n "), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	present, err = r.ReadLastMessage(id)
	if err != nil || present {
		t.Fatalf("whitespace = %t, %v", present, err)
	}
	want := []byte("last message\n")
	if err := os.WriteFile(p, want, taskFilePerm); err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadLastMessageContent(id)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("content=%q err=%v", got, err)
	}
	present, err = r.ReadLastMessage(id)
	if err != nil || !present {
		t.Fatalf("present = %t, %v", present, err)
	}
}
func TestReadPromptContentAndExitCode(t *testing.T) {
	root, id := readerDir(t)
	r := NewFileContractReader(root)
	prompt, _ := PromptMDPath(root, id)
	if err := os.WriteFile(prompt, []byte{0, 1, 2}, taskFilePerm); err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadPromptContent(id)
	if err != nil || !bytes.Equal(got, []byte{0, 1, 2}) {
		t.Fatalf("prompt=%v err=%v", got, err)
	}
	code, _ := ExitCodePath(root, id)
	if err := os.WriteFile(code, []byte("124\n"), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	v, ok, err := r.ReadExitCode(id)
	if err != nil || !ok || v != 124 {
		t.Fatalf("exit=(%d,%t,%v)", v, ok, err)
	}
	if err := os.Remove(code); err != nil {
		t.Fatal(err)
	}
	_, ok, err = r.ReadExitCode(id)
	if err != nil || ok {
		t.Fatalf("missing exit=(%t,%v)", ok, err)
	}
	if err := os.WriteFile(code, []byte("bad"), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := r.ReadExitCode(id); err == nil || !ok {
		t.Fatalf("malformed exit=(%t,%v)", ok, err)
	}
}
func TestReadPartialOutputContent(t *testing.T) {
	root, id := readerDir(t)
	r := NewFileContractReader(root)
	p, err := PartialOutputMDPath(root, id)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0, 'x', '\n', 0xff}
	if err := os.WriteFile(p, want, taskFilePerm); err != nil {
		t.Fatal(err)
	}
	got, err := r.ReadPartialOutputContent(id)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatalf("content=%v err=%v", got, err)
	}
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
	got, err = r.ReadPartialOutputContent(id)
	if err != nil || got != nil {
		t.Fatalf("missing=%v err=%v", got, err)
	}
	if err := os.WriteFile(p, nil, taskFilePerm); err != nil {
		t.Fatal(err)
	}
	got, err = r.ReadPartialOutputContent(id)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("empty=%v err=%v", got, err)
	}
	if _, err := r.ReadPartialOutputContent(domain.TaskID{}); err == nil {
		t.Fatal("zero task ID was accepted")
	}
}
func TestContractReaderRejectsSymlinkedContractPaths(t *testing.T) {
	root, id := readerDir(t)
	r := NewFileContractReader(root)
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, []byte("x"), taskFilePerm); err != nil {
		t.Fatal(err)
	}
	for name, path := range map[string]func(string, domain.TaskID) (string, error){"prompt": PromptMDPath, "last": func(root string, id domain.TaskID) (string, error) {
		p, e := newTaskPaths(root, id)
		return p.lastMessageMD(), e
	}, "partial": PartialOutputMDPath, "exit": ExitCodePath} {
		p, err := path(root, id)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, p); err != nil {
			t.Fatal(err)
		}
		switch name {
		case "prompt":
			_, err = r.ReadPromptContent(id)
		case "last":
			_, err = r.ReadLastMessageContent(id)
		case "partial":
			_, err = r.ReadPartialOutputContent(id)
		case "exit":
			_, _, err = r.ReadExitCode(id)
		}
		if err == nil {
			t.Errorf("%s followed symlink", name)
		}
		if err := os.Remove(p); err != nil {
			t.Fatal(err)
		}
	}
}
