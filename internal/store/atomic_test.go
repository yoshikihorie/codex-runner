package store

import (
	"bytes"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestWriteAtomicWritesAndReplacesWithRequestedPermissions(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.json")
	if err := WriteAtomic(path, []byte("first"), 0o640); err != nil {
		t.Fatal(err)
	}
	if err := WriteAtomic(path, []byte("second"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "second" {
		t.Fatalf("content = %q", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("permissions = %o", info.Mode().Perm())
	}
}

func TestWriteAtomicReadersNeverObservePartialContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.json")
	contents := [][]byte{bytes.Repeat([]byte("a"), 128*1024), bytes.Repeat([]byte("b"), 128*1024)}
	if err := WriteAtomic(path, contents[0], 0o600); err != nil {
		t.Fatal(err)
	}
	valid := map[string]struct{}{string(contents[0]): {}, string(contents[1]): {}}
	finished := make(chan struct{})
	readerDone := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-finished:
				return
			default:
			}
			data, err := os.ReadFile(path)
			if err != nil {
				errs <- err
				return
			}
			if _, ok := valid[string(data)]; !ok {
				errs <- &partialWriteError{data: data}
				return
			}
		}
	}()
	for index := 0; index < 16; index++ {
		if err := WriteAtomic(path, contents[index%len(contents)], 0o600); err != nil {
			t.Fatal(err)
		}
	}
	close(finished)
	<-readerDone
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
}

func TestWriteAtomicConcurrentWritersNeverExposeIntermediateContent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.json")
	contents := [][]byte{bytes.Repeat([]byte("a"), 96*1024), bytes.Repeat([]byte("b"), 96*1024), bytes.Repeat([]byte("c"), 96*1024)}
	if err := WriteAtomic(path, contents[0], 0o600); err != nil {
		t.Fatal(err)
	}
	valid := map[string]struct{}{}
	for _, content := range contents {
		valid[string(content)] = struct{}{}
	}
	done := make(chan struct{})
	readerDone := make(chan struct{})
	errs := make(chan error, 1+len(contents))
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-done:
				return
			default:
			}
			data, err := os.ReadFile(path)
			if err != nil {
				errs <- err
				return
			}
			if _, ok := valid[string(data)]; !ok {
				errs <- &partialWriteError{data: data}
				return
			}
		}
	}()

	var writers sync.WaitGroup
	for _, content := range contents {
		content := content
		writers.Add(1)
		go func() {
			defer writers.Done()
			for index := 0; index < 12; index++ {
				if err := WriteAtomic(path, content, 0o600); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	writers.Wait()
	close(done)
	<-readerDone
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := valid[string(data)]; !ok {
		t.Fatal(&partialWriteError{data: data})
	}
}

func TestWriteAtomicCleansSameDirectoryTemporaryFileAfterRenameFailure(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := WriteAtomic(path, []byte("content"), 0o600); err == nil {
		t.Fatal("WriteAtomic succeeded for a directory target")
	}
	matches, err := filepath.Glob(filepath.Join(dir, ".tmp-*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("temporary files remain: %v", matches)
	}
}

func TestWriteAtomicReturnsErrorForMissingParent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "task.json")
	if err := WriteAtomic(path, []byte("content"), 0o600); err == nil {
		t.Fatal("WriteAtomic succeeded with a missing parent directory")
	}
}

type partialWriteError struct{ data []byte }

func (e *partialWriteError) Error() string { return "observed partial atomic write" }
