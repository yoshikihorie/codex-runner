//go:build darwin

package proc

import (
	"context"
	"errors"
	"syscall"
	"testing"
)

func TestWatchExitWithoutReapingRejectsInvalidPID(t *testing.T) {
	watcher, err := WatchExitWithoutReaping(1)
	if err == nil {
		t.Fatal("invalid PID was accepted")
	}
	if watcher != nil {
		t.Fatal("invalid PID returned a watcher")
	}
}

func TestExitWatcherWaitPrefersExitOverWakeup(t *testing.T) {
	originalKevent := syscallKevent
	t.Cleanup(func() { syscallKevent = originalKevent })
	syscallKevent = func(_ int, _ []syscall.Kevent_t, events []syscall.Kevent_t, _ *syscall.Timespec) (int, error) {
		if len(events) == 0 {
			return 0, nil
		}
		events[0] = syscall.Kevent_t{Ident: 42, Filter: syscall.EVFILT_PROC, Fflags: syscall.NOTE_EXIT}
		events[1] = syscall.Kevent_t{Ident: exitWatcherWakeIdent, Filter: syscall.EVFILT_USER}
		return 2, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &exitWatcher{kq: 9, pid: 42}
	if err := w.Wait(ctx); err != nil {
		t.Fatalf("Wait() error = %v, want nil", err)
	}
	if err := w.Wait(context.Background()); err != nil {
		t.Fatalf("second Wait() error = %v, want cached exit", err)
	}
}

func TestExitWatcherRetriesInterruptedKqueueAndKevent(t *testing.T) {
	originalKqueue := syscallKqueue
	originalKevent := syscallKevent
	originalClose := syscallClose
	t.Cleanup(func() {
		syscallKqueue = originalKqueue
		syscallKevent = originalKevent
		syscallClose = originalClose
	})
	kqueueCalls := 0
	syscallKqueue = func() (int, error) {
		kqueueCalls++
		if kqueueCalls == 1 {
			return -1, syscall.EINTR
		}
		return 9, nil
	}
	keventCalls := 0
	syscallKevent = func(_ int, _ []syscall.Kevent_t, _ []syscall.Kevent_t, _ *syscall.Timespec) (int, error) {
		keventCalls++
		if keventCalls == 1 {
			return 0, syscall.EINTR
		}
		return 0, nil
	}
	syscallClose = func(int) error { return nil }
	w, err := WatchExitWithoutReaping(42)
	if err != nil {
		t.Fatal(err)
	}
	if kqueueCalls != 2 || keventCalls != 2 {
		t.Fatalf("kqueue=%d kevent=%d", kqueueCalls, keventCalls)
	}
	if err := w.Close(); err != nil && !errors.Is(err, syscall.EINTR) {
		t.Fatal(err)
	}
}
