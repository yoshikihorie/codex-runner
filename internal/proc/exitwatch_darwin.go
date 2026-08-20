//go:build darwin

package proc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
)

const exitWatcherWakeIdent = 1

var (
	syscallKqueue = syscall.Kqueue
	syscallKevent = syscall.Kevent
	syscallClose  = syscall.Close

	errExitWatcherClosed = errors.New("exit watcher is closed")
)

// ExitWatcher observes a process exit without reaping it.
type ExitWatcher interface {
	Wait(context.Context) error
	Close() error
}

type exitWatcher struct {
	mu        sync.Mutex
	kq        int
	pid       int
	closed    bool
	exited    bool
	waiting   bool
	waiters   sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// WatchExitWithoutReaping registers a Darwin kqueue NOTE_EXIT observer for pid.
func WatchExitWithoutReaping(pid int) (ExitWatcher, error) {
	if pid <= 1 {
		return nil, fmt.Errorf("invalid pid %d: must be greater than 1", pid)
	}
	kq, err := kqueueRetry()
	if err != nil {
		return nil, fmt.Errorf("create exit watcher kqueue: %w", err)
	}
	w := &exitWatcher{kq: kq, pid: pid}
	changes := []syscall.Kevent_t{
		{Ident: uint64(pid), Filter: syscall.EVFILT_PROC, Flags: syscall.EV_ADD | syscall.EV_ONESHOT, Fflags: syscall.NOTE_EXIT},
		{Ident: exitWatcherWakeIdent, Filter: syscall.EVFILT_USER, Flags: syscall.EV_ADD | syscall.EV_CLEAR},
	}
	if err := keventRetry(kq, changes, nil, nil); err != nil {
		// Do not retry close on EINTR: the descriptor could already be closed.
		_ = syscallClose(kq)
		return nil, fmt.Errorf("register exit watcher events: %w", err)
	}
	return w, nil
}

func kqueueRetry() (int, error) {
	for {
		fd, err := syscallKqueue()
		if !errors.Is(err, syscall.EINTR) {
			return fd, err
		}
	}
}

func keventRetry(kq int, changes, events []syscall.Kevent_t, timeout *syscall.Timespec) error {
	for {
		_, err := syscallKevent(kq, changes, events, timeout)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

func keventWaitRetry(kq int, events []syscall.Kevent_t) (int, error) {
	for {
		n, err := syscallKevent(kq, nil, events, nil)
		if !errors.Is(err, syscall.EINTR) {
			return n, err
		}
	}
}

func (w *exitWatcher) Wait(ctx context.Context) error {
	w.mu.Lock()
	if w.exited {
		w.mu.Unlock()
		return nil
	}
	if w.closed {
		w.mu.Unlock()
		return errExitWatcherClosed
	}
	if w.waiting {
		w.mu.Unlock()
		return errors.New("exit watcher already has an active wait")
	}
	w.waiting = true
	w.waiters.Add(1)
	fd := w.kq
	w.mu.Unlock()

	stopContextWatch := make(chan struct{})
	contextWatchDone := make(chan struct{})
	go func() {
		defer close(contextWatchDone)
		select {
		case <-ctx.Done():
			_ = w.triggerWake(fd)
		case <-stopContextWatch:
		}
	}()
	defer func() {
		close(stopContextWatch)
		<-contextWatchDone
		w.mu.Lock()
		w.waiting = false
		w.mu.Unlock()
		w.waiters.Done()
	}()

	events := make([]syscall.Kevent_t, 2)
	n, err := keventWaitRetry(fd, events)
	if err != nil {
		return fmt.Errorf("wait for process exit: %w", err)
	}
	sawWake := false
	for _, event := range events[:n] {
		switch {
		case isProcessExitEvent(event, w.pid):
			w.mu.Lock()
			w.exited = true
			w.mu.Unlock()
		case isWakeEvent(event):
			sawWake = true
		default:
			return fmt.Errorf("unexpected exit watcher event: %#v", event)
		}
	}
	if w.hasExited() {
		return nil
	}
	if w.isClosed() {
		return errExitWatcherClosed
	}
	if sawWake && ctx.Err() != nil {
		return ctx.Err()
	}
	return errors.New("exit watcher woke without an exit or cancellation")
}

func (w *exitWatcher) Close() error {
	w.closeOnce.Do(func() {
		w.mu.Lock()
		w.closed = true
		fd := w.kq
		w.mu.Unlock()

		wakeErr := w.triggerWake(fd)
		w.waiters.Wait()
		// Do not retry close on EINTR: its outcome is unknown and fd reuse is unsafe.
		closeErr := syscallClose(fd)
		w.closeErr = errors.Join(wakeErr, closeErr)
	})
	return w.closeErr
}

func (w *exitWatcher) triggerWake(fd int) error {
	change := []syscall.Kevent_t{{Ident: exitWatcherWakeIdent, Filter: syscall.EVFILT_USER, Fflags: syscall.NOTE_TRIGGER}}
	return keventRetry(fd, change, nil, nil)
}

func (w *exitWatcher) hasExited() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.exited
}

func (w *exitWatcher) isClosed() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closed
}

func isProcessExitEvent(event syscall.Kevent_t, pid int) bool {
	return event.Ident == uint64(pid) && event.Filter == syscall.EVFILT_PROC && event.Fflags&syscall.NOTE_EXIT != 0
}

func isWakeEvent(event syscall.Kevent_t) bool {
	return event.Ident == exitWatcherWakeIdent && event.Filter == syscall.EVFILT_USER
}
