package proc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// PTYSession owns a child process attached to a Darwin pseudo-terminal.
type PTYSession struct {
	mu          sync.Mutex
	writeMu     sync.Mutex
	closed      bool
	sending     bool
	closeCh     chan struct{}
	master      *os.File
	cmd         *exec.Cmd
	waitOnce    sync.Once
	processDone chan struct{}
	waitCode    int
	waitErr     error
}

// StartPTYSession starts an absolute executable in a new session with a controlling PTY.
func StartPTYSession(executable string, argv []string, cwd string, env []string, cols, rows uint16) (*PTYSession, error) {
	if !filepath.IsAbs(executable) {
		return nil, errors.New("executable must be absolute")
	}
	if !filepath.IsAbs(cwd) {
		return nil, errors.New("cwd must be absolute")
	}
	if len(argv) == 0 || argv[0] != executable {
		return nil, errors.New("argv[0] must equal executable")
	}
	if err := ValidateSessionEnv(env); err != nil {
		return nil, fmt.Errorf("invalid session environment: %w", err)
	}
	env = setSessionPTYEnv(env, cols, rows)
	session, err := startPTYSessionDarwin(executable, argv, cwd, env, cols, rows)
	if err == nil {
		session.processDone = make(chan struct{})
		session.closeCh = make(chan struct{})
	}
	return session, err
}

func setSessionPTYEnv(env []string, cols, rows uint16) []string {
	return mergeSessionEnv(env, []string{"TERM=" + sessionTerm, "CODEXD_SUPERVISED=" + sessionMarker, "COLUMNS=" + strconv.Itoa(int(cols)), "LINES=" + strconv.Itoa(int(rows))})
}

// Read reads output from the child process.
func (s *PTYSession) Read(p []byte) (int, error) {
	n, err := s.master.Read(p)
	if errors.Is(err, syscall.EIO) {
		return 0, io.EOF
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed && errors.Is(err, os.ErrClosed) {
		return 0, os.ErrClosed
	}
	return n, err
}

// Write writes bytes to the child unless a WriteLine is in progress.
func (s *PTYSession) Write(p []byte) (int, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.mu.Lock()
	closed, sending := s.closed, s.sending
	s.mu.Unlock()
	if closed {
		return 0, os.ErrClosed
	}
	if sending {
		return 0, errors.New("PTY line write in progress")
	}
	return s.master.Write(p)
}

// ErrBeforeEnter reports that the optional before-enter callback canceled a line.
var ErrBeforeEnter = errors.New("PTY line aborted before enter")

// WriteLine sends text and then a distinct CR write after delay. LF is never sent.
func (s *PTYSession) WriteLine(ctx context.Context, text string, delay time.Duration, beforeEnter func() bool) error {
	if strings.ContainsAny(text, "\r\n") {
		return errors.New("line text must not contain CR or LF")
	}
	if delay < 0 {
		return errors.New("delay must not be negative")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return os.ErrClosed
	}
	if s.sending {
		s.mu.Unlock()
		return errors.New("PTY line write in progress")
	}
	s.sending = true
	closeCh := s.closeCh
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.sending = false; s.mu.Unlock() }()
	s.writeMu.Lock()
	_, err := io.WriteString(s.master, text)
	s.writeMu.Unlock()
	if err != nil {
		return err
	}
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-closeCh:
			return os.ErrClosed
		case <-timer.C:
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-closeCh:
		return os.ErrClosed
	default:
	}
	if beforeEnter != nil && !beforeEnter() {
		return ErrBeforeEnter
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-closeCh:
		return os.ErrClosed
	default:
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err = s.master.Write([]byte{'\r'})
	return err
}

// WriteEnter sends a carriage return to the child.
func (s *PTYSession) WriteEnter() (int, error) { return s.Write([]byte{'\r'}) }

// Resize changes the terminal dimensions.
func (s *PTYSession) Resize(cols, rows uint16) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return os.ErrClosed
	}
	return resizePTYDarwin(s.master, cols, rows)
}

// Pid returns the child process ID.
func (s *PTYSession) Pid() int {
	if s.cmd.Process == nil {
		return 0
	}
	return s.cmd.Process.Pid
}

// Wait returns the child's exit code. Repeated calls return the same result.
func (s *PTYSession) Wait() (int, error) {
	s.waitOnce.Do(func() {
		defer close(s.processDone)
		err := s.cmd.Wait()
		if err == nil {
			return
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			s.waitCode = exit.ExitCode()
			return
		}
		s.waitErr = err
	})
	return s.waitCode, s.waitErr
}

// Close closes the PTY master; the child observes terminal hangup.
// Close closes the master. Repeated calls return nil.
func (s *PTYSession) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	close(s.closeCh)
	err := s.master.Close()
	s.mu.Unlock()
	return err
}

// Kill sends a signal to the child process group.
func (s *PTYSession) Kill(sig syscall.Signal) error {
	if s.cmd == nil || s.cmd.Process == nil {
		return os.ErrProcessDone
	}
	if err := syscall.Kill(-s.cmd.Process.Pid, sig); err != nil {
		if errors.Is(err, syscall.EPERM) {
			return s.cmd.Process.Signal(sig)
		}
		return err
	}
	return nil
}
