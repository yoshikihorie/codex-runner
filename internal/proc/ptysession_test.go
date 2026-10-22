//go:build darwin

package proc

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func writeShell(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fake-cli.sh")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"+body), 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

var ptyReadBuffers sync.Map

func readPTYUntil(t *testing.T, session *PTYSession, needle string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var output bytes.Buffer
	if buffered, ok := ptyReadBuffers.LoadAndDelete(session); ok {
		output.Write(buffered.([]byte))
	}
	readResult := make(chan struct {
		data []byte
		err  error
	}, 1)
	for {
		if index := strings.Index(output.String(), needle); index >= 0 {
			all := output.Bytes()
			end := index + len(needle)
			if end < len(all) {
				ptyReadBuffers.Store(session, append([]byte(nil), all[end:]...))
			}
			return string(all[:end])
		}
		if err := ctx.Err(); err != nil {
			t.Fatalf("waiting for %q: %v; output=%q", needle, err, output.String())
		}
		go func() {
			buf := make([]byte, 512)
			n, err := session.Read(buf)
			readResult <- struct {
				data []byte
				err  error
			}{data: buf[:n], err: err}
		}()
		select {
		case result := <-readResult:
			if len(result.data) > 0 {
				output.Write(result.data)
			}
			if result.err != nil {
				t.Fatalf("PTY read: %v; output=%q", result.err, output.String())
			}
		case <-ctx.Done():
			t.Fatalf("waiting for %q: %v; output=%q", needle, ctx.Err(), output.String())
		}
	}
}

func TestPTYSessionWriteLineResizeWaitAndControllingTerminal(t *testing.T) {
	script := writeShell(t, "stty -echo -icanon -icrnl min 1 time 0\nprintf 'READY:%s:%s\\n' \"$$\" \"$(tty)\"\n text=$(dd bs=1 count=4 2>/dev/null | od -An -tx1 | tr -d ' \\n'); printf 'TEXT:%s\\n' \"$text\"\n enter=$(dd bs=1 count=1 2>/dev/null | od -An -tx1 | tr -d ' \\n'); printf 'ENTER:%s\\nDONE\\n' \"$enter\"\nexit 7\n")
	env := BuildSessionEnv([]string{"PATH=/bin:/usr/bin"})
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", script}, t.TempDir(), env, 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if session.Pid() <= 0 {
		t.Fatalf("invalid child pid: %d", session.Pid())
	}
	initial := readPTYUntil(t, session, "\r\n")
	fields := strings.Split(strings.TrimSpace(strings.SplitN(initial, "READY:", 2)[1]), ":")
	sid, _, errno := syscall.Syscall(syscall.SYS_GETSID, uintptr(session.Pid()), 0, 0)
	if errno != 0 || int(sid) != session.Pid() || len(fields) != 2 || fields[1] == "not a tty" {
		t.Fatalf("child is not session leader with controlling terminal: %q", initial)
	}
	if err := session.WriteLine(context.Background(), "bad\n", 20*time.Millisecond, nil); err == nil {
		t.Fatal("WriteLine accepted embedded LF")
	}
	writeDone := make(chan error, 1)
	go func() { writeDone <- session.WriteLine(context.Background(), "ping", 100*time.Millisecond, nil) }()
	textOutput := readPTYUntil(t, session, "TEXT:70696e67")
	_ = textOutput
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	output := readPTYUntil(t, session, "DONE")
	if !strings.Contains(output, "ENTER:0d") {
		t.Fatalf("received enter bytes=%q, want CR only", output)
	}
	code, err := session.Wait()
	if err != nil || code != 7 {
		t.Fatalf("Wait()=(%d,%v), want (7,nil)", code, err)
	}
}

func TestPTYSessionResizeAndCloseWait(t *testing.T) {
	script := writeShell(t, `stty -echo
printf 'SIZE:'; stty size
while [ "$(stty size)" != "40 100" ]; do sleep 0.05; done
printf 'RESIZED:'; stty size
`)
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", script}, t.TempDir(), BuildSessionEnv([]string{"PATH=/bin:/usr/bin"}), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	initial := readPTYUntil(t, session, "SIZE:24 80")
	if !strings.Contains(initial, "SIZE:24 80") {
		t.Fatalf("initial size missing: %q", initial)
	}
	if err := session.Resize(100, 40); err != nil {
		t.Fatal(err)
	}
	resized := readPTYUntil(t, session, "RESIZED:40 100")
	if !strings.Contains(resized, "RESIZED:40 100") {
		t.Fatalf("resized size missing: %q", resized)
	}
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _, _ = session.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after Close")
	}
}

// Keep a named frame and the goroutine identity so another reader cannot satisfy
// the observation. A notification immediately before Read would not prove a wait.
func ptyTimingBlockedRead(session *PTYSession, identity chan<- string, result chan<- error, exited chan<- struct{}) {
	defer close(exited)
	stack := make([]byte, 1024)
	n := runtime.Stack(stack, false)
	identity <- strings.Fields(string(stack[:n]))[1]
	_, err := session.Read(make([]byte, 32))
	result <- err
}

func awaitPTYReadWait(t *testing.T, id string) {
	t.Helper()
	watchdog := time.NewTimer(ptyCloseWatchdog)
	defer watchdog.Stop()
	var last string
	for {
		stack := make([]byte, 1<<20)
		n := runtime.Stack(stack, true)
		for _, entry := range strings.Split(string(stack[:n]), "\n\n") {
			if !strings.HasPrefix(entry, "goroutine "+id+" [") {
				continue
			}
			last = entry
			header := strings.SplitN(entry, "\n", 2)[0]
			state := strings.SplitN(header, "[", 2)[1]
			if strings.HasPrefix(state, "IO wait") && strings.Contains(entry, "ptyTimingBlockedRead(") && strings.Contains(entry, "(*PTYSession).Read(") {
				return
			}
		}
		select {
		case <-watchdog.C:
			t.Fatalf("Read did not reach I/O wait: %s", last)
		default:
			runtime.Gosched()
		}
	}
}

func TestPTYSessionCloseUnblocksReadAndIsIdempotent(t *testing.T) {
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", "-c", "printf 'READY\\n'; read gate"}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	trackPTYCloseChild(t, session)
	readPTYCloseMarker(t, session, bufio.NewReader(session), "READY")
	identity := make(chan string, 1)
	readResult := make(chan error, 1)
	exited := make(chan struct{})
	go ptyTimingBlockedRead(session, identity, readResult, exited)
	defer func() { _ = session.Close(); awaitPTYClose(t, exited, "reader cleanup") }()
	awaitPTYReadWait(t, awaitPTYClose(t, identity, "reader identity"))
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := awaitPTYClose(t, readResult, "Read after Close"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Read after Close: %v", err)
	}
	if err := session.Resize(100, 40); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Resize after Close: %v", err)
	}
	if _, err := session.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Write after Close: %v", err)
	}
}

// Test-only hang watchdog, independent of production delays.
const ptyCloseWatchdog = 10 * time.Second
const ptyCloseWriteDelay = time.Hour

func awaitPTYClose[T any](t *testing.T, ch <-chan T, operation string) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(ptyCloseWatchdog):
		t.Fatalf("watchdog waiting for %s", operation)
		var zero T
		return zero
	}
}

// Read synchronously on the test goroutine, bounded by the PTY poller deadline.
func readPTYCloseMarker(t *testing.T, session *PTYSession, reader *bufio.Reader, marker string) {
	t.Helper()
	if err := session.master.SetReadDeadline(time.Now().Add(ptyCloseWatchdog)); err != nil {
		t.Fatal(err)
	}
	line, err := reader.ReadString('\n')
	if err != nil || strings.TrimSpace(line) != marker {
		t.Fatalf("watchdog/PTY marker %q: line=%q err=%v", marker, line, err)
	}
}

func trackPTYCloseChild(t *testing.T, session *PTYSession) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() { _, _ = session.Wait(); close(done) }()
	t.Cleanup(func() {
		select {
		case <-done:
			// Do not signal a PID after its child has already been reaped.
		default:
			_ = session.Kill(syscall.SIGKILL)
		}
		_ = session.Close()
		awaitPTYClose(t, done, "child cleanup")
	})
	return done
}

func TestPTYSessionCloseLeavesHUPTrappingChildForExplicitKill(t *testing.T) {
	// A FIFO provides a response path independent of the closed terminal.
	replyPath := filepath.Join(t.TempDir(), "alive")
	if err := syscall.Mkfifo(replyPath, 0o600); err != nil {
		t.Fatal(err)
	}
	reply, err := os.OpenFile(replyPath, os.O_RDWR|syscall.O_NONBLOCK, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reply.Close()
	script := writeShell(t, `trap '' HUP
trap 'printf ALIVE > "$1"' USR1
printf 'READY\n'
while :; do sleep 1 & wait $!; done
`)
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", script, replyPath}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	done := trackPTYCloseChild(t, session)
	readPTYCloseMarker(t, session, bufio.NewReader(session), "READY")
	closeDone := make(chan error, 1)
	go func() { closeDone <- session.Close() }()
	if err := awaitPTYClose(t, closeDone, "Close"); err != nil {
		t.Fatal(err)
	}

	// Only the shell receives USR1; its trap runs while the background sleep is waited on.
	if err := syscall.Kill(session.Pid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	// Darwin FIFOs do not support os.File deadlines. Select bounds this independent read.
	fd := int(reply.Fd())
	var readable syscall.FdSet
	const fdSetWordBits = 32
	if fd >= len(readable.Bits)*fdSetWordBits {
		t.Fatal("FIFO descriptor exceeds select capacity")
	}
	deadline := time.Now().Add(ptyCloseWatchdog)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			t.Fatal("watchdog waiting for independent child reply")
		}
		readable = syscall.FdSet{}
		readable.Bits[fd/fdSetWordBits] |= 1 << uint(fd%fdSetWordBits)
		timeout := syscall.NsecToTimeval(remaining.Nanoseconds())
		err := syscall.Select(fd+1, &readable, nil, nil, &timeout)
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if readable.Bits[fd/fdSetWordBits]&(1<<uint(fd%fdSetWordBits)) == 0 {
			t.Fatal("watchdog waiting for independent child reply")
		}
		break
	}
	response := make([]byte, len("ALIVE"))
	if _, err := io.ReadFull(reply, response); err != nil {
		t.Fatalf("watchdog waiting for independent child reply: %v", err)
	}
	if string(response) != "ALIVE" {
		t.Fatalf("child reply=%q", response)
	}
	if err := session.Kill(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	awaitPTYClose(t, done, "explicit kill and Wait")
	if code, err := session.Wait(); code != -1 || err != nil {
		t.Fatalf("Wait=(%d,%v), want killed child", code, err)
	}
}

func TestPTYSessionConcurrentWaitReturnsSameResult(t *testing.T) {
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", "-c", "exit 7"}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	const n = 8
	results := make(chan struct {
		code int
		err  error
	}, n)
	for i := 0; i < n; i++ {
		go func() {
			code, err := session.Wait()
			results <- struct {
				code int
				err  error
			}{code, err}
		}()
	}
	for i := 0; i < n; i++ {
		got := <-results
		if got.code != 7 || got.err != nil {
			t.Fatalf("Wait=(%d,%v)", got.code, got.err)
		}
	}
}

func TestStartPTYSessionRequiresArgvZeroAndAbsoluteExecutableAndCWD(t *testing.T) {
	env := BuildSessionEnv(nil)
	cwd := t.TempDir()
	for _, tc := range []struct {
		executable string
		argv       []string
		cwd        string
	}{{"/bin/sh", []string{"sh"}, cwd}, {"sh", []string{"sh"}, cwd}, {"/bin/sh", []string{"/bin/sh"}, "relative"}} {
		if _, err := StartPTYSession(tc.executable, tc.argv, tc.cwd, env, 80, 24); err == nil {
			t.Errorf("accepted executable=%q argv=%q cwd=%q", tc.executable, tc.argv, tc.cwd)
		}
	}
}

func TestPTYSessionWriteLineHonorsAbortAndContext(t *testing.T) {
	script := writeShell(t, "stty -echo -icanon min 1 time 0\nprintf 'READY\\n'\nb=$(dd bs=1 count=1 2>/dev/null | od -An -tx1 | tr -d ' \\n'); printf 'BYTE:%s\\n' \"$b\"\nsleep 2\n")
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", script}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	_ = readPTYUntil(t, session, "READY")
	err = session.WriteLine(context.Background(), "x", 0, func() bool { return false })
	if !errors.Is(err, ErrBeforeEnter) {
		t.Fatalf("WriteLine error=%v", err)
	}
	_ = readPTYUntil(t, session, "BYTE:78")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := session.WriteLine(ctx, "y", time.Second, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled WriteLine=%v", err)
	}
}

func TestPTYSessionChildHasNoExtraPTYMasterFD(t *testing.T) {
	script := writeShell(t, `stty -echo
/bin/ls -l /dev/fd | grep -E '/dev/(ptmx|pty[a-zA-Z0-9]+)' || true
printf 'FD-CHECK-DONE\n'`)
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", script}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	output := readPTYUntil(t, session, "FD-CHECK-DONE")
	if strings.Contains(output, "/dev/ptmx") || strings.Contains(output, "/dev/pty") {
		t.Fatalf("child inherited an extra PTY descriptor: %q", output)
	}
}

func TestStartPTYSessionFailuresDoNotLeakFDs(t *testing.T) {
	cwd := t.TempDir()
	validEnv := BuildSessionEnv(nil)
	forbidden := append(append([]string(nil), validEnv...), "ANTHROPIC_API_KEY=dummy")
	duplicate := append(append([]string(nil), validEnv...), "DUPLICATE=x", "DUPLICATE=y")
	cases := []struct {
		name, executable string
		argv, env        []string
		cwd              string
	}{
		{"relative executable", "sh", []string{"sh"}, validEnv, cwd},
		{"relative cwd", "/bin/sh", []string{"/bin/sh"}, validEnv, "relative"},
		{"argv zero mismatch", "/bin/sh", []string{"sh"}, validEnv, cwd},
		{"forbidden environment", "/bin/sh", []string{"/bin/sh"}, forbidden, cwd},
		{"duplicate environment", "/bin/sh", []string{"/bin/sh"}, duplicate, cwd},
		{"missing executable", filepath.Join(cwd, "missing"), []string{filepath.Join(cwd, "missing")}, validEnv, cwd},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := fdCount(t)
			_, err := StartPTYSession(tc.executable, tc.argv, tc.cwd, tc.env, 80, 24)
			if err == nil {
				t.Fatal("expected startup failure")
			}
			after := fdCount(t)
			if after > before {
				t.Fatalf("fd count grew: before=%d after=%d", before, after)
			}
		})
	}
}

func fdCount(t *testing.T) int {
	t.Helper()
	dir, err := os.Open("/dev/fd")
	if err != nil {
		t.Fatal(err)
	}
	entries, err := dir.Readdirnames(-1)
	_ = dir.Close()
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func TestPTYSessionChildEnvironmentAndArgv(t *testing.T) {
	blocked := make([]string, 0, len(strippedSessionEnvNames))
	for name := range strippedSessionEnvNames {
		blocked = append(blocked, name+"=dummy")
	}
	parent := append(os.Environ(), blocked...)
	env := BuildSessionEnv(parent)
	script := writeShell(t, `printf 'ZERO=%s\nONE=%s\n' "$0" "$1"; env; printf 'ENV-DONE\n'`)
	session, err := StartPTYSession(script, []string{script, "first-argument"}, t.TempDir(), env, 91, 37)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	output := readPTYUntil(t, session, "ENV-DONE")
	if !strings.Contains(output, "ZERO="+script+"\r\nONE=first-argument\r\n") {
		t.Fatalf("unexpected child argv: %q", output)
	}
	got := make(map[string]string)
	for _, line := range strings.Split(output, "\r\n") {
		key, value, ok := strings.Cut(line, "=")
		if ok {
			got[key] = value
		}
	}
	for key, want := range map[string]string{"TERM": "xterm-256color", "CODEXD_SUPERVISED": "1", "COLUMNS": "91", "LINES": "37"} {
		if got[key] != want {
			t.Errorf("%s=%q, want %q", key, got[key], want)
		}
	}
	for name := range strippedSessionEnvNames {
		if _, exists := got[name]; exists {
			t.Errorf("child retained stripped environment key %s", name)
		}
	}
}

func TestPTYSessionWriteLineCRArrivesAfterDelayAsSingleByte(t *testing.T) {
	for _, callback := range []bool{true, false} {
		name := "without callback"
		if callback {
			name = "with callback"
		}
		t.Run(name, func(t *testing.T) {
			script := writeShell(t, `stty raw -echo
printf 'READY\n'
text=$(dd bs=1 count=4 2>/dev/null | od -An -tx1 | tr -d ' \n')
printf 'BODY:%s\n' "$text"
if [ "$1" = callback ]; then
 read gate < "$2"
 stty min 0 time 0
 early=$(dd bs=1 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n')
 printf 'BEFORE:%s\n' "$early"
 stty min 1 time 0
 printf 'GATED\n'
fi
enter=$(dd bs=1 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n')
printf 'ENTER:%s\n' "$enter"
dd bs=1 count=1 >/dev/null 2>&1
`)
			var check *os.File
			argv := []string{"/bin/sh", script}
			if callback {
				gatePath := filepath.Join(t.TempDir(), "check-before-enter")
				if err := syscall.Mkfifo(gatePath, 0600); err != nil {
					t.Fatal(err)
				}
				var err error
				check, err = os.OpenFile(gatePath, os.O_RDWR|syscall.O_NONBLOCK, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer check.Close()
				argv = append(argv, "callback", gatePath)
			}
			session, err := StartPTYSession("/bin/sh", argv, t.TempDir(), BuildSessionEnv(nil), 80, 24)
			if err != nil {
				t.Fatal(err)
			}
			childDone := trackPTYCloseChild(t, session)
			reader := bufio.NewReader(session)
			readPTYCloseMarker(t, session, reader, "READY")
			const delay = 50 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			permit := make(chan struct{})
			var release sync.Once
			arrived := make(chan time.Duration, 1)
			writeErr := make(chan error, 1)
			exited := make(chan struct{})
			started := make(chan time.Time, 1)
			go func() {
				defer close(exited)
				start := time.Now()
				started <- start
				var beforeEnter func() bool
				if callback {
					beforeEnter = func() bool {
						arrived <- time.Since(start)
						select {
						case <-permit:
							return true
						case <-ctx.Done():
							return false
						}
					}
				}
				writeErr <- session.WriteLine(ctx, "test", delay, beforeEnter)
			}()
			defer func() {
				cancel()
				release.Do(func() { close(permit) })
				_ = session.Close()
				awaitPTYClose(t, exited, "WriteLine cleanup")
			}()
			start := awaitPTYClose(t, started, "WriteLine start")
			readPTYCloseMarker(t, session, reader, "BODY:74657374")
			if callback {
				if elapsed := awaitPTYClose(t, arrived, "beforeEnter"); elapsed < delay {
					t.Fatalf("beforeEnter after %s, want at least %s", elapsed, delay)
				}
				// Ask the child to inspect its input while beforeEnter remains gated.
				if _, err := io.WriteString(check, "inspect\n"); err != nil {
					t.Fatal(err)
				}
				readPTYCloseMarker(t, session, reader, "BEFORE:")
				readPTYCloseMarker(t, session, reader, "GATED")
				release.Do(func() { close(permit) })
			}
			readPTYCloseMarker(t, session, reader, "ENTER:0d")
			if elapsed := time.Since(start); elapsed < delay {
				t.Fatalf("CR after %s, want at least %s", elapsed, delay)
			}
			if err := awaitPTYClose(t, writeErr, "WriteLine completion"); err != nil {
				t.Fatal(err)
			}
			if _, err := session.Write([]byte("!")); err != nil {
				t.Fatal(err)
			}
			awaitPTYClose(t, childDone, "explicit release and Wait")
		})
	}
}

func TestPTYSessionChildOwnsProcessGroupAndControllingTerminal(t *testing.T) {
	script := writeShell(t, "stty -echo\n: </dev/tty && printf 'TTY-OK\\n'\nread gate\n")
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", script}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	done := trackPTYCloseChild(t, session)
	readPTYCloseMarker(t, session, bufio.NewReader(session), "TTY-OK")
	pid := session.Pid()
	pgid, err := syscall.Getpgid(pid)
	if err != nil || pgid != pid {
		t.Fatalf("pid=%d pgid=%d err=%v", pid, pgid, err)
	}
	var foreground int32
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, session.master.Fd(), syscall.TIOCGPGRP, uintptr(unsafe.Pointer(&foreground)))
	if errno != 0 || int(foreground) != pgid {
		t.Fatalf("foreground=%d want=%d errno=%v", foreground, pgid, errno)
	}
	if _, err := session.Write([]byte("release\n")); err != nil {
		t.Fatal(err)
	}
	awaitPTYClose(t, done, "released controlling-terminal child")
}

func TestPTYSessionCloseOperations(t *testing.T) {
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", "-c", "sleep 10"}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := session.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := session.Resize(80, 24); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Resize after Close: %v", err)
	}
	if _, err := session.Write([]byte("x")); !errors.Is(err, os.ErrClosed) {
		t.Errorf("Write after Close: %v", err)
	}
	_, _ = session.Wait()
}

func TestPTYSessionSetReadDeadlineAndWriteLineCallbackCanReenter(t *testing.T) {
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", "-c", "sleep 10"}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.master.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("PTY master is not registered with the runtime poller: %v", err)
	}
	if err := session.master.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := session.WriteLine(context.Background(), "x", 0, func() bool {
		if session.Pid() <= 0 {
			t.Error("Pid returned an invalid process ID")
		}
		if err := session.Resize(81, 25); err != nil {
			t.Errorf("Resize from beforeEnter: %v", err)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPTYSessionCloseDoesNotWaitForWriteLineDelay(t *testing.T) {
	script := writeShell(t, `stty -echo -icanon min 1 time 0
printf 'READY\n'
dd bs=1 count=1 >/dev/null 2>&1
printf 'BODY\n'
while :; do sleep 1 & wait $!; done
`)
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", script}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	childDone := trackPTYCloseChild(t, session)
	reader := bufio.NewReader(session)
	readPTYCloseMarker(t, session, reader, "READY")
	ctx, cancel := context.WithCancel(context.Background())
	writeDone := make(chan error, 1)
	writeExited := make(chan struct{})
	go func() { defer close(writeExited); writeDone <- session.WriteLine(ctx, "x", ptyCloseWriteDelay, nil) }()
	defer func() { cancel(); _ = session.Close(); awaitPTYClose(t, writeExited, "writer cleanup") }()
	readPTYCloseMarker(t, session, reader, "BODY")
	closeDone := make(chan error, 1)
	go func() { closeDone <- session.Close() }()
	if err := awaitPTYClose(t, closeDone, "Close during WriteLine delay"); err != nil {
		t.Fatal(err)
	}
	if err := awaitPTYClose(t, writeDone, "WriteLine after Close"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("WriteLine=%v, want os.ErrClosed", err)
	}
	if err := ctx.Err(); err != nil {
		t.Fatalf("WriteLine context ended: %v", err)
	}
	awaitPTYClose(t, writeExited, "writer completion")
	awaitPTYClose(t, childDone, "child Wait after Close")
	if _, err := session.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestPTYSessionCanceledWriteLineSendsNoBytesAtZeroDelay(t *testing.T) {
	script := writeShell(t, `stty -echo -icanon min 0 time 10
printf 'READY\n'
byte=$(dd bs=1 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n')
printf 'BYTE:%s\nDONE\n' "$byte"`)
	session, err := StartPTYSession(script, []string{script}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	_ = readPTYUntil(t, session, "READY")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := session.WriteLine(ctx, "must-not-arrive", 0, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled WriteLine error=%v", err)
	}
	output := readPTYUntil(t, session, "DONE")
	if strings.Contains(output, "BYTE:6d") || strings.Contains(output, "BYTE:0d") {
		t.Fatalf("canceled WriteLine sent input: %q", output)
	}
}

func TestPTYSessionMasterUsesPollerAndNoBusyLoop(t *testing.T) {
	session, err := StartPTYSession("/bin/sh", []string{"/bin/sh", "-c", "sleep 10"}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	if err := session.master.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	_ = session.master.SetReadDeadline(time.Time{})
}
