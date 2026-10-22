//go:build darwin

package proc

import (
	"bufio"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestPeerKinfo_SCNsup0544_ParentOffset(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	info, err := processPeerInfo(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if info.PID != os.Getpid() || info.ParentPID != os.Getppid() {
		t.Fatalf("PID=%d parent=%d", info.PID, info.ParentPID)
	}
}

func TestPeerCredentials_SCNsup0533_LocalUIDAndPID(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	path := filepath.Join(home, "peer.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	client, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server, err := listener.AcceptUnix()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	uid, pid, err := PeerCredentials(server)
	if err != nil || uid != os.Geteuid() || pid != os.Getpid() {
		t.Fatalf("uid=%d pid=%d err=%v", uid, pid, err)
	}
}

func TestPeerResumeConditions_SCNsup0544_CurrentProcessProbe(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	_, _, err = PeerResumeConditions(os.Getpid(), map[int]bool{})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPeerKinfo_SCNsup0544_ControllingTTYChildren(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	const gatedChild = "printf 'READY\\n'; IFS= read -r gate && [ \"$gate\" = release ]"
	plain := exec.Command("/bin/sh", "-c", gatedChild)
	plain.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	plain.Env = BuildSessionEnv(nil)
	gate, err := plain.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	ready, err := plain.StdoutPipe()
	if err != nil {
		_ = gate.Close()
		t.Fatal(err)
	}
	if err := plain.Start(); err != nil {
		_ = gate.Close()
		_ = ready.Close()
		t.Fatal(err)
	}
	plainDone := make(chan struct{})
	plainWaiting := false
	// Wait must not close StdoutPipe before READY has been consumed.
	defer func() {
		_ = gate.Close()
		if plainWaiting {
			select {
			case <-plainDone:
			default:
				_ = plain.Process.Kill()
			}
			awaitPTYClose(t, plainDone, "plain child cleanup")
		} else {
			_ = plain.Process.Kill()
			_ = plain.Wait()
		}
		_ = ready.Close()
	}()
	if err := ready.(*os.File).SetReadDeadline(time.Now().Add(ptyCloseWatchdog)); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(ready).ReadString('\n')
	if err != nil || line != "READY\n" {
		t.Fatalf("plain READY=%q err=%v", line, err)
	}
	plainWaiting = true
	var plainWaitErr error
	go func() { plainWaitErr = plain.Wait(); close(plainDone) }()
	plainInfo, err := processPeerInfo(plain.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	plainPGID, err := syscall.Getpgid(plain.Process.Pid)
	if err != nil || plainPGID != plain.Process.Pid {
		t.Fatalf("plain pgid=%d err=%v", plainPGID, err)
	}
	// Do not leave terminal echo queued while waiting for the child to exit.
	pty, err := StartPTYSession("/bin/sh", []string{"/bin/sh", "-c", "stty -echo && " + gatedChild}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	ptyDone := trackPTYCloseChild(t, pty)
	reader := bufio.NewReader(pty)
	readPTYCloseMarker(t, pty, reader, "READY")
	ptyInfo, err := processPeerInfo(pty.Pid())
	if err != nil {
		t.Fatal(err)
	}
	ptyPGID, err := syscall.Getpgid(pty.Pid())
	if err != nil || ptyPGID != pty.Pid() {
		t.Fatalf("PTY pgid=%d err=%v", ptyPGID, err)
	}
	if plainInfo.HasControllingTerminal || !ptyInfo.HasControllingTerminal {
		t.Fatalf("plain=%+v pty=%+v", plainInfo, ptyInfo)
	}
	const release = "release\n"
	if n, err := io.WriteString(gate, release); err != nil || n != len(release) {
		t.Fatalf("plain release wrote=%d err=%v", n, err)
	}
	if n, err := pty.Write([]byte(release)); err != nil || n != len(release) {
		t.Fatalf("PTY release wrote=%d err=%v", n, err)
	}
	// Drain output while the child exits; do not leave PTY output unread.
	// The existing read deadline bounds this operation without closing the master.
	if _, err := io.Copy(io.Discard, reader); err != nil {
		t.Fatalf("PTY output through exit: %v", err)
	}
	awaitPTYClose(t, ptyDone, "PTY release and Wait")
	if code, err := pty.Wait(); err != nil || code != 0 {
		t.Fatalf("PTY release exit=%d err=%v", code, err)
	}
	awaitPTYClose(t, plainDone, "plain release and Wait")
	if plainWaitErr != nil {
		t.Fatalf("plain release exit: %v", plainWaitErr)
	}
}
