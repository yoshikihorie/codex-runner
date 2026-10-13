//go:build darwin

package proc

import (
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
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
	plain := exec.Command("/bin/sh", "-c", "sleep 1")
	plain.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := plain.Start(); err != nil {
		t.Fatal(err)
	}
	defer plain.Wait()
	plainInfo, err := processPeerInfo(plain.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
	pty, err := StartPTYSession("/bin/sh", []string{"/bin/sh", "-c", "sleep 1"}, t.TempDir(), BuildSessionEnv(nil), 80, 24)
	if err != nil {
		t.Fatal(err)
	}
	defer pty.Close()
	defer pty.Wait()
	ptyInfo, err := processPeerInfo(pty.Pid())
	if err != nil {
		t.Fatal(err)
	}
	if plainInfo.HasControllingTerminal || !ptyInfo.HasControllingTerminal {
		t.Fatalf("plain=%+v pty=%+v", plainInfo, ptyInfo)
	}
}
