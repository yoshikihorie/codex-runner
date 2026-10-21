//go:build darwin

package proc

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"unsafe"
)

type ptyWindowSize struct{ Row, Col, Xpixel, Ypixel uint16 }

func startPTYSessionDarwin(executable string, argv []string, cwd string, env []string, cols, rows uint16) (*PTYSession, error) {
	fd, err := syscall.Open("/dev/ptmx", syscall.O_RDWR|syscall.O_NOCTTY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	if err := syscall.SetNonblock(fd, true); err != nil {
		_ = syscall.Close(fd)
		return nil, err
	}
	master := os.NewFile(uintptr(fd), "pty-master")
	closeMaster := func(err error) (*PTYSession, error) { _ = master.Close(); return nil, err }
	if err := ptyIoctl(fd, syscall.TIOCPTYGRANT, 0); err != nil {
		return closeMaster(err)
	}
	name := make([]byte, 128)
	if err := ptyIoctl(fd, syscall.TIOCPTYGNAME, uintptr(unsafe.Pointer(&name[0]))); err != nil {
		return closeMaster(err)
	}
	end := strings.IndexByte(string(name), 0)
	if end <= 0 {
		return closeMaster(fmt.Errorf("invalid PTY slave name"))
	}
	if err := ptyIoctl(fd, syscall.TIOCPTYUNLK, 0); err != nil {
		return closeMaster(err)
	}
	slave, err := os.OpenFile(string(name[:end]), os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return closeMaster(err)
	}
	if err := setPTYSize(fd, cols, rows); err != nil {
		_ = slave.Close()
		return closeMaster(err)
	}
	cmd := exec.Command(executable)
	cmd.Args = append([]string(nil), argv...)
	cmd.Dir, cmd.Env = cwd, env
	cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
	if err := cmd.Start(); err != nil {
		_ = slave.Close()
		return closeMaster(err)
	}
	_ = slave.Close()
	return &PTYSession{master: master, cmd: cmd}, nil
}

func resizePTYDarwin(master *os.File, cols, rows uint16) error {
	return setPTYSize(int(master.Fd()), cols, rows)
}
func setPTYSize(fd int, cols, rows uint16) error {
	ws := ptyWindowSize{Row: rows, Col: cols}
	return ptyIoctl(fd, syscall.TIOCSWINSZ, uintptr(unsafe.Pointer(&ws)))
}
func ptyIoctl(fd int, request uintptr, arg uintptr) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), request, arg)
	if errno != 0 {
		return errno
	}
	return nil
}
