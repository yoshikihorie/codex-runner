//go:build darwin

package proc

import (
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"syscall"
	"unsafe"
)

// Values and offsets follow Darwin's sys/socket.h, sys/sysctl.h and sys/proc.h.
const (
	localPeerCredential = 1
	localPeerPID        = 2
	kinfoSize           = 648
	kinfoPIDOffset      = 40
	kinfoParentOffset   = 560
	kinfoTTYOffset      = 572
	kernProcArgs2       = 49
)

type PeerProcessInfo struct {
	PID                    int
	ParentPID              int
	HasControllingTerminal bool
}

func PeerCredentials(conn *net.UnixConn) (uid, pid int, err error) {
	if conn == nil {
		return 0, 0, errors.New("nil peer connection")
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, 0, err
	}
	var probeErr error
	controlErr := raw.Control(func(fd uintptr) {
		var cred [76]byte // sizeof(struct xucred), Darwin arm64/amd64.
		length := uint32(len(cred))
		_, _, errno := syscall.Syscall6(syscall.SYS_GETSOCKOPT, fd, 0, localPeerCredential, uintptr(unsafe.Pointer(&cred[0])), uintptr(unsafe.Pointer(&length)), 0)
		if errno != 0 {
			probeErr = errno
			return
		}
		if length < 8 {
			probeErr = errors.New("short peer credential")
			return
		}
		uid = int(binary.LittleEndian.Uint32(cred[4:8]))
		pid, probeErr = syscall.GetsockoptInt(int(fd), 0, localPeerPID)
	})
	if controlErr != nil {
		return 0, 0, controlErr
	}
	if probeErr != nil {
		return 0, 0, probeErr
	}
	if pid <= 0 {
		return 0, 0, errors.New("invalid peer pid")
	}
	return uid, pid, nil
}

func sysctlData(mib []int32) ([]byte, error) {
	var size uintptr
	_, _, errno := syscall.Syscall6(syscall.SYS___SYSCTL, uintptr(unsafe.Pointer(&mib[0])), uintptr(len(mib)), 0, uintptr(unsafe.Pointer(&size)), 0, 0)
	if errno != 0 {
		return nil, errno
	}
	if size == 0 {
		return nil, errors.New("empty process record")
	}
	data := make([]byte, size)
	_, _, errno = syscall.Syscall6(syscall.SYS___SYSCTL, uintptr(unsafe.Pointer(&mib[0])), uintptr(len(mib)), uintptr(unsafe.Pointer(&data[0])), uintptr(unsafe.Pointer(&size)), 0, 0)
	if errno != 0 {
		return nil, errno
	}
	return data[:size], nil
}

func processPeerInfo(pid int) (PeerProcessInfo, error) {
	if pid <= 0 {
		return PeerProcessInfo{}, errors.New("invalid process pid")
	}
	data, err := sysctlData([]int32{1, 14, 1, int32(pid)})
	if err != nil {
		return PeerProcessInfo{}, err
	}
	if len(data) < kinfoSize {
		return PeerProcessInfo{}, errors.New("short process record")
	}
	got := int(int32(binary.LittleEndian.Uint32(data[kinfoPIDOffset:])))
	if got != pid {
		return PeerProcessInfo{}, errors.New("process identity changed")
	}
	parent := int(int32(binary.LittleEndian.Uint32(data[kinfoParentOffset:])))
	dev := binary.LittleEndian.Uint32(data[kinfoTTYOffset:])
	return PeerProcessInfo{PID: pid, ParentPID: parent, HasControllingTerminal: dev != ^uint32(0) && dev != 0}, nil
}

func processHasSupervisedMarker(pid int) (bool, error) {
	data, err := sysctlData([]int32{1, kernProcArgs2, int32(pid)})
	if err != nil {
		return false, err
	}
	if len(data) < 5 {
		return false, errors.New("short process arguments")
	}
	argc := int(int32(binary.LittleEndian.Uint32(data[:4])))
	if argc < 0 {
		return false, errors.New("invalid process argc")
	}
	parts := data[4:]
	end := 0
	for end < len(parts) && parts[end] != 0 {
		end++
	}
	if end == len(parts) {
		return false, errors.New("unterminated executable path")
	}
	parts = parts[end+1:]
	for len(parts) > 0 && parts[0] == 0 {
		parts = parts[1:]
	}
	for i := 0; i < argc; i++ {
		end = 0
		for end < len(parts) && parts[end] != 0 {
			end++
		}
		if end == len(parts) {
			return false, errors.New("unterminated process argument")
		}
		parts = parts[end+1:]
	}
	for len(parts) > 0 && parts[0] == 0 {
		parts = parts[1:]
	}
	for len(parts) > 0 {
		end = 0
		for end < len(parts) && parts[end] != 0 {
			end++
		}
		if end == len(parts) {
			return false, errors.New("unterminated process environment")
		}
		if strings.HasPrefix(string(parts[:end]), "CODEXD_SUPERVISED=") {
			return true, nil
		}
		parts = parts[end+1:]
	}
	return false, nil
}

func PeerResumeConditions(pid int, owned map[int]bool) (isatty, supervised bool, err error) {
	info, err := processPeerInfo(pid)
	if err != nil {
		return false, false, err
	}
	marker, err := processHasSupervisedMarker(pid)
	if err != nil {
		return false, false, err
	}
	ancestors := []int{pid}
	seen := map[int]bool{pid: true}
	parent := info.ParentPID
	for len(ancestors) < 64 && parent > 0 && !seen[parent] {
		ancestors = append(ancestors, parent)
		seen[parent] = true
		if owned[parent] {
			break
		}
		ancestor, probeErr := processPeerInfo(parent)
		if probeErr != nil {
			return false, false, probeErr
		}
		parent = ancestor.ParentPID
	}
	if parent > 0 && len(ancestors) >= 64 {
		return false, false, errors.New("peer ancestry limit")
	}
	isatty, supervised = JudgePeer(info.HasControllingTerminal, ancestors, owned, marker, true)
	return isatty, supervised, nil
}
