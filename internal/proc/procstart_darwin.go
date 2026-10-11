//go:build darwin

package proc

import (
	"encoding/binary"
	"syscall"
	"time"
	"unsafe"
)

// ProcessStartTime reads extern_proc.p_starttime from kern.proc.pid on macOS.
// Darwin's kinfo_proc begins with extern_proc, whose first field is timeval.
func ProcessStartTime(pid int) (time.Time, bool) {
	if pid <= 0 {
		return time.Time{}, false
	}
	// CTL_KERN, KERN_PROC, KERN_PROC_PID from Darwin sys/sysctl.h.
	mib := [4]int32{1, 14, 1, int32(pid)}
	var size uintptr
	_, _, errno := syscall.Syscall6(syscall.SYS___SYSCTL, uintptr(unsafe.Pointer(&mib[0])), uintptr(len(mib)), 0, uintptr(unsafe.Pointer(&size)), 0, 0)
	if errno != 0 || size < 16 {
		return time.Time{}, false
	}
	data := make([]byte, size)
	_, _, errno = syscall.Syscall6(syscall.SYS___SYSCTL, uintptr(unsafe.Pointer(&mib[0])), uintptr(len(mib)), uintptr(unsafe.Pointer(&data[0])), uintptr(unsafe.Pointer(&size)), 0, 0)
	if errno != 0 || size < 16 {
		return time.Time{}, false
	}
	sec := int64(binary.LittleEndian.Uint64(data[:8]))
	usec := int64(binary.LittleEndian.Uint32(data[8:12]))
	if sec <= 0 || usec < 0 || usec >= 1000000 {
		return time.Time{}, false
	}
	return time.Unix(sec, usec*1000).UTC(), true
}
