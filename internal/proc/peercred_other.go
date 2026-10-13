//go:build !darwin

package proc

import (
	"errors"
	"net"
)

type PeerProcessInfo struct {
	PID                    int
	ParentPID              int
	HasControllingTerminal bool
}

var errPeerUnsupported = errors.New("peer process inspection requires Darwin")

func PeerCredentials(*net.UnixConn) (int, int, error) { return 0, 0, errPeerUnsupported }
func processPeerInfo(int) (PeerProcessInfo, error)    { return PeerProcessInfo{}, errPeerUnsupported }
func PeerResumeConditions(int, map[int]bool) (bool, bool, error) {
	return false, false, errPeerUnsupported
}
