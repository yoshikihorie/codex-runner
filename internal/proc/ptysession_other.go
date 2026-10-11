//go:build !darwin

package proc

import (
	"errors"
	"os"
)

func startPTYSessionDarwin(string, []string, string, []string, uint16, uint16) (*PTYSession, error) {
	return nil, errors.New("PTY sessions are supported on Darwin only")
}

func resizePTYDarwin(*os.File, uint16, uint16) error {
	return errors.New("PTY sessions are supported on Darwin only")
}
