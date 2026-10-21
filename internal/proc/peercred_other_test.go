//go:build !darwin

package proc

import (
	"os"
	"testing"
)

func TestPeerOther_SCNsup0544_RejectsProbe(t *testing.T) {
	home, err := os.MkdirTemp("/tmp", "sup-home-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if _, err := processPeerInfo(os.Getpid()); err == nil {
		t.Fatal("unsupported process probe accepted")
	}
}
