package domain

import "testing"

func TestResolveTimeout(t *testing.T) {
	resolved, err := ResolveTimeout(nil)
	if err != nil || resolved.RequestedSeconds() != nil || resolved.ResolvedSeconds() != timeoutMinSeconds {
		t.Fatalf("default timeout = %#v, %v", resolved, err)
	}
	requested := timeoutMinSeconds
	resolved, err = ResolveTimeout(&requested)
	if err != nil || resolved.ResolvedSeconds() != requested {
		t.Fatalf("requested timeout = %#v, %v", resolved, err)
	}
	requested++
	if got := *resolved.RequestedSeconds(); got != timeoutMinSeconds {
		t.Fatalf("requested timeout aliases input: %d", got)
	}
	zero := 0
	if _, err := ResolveTimeout(&zero); err == nil {
		t.Fatal("explicit zero was accepted")
	}
}
