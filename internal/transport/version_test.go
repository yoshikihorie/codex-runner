package transport

import "testing"

func TestProtocolVersion(t *testing.T) {
	if ProtocolVersion != "1" {
		t.Fatalf("ProtocolVersion = %q, want %q", ProtocolVersion, "1")
	}
}
