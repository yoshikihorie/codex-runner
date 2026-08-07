package transport

import (
	"strings"
	"testing"
)

func TestNewDispatcherRejectsNilHandlers(t *testing.T) {
	handler := func(Request) Response { return Response{} }
	for _, tc := range []struct {
		name                         string
		submit, status, cancel, ping func(Request) Response
	}{
		{"submit", nil, handler, handler, handler}, {"status", handler, nil, handler, handler},
		{"cancel", handler, handler, nil, handler}, {"ping", handler, handler, handler, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewDispatcher(tc.submit, tc.status, tc.cancel, tc.ping)
			if err == nil || !strings.Contains(err.Error(), tc.name+" handler is nil") {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestNewDispatcherAcceptsHandlersAndTypedNil(t *testing.T) {
	handler := func(Request) Response { return Response{} }
	if _, err := NewDispatcher(handler, handler, handler, handler); err != nil {
		t.Fatal(err)
	}
	var typedNil func(Request) Response
	if _, err := NewDispatcher(typedNil, handler, handler, handler); err == nil {
		t.Fatal("typed nil was accepted")
	}
}

func TestDispatcherDispatchesKnownVerbsAndRejectsOthers(t *testing.T) {
	called := ""
	makeHandler := func(name string) func(Request) Response {
		return func(Request) Response { called = name; return Response{OK: true} }
	}
	d, err := NewDispatcher(makeHandler("submit"), makeHandler("status"), makeHandler("cancel"), makeHandler("ping"))
	if err != nil {
		t.Fatal(err)
	}
	for _, verb := range []string{"submit", "status", "cancel", "ping"} {
		called = ""
		d.Dispatch(Request{Verb: verb})
		if called != verb {
			t.Fatalf("%s routed to %q", verb, called)
		}
	}
	for _, verb := range []string{"unknown-verb", "tail"} {
		resp := d.Dispatch(Request{Verb: verb, RequestID: "r"})
		if resp.OK || resp.Error == nil || resp.Error.Code != protocolUnknownVerbCode || resp.Error.MessageKey != protocolUnknownVerbMessageKey {
			t.Fatalf("unexpected response: %#v", resp)
		}
	}
}
