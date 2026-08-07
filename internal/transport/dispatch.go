package transport

import (
	"fmt"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// Dispatcher routes the four non-tail protocol verbs to their handlers.
type Dispatcher struct {
	Submit func(Request) Response
	Status func(Request) Response
	Cancel func(Request) Response
	Ping   func(Request) Response
}

// NewDispatcher constructs a Dispatcher whose handlers are safe to invoke.
func NewDispatcher(submit, status, cancel, ping func(Request) Response) (*Dispatcher, error) {
	if submit == nil {
		return nil, fmt.Errorf("dispatcher: submit handler is nil")
	}
	if status == nil {
		return nil, fmt.Errorf("dispatcher: status handler is nil")
	}
	if cancel == nil {
		return nil, fmt.Errorf("dispatcher: cancel handler is nil")
	}
	if ping == nil {
		return nil, fmt.Errorf("dispatcher: ping handler is nil")
	}
	return &Dispatcher{Submit: submit, Status: status, Cancel: cancel, Ping: ping}, nil
}

// Dispatch routes a request to its verb-specific handler, rejecting all others.
func (d *Dispatcher) Dispatch(req Request) Response {
	switch req.Verb {
	case string(domain.ProtocolVerbSubmit):
		return d.Submit(req)
	case string(domain.ProtocolVerbStatus):
		return d.Status(req)
	case string(domain.ProtocolVerbCancel):
		return d.Cancel(req)
	case string(domain.ProtocolVerbPing):
		return d.Ping(req)
	default:
		return unknownVerbResponse(req.RequestID)
	}
}
