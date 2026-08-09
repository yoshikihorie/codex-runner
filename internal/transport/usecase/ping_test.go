package usecase

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/transport"
)

func TestPingUseCaseExecute(t *testing.T) {
	useCase := &PingUseCase{}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	for _, ctx := range []context.Context{context.Background(), cancelled} {
		got, err := useCase.Execute(ctx)
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		want := PingResult{ProtocolVersion: transport.ProtocolVersion}
		if got != want {
			t.Fatalf("Execute() = %#v, want %#v", got, want)
		}
	}
}

func TestPingUseCaseDoesNotReferenceQueue(t *testing.T) {
	got, err := (&PingUseCase{}).Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got.ProtocolVersion != transport.ProtocolVersion {
		t.Fatalf("Execute() = %#v", got)
	}
}

func TestPingUseCaseHandleIsIdempotent(t *testing.T) {
	useCase := &PingUseCase{}
	req := transport.Request{RequestID: "r-7", Verb: "ping"}
	var first PingResult

	for i := 0; i < 10; i++ {
		resp := useCase.Handle(req)
		var got PingResult
		if err := json.Unmarshal(resp.Result, &got); err != nil {
			t.Fatalf("response %d result: %v", i, err)
		}
		if i == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("response %d = %#v, want %#v", i, got, first)
		}
	}
}

func TestPingResultJSON(t *testing.T) {
	body, err := json.Marshal(PingResult{ProtocolVersion: transport.ProtocolVersion})
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"protocol_version":"1"}` {
		t.Fatalf("Marshal(PingResult) = %s", body)
	}
}

func TestPingHandleSuccess(t *testing.T) {
	resp := (&PingUseCase{}).Handle(transport.Request{RequestID: "r-1", Verb: "ping"})
	var result PingResult
	if err := json.Unmarshal(resp.Result, &result); err != nil {
		t.Fatal(err)
	}

	if !resp.OK || resp.RequestID != "r-1" || resp.ProtocolVersion != transport.ProtocolVersion {
		t.Fatalf("response = %#v", resp)
	}
	if result.ProtocolVersion != transport.ProtocolVersion || result.ProtocolVersion != resp.ProtocolVersion {
		t.Fatalf("result = %#v, response protocol version = %q", result, resp.ProtocolVersion)
	}
}

func TestPingHandleIgnoresTaskIDAndParams(t *testing.T) {
	useCase := &PingUseCase{}
	withExtras := useCase.Handle(transport.Request{
		RequestID: "r-2",
		Verb:      "ping",
		TaskID:    "impl-example",
		Params:    json.RawMessage(`{"x":1}`),
	})
	withoutExtras := useCase.Handle(transport.Request{RequestID: "r-2", Verb: "ping"})

	if !withExtras.OK || !reflect.DeepEqual(withExtras, withoutExtras) {
		t.Fatalf("response with extras = %#v, without extras = %#v", withExtras, withoutExtras)
	}
}

func TestPingHandleAcceptsUnknownRequestProtocolVersion(t *testing.T) {
	resp := (&PingUseCase{}).Handle(transport.Request{
		ProtocolVersion: "999",
		RequestID:       "r-4",
		Verb:            "ping",
	})
	if !resp.OK || resp.ProtocolVersion != transport.ProtocolVersion {
		t.Fatalf("response = %#v", resp)
	}
}

func TestPingHandleEchoesEmptyRequestID(t *testing.T) {
	resp := (&PingUseCase{}).Handle(transport.Request{Verb: "ping"})
	if resp.RequestID != "" || !resp.OK {
		t.Fatalf("response = %#v", resp)
	}
}
