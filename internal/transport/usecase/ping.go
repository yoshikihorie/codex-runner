package usecase

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/yoshikihorie/codex-runner/internal/transport"
)

// PingUseCase returns the protocol version without depending on task state.
type PingUseCase struct{}

// Execute returns the protocol version implemented by this build.
func (u *PingUseCase) Execute(ctx context.Context) (PingResult, error) {
	return PingResult{ProtocolVersion: transport.ProtocolVersion}, nil
}

// PingResult is the result body returned for a ping request.
type PingResult struct {
	ProtocolVersion string `json:"protocol_version"`
}

// Handle adapts PingUseCase to a transport Dispatcher handler.
func (u *PingUseCase) Handle(req transport.Request) transport.Response {
	result, err := u.Execute(context.Background())
	if err != nil {
		// PingUseCase.Execute always returns nil. A non-nil error violates that invariant.
		panic(fmt.Errorf("ping: PingUseCase.Execute returned non-nil error, invariant violated: %w", err))
	}
	body, err := json.Marshal(result)
	if err != nil {
		// PingResult has only fixed fields. Marshaling failure is an invariant violation.
		panic(fmt.Errorf("ping: failed to marshal PingResult, invariant violated: %w", err))
	}
	return transport.Response{
		ProtocolVersion: transport.ProtocolVersion,
		RequestID:       req.RequestID,
		OK:              true,
		Result:          body,
	}
}
