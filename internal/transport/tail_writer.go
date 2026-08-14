package transport

import (
	"encoding/json"
	"io"

	"github.com/yoshikihorie/codex-runner/internal/transport/schema"
)

type progressWriter struct {
	encoder   *json.Encoder
	requestID string
}

var _ schema.ProgressWriter = (*progressWriter)(nil)

func NewProgressWriter(out io.Writer, requestID string) schema.ProgressWriter {
	return &progressWriter{
		encoder:   json.NewEncoder(out),
		requestID: requestID,
	}
}

func (w *progressWriter) WriteProgress(line schema.ProgressLine) error {
	return w.writeLine(line)
}

func (w *progressWriter) WriteComplete(line schema.CompleteLine) error {
	return w.writeLine(line)
}

func (w *progressWriter) writeLine(line any) error {
	result, err := json.Marshal(line)
	if err != nil {
		return err
	}
	return w.encoder.Encode(Response{
		ProtocolVersion: ProtocolVersion,
		RequestID:       w.requestID,
		OK:              true,
		Result:          result,
	})
}
