package transport

import (
	"encoding/json"
	"io"

	"github.com/yoshikihorie/codex-runner/internal/transport/schema"
)

type progressWriter struct {
	encoder   *json.Encoder
	out       io.Writer
	requestID string
}

var _ schema.ProgressWriter = (*progressWriter)(nil)

func NewProgressWriter(out io.Writer, requestID string) schema.ProgressWriter {
	return &progressWriter{
		encoder:   json.NewEncoder(out),
		out:       out,
		requestID: requestID,
	}
}

func (w *progressWriter) WriteProgress(line schema.ProgressLine) error {
	encoded, err := w.marshalProgress(line)
	if err != nil {
		return err
	}
	if len(encoded) > protocolLineMaxBytes {
		raw, err := json.Marshal(line.Raw)
		if err != nil {
			return err
		}
		line.Raw = map[string]any{}
		line.Truncated = true
		line.RawBytes = len(raw)
		encoded, err = w.marshalProgress(line)
		if err != nil {
			return err
		}
		if len(encoded) > protocolLineMaxBytes {
			return errProtocolLineTooLong
		}
	}
	frame := make([]byte, len(encoded)+1)
	copy(frame, encoded)
	frame[len(encoded)] = '\n'
	return writeProtocolFrame(w.out, frame)
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

func (w *progressWriter) marshalProgress(line schema.ProgressLine) ([]byte, error) {
	result, err := json.Marshal(line)
	if err != nil {
		return nil, err
	}
	return json.Marshal(Response{
		ProtocolVersion: ProtocolVersion,
		RequestID:       w.requestID,
		OK:              true,
		Result:          result,
	})
}
