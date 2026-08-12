package execution

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
)

const eventLineMaxBytes = 1 << 20

var knownEventTypes = map[string]struct{}{
	"thread.started": {}, "turn.started": {}, "item.started": {},
	"item.completed": {}, "turn.completed": {}, "turn.failed": {},
}

type EventMonitor interface {
	Observe(context.Context, io.Reader, func(string, json.RawMessage), func(string, json.RawMessage)) error
}

type EventMonitorFunc func(context.Context, io.Reader, func(string, json.RawMessage), func(string, json.RawMessage)) error

func (f EventMonitorFunc) Observe(ctx context.Context, stdout io.Reader, known func(string, json.RawMessage), unknown func(string, json.RawMessage)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return f(ctx, stdout, known, unknown)
}

func NewEventMonitor() EventMonitor { return EventMonitorFunc(ObserveEvents) }

// ObserveEvents consumes newline-delimited JSON without logging its raw contents.
func ObserveEvents(ctx context.Context, stdout io.Reader, onKnown func(string, json.RawMessage), onUnknown func(string, json.RawMessage)) error {
	reader := bufio.NewReader(stdout)
	for lineNo := 1; ; lineNo++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, size, err := readEventLine(reader)
		if err := ctx.Err(); err != nil {
			return err
		}
		if size > 0 {
			var envelope struct {
				Type any `json:"type"`
			}
			if size > eventLineMaxBytes || !json.Valid(line) {
				slog.Default().Warn("malformed event stream line", "code", "EVENT_STREAM_MALFORMED", "line", lineNo, "bytes", size)
			} else {
				_ = json.Unmarshal(line, &envelope)
				typ, ok := envelope.Type.(string)
				if !ok || typ == "" {
					typ = "unknown"
				}
				raw := json.RawMessage(append([]byte(nil), line...))
				if _, ok := knownEventTypes[typ]; ok {
					onKnown(typ, raw)
				} else {
					onUnknown(typ, raw)
				}
			}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err == nil {
			continue
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	}
}

// readEventLine returns one line without its line ending and its original byte length.
func readEventLine(reader *bufio.Reader) ([]byte, int, error) {
	var line []byte
	size := 0
	for {
		part, err := reader.ReadSlice('\n')
		size += len(part)
		content := part
		if err == nil {
			content = content[:len(content)-1]
			if len(content) > 0 && content[len(content)-1] == '\r' {
				content = content[:len(content)-1]
			}
		}
		if size <= eventLineMaxBytes {
			line = append(line, content...)
		} else {
			line = nil
		}
		if err == nil {
			return line, size, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, size, err
	}
}
