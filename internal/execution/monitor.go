package execution

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
)

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
		line, size, err := readEventLineWithContext(ctx, reader)
		if err := ctx.Err(); err != nil {
			return err
		}
		if size > 0 {
			var envelope struct {
				Type any `json:"type"`
			}
			if !json.Valid(line) {
				slog.Default().Warn("malformed event stream line", "code", "EVENT_STREAM_MALFORMED", "line", lineNo, "bytes", size)
			} else {
				_ = json.Unmarshal(line, &envelope)
				typ, ok := envelope.Type.(string)
				if !ok || typ == "" {
					typ = "unknown"
				}
				raw := json.RawMessage(append([]byte(nil), line...))
				if _, ok := knownEventTypes[typ]; ok {
					if onKnown != nil {
						onKnown(typ, raw)
					}
				} else if onUnknown != nil {
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

type eventLineResult struct {
	line []byte
	size int
	err  error
}

func readEventLineWithContext(ctx context.Context, reader *bufio.Reader) ([]byte, int, error) {
	result := make(chan eventLineResult, 1)
	go func() {
		line, size, err := readEventLine(reader)
		result <- eventLineResult{line: line, size: size, err: err}
	}()

	select {
	case result := <-result:
		return result.line, result.size, result.err
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

// readEventLine returns one line without its line ending and its original byte length.
func readEventLine(reader *bufio.Reader) ([]byte, int, error) {
	var line []byte
	size := 0
	for {
		part, err := reader.ReadSlice('\n')
		size += len(part)
		line = append(line, part...)
		if err == nil {
			line = line[:len(line)-1]
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			return line, size, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, size, err
	}
}
