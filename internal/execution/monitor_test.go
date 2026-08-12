package execution

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
)

func TestEventMonitorKnownAndUnknownEvents(t *testing.T) {
	var known, unknown []string
	err := ObserveEvents(context.Background(), strings.NewReader("{\"type\":\"item.completed\"}\n{\"type\":\"future.event\"}\n{\"x\":1}\n{\"type\":\"\"}\n{\"type\":3}\n"), func(typ string, _ json.RawMessage) { known = append(known, typ) }, func(typ string, _ json.RawMessage) { unknown = append(unknown, typ) })
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(known, ",") != "item.completed" {
		t.Fatalf("known=%v", known)
	}
	if strings.Join(unknown, ",") != "future.event,unknown,unknown,unknown" {
		t.Fatalf("unknown=%v", unknown)
	}
}

func TestNewEventMonitorProvidesEventMonitorAndHonorsCanceledContext(t *testing.T) {
	var _ EventMonitor = NewEventMonitor()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := NewEventMonitor().Observe(ctx, strings.NewReader(""), func(string, json.RawMessage) {}, func(string, json.RawMessage) {})
	if err != context.Canceled {
		t.Fatalf("error=%v", err)
	}
}

func TestObserveEventsMalformedLinesDoNotLogPayload(t *testing.T) {
	const secret = "EVENT_PAYLOAD_SENTINEL"
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	over := "{\"type\":\"" + strings.Repeat("x", eventLineMaxBytes) + "\"}\n"
	err := ObserveEvents(context.Background(), strings.NewReader("{"+secret+"}\n\n"+over+"{\"type\":\"item.completed\"}"), func(string, json.RawMessage) {}, func(string, json.RawMessage) {})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "EVENT_STREAM_MALFORMED") {
		t.Fatalf("logs=%s", logs.String())
	}
	if strings.Contains(logs.String(), secret) {
		t.Fatalf("payload leaked: %s", logs.String())
	}
}

func TestReadEventLineHandlesUnterminatedFinalLine(t *testing.T) {
	line, size, err := readEventLine(bufio.NewReader(bytes.NewBufferString("final")))
	if err != io.EOF || string(line) != "final" || size != len("final") {
		t.Fatalf("line=%q size=%d err=%v", line, size, err)
	}
}

func TestReadEventLineDiscardsOversizedPayloadAndContinues(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader(strings.Repeat("x", eventLineMaxBytes+1) + "\nnext\n"))
	line, size, err := readEventLine(reader)
	if err != nil || line != nil || size != eventLineMaxBytes+2 {
		t.Fatalf("line=%q size=%d err=%v", line, size, err)
	}
	line, size, err = readEventLine(reader)
	if err != nil || string(line) != "next" || size != len("next\n") {
		t.Fatalf("line=%q size=%d err=%v", line, size, err)
	}
}

func TestReadEventLineCountsLineEndingAtMaximumBoundary(t *testing.T) {
	for _, tc := range []struct {
		name     string
		bodySize int
		wantLine bool
		wantSize int
	}{
		{name: "within-limit", bodySize: eventLineMaxBytes - 1, wantLine: true, wantSize: eventLineMaxBytes},
		{name: "over-limit", bodySize: eventLineMaxBytes, wantLine: false, wantSize: eventLineMaxBytes + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Repeat("x", tc.bodySize)
			line, size, err := readEventLine(bufio.NewReader(strings.NewReader(body + "\n")))
			if err != nil || size != tc.wantSize {
				t.Fatalf("line=%q size=%d err=%v", line, size, err)
			}
			if tc.wantLine {
				if string(line) != body {
					t.Fatalf("line length=%d want length=%d", len(line), len(body))
				}
			} else if line != nil {
				t.Fatalf("line=%q", line)
			}
		})
	}
}

func TestObserveEventsValidNonObjectJSONIsUnknown(t *testing.T) {
	var got []string
	err := ObserveEvents(context.Background(), strings.NewReader("[]\n\"value\"\n123\n"), func(string, json.RawMessage) {}, func(typ string, raw json.RawMessage) {
		got = append(got, typ+":"+string(raw))
	})
	if err != nil || strings.Join(got, ",") != "unknown:[],unknown:\"value\",unknown:123" {
		t.Fatalf("got=%v err=%v", got, err)
	}
}

func TestObserveEventsStopsAfterCallbackCancelsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var known []string
	err := ObserveEvents(ctx, strings.NewReader("{\"type\":\"item.completed\"}\n{\"type\":\"item.completed\"}\n"), func(typ string, _ json.RawMessage) {
		known = append(known, typ)
		cancel()
	}, func(string, json.RawMessage) {})
	if err != context.Canceled || len(known) != 1 {
		t.Fatalf("known=%v err=%v", known, err)
	}
}
