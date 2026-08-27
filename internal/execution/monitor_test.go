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
	"time"
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

func TestObserveEventsNilKnownCallback(t *testing.T) {
	unknownCalls := 0
	err := ObserveEvents(context.Background(), strings.NewReader("{\"type\":\"item.completed\"}\n"), nil, func(string, json.RawMessage) {
		unknownCalls++
	})
	if err != nil {
		t.Fatal(err)
	}
	if unknownCalls != 0 {
		t.Fatalf("unknown callback calls=%d", unknownCalls)
	}
}

func TestObserveEventsNilUnknownCallback(t *testing.T) {
	knownCalls := 0
	err := ObserveEvents(context.Background(), strings.NewReader("{\"type\":\"future.event\"}\n"), func(string, json.RawMessage) {
		knownCalls++
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if knownCalls != 0 {
		t.Fatalf("known callback calls=%d", knownCalls)
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
	invalid := "{\"type\":\"" + strings.Repeat("x", 1<<20+1) + secret + "\n"
	var known []string
	err := ObserveEvents(context.Background(), strings.NewReader(invalid+"{\"type\":\"item.completed\"}"), func(typ string, _ json.RawMessage) { known = append(known, typ) }, func(string, json.RawMessage) {})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logs.String(), "EVENT_STREAM_MALFORMED") {
		t.Fatal("malformed event log missing")
	}
	if strings.Contains(logs.String(), secret) {
		t.Fatal("payload leaked into logs")
	}
	if strings.Join(known, ",") != "item.completed" {
		t.Fatalf("known=%v", known)
	}
}

func TestReadEventLineHandlesUnterminatedFinalLine(t *testing.T) {
	line, size, err := readEventLine(bufio.NewReader(bytes.NewBufferString("final")))
	if err != io.EOF || string(line) != "final" || size != len("final") {
		t.Fatalf("line=%q size=%d err=%v", line, size, err)
	}
}

func TestReadEventLineHandlesOversizedLines(t *testing.T) {
	body := strings.Repeat("x", 1<<20+1)
	reader := bufio.NewReader(strings.NewReader(body + "\nnext\n"))
	line, size, err := readEventLine(reader)
	if err != nil || string(line) != body || size != len(body)+1 {
		t.Fatalf("line=%q size=%d err=%v", line, size, err)
	}
	line, size, err = readEventLine(reader)
	if err != nil || string(line) != "next" || size != len("next\n") {
		t.Fatalf("line=%q size=%d err=%v", line, size, err)
	}
}

func TestReadEventLineHandlesOversizedUnterminatedFinalLine(t *testing.T) {
	body := strings.Repeat("x", 1<<20+1)
	line, size, err := readEventLine(bufio.NewReader(strings.NewReader(body)))
	if err != io.EOF || string(line) != body || size != len(body) {
		t.Fatalf("line length=%d size=%d err=%v", len(line), size, err)
	}
}

func TestObserveEventsDeliversOversizedKnownEvent(t *testing.T) {
	raw := "{\"type\":\"item.completed\",\"text\":\"" + strings.Repeat("x", 1<<20+1) + "\"}"
	var got []string
	err := ObserveEvents(context.Background(), strings.NewReader(raw+"\n"), func(typ string, event json.RawMessage) {
		got = append(got, typ+":"+string(event))
	}, func(string, json.RawMessage) {})
	if err != nil || len(got) != 1 || got[0] != "item.completed:"+raw {
		t.Fatalf("got count=%d err=%v", len(got), err)
	}
}

func TestObserveEventsDeliversOversizedUnknownEvent(t *testing.T) {
	raw := "{\"type\":\"future.event\",\"text\":\"" + strings.Repeat("x", 1<<20+1) + "\"}"
	var got []string
	err := ObserveEvents(context.Background(), strings.NewReader(raw+"\n"), func(string, json.RawMessage) {}, func(typ string, event json.RawMessage) {
		got = append(got, typ+":"+string(event))
	})
	if err != nil || len(got) != 1 || got[0] != "future.event:"+raw {
		t.Fatalf("got count=%d err=%v", len(got), err)
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

func TestObserveEventsReturnsWhenContextCanceledDuringBlockingRead(t *testing.T) {
	reader := &blockingReader{
		readStarted:  make(chan struct{}),
		release:      make(chan struct{}),
		readReturned: make(chan struct{}),
	}
	t.Cleanup(func() {
		close(reader.release)
		select {
		case <-reader.readReturned:
		case <-time.After(time.Second):
			t.Error("blocking reader did not return after release")
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- ObserveEvents(ctx, reader, nil, nil)
	}()

	select {
	case <-reader.readStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking reader did not start")
	}
	cancel()

	select {
	case err := <-result:
		if err != context.Canceled {
			t.Fatalf("error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ObserveEvents did not return after context cancellation")
	}
}

type blockingReader struct {
	readStarted  chan struct{}
	release      chan struct{}
	readReturned chan struct{}
}

func (r *blockingReader) Read([]byte) (int, error) {
	close(r.readStarted)
	<-r.release
	close(r.readReturned)
	return 0, io.EOF
}
