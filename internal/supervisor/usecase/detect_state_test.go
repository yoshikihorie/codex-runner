package usecase

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
)

func TestSCNsup01050RejectsNonregularAndOversizeStatusline(t *testing.T) {
	for _, kind := range []string{"symlink", "directory", "oversize"} {
		t.Run(kind, func(t *testing.T) {
			root := t.TempDir()
			statusDir := filepath.Join(root, "statusline")
			if err := os.Mkdir(statusDir, 0700); err != nil {
				t.Fatal(err)
			}
			id, _ := supervisor.NewSessionID("status-1")
			path := filepath.Join(statusDir, id.String()+".json")
			switch kind {
			case "symlink":
				target := filepath.Join(root, "target.json")
				if err := os.WriteFile(target, []byte(`{"schema_version":1}`), 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "directory":
				if err := os.Mkdir(path, 0700); err != nil {
					t.Fatal(err)
				}
			case "oversize":
				if err := os.WriteFile(path, []byte(strings.Repeat("x", 64*1024+1)), 0600); err != nil {
					t.Fatal(err)
				}
			}
			detector := &statuslineCaptureDetector{}
			now := time.Now().UTC()
			session := supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude, StateSince: now}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := (DetectSessionStateUseCase{StateDir: root, Detector: detector}).Execute(ctx, session, supervisor.NewVirtualTerminal(80, 24), now, false, nil); err != nil {
				t.Fatal(err)
			}
			if detector.gotStructured {
				t.Fatal("invalid statusline was accepted")
			}
		})
	}
}

func TestSCNsup01016StatuslineFreshness(t *testing.T) {
	now := time.Now().UTC()
	structured := &detect.StructuredInfo{ObservedAt: now.Add(-time.Minute), UsedPercentage: 42}
	input := detect.DetectInput{Now: now, LastSendConfirmedAt: &now, Structured: structured}
	if statuslineIsFresh(input, structured) {
		t.Fatal("statusline older than last confirmed send must be stale")
	}
	input.LastSendConfirmedAt = nil
	if !statuslineIsFresh(input, structured) {
		t.Fatal("first statusline observation must be accepted before any send")
	}
}

func TestSCNsup01016StatuslineRejectsInvalidUUID(t *testing.T) {
	now := time.Now().UTC()
	stateDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(stateDir, "statusline"), 0700); err != nil {
		t.Fatal(err)
	}
	id, _ := supervisor.NewSessionID("demo-1")
	path := filepath.Join(stateDir, "statusline", id.String()+".json")
	if err := os.WriteFile(path, []byte(`{"schema_version":1,"observed_at":"`+now.Format(time.RFC3339Nano)+`","used_percentage":42,"claude_session_id":"not-a-uuid"}`), 0600); err != nil {
		t.Fatal(err)
	}
	session := supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude}
	detector := &statuslineCaptureDetector{}
	_, err := (DetectSessionStateUseCase{StateDir: stateDir, Detector: detector}).Execute(context.Background(), session, supervisor.NewVirtualTerminal(80, 24), now, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !detector.gotStructured || detector.gotSessionID != "" || detector.gotPercentage != 42 {
		t.Fatalf("invalid UUID must preserve usage only: structured=%t id=%q percentage=%d", detector.gotStructured, detector.gotSessionID, detector.gotPercentage)
	}
}

func TestSCNsup01016StatuslineRejectsFIFO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "statusline.json")
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := readStatusline(path)
		result <- err
	}()
	select {
	case <-result:
	case <-time.After(100 * time.Millisecond):
		writer, err := os.OpenFile(path, os.O_WRONLY, 0)
		if err == nil {
			_ = writer.Close()
		}
		<-result
		t.Fatal("FIFO statusline read blocked")
	}
}

func TestSCNsup01016StatuslineRejectsOutsideGenerationAndFuture(t *testing.T) {
	now := time.Date(2026, 10, 11, 1, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		observed time.Time
		started  time.Time
	}{
		{name: "before_generation", observed: now.Add(-time.Second), started: now},
		{name: "too_far_in_future", observed: now.Add(6 * time.Second), started: now},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stateDir := t.TempDir()
			statusDir := filepath.Join(stateDir, "statusline")
			if err := os.Mkdir(statusDir, 0700); err != nil {
				t.Fatal(err)
			}
			id, _ := supervisor.NewSessionID("freshness-1")
			body := `{"schema_version":1,"observed_at":"` + tc.observed.Format(time.RFC3339Nano) + `","used_percentage":20,"claude_session_id":"123e4567-e89b-12d3-a456-426614174000"}`
			if err := os.WriteFile(filepath.Join(statusDir, id.String()+".json"), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			detector := &statuslineCaptureDetector{}
			session := supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude, StateSince: tc.started}
			_, err := (DetectSessionStateUseCase{StateDir: stateDir, Detector: detector, Now: func() time.Time { return now }}).Execute(context.Background(), session, supervisor.NewVirtualTerminal(80, 24), now, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			if detector.gotStructured {
				t.Fatal("out-of-range statusline timestamp was accepted")
			}
		})
	}
}

type statuslineCaptureDetector struct {
	gotStructured bool
	gotSessionID  string
	gotPercentage int
}

func (d *statuslineCaptureDetector) Detect(_ context.Context, in detect.DetectInput) (detect.DetectResult, error) {
	d.gotStructured = in.Structured != nil
	if in.Structured != nil {
		d.gotSessionID = in.Structured.SessionID
		d.gotPercentage = in.Structured.UsedPercentage
	}
	return detect.DetectResult{}, nil
}

func TestSCNsup01066MissingUsagePreservesSessionID(t *testing.T) {
	const uuid = "123e4567-e89b-12d3-a456-426614174000"
	for _, tc := range []struct {
		name, percentage, claudeID string
		offset                     time.Duration
		wantID                     string
		wantUsage                  bool
	}{
		{name: "missing", claudeID: uuid, wantID: uuid},
		{name: "null", percentage: `,"used_percentage":null`, claudeID: uuid, wantID: uuid},
		{name: "invalid_id", claudeID: "invalid"},
		{name: "before_generation", claudeID: uuid, offset: -time.Second},
		{name: "future", claudeID: uuid, offset: 6 * time.Second},
		{name: "out_of_range", percentage: `,"used_percentage":150`, claudeID: uuid},
		{name: "zero", percentage: `,"used_percentage":0`, claudeID: uuid, wantID: uuid, wantUsage: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			session, sessions, _ := observeSendFixture(t, supervisor.SessionIdle)
			now := *session.ChildStartedAt
			root := t.TempDir()
			if err := os.Mkdir(filepath.Join(root, "statusline"), 0700); err != nil {
				t.Fatal(err)
			}
			body := `{"schema_version":1,"observed_at":"` + now.Add(tc.offset).Format(time.RFC3339Nano) + `","claude_session_id":"` + tc.claudeID + `"` + tc.percentage + `}`
			if err := os.WriteFile(filepath.Join(root, "statusline", session.SessionID.String()+".json"), []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			detector := statuslineStructuredDetector{}
			result, err := (DetectSessionStateUseCase{StateDir: root, Sessions: sessions, Detector: detector, Now: func() time.Time { return now }}).Execute(context.Background(), session, supervisor.NewVirtualTerminal(80, 24), now, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			if result.SessionID != tc.wantID || (result.ContextPercent != nil) != tc.wantUsage {
				t.Fatalf("result=%+v", result)
			}
			if result.ContextPercent != nil && *result.ContextPercent != 0 {
				t.Fatalf("percentage=%d", *result.ContextPercent)
			}
			saved, err := sessions.Load(session.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			if saved.ClaudeSessionID != tc.wantID {
				t.Fatalf("persisted ID=%q want=%q", saved.ClaudeSessionID, tc.wantID)
			}
		})
	}
}

type statuslineStructuredDetector struct{}

func (statuslineStructuredDetector) Detect(_ context.Context, in detect.DetectInput) (detect.DetectResult, error) {
	return detect.NewStructuredInfoDetector().Detect(in), nil
}
