package recovery

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

const expectedPartialOutputHeader = "## 途中経過（未完了）\n\n" +
	"タイムアウトで最終回答が生成されなかったため、stderr.log の末尾を途中経過として保存しました。" +
	"以下は完全な成果物ではありません。参考情報として扱い、レビュー実績には数えないでください。" +
	"\n\n---\n\n"

type partialTestReader struct {
	lastPresent bool
	lastErr     error
	stderr      []byte
	stderrErr   error
	lastCalls   int
	stderrCalls int
}

func (r *partialTestReader) ReadLastMessage(domain.TaskID) (bool, error) {
	r.lastCalls++
	return r.lastPresent, r.lastErr
}

func (r *partialTestReader) ReadStderrLog(domain.TaskID) ([]byte, error) {
	r.stderrCalls++
	return r.stderr, r.stderrErr
}

type partialTestWriter struct {
	err      error
	contents []string
}

func (w *partialTestWriter) WritePartialOutput(_ domain.TaskID, content string) error {
	w.contents = append(w.contents, content)
	return w.err
}

func TestNewSavePartialOutputUseCaseRejectsNilDependencies(t *testing.T) {
	for _, tc := range []struct {
		name  string
		build func()
	}{
		{"nil reader", func() { NewSavePartialOutputUseCase(nil, &partialTestWriter{}) }},
		{"typed nil reader", func() { NewSavePartialOutputUseCase((*partialTestReader)(nil), &partialTestWriter{}) }},
		{"nil contract", func() { NewSavePartialOutputUseCase(&partialTestReader{}, nil) }},
		{"typed nil contract", func() { NewSavePartialOutputUseCase(&partialTestReader{}, (*partialTestWriter)(nil)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.build()
		})
	}
}

func TestSavePartialOutputUseCaseExecute(t *testing.T) {
	validTaskID, err := domain.NewTaskID("impl-20260808-120000-abcd-save-partial")
	if err != nil {
		t.Fatalf("NewTaskID: %v", err)
	}

	bytesAtLimit := bytes.Repeat([]byte("a"), partialOutputTailBytes)
	bytesOverLimit := append([]byte("x"), bytes.Repeat([]byte("a"), partialOutputTailBytes)...)
	utf8Boundary := append([]byte("xあ"), bytes.Repeat([]byte("z"), partialOutputTailBytes-2)...)
	lines400 := strings.Repeat("line\n", partialOutputTailLines)
	lines401 := "discard\n" + lines400

	tests := []struct {
		name      string
		ctx       context.Context
		input     SavePartialOutputInput
		configure func(*partialTestReader, *partialTestWriter)
		check     func(*testing.T, SavePartialOutputOutput, error, *partialTestReader, *partialTestWriter, string)
	}{
		{
			name:      "01 saves non-empty stderr with canonical header",
			ctx:       context.Background(),
			input:     SavePartialOutputInput{TaskID: validTaskID, OccurredAt: time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = []byte("progress") },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, _ string) {
				if err != nil || !out.Saved || len(w.contents) != 1 || !strings.HasPrefix(w.contents[0], expectedPartialOutputHeader) {
					t.Fatalf("result = (%+v, %v), contents = %#v", out, err, w.contents)
				}
			},
		},
		{
			name: "02 keeps byte tail at exact limit", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = bytesAtLimit },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, _ string) {
				assertSavedContent(t, out, err, w, string(bytesAtLimit))
			},
		},
		{
			name: "03 keeps only byte tail over limit", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = bytesOverLimit },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, _ string) {
				assertSavedContent(t, out, err, w, string(bytesOverLimit[1:]))
			},
		},
		{
			name: "04 missing stderr is not saved", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(_ *partialTestReader, _ *partialTestWriter) {},
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, logs string) {
				assertNotSaved(t, out, err, w)
				if logs != "" {
					t.Fatalf("unexpected logs: %q", logs)
				}
			},
		},
		{
			name: "05 empty stderr is not saved", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = []byte{} },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, _ string) {
				assertNotSaved(t, out, err, w)
			},
		},
		{
			name: "06 stderr read error is logged but not returned", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) {
				r.stderr = []byte("secret stderr")
				r.stderrErr = errors.New("io error")
			},
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, logs string) {
				assertNotSaved(t, out, err, w)
				assertLogHas(t, logs, "task_id", "io error")
				assertLogLacks(t, logs, "secret stderr")
			},
		},
		{
			name: "07 write error is logged but not returned", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, w *partialTestWriter) {
				r.stderr = []byte("progress")
				w.err = errors.New("write failed")
			},
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, _ *partialTestWriter, logs string) {
				if err != nil || out.Saved {
					t.Fatalf("result = (%+v, %v)", out, err)
				}
				assertLogHas(t, logs, "write failed")
			},
		},
		{
			name: "08 strips ANSI escape sequences", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = []byte("\x1b[31mRED\x1b[0m\n") },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, _ string) {
				assertSavedContent(t, out, err, w, "RED\n")
				assertLogLacks(t, w.contents[0], "\x1b")
			},
		},
		{
			name: "09 sequential calls are idempotent", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = []byte("same output") },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, r *partialTestReader, w *partialTestWriter, logs string) {
				assertSavedContent(t, out, err, w, "same output")
				uc := NewSavePartialOutputUseCase(r, w, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
				second, secondErr := uc.Execute(context.Background(), SavePartialOutputInput{TaskID: validTaskID})
				if secondErr != nil || !second.Saved || len(w.contents) != 2 || w.contents[0] != w.contents[1] || logs != "" {
					t.Fatalf("second = (%+v, %v), contents = %#v", second, secondErr, w.contents)
				}
			},
		},
		{
			name: "10 present last message short-circuits", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.lastPresent = true },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, r *partialTestReader, w *partialTestWriter, logs string) {
				assertNotSaved(t, out, err, w)
				if r.stderrCalls != 0 || logs != "" {
					t.Fatalf("stderr calls = %d, logs = %q", r.stderrCalls, logs)
				}
			},
		},
		{
			name: "11 last message read error is logged and short-circuits", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.lastErr = errors.New("last message io error") },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, r *partialTestReader, w *partialTestWriter, logs string) {
				assertNotSaved(t, out, err, w)
				if r.stderrCalls != 0 {
					t.Fatalf("stderr calls = %d", r.stderrCalls)
				}
				assertLogHas(t, logs, "last message io error")
			},
		},
		{
			name: "12 drops incomplete UTF-8 prefix", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = utf8Boundary },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, _ string) {
				assertSavedContent(t, out, err, w, strings.Repeat("z", partialOutputTailBytes-2))
				if !utf8.ValidString(strings.TrimPrefix(w.contents[0], partialOutputHeader)) {
					t.Fatal("body is invalid UTF-8")
				}
			},
		},
		{
			name: "13 normalizes CRLF", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = []byte("a\r\nb\nc\r\n") },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, _ string) {
				assertSavedContent(t, out, err, w, "a\nb\nc\n")
			},
		},
		{
			name: "14 keeps exactly 400 logical lines", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = []byte(lines400) },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, _ string) {
				assertSavedContent(t, out, err, w, lines400)
			},
		},
		{
			name: "15 keeps final 400 of 401 logical lines", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = []byte(lines401) },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, _ string) {
				assertSavedContent(t, out, err, w, lines400)
			},
		},
		{
			name: "16 trailing newline does not add logical line", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = []byte("a\nb\n") },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, _ string) {
				assertSavedContent(t, out, err, w, "a\nb\n")
			},
		},
		{
			name: "17 canceled context returns its error before collaborators", ctx: canceledContext(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(_ *partialTestReader, _ *partialTestWriter) {},
			check: func(t *testing.T, _ SavePartialOutputOutput, err error, r *partialTestReader, w *partialTestWriter, _ string) {
				if !errors.Is(err, context.Canceled) || r.lastCalls != 0 || r.stderrCalls != 0 || len(w.contents) != 0 {
					t.Fatalf("err = %v, calls = (%d, %d, %d)", err, r.lastCalls, r.stderrCalls, len(w.contents))
				}
			},
		},
		{
			name: "18 zero task ID returns input error before collaborators", ctx: context.Background(), input: SavePartialOutputInput{},
			configure: func(_ *partialTestReader, _ *partialTestWriter) {},
			check: func(t *testing.T, _ SavePartialOutputOutput, err error, r *partialTestReader, w *partialTestWriter, _ string) {
				if err == nil || r.lastCalls != 0 || r.stderrCalls != 0 || len(w.contents) != 0 {
					t.Fatalf("err = %v, calls = (%d, %d, %d)", err, r.lastCalls, r.stderrCalls, len(w.contents))
				}
			},
		},
		{
			name: "19 expired context returns its error before collaborators", ctx: expiredContext(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(_ *partialTestReader, _ *partialTestWriter) {},
			check: func(t *testing.T, _ SavePartialOutputOutput, err error, r *partialTestReader, w *partialTestWriter, _ string) {
				if !errors.Is(err, context.DeadlineExceeded) || r.lastCalls != 0 || r.stderrCalls != 0 || len(w.contents) != 0 {
					t.Fatalf("err = %v, calls = (%d, %d, %d)", err, r.lastCalls, r.stderrCalls, len(w.contents))
				}
			},
		},
		{
			name: "20 zero occurred at is accepted", ctx: context.Background(), input: SavePartialOutputInput{TaskID: validTaskID},
			configure: func(r *partialTestReader, _ *partialTestWriter) { r.stderr = []byte("progress") },
			check: func(t *testing.T, out SavePartialOutputOutput, err error, _ *partialTestReader, w *partialTestWriter, _ string) {
				assertSavedContent(t, out, err, w, "progress")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &partialTestReader{}
			w := &partialTestWriter{}
			tt.configure(r, w)
			var logBuffer bytes.Buffer
			uc := NewSavePartialOutputUseCase(r, w, slog.New(slog.NewTextHandler(&logBuffer, nil)))
			out, err := uc.Execute(tt.ctx, tt.input)
			tt.check(t, out, err, r, w, logBuffer.String())
		})
	}
}

func assertSavedContent(t *testing.T, out SavePartialOutputOutput, err error, w *partialTestWriter, wantBody string) {
	t.Helper()
	if err != nil || !out.Saved || len(w.contents) != 1 || w.contents[0] != partialOutputHeader+wantBody {
		t.Fatalf("result = (%+v, %v), contents = %#v", out, err, w.contents)
	}
}

func assertNotSaved(t *testing.T, out SavePartialOutputOutput, err error, w *partialTestWriter) {
	t.Helper()
	if err != nil || out.Saved || len(w.contents) != 0 {
		t.Fatalf("result = (%+v, %v), contents = %#v", out, err, w.contents)
	}
}

func assertLogHas(t *testing.T, logs string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(logs, want) {
			t.Fatalf("logs %q do not contain %q", logs, want)
		}
	}
}

func assertLogLacks(t *testing.T, actual, unwanted string) {
	t.Helper()
	if strings.Contains(actual, unwanted) {
		t.Fatalf("%q unexpectedly contains %q", actual, unwanted)
	}
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func expiredContext() context.Context {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	return ctx
}
