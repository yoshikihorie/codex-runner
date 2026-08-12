package execution

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type testPoller struct {
	onSleep func(context.Context) error
	calls   int
}

func (p *testPoller) Sleep(ctx context.Context, _ time.Duration) error {
	p.calls++
	if p.onSleep != nil {
		return p.onSleep(ctx)
	}
	return nil
}

func tailID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260812-120000-a1b2-tail")
	if err != nil {
		t.Fatal(err)
	}
	return id
}
func tailLiveness(fn func(string) (bool, error)) *CheckLivenessUseCase {
	return NewCheckLivenessUseCase(domain.LivenessLockFunc(fn), func(id domain.TaskID) string { return id.String() })
}

func TestStdoutTailReaderFinalEOFAndRetryClassification(t *testing.T) {
	file, err := os.OpenFile(t.TempDir()+"/stdout.log", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var logs bytes.Buffer
	reader := NewStdoutTailReader(context.Background(), file, tailID(t), tailLiveness(func(string) (bool, error) { return false, errors.New("lock io failed") }), time.Nanosecond, slog.New(slog.NewTextHandler(&logs, nil)))
	poll := &testPoller{onSleep: func(context.Context) error {
		reader.liveness = tailLiveness(func(string) (bool, error) { return true, nil })
		return nil
	}}
	reader.poller = poll
	buf := make([]byte, 1)
	n, err := reader.Read(buf)
	if n != 0 || err != io.EOF || poll.calls != 1 {
		t.Fatalf("n=%d err=%v polls=%d", n, err, poll.calls)
	}
	if !strings.Contains(logs.String(), "LIVENESS_LOCK_IO_ERROR") {
		t.Fatalf("logs=%s", logs.String())
	}
}

func TestStdoutTailReaderNotFoundAndContextErrorsAreNotLivenessIO(t *testing.T) {
	for _, want := range []error{domain.ErrTaskNotFound, context.Canceled, context.DeadlineExceeded} {
		t.Run(want.Error(), func(t *testing.T) {
			file, err := os.OpenFile(t.TempDir()+"/stdout.log", os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			ctx := context.Background()
			if want == context.Canceled {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			if want == context.DeadlineExceeded {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
				defer cancel()
			}
			var logs bytes.Buffer
			reader := NewStdoutTailReader(ctx, file, tailID(t), tailLiveness(func(string) (bool, error) { return false, want }), time.Nanosecond, slog.New(slog.NewTextHandler(&logs, nil)))
			reader.poller = &testPoller{onSleep: func(context.Context) error { return want }}
			_, got := reader.Read(make([]byte, 1))
			if got != want {
				t.Fatalf("error=%v want=%v", got, want)
			}
			if strings.Contains(logs.String(), "LIVENESS_LOCK_IO_ERROR") {
				t.Fatalf("logs=%s", logs.String())
			}
		})
	}
}

func TestStdoutTailReaderSuppressesConsecutiveLivenessIOErrors(t *testing.T) {
	file, err := os.OpenFile(t.TempDir()+"/stdout.log", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var logs bytes.Buffer
	reader := NewStdoutTailReader(context.Background(), file, tailID(t), tailLiveness(func(string) (bool, error) { return false, errors.New("io") }), time.Nanosecond, slog.New(slog.NewTextHandler(&logs, nil)))
	reader.poller = &testPoller{onSleep: func(context.Context) error {
		reader.liveness = tailLiveness(func(string) (bool, error) { return true, nil })
		return nil
	}}
	_, err = reader.Read(make([]byte, 1))
	if err != io.EOF || strings.Count(logs.String(), "LIVENESS_LOCK_IO_ERROR") != 1 {
		t.Fatalf("err=%v logs=%s", err, logs.String())
	}
}

func TestStdoutTailReaderPollCancellationPropagates(t *testing.T) {
	file, err := os.OpenFile(t.TempDir()+"/stdout.log", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	reader := NewStdoutTailReader(ctx, file, tailID(t), tailLiveness(func(string) (bool, error) { return false, domain.ErrTaskNotFound }), time.Nanosecond)
	reader.poller = &testPoller{onSleep: func(context.Context) error { cancel(); return context.Canceled }}
	if _, err := reader.Read(make([]byte, 1)); err != context.Canceled {
		t.Fatalf("err=%v", err)
	}
}

func TestStdoutTailReaderReturnsDataWrittenBeforeDeathThenEOF(t *testing.T) {
	path := t.TempDir() + "/stdout.log"
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader := NewStdoutTailReader(context.Background(), file, tailID(t), tailLiveness(func(string) (bool, error) { return false, nil }), time.Nanosecond)
	reader.poller = &testPoller{onSleep: func(context.Context) error {
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			return err
		}
		reader.liveness = tailLiveness(func(string) (bool, error) { return true, nil })
		return nil
	}}
	buf := make([]byte, 1)
	if n, err := reader.Read(buf); n != 1 || err != nil || string(buf) != "x" {
		t.Fatalf("n=%d err=%v buf=%q", n, err, buf)
	}
	if n, err := reader.Read(buf); n != 0 || err != io.EOF {
		t.Fatalf("n=%d err=%v", n, err)
	}
}

func TestStdoutTailReaderConstructorValidation(t *testing.T) {
	file, _ := os.CreateTemp(t.TempDir(), "stdout")
	live := tailLiveness(func(string) (bool, error) { return false, nil })
	for _, tc := range []struct {
		name string
		call func()
	}{{"nil-context", func() { NewStdoutTailReader(nil, file, tailID(t), live, 0) }}, {"nil-file", func() { NewStdoutTailReader(context.Background(), nil, tailID(t), live, 0) }}, {"nil-liveness", func() { NewStdoutTailReader(context.Background(), file, tailID(t), nil, 0) }}, {"many-loggers", func() {
		NewStdoutTailReader(context.Background(), file, tailID(t), live, 0, slog.Default(), slog.Default())
	}}} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			tc.call()
		})
	}
	if got := NewStdoutTailReader(context.Background(), file, tailID(t), live, 0); got.pollInterval != stdoutTailPollInterval {
		t.Fatalf("interval=%v", got.pollInterval)
	}
}
