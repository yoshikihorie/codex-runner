package usecase

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/transport"
)

type submitStoreFake struct {
	reserveErrors []error
	reserved      []domain.TaskID
	released      []domain.TaskID
	releaseErr    error
}

func (f *submitStoreFake) Reserve(id domain.TaskID) error {
	f.reserved = append(f.reserved, id)
	if len(f.reserveErrors) == 0 {
		return nil
	}
	err := f.reserveErrors[0]
	f.reserveErrors = f.reserveErrors[1:]
	return err
}
func (f *submitStoreFake) Release(id domain.TaskID) error {
	f.released = append(f.released, id)
	return f.releaseErr
}

type submitPathLockFake struct {
	normalized []domain.NormalizedPath
	err        error
	calls      int
	ids        []domain.TaskID
	paths      [][]string
}

func (f *submitPathLockFake) Acquire(id domain.TaskID, paths []string) ([]domain.NormalizedPath, error) {
	f.calls++
	f.ids = append(f.ids, id)
	f.paths = append(f.paths, append([]string(nil), paths...))
	return f.normalized, f.err
}

type submitPathLockReleaserFake struct {
	calls     int
	ids       []domain.TaskID
	cancelled []bool
	err       error
}

func (f *submitPathLockReleaserFake) Release(ctx context.Context, id domain.TaskID) error {
	f.calls++
	f.ids = append(f.ids, id)
	f.cancelled = append(f.cancelled, ctx.Err() != nil)
	return f.err
}

type submitAdmitterFake struct {
	input         execution.TaskAdmissionInput
	calls         int
	result        execution.TaskAdmissionResult
	err           error
	compensated   []domain.TaskID
	compensateErr error
}

func (f *submitAdmitterFake) Admit(in execution.TaskAdmissionInput) (execution.TaskAdmissionResult, error) {
	f.calls++
	f.input = in
	return f.result, f.err
}

func (f *submitAdmitterFake) CompensateRejectedStart(id domain.TaskID) error {
	f.compensated = append(f.compensated, id)
	return f.compensateErr
}

type submitStarterFake struct {
	payloads []execution.TaskLaunchPayload
	rejected bool
}

func (f *submitStarterFake) Start(p execution.TaskLaunchPayload) bool {
	f.payloads = append(f.payloads, p)
	return !f.rejected
}

func (*submitStarterFake) Shutdown(context.Context) {}

type submitOptionsFake struct {
	model, effort     string
	modelOK, effortOK bool
	effortValue       *string
}

func (f submitOptionsFake) ResolveModel(domain.Subcommand, *string) (string, bool) {
	return f.model, f.modelOK
}
func (f submitOptionsFake) ResolveReasoningEffort(domain.Subcommand, *string) (*string, bool) {
	if f.effortValue != nil {
		value := *f.effortValue
		return &value, f.effortOK
	}
	return nil, f.effortOK
}

type submitFixture struct {
	uc       *SubmitTaskUseCase
	store    *submitStoreFake
	locks    *submitPathLockFake
	releaser *submitPathLockReleaserFake
	admitter *submitAdmitterFake
	starter  *submitStarterFake
}

func newSubmitFixture() submitFixture {
	store := &submitStoreFake{}
	locks := &submitPathLockFake{}
	releaser := &submitPathLockReleaserFake{}
	admitter := &submitAdmitterFake{}
	starter := &submitStarterFake{}
	uc := NewSubmitTaskUseCase(store, locks, releaser, admitter, 10, starter, submitOptionsFake{model: "test-model", modelOK: true, effortOK: true}, domain.ClockFunc(func() time.Time { return time.Date(2026, 8, 9, 1, 2, 3, 0, time.UTC) }), nil)
	uc.random = &submitByteReader{bytes: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24}}
	return submitFixture{uc, store, locks, releaser, admitter, starter}
}

type submitByteReader struct {
	bytes  []byte
	offset int
	err    error
}

func (r *submitByteReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	if r.offset >= len(r.bytes) {
		return 0, io.EOF
	}
	n := copy(p, r.bytes[r.offset:])
	r.offset += n
	return n, nil
}

func validSubmitInput(t *testing.T) SubmitTaskInput {
	t.Helper()
	return SubmitTaskInput{Subcommand: string(domain.SubcommandReview), RawSlug: "valid-slug", Prompt: "safe prompt", RawWorkingDir: t.TempDir(), RequestedAt: time.Date(2026, 8, 9, 1, 2, 3, 0, time.UTC)}
}

func assertSubmitError(t *testing.T, err error, code, message string, detail map[string]any) {
	t.Helper()
	value, ok := err.(*submitError)
	if !ok {
		t.Fatalf("error type=%T value=%v", err, err)
	}
	if value.code != code || value.message != message {
		t.Fatalf("code/message=(%q,%q), want=(%q,%q)", value.code, value.message, code, message)
	}
	if detail != nil && !reflect.DeepEqual(value.detail, detail) {
		t.Fatalf("detail=%#v want=%#v", value.detail, detail)
	}
}

func TestSubmitHandleMalformedParams(t *testing.T) {
	cases := []string{
		`{`,
		`{"requested_timeout_seconds":"bad"}`,
		`{"paths":"bad"}`,
		`{"paths":[1]}`,
		`null`, `[]`, `"scalar"`, `1`, `true`, ``,
	}
	for _, params := range cases {
		t.Run(params, func(t *testing.T) {
			fixture := newSubmitFixture()
			resp := fixture.uc.Handle(transport.Request{RequestID: "request", Params: json.RawMessage(params)})
			if resp.OK || resp.Error == nil || resp.Error.Code != "SUBMIT_PARAMS_MALFORMED" || resp.Error.MessageKey != "error.submit.paramsMalformed" {
				t.Fatalf("response=%#v", resp)
			}
			if resp.RequestID != "request" || resp.ProtocolVersion != transport.ProtocolVersion {
				t.Fatalf("metadata=%#v", resp)
			}
			if len(fixture.store.reserved) != 0 || fixture.locks.calls != 0 || fixture.admitter.calls != 0 || len(fixture.starter.payloads) != 0 {
				t.Fatalf("side effects occurred")
			}
		})
	}
}

func TestSubmitHandleAcceptsOptionalNullAndUnknownFields(t *testing.T) {
	for _, params := range []string{
		fmt.Sprintf(`{"subcommand":"review","slug":"valid-slug","prompt":"safe","working_dir":%q}`, t.TempDir()),
		fmt.Sprintf(`{"subcommand":"review","slug":"valid-slug","prompt":"safe","working_dir":%q,"model":null,"reasoning_effort":null,"paths":null,"unknown":true}`, t.TempDir()),
	} {
		t.Run(params, func(t *testing.T) {
			fixture := newSubmitFixture()
			resp := fixture.uc.Handle(transport.Request{RequestID: "request", Params: json.RawMessage(params)})
			if !resp.OK {
				t.Fatalf("response=%#v", resp)
			}
		})
	}
}

func TestSubmitExecuteValidationOrderAndNoSideEffects(t *testing.T) {
	timeout := 1
	cases := []struct {
		name, code string
		in         SubmitTaskInput
	}{
		{"slug before timeout", "SLUG_INVALID_FORMAT", SubmitTaskInput{RawSlug: "INVALID", RequestedTimeoutSeconds: &timeout}},
		{"timeout before prompt", "TIMEOUT_BELOW_MINIMUM", SubmitTaskInput{RawSlug: "valid-slug", RequestedTimeoutSeconds: &timeout}},
		{"prompt before subcommand", "PROMPT_EMPTY", SubmitTaskInput{RawSlug: "valid-slug", Prompt: " ", Subcommand: "bad", RequestedTimeoutSeconds: intPtr(1800)}},
		{"subcommand before model", "SUBCOMMAND_NOT_SUBMITTABLE", SubmitTaskInput{RawSlug: "valid-slug", Prompt: "safe", Subcommand: "bad", RequestedTimeoutSeconds: intPtr(1800)}},
		{"working dir before paths", "WORKING_DIR_NOT_ABSOLUTE", SubmitTaskInput{RawSlug: "valid-slug", Prompt: "safe", Subcommand: "impl", RequestedTimeoutSeconds: intPtr(1800), RawWorkingDir: "relative", RawPaths: []string{"relative"}}},
		{"paths last", "PATHS_NOT_ABSOLUTE", SubmitTaskInput{RawSlug: "valid-slug", Prompt: "safe", Subcommand: "impl", RequestedTimeoutSeconds: intPtr(1800), RawWorkingDir: t.TempDir(), RawPaths: []string{"relative"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSubmitFixture()
			_, err := fixture.uc.Execute(context.Background(), tc.in)
			assertSubmitError(t, err, tc.code, mapSubmitMessage(tc.code), nil)
			if len(fixture.store.reserved) != 0 || fixture.locks.calls != 0 || fixture.admitter.calls != 0 || len(fixture.starter.payloads) != 0 {
				t.Fatalf("side effects occurred")
			}
		})
	}
}

func TestSubmitExecuteResolvesOptionsAndBuildsAdmissionInput(t *testing.T) {
	effort := "high"
	fixture := newSubmitFixture()
	fixture.uc.options = submitOptionsFake{model: "resolved-model", modelOK: true, effortOK: true, effortValue: &effort}
	in := validSubmitInput(t)
	in.RawWorkingDir = in.RawWorkingDir + "/../" + filepath.Base(in.RawWorkingDir)
	if _, err := fixture.uc.Execute(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if fixture.admitter.input.Model != "resolved-model" || fixture.admitter.input.ReasoningEffort == nil || *fixture.admitter.input.ReasoningEffort != effort {
		t.Fatalf("input=%#v", fixture.admitter.input)
	}
	if fixture.admitter.input.SourceWorkingDir != filepath.Clean(in.RawWorkingDir) || fixture.admitter.input.SandboxMode != "read-only" {
		t.Fatalf("input=%#v", fixture.admitter.input)
	}
	if fixture.locks.calls != 0 || fixture.releaser.calls != 0 {
		t.Fatalf("non-impl path lock calls acquire=%d release=%d", fixture.locks.calls, fixture.releaser.calls)
	}
}

func TestSubmitExecuteRejectsDisallowedOptions(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		options    submitOptionsFake
	}{
		{"model", "MODEL_NOT_ALLOWED", submitOptionsFake{modelOK: false, effortOK: true}},
		{"effort", "REASONING_EFFORT_NOT_ALLOWED", submitOptionsFake{model: "model", modelOK: true, effortOK: false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSubmitFixture()
			fixture.uc.options = tc.options
			_, err := fixture.uc.Execute(context.Background(), validSubmitInput(t))
			assertSubmitError(t, err, tc.code, mapSubmitMessage(tc.code), nil)
			if len(fixture.store.reserved) != 0 || fixture.locks.calls != 0 || fixture.admitter.calls != 0 {
				t.Fatal("side effects occurred")
			}
		})
	}
}

func TestSubmitExecuteImplPathLockAndSandbox(t *testing.T) {
	fixture := newSubmitFixture()
	path, err := domain.NewNormalizedPath(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	fixture.locks.normalized = []domain.NormalizedPath{path}
	in := validSubmitInput(t)
	in.Subcommand = "impl"
	in.RawPaths = []string{t.TempDir()}
	if _, err := fixture.uc.Execute(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if fixture.locks.calls != 1 || !reflect.DeepEqual(fixture.admitter.input.NormalizedPaths, []domain.NormalizedPath{path}) || fixture.admitter.input.SandboxMode != "workspace-write" {
		t.Fatalf("lock/admission input mismatch")
	}
	if len(fixture.store.released) != 0 || fixture.releaser.calls != 0 {
		t.Fatal("successful submit released resources")
	}
}

func TestSubmitExecuteAdmissionResultsStartOnlyImmediatePayload(t *testing.T) {
	for _, tc := range []struct {
		name     string
		payload  bool
		position *int
	}{{"immediate", true, nil}, {"queued", false, intPtr(1)}} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := newSubmitFixture()
			if tc.payload {
				fixture.admitter.result.LaunchPayload = &execution.TaskLaunchPayload{Model: "payload"}
			}
			fixture.admitter.result.QueuePosition = tc.position
			fixture.admitter.result.Events = []domain.Event{}
			out, err := fixture.uc.Execute(context.Background(), validSubmitInput(t))
			if err != nil {
				t.Fatal(err)
			}
			if out.State != domain.StateQueued || !reflect.DeepEqual(out.QueuePosition, tc.position) || len(fixture.starter.payloads) != boolCount(tc.payload) {
				t.Fatalf("output=%#v payloads=%d", out, len(fixture.starter.payloads))
			}
			if tc.payload && !reflect.DeepEqual(fixture.starter.payloads[0], *fixture.admitter.result.LaunchPayload) {
				t.Fatalf("starter payload mismatch")
			}
		})
	}
}

func TestSubmitTaskUseCaseCompensatesRejectedLifecycleStart(t *testing.T) {
	fixture := newSubmitFixture()
	fixture.starter.rejected = true
	fixture.admitter.result.LaunchPayload = &execution.TaskLaunchPayload{Model: "payload"}
	in := validSubmitInput(t)
	in.Subcommand = "impl"
	in.RawPaths = []string{"/private/tmp/submit-rejected-start"}

	_, err := fixture.uc.Execute(context.Background(), in)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if len(fixture.starter.payloads) != 1 || len(fixture.admitter.compensated) != 1 || len(fixture.store.released) != 1 || fixture.releaser.calls != 1 {
		t.Fatalf("starts=%d compensations=%d reservations=%d path_locks=%d", len(fixture.starter.payloads), len(fixture.admitter.compensated), len(fixture.store.released), fixture.releaser.calls)
	}
}

func TestSubmitHandleMapsQueueFullWithDetailContract(t *testing.T) {
	fixture := newSubmitFixture()
	fixture.admitter.err = domain.ErrQueueFull
	resp := fixture.uc.Handle(transport.Request{RequestID: "request", Params: json.RawMessage(fmt.Sprintf(`{"subcommand":"review","slug":"valid-slug","prompt":"safe","working_dir":%q}`, t.TempDir()))})
	if resp.OK || resp.Error == nil || resp.Error.Code != "QUEUE_FULL" || resp.Error.MessageKey != "error.queue.full" {
		t.Fatalf("response=%#v", resp)
	}
	if !reflect.DeepEqual(resp.Error.Detail, map[string]any{"queue_max_depth": fixture.uc.queueMaxDepth}) {
		t.Fatalf("detail=%#v", resp.Error.Detail)
	}
	if len(fixture.store.released) != 1 || len(fixture.starter.payloads) != 0 {
		t.Fatalf("cleanup/start mismatch")
	}
}

func TestSubmitExecuteTaskIDFailuresAndCollisions(t *testing.T) {
	t.Run("random reader", func(t *testing.T) {
		fixture := newSubmitFixture()
		fixture.uc.random = &submitByteReader{err: errors.New("reader failure")}
		_, err := fixture.uc.Execute(context.Background(), validSubmitInput(t))
		assertSubmitError(t, fixture.uc.mapError(err), "TASK_ID_RANDOM_READ_FAILED", "error.taskId.randomReadFailed", nil)
		if len(fixture.store.reserved) != 0 || fixture.locks.calls != 0 || fixture.admitter.calls != 0 {
			t.Fatal("side effects occurred")
		}
	})
	t.Run("collision then success", func(t *testing.T) {
		fixture := newSubmitFixture()
		fixture.store.reserveErrors = []error{os.ErrExist, nil}
		if _, err := fixture.uc.Execute(context.Background(), validSubmitInput(t)); err != nil {
			t.Fatal(err)
		}
		if len(fixture.store.reserved) != 2 || fixture.store.reserved[0] == fixture.store.reserved[1] || fixture.admitter.calls != 1 || len(fixture.store.released) != 0 {
			t.Fatalf("reserve=%v admit=%d release=%v", fixture.store.reserved, fixture.admitter.calls, fixture.store.released)
		}
	})
	t.Run("collision limit", func(t *testing.T) {
		fixture := newSubmitFixture()
		fixture.store.reserveErrors = make([]error, taskIDGenerationMaxAttempts)
		for i := range fixture.store.reserveErrors {
			fixture.store.reserveErrors[i] = os.ErrExist
		}
		_, err := fixture.uc.Execute(context.Background(), validSubmitInput(t))
		assertSubmitError(t, fixture.uc.mapError(err), "TASK_DIR_CREATE_FAILED", "error.taskDir.createFailed", map[string]any{"task_id": fixture.store.reserved[len(fixture.store.reserved)-1].String()})
		if len(fixture.store.reserved) != taskIDGenerationMaxAttempts || fixture.locks.calls != 0 || fixture.admitter.calls != 0 || len(fixture.store.released) != 0 {
			t.Fatal("unexpected retry side effect")
		}
	})
	t.Run("reservation I/O failure retains task ID", func(t *testing.T) {
		fixture := newSubmitFixture()
		fixture.store.reserveErrors = []error{errors.New("reservation unavailable")}
		_, err := fixture.uc.Execute(context.Background(), validSubmitInput(t))
		assertSubmitError(t, err, "TASK_DIR_CREATE_FAILED", "error.taskDir.createFailed", map[string]any{"task_id": fixture.store.reserved[0].String()})
		if fixture.locks.calls != 0 || fixture.admitter.calls != 0 || len(fixture.starter.payloads) != 0 {
			t.Fatal("reservation failure crossed an execution boundary")
		}
	})
}

func TestNewSubmitTaskUseCaseRequiresPairedPathLockDependencies(t *testing.T) {
	for _, tc := range []struct {
		name     string
		locks    SubmitPathLockAcquirer
		releaser SubmitPathLockReleaser
		panics   bool
	}{
		{name: "both nil"},
		{name: "both set", locks: &submitPathLockFake{}, releaser: &submitPathLockReleaserFake{}},
		{name: "acquirer only", locks: &submitPathLockFake{}, panics: true},
		{name: "releaser only", releaser: &submitPathLockReleaserFake{}, panics: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if (recover() != nil) != tc.panics {
					t.Fatal("unexpected constructor panic state")
				}
			}()
			NewSubmitTaskUseCase(&submitStoreFake{}, tc.locks, tc.releaser, &submitAdmitterFake{}, 10, &submitStarterFake{}, submitOptionsFake{modelOK: true, effortOK: true}, domain.ClockFunc(time.Now), nil)
		})
	}
}

func TestSubmitExecuteAdmissionFailureCompensatesWithoutCancelledContext(t *testing.T) {
	fixture := newSubmitFixture()
	fixture.admitter.err = errors.New("admission failed")
	const canary = "CANARY-SECRET-VALUE-DO-NOT-LOG"
	fixture.releaser.err = errors.New("release path lock " + canary)
	fixture.store.releaseErr = errors.New("release reservation " + canary)
	var logs bytes.Buffer
	fixture.uc.logger = slog.New(slog.NewTextHandler(&logs, nil))
	in := validSubmitInput(t)
	in.Subcommand = "impl"
	in.RawPaths = []string{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := fixture.uc.Execute(ctx, in)
	if err == nil || fixture.releaser.calls != 1 || fixture.releaser.cancelled[0] || len(fixture.store.released) != 1 || len(fixture.starter.payloads) != 0 {
		t.Fatalf("error=%v releases=%d/%d cancelled=%v", err, fixture.releaser.calls, len(fixture.store.released), fixture.releaser.cancelled)
	}
	if strings.Contains(logs.String(), canary) || !strings.Contains(logs.String(), "error=") {
		t.Fatalf("unsafe compensation logs: %s", logs.String())
	}
}

func mapSubmitMessage(code string) string {
	return map[string]string{"SLUG_INVALID_FORMAT": "error.slug.invalidFormat", "TIMEOUT_BELOW_MINIMUM": "error.timeout.belowMinimum", "PROMPT_EMPTY": "error.prompt.empty", "SUBCOMMAND_NOT_SUBMITTABLE": "error.subcommand.notSubmittable", "MODEL_NOT_ALLOWED": "error.model.notAllowed", "REASONING_EFFORT_NOT_ALLOWED": "error.reasoningEffort.notAllowed", "WORKING_DIR_NOT_ABSOLUTE": "error.workingDir.notAbsolute", "PATHS_NOT_ABSOLUTE": "error.paths.notAbsolute"}[code]
}
func intPtr(value int) *int { return &value }
func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}
