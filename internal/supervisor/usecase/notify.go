package usecase

import (
	"context"
	"errors"
	"os/exec"
	"regexp"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// Canonical notify limits are copied from specifications/10-shared/validation-rules.md.
const notifyTimeout = 10 * time.Second
const notifyDedup = 60 * time.Second
const notifyQueueMax = 32

var notifyArgPattern = regexp.MustCompile(`^[a-z0-9_-]+$`)

type EventRecord struct {
	Kind      supervisor.SupervisorEventKind
	SessionID string
	Reason    string
}
type NotifyRecorder interface{ RecordEvent(EventRecord) }
type NotifyRunner interface {
	Run(context.Context, string, []string, []string) error
}
type execNotifyRunner struct{}

func (execNotifyRunner) Run(ctx context.Context, path string, args []string, env []string) error {
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = env
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	return cmd.Run()
}

type notifyRequest struct {
	kind supervisor.SupervisorEventKind
	id   string
}
type NotifyUseCase struct {
	command        string
	runner         NotifyRunner
	recorder       NotifyRecorder
	now            func() time.Time
	timeout, dedup time.Duration
	max            int
	queue          chan notifyRequest
	mu             sync.Mutex
	last           map[string]time.Time
	closeOnce      sync.Once
	done           chan struct{}
	closed         bool
}

func NewNotifyUseCase(command string, runner NotifyRunner, recorder NotifyRecorder, now func() time.Time) *NotifyUseCase {
	return NewNotifyUseCaseWithOptions(command, runner, recorder, now, notifyTimeout, notifyDedup, notifyQueueMax)
}
func NewNotifyUseCaseWithOptions(command string, runner NotifyRunner, recorder NotifyRecorder, now func() time.Time, timeout, dedup time.Duration, max int) *NotifyUseCase {
	if command == "" {
		command = "/usr/bin/osascript"
	}
	if runner == nil {
		runner = execNotifyRunner{}
	}
	if now == nil {
		now = time.Now
	}
	if timeout <= 0 {
		timeout = notifyTimeout
	}
	if dedup <= 0 {
		dedup = notifyDedup
	}
	if max <= 0 {
		max = notifyQueueMax
	}
	u := &NotifyUseCase{command: command, runner: runner, recorder: recorder, now: now, timeout: timeout, dedup: dedup, max: max, queue: make(chan notifyRequest, max), last: make(map[string]time.Time), done: make(chan struct{})}
	go u.worker()
	return u
}
func (u *NotifyUseCase) Notify(_ context.Context, kind supervisor.SupervisorEventKind, id string) error {
	if !notificationTarget(kind) {
		return nil
	}
	if !notifyArgPattern.MatchString(string(kind)) {
		u.record(supervisor.EventKindNotifyFailed, "")
		return errors.New("invalid notification kind")
	}
	if id == "" {
		id = "-"
	} else if !notifyArgPattern.MatchString(id) {
		u.record(supervisor.EventKindNotifyFailed, "")
		return errors.New("invalid notification session id")
	}
	key := string(kind) + "\x00" + id
	u.mu.Lock()
	if u.closed {
		u.mu.Unlock()
		return nil
	}
	now := u.now()
	for previousKey, at := range u.last {
		if now.Sub(at) >= u.dedup {
			delete(u.last, previousKey)
		}
	}
	if last, ok := u.last[key]; ok && now.Sub(last) < u.dedup {
		u.mu.Unlock()
		return nil
	}
	select {
	case u.queue <- notifyRequest{kind, id}:
		u.last[key] = now
		u.mu.Unlock()
		return nil
	default:
		select {
		case old := <-u.queue:
			u.record(supervisor.EventKindNotifyDropped, validRecordID(old.id))
		default:
		}
		select {
		case u.queue <- notifyRequest{kind, id}:
			u.last[key] = now
		default:
			u.record(supervisor.EventKindNotifyDropped, validRecordID(id))
		}
		u.mu.Unlock()
		return nil
	}
}
func (u *NotifyUseCase) worker() {
	defer close(u.done)
	for req := range u.queue {
		if u.command == "none" {
			continue
		}
		path := u.command
		if path != "none" {
			resolved, err := proc.ValidateExecutable(path)
			if err != nil {
				reason := "invalid_executable"
				var validationErr *proc.ExecutableValidationError
				if errors.As(err, &validationErr) {
					reason = validationErr.Reason
				}
				u.recordWithReason(supervisor.EventKindNotifyFailed, validRecordID(req.id), reason)
				continue
			}
			path = resolved
		}
		args := []string{string(req.kind), req.id}
		if path == "/usr/bin/osascript" {
			args = []string{"-e", `display notification "` + string(req.kind) + ` ` + req.id + `" with title "codexd-supervisor"`}
		}
		ctx, cancel := context.WithTimeout(context.Background(), u.timeout)
		err := u.runner.Run(ctx, path, args, []string{"PATH=" + proc.FixedPath(), "LANG=en_US.UTF-8"})
		cancel()
		if err != nil {
			u.record(supervisor.EventKindNotifyFailed, req.id)
		}
	}
}

func (u *NotifyUseCase) Close() {
	u.closeOnce.Do(func() { u.mu.Lock(); u.closed = true; close(u.queue); u.mu.Unlock(); <-u.done })
}
func (u *NotifyUseCase) record(kind supervisor.SupervisorEventKind, id string) {
	u.recordWithReason(kind, id, "")
}
func (u *NotifyUseCase) recordWithReason(kind supervisor.SupervisorEventKind, id, reason string) {
	if u.recorder != nil {
		u.recorder.RecordEvent(EventRecord{Kind: kind, SessionID: id, Reason: reason})
	}
}
func validRecordID(id string) string {
	if id == "" || id == "-" || !notifyArgPattern.MatchString(id) {
		return ""
	}
	return id
}
func notificationTarget(k supervisor.SupervisorEventKind) bool {
	switch k {
	case supervisor.EventKindSessionStopped, supervisor.EventKindSessionFailed, supervisor.EventKindSessionExited, supervisor.EventKindSendUnconfirmed, supervisor.EventKindRecommendationBlocked, supervisor.EventKindPermissionPending, supervisor.EventKindRotateFailed, supervisor.EventKindRotateCompleted, supervisor.EventKindRotateIdleWaitTimeout, supervisor.EventKindVerifyFailed, supervisor.EventKindCommitFailed, supervisor.EventKindPushBlocked, supervisor.EventKindPushFailed, supervisor.EventKindRestartLimitExceeded, supervisor.EventKindSessionRestarted, supervisor.EventKindOrphanChildAlive, supervisor.EventKindUndeterminedLong, supervisor.EventKindSensitivePathBlocked, supervisor.EventKindStateFileCorrupt, supervisor.EventKindStopFileRemoved, supervisor.EventKindSupervisorStarted, supervisor.EventKindPermissionModeUnsafe, supervisor.EventKindSessionStartFailed, supervisor.EventKindContextUnknown, supervisor.EventKindLunaImplNotAllowed:
		return true
	}
	return false
}
