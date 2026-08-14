package execution

import (
	"context"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// SCN-exec-04-10 / SCN-exec-02-10: adoption bases its decision on the lock,
// including a starting task that has not yet recorded a PID.
func TestCheckLivenessAndTaskAdoptHandleStartingRunningAndStalled(t *testing.T) {
	for _, state := range []domain.TaskState{domain.StateStarting, domain.StateRunning, domain.StateStalled} {
		t.Run(string(state), func(t *testing.T) {
			id, err := domain.NewTaskID("impl-20260814-120001-abcd-adoption")
			if err != nil {
				t.Fatal(err)
			}
			liveness := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(domain.TaskID) string { return "/private/tmp/task.lock" })
			dead, err := liveness.Execute(context.Background(), id)
			if err != nil || dead {
				t.Fatalf("liveness=(%v,%v)", dead, err)
			}
			at := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
			slug, _ := domain.NewSlug("adoption")
			task, _, err := domain.NewTask(id, domain.SubcommandImpl, slug, nil, at, 1)
			if err != nil {
				t.Fatal(err)
			}
			timeout, _ := domain.NewTimeout(nil, 1800)
			if _, err = task.Start(timeout, "gpt-5", at); err != nil {
				t.Fatal(err)
			}
			if state != domain.StateStarting {
				if _, err = task.RecordProcessInfo(123, at, at); err != nil {
					t.Fatal(err)
				}
				if err = task.ConfirmRunning(at); err != nil {
					t.Fatal(err)
				}
			}
			if state == domain.StateStalled {
				if _, err = task.MarkStalled(1, at); err != nil {
					t.Fatal(err)
				}
			}
			events, err := task.Adopt(dead, at)
			if err != nil || task.State() != domain.StateRunning || len(events) != 1 {
				t.Fatalf("state=%q events=%#v err=%v", task.State(), events, err)
			}
		})
	}
}
