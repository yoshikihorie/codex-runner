package usecase

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

func TestConfirmTaskRunningUseCaseContract(t *testing.T) {
	var output ConfirmTaskRunningOutput
	if output.Dead {
		t.Fatal("zero output must not report dead")
	}
}
func TestConfirmTaskRunningUseCaseRejectsZeroOccurredAtBeforeLocking(t *testing.T) {
	uc := &ConfirmTaskRunningUseCase{}
	if _, err := uc.Execute(context.Background(), testID(t, "confirm-zero-time"), time.Time{}); err == nil {
		t.Fatal("zero occurredAt was accepted")
	}
}

func TestConfirmTaskRunningUseCaseLivenessBranches(t *testing.T) {
	cases := []struct {
		name       string
		dead       bool
		liveErr    error
		wantState  domain.TaskState
		wantEvents int
	}{{"alive", false, nil, domain.StateRunning, 0}, {"dead", true, nil, domain.StateOrphaned, 1}, {"error", false, errors.New("liveness"), domain.StateStarting, 0}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			trace := []string{}
			task := lifecycleTask(t, domain.SubcommandImpl)
			starting := lifecycleSnapshot(t, task, domain.StateStarting)
			tasks := &lifecycleRecordingTaskStore{loads: []lifecycleLoadResult{{snapshot: starting}}, trace: &trace, loadName: "load", saveName: "save"}
			locker := &lifecycleRecordingTaskLocker{trace: &trace}
			lock := &lifecycleRecordingLivenessLock{dead: tc.dead, err: tc.liveErr, trace: &trace, traceName: "check-liveness"}
			writer := &lifecycleRecordingContractWriter{trace: &trace}
			uc := NewConfirmTaskRunningUseCase(tasks, locker, execution.NewCheckLivenessUseCase(lock, func(domain.TaskID) string { return "/private/tmp/task.lock" }), writer)
			output, err := uc.Execute(context.Background(), task.ID(), testLifecycleTime)
			if tc.liveErr != nil {
				if !errors.Is(err, tc.liveErr) || tasks.saveCalls != 0 || writer.appendCalls != 0 || starting.State != domain.StateStarting {
					t.Fatalf("error branch output=%+v err=%v", output, err)
				}
			} else {
				if err != nil || output.Dead != tc.dead || output.State != tc.wantState || tasks.saveCalls != 1 || tasks.saved[0].State != tc.wantState || writer.appendCalls != tc.wantEvents {
					t.Fatalf("output=%+v err=%v", output, err)
				}
				if tc.dead && writer.events[0].Type() != "TaskOrphanDetected" {
					t.Fatalf("events=%v", writer.events)
				}
			}
			if lock.calls != 1 || locker.lockCalls != 1 || locker.unlockCalls != 1 {
				t.Fatalf("liveness/lock calls trace=%v", trace)
			}
		})
	}
}
