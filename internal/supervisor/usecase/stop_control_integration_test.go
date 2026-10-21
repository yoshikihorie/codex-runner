package usecase

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

type integrationSender struct{ calls atomic.Int32 }

func (s *integrationSender) Execute(context.Context, SendInput) (SendResult, error) {
	s.calls.Add(1)
	return SendResult{}, nil
}

func integrationHome(t *testing.T) string {
	t.Helper()
	home, err := os.MkdirTemp("/tmp", "g42-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(home) })
	t.Setenv("HOME", home)
	if err := os.MkdirAll(filepath.Join(home, ".codex"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, ".codex", "auth.json"), []byte(`{"auth_mode":"chatgpt"}`), 0600); err != nil {
		t.Fatal(err)
	}
	return home
}

func TestStopControlIntegration_G42HandoffResumeStages(t *testing.T) {
	for _, step := range []supervisor.SupervisorRotateStep{supervisor.RotateSendEnd, supervisor.RotateConfirmHandoff, supervisor.RotateSendExit} {
		t.Run(string(step), func(t *testing.T) {
			home := integrationHome(t)
			id := supervisor.SessionID("g42-handoff-1")
			now := time.Now().UTC()
			initial := supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude, State: supervisor.SessionRotating, ClaudeSessionID: "claude-handoff", Generation: 1, ChildPID: 42, Rotation: &supervisor.RotationRecord{Step: step, StartedAt: now, StepStartedAt: &now}}
			if step == supervisor.RotateSendExit {
				initial.ChildPID = 0
				initial.LastSend = &supervisor.LastSendRecord{Kind: supervisor.SendKindHandoffExit, Confirmed: true}
			}
			records := &regressionSessionStore{session: initial, saved: make(chan struct{}, 1)}
			stops := NewStopState(home)
			ctx, cancel := context.WithCancel(context.Background())
			respawn := &stopRotationRespawner{store: records}
			sender := &integrationSender{}
			rotation := &RotateSessionUseCase{Sessions: records, Stop: stops, Send: sender, Respawn: respawn, PendingRemoteControl: &PendingRemoteControlUseCase{}, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking }, ChildAlive: func(supervisor.SessionID) bool { return step != supervisor.RotateSendExit }, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
				return supervisor.RotationCheckpointSnapshot{}, nil
			}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return false }, Wake: func(supervisor.SessionID) <-chan struct{} { return ctx.Done() }}
			t.Cleanup(func() {
				cancel()
				deadline := time.After(time.Second)
				for rotation.IsRunning(id) {
					select {
					case <-deadline:
						t.Error("rotation worker did not stop")
						return
					case <-time.After(time.Millisecond):
					}
				}
			})
			control := NewStopControlUseCase(stops, regressionStopRepository{records}, nil, nil)
			control.SetRotationUseCase(rotation, ctx)
			control.SetResumeTransitionContext(func(context.Context, supervisor.ManagedSession) (supervisor.TransitionContext, error) {
				return supervisor.TransitionContext{ChildAlive: step != supervisor.RotateSendExit, HasRotation: true}, nil
			})
			if _, err := control.Stop(context.Background(), &id); err != nil {
				t.Fatal(err)
			}
			out, err := control.Resume(context.Background(), &id, true, false)
			if err != nil || !slices.Equal(out.Resumed, []supervisor.SessionID{id}) {
				t.Fatalf("resume=%+v err=%v", out, err)
			}
			current, err := records.Load(id)
			if step == supervisor.RotateSendExit {
				deadline := time.NewTimer(time.Second)
				defer deadline.Stop()
				for err == nil && current.Rotation != nil && current.Rotation.Step != supervisor.RotateRestart {
					select {
					case <-records.saved:
						current, err = records.Load(id)
					case <-deadline.C:
						t.Fatal("confirmed exit did not advance to restart")
					}
				}
			}
			if err != nil || current.State != supervisor.SessionRotating || current.Rotation == nil || current.RestartCount != 0 || sender.calls.Load() != 0 {
				t.Fatalf("handoff=%+v sends=%d err=%v", current, sender.calls.Load(), err)
			}
			if step == supervisor.RotateSendExit && current.Rotation.Step != supervisor.RotateRestart {
				t.Fatalf("confirmed exit did not advance to restart: %+v", current.Rotation)
			}
		})
	}
}

func TestStopControlIntegration_G42RecoveryResume(t *testing.T) {
	integrationHome(t)
	respawn, records, starter, id := respawnFixture(t)
	records.session.State = supervisor.SessionWorking
	stops := NewStopState(t.TempDir())
	control := NewStopControlUseCase(stops, regressionStopRepository{records}, nil, nil)
	control.SetResumeContinuations(nil, func(ctx context.Context, session supervisor.ManagedSession) error {
		return respawn.Execute(ctx, session.SessionID, RespawnResumeRecovery)
	})
	if _, err := control.Stop(context.Background(), &id); err != nil {
		t.Fatal(err)
	}
	out, err := control.Resume(context.Background(), &id, true, false)
	if err != nil || !slices.Equal(out.Resumed, []supervisor.SessionID{id}) || starter.calls != 1 || !slices.Contains(starter.argv, "--resume") || records.session.RestartCount != 0 || len(records.session.RestartTimes) != 0 {
		t.Fatalf("resume=%+v session=%+v argv=%v calls=%d err=%v", out, records.session, starter.argv, starter.calls, err)
	}
}

func TestStopControlIntegration_SCNSup0542OrphanResume(t *testing.T) {
	home := integrationHome(t)
	id := supervisor.SessionID("g42-orphan-1")
	now := time.Now().UTC()
	records := &regressionSessionStore{session: supervisor.ManagedSession{SessionID: id, CLI: supervisor.CLIClaude, State: supervisor.SessionWorking, ChildPID: 142, ChildStartedAt: &now, ClaudeSessionID: "claude-orphan"}}
	stops := NewStopState(home)
	control := NewStopControlUseCase(stops, regressionStopRepository{records}, nil, nil)
	if _, err := control.Stop(context.Background(), &id); err != nil {
		t.Fatal(err)
	}
	pty := &stopSessionPTY{pid: 142, done: make(chan struct{})}
	ptys := &PTYRegistry{}
	ptys.RegisterManaged(id, pty)
	childAlive := true
	stopSession := StopSessionUseCase{Sessions: records, PTYs: ptys, ConfirmTimeout: time.Millisecond, PollInterval: time.Millisecond, ChildAlive: func(int) bool { return childAlive }}
	result, err := stopSession.Execute(context.Background(), id)
	if err != nil || result.Reason != supervisor.StopOrphanChildAlive || !pty.closed {
		t.Fatalf("stop-session=%+v err=%v", result, err)
	}
	control.SetResumeTransitionContext(NewProcessResumeTransitionContext(func(int) bool { return childAlive }, func(int) (time.Time, bool) { return now, true }))
	if out, err := control.Resume(context.Background(), &id, true, false); err != nil || len(out.StillStopped) != 1 || out.StillStopped[0].Reason != "orphan_child_alive" {
		t.Fatalf("living orphan resume=%+v err=%v", out, err)
	}
	childAlive = false
	control.SetResumeContinuations(nil, func(context.Context, supervisor.ManagedSession) error { return nil })
	if out, err := control.Resume(context.Background(), &id, true, false); err != nil || !slices.Equal(out.Resumed, []supervisor.SessionID{id}) {
		t.Fatalf("dead orphan resume=%+v err=%v", out, err)
	}
}
