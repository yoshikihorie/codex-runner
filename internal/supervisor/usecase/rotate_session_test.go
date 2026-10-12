package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type rotationStore struct {
	session supervisor.ManagedSession
	others  map[supervisor.SessionID]supervisor.ManagedSession
	saves   int
	history []supervisor.ManagedSession
}

func (s *rotationStore) Load(id supervisor.SessionID) (supervisor.ManagedSession, error) {
	if id != s.session.SessionID {
		if other, ok := s.others[id]; ok {
			return other, nil
		}
		return supervisor.ManagedSession{}, errors.New("missing session")
	}
	return s.session, nil
}
func (s *rotationStore) Save(v supervisor.ManagedSession) error {
	if v.SessionID != s.session.SessionID {
		s.others[v.SessionID] = v
		return nil
	}
	s.session = v
	s.saves++
	s.history = append(s.history, v)
	return nil
}

type rotationSender struct {
	calls  []SendInput
	fail   supervisor.SupervisorSendKind
	onSend func(SendInput)
}

func (s *rotationSender) Execute(_ context.Context, in SendInput) (SendResult, error) {
	s.calls = append(s.calls, in)
	if s.onSend != nil {
		s.onSend(in)
	}
	if in.Kind == s.fail {
		return SendResult{}, errors.New("unconfirmed")
	}
	return SendResult{SendID: "test-s1", Confirmed: true}, nil
}

type rotationRespawn struct {
	store *rotationStore
	kinds []RespawnKind
	err   error
}

type persistedRotationRespawn struct{ sessions *store.SessionStore }

func (r persistedRotationRespawn) Execute(_ context.Context, id supervisor.SessionID, _ RespawnKind) error {
	return r.sessions.UpdateSession(id, func(s *supervisor.ManagedSession) error {
		s.ChildPID = 20
		s.Generation++
		return nil
	})
}

func (r *rotationRespawn) Execute(_ context.Context, _ supervisor.SessionID, kind RespawnKind) error {
	r.kinds = append(r.kinds, kind)
	if r.err != nil {
		return r.err
	}
	r.store.session.ChildPID = 20
	r.store.session.Generation++
	return nil
}

func rotationFixture() (*rotationStore, *RotateSessionUseCase, *time.Time, *[]store.Event) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	id := supervisor.SessionID("rotation-one")
	s := &rotationStore{session: supervisor.ManagedSession{SessionID: id, ProjectID: "project", CLI: supervisor.CLIClaude, State: supervisor.SessionIdle, Generation: 1, ChildPID: 10}}
	events := &[]store.Event{}
	u := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }, Sessions: s, Send: &rotationSender{}, Now: func() time.Time { return now }, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle }, ChildAlive: func(supervisor.SessionID) bool { return true }, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return supervisor.RotationCheckpointSnapshot{}, nil
	}, RecordStructured: func(e store.Event) error { *events = append(*events, e); return nil }}
	u.ResumeReady = func(supervisor.SessionID, time.Time) bool { return true }
	u.IsStopped = func(supervisor.SessionID) bool { return false }
	return s, u, &now, events
}

func TestRotateSession_ValidateCoreRequiresInjectedWorkerDependencies(t *testing.T) {
	_, u, _, _ := rotationFixture()
	u.Wake = nil
	u.PendingRemoteControl = nil
	u.ResumeReady = nil
	if err := u.validateCore(); err == nil {
		t.Fatal("missing wake and pending-control dependencies accepted")
	}
}

func TestRotateSession_EndSessionClearsWorkerState(t *testing.T) {
	_, u, now, _ := rotationFixture()
	id := supervisor.SessionID("finished-rotation-1")
	u.running = map[supervisor.SessionID]bool{id: true}
	u.stepLocks = map[supervisor.SessionID]*sync.Mutex{id: {}}
	u.resumeSent = map[supervisor.SessionID]time.Time{id: *now}
	u.EndSession(id)
	if u.running[id] || u.stepLocks[id] != nil || !u.resumeSent[id].IsZero() {
		t.Fatal("transient worker state retained after session end")
	}
}

func TestRotateSession_CompletionWaitsForDeterminateObservation(t *testing.T) {
	for _, target := range []supervisor.SupervisorSessionState{supervisor.SessionIdle, supervisor.SessionWorking, supervisor.SessionAwaitingPermission, supervisor.SessionAwaitingRecommendation} {
		t.Run(string(target), func(t *testing.T) {
			s, u, now, events := rotationFixture()
			s.session.State = supervisor.SessionRotating
			s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendParallelInstruction, StepStartedAt: now}
			s.session.LastSend = &supervisor.LastSendRecord{Kind: supervisor.SendKindParallelInstruction, Confirmed: true}
			observed := supervisor.SupervisorSessionState("")
			u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return observed }
			if err := u.Step(context.Background(), s.session.SessionID); err != nil {
				t.Fatal(err)
			}
			if s.session.State != supervisor.SessionRotating || s.session.Rotation == nil || len(*events) != 0 || len(u.Send.(*rotationSender).calls) != 0 {
				t.Fatalf("completion was not held: session=%+v events=%+v", s.session, *events)
			}
			observed = target
			if err := u.Step(context.Background(), s.session.SessionID); err != nil {
				t.Fatal(err)
			}
			if s.session.State != target || s.session.Rotation != nil || len(u.Send.(*rotationSender).calls) != 0 {
				t.Fatalf("completion state=%+v", s.session)
			}
			completed := 0
			for _, event := range *events {
				if event.Kind == supervisor.EventKindRotateCompleted {
					completed++
				}
			}
			if completed != 1 {
				t.Fatalf("completion events=%+v", *events)
			}
		})
	}
}

func TestRotateSession_Stage7IdleWaitTimesOutBeforeSend(t *testing.T) {
	s, u, now, events := rotationFixture()
	started := *now
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendParallelInstruction, StepStartedAt: &started}
	u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState {
		return supervisor.SessionAwaitingRecommendation
	}
	*now = now.Add(rotationStepTimeout)
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionStopped || s.session.Rotation == nil || s.session.Rotation.FailReason != supervisor.RotateFailSendNotConfirmed || len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatalf("session=%+v sends=%+v", s.session, u.Send.(*rotationSender).calls)
	}
	if len(*events) == 0 || (*events)[len(*events)-1].Kind != supervisor.EventKindRotateFailed {
		t.Fatalf("events=%+v", *events)
	}
}

func TestRotateSession_ResumeUnsentEndKeepsCheckpointSnapshot(t *testing.T) {
	s, u, now, _ := rotationFixture()
	s.session.State = supervisor.SessionRotating
	before := supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "original"}
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendEnd, StartedAt: *now, StepStartedAt: now, CheckpointSnapshot: &before}
	inspections := 0
	u.Inspect = func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		inspections++
		return supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "changed"}, nil
	}
	if err := u.Resume(context.Background(), s.session); err != nil {
		t.Fatal(err)
	}
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.Rotation.CheckpointSnapshot.HeadHash != before.HeadHash || s.session.Rotation.EndIssuedAt == nil || inspections != 1 {
		t.Fatalf("snapshot=%+v issued=%v inspections=%d", s.session.Rotation.CheckpointSnapshot, s.session.Rotation.EndIssuedAt, inspections)
	}
}

func TestRotateSession_SCN06_SendEndFailureStops(t *testing.T) {
	s, u, _, events := rotationFixture()
	u.Send.(*rotationSender).fail = supervisor.SendKindHandoffEnd
	if err := u.Start(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionStopped || s.session.Rotation == nil || s.session.Rotation.FailReason != supervisor.RotateFailSendNotConfirmed {
		t.Fatalf("session = %+v", s.session)
	}
	if len(*events) == 0 || (*events)[len(*events)-1].Kind != supervisor.EventKindRotateFailed {
		t.Fatalf("events = %+v", *events)
	}
}

func TestRotateSession_SCN41_UndeterminableSnapshotNeverExits(t *testing.T) {
	s, u, now, _ := rotationFixture()
	u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking }
	u.Inspect = func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return supervisor.RotationCheckpointSnapshot{Undeterminable: true}, errors.New("unavailable")
	}
	if err := u.Start(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if !s.session.Rotation.CheckpointSnapshot.Undeterminable {
		t.Fatal("snapshot lost")
	}
	*now = now.Add(rotationStepTimeout)
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.Rotation.FailReason != supervisor.RotateFailMtimeNotUpdated || s.session.State != supervisor.SessionStopped {
		t.Fatalf("session = %+v", s.session)
	}
	if len(u.Send.(*rotationSender).calls) != 1 {
		t.Fatalf("sent = %+v", u.Send.(*rotationSender).calls)
	}
}

func TestRotateSession_SCN01_CompletesAndRegistersPendingInOneWrite(t *testing.T) {
	s, u, now, events := rotationFixture()
	pendingUC := &PendingRemoteControlUseCase{Sessions: s}
	u.PendingRemoteControl = pendingUC
	state := supervisor.SessionIdle
	updated := false
	u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return state }
	u.Send.(*rotationSender).onSend = func(in SendInput) {
		if in.Kind == supervisor.SendKindHandoffEnd {
			state = supervisor.SessionWorking
		}
	}
	u.Inspect = func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		if !updated {
			return supervisor.RotationCheckpointSnapshot{}, nil
		}
		mtime := now.Add(time.Second)
		return supervisor.RotationCheckpointSnapshot{Exists: true, Mtime: &mtime, HeadHash: "new"}, nil
	}
	r := &rotationRespawn{store: s}
	u.Respawn = r
	if err := u.Start(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.Rotation.Step != supervisor.RotateConfirmHandoff {
		t.Fatalf("step = %s", s.session.Rotation.Step)
	}
	state = supervisor.SessionIdle
	updated = true
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.Rotation.Step != supervisor.RotateRestart {
		t.Fatalf("step = %s", s.session.Rotation.Step)
	}
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionIdle || s.session.Rotation != nil || !s.session.PendingRemoteControl {
		t.Fatalf("session = %+v", s.session)
	}
	if pendingUC.registered[s.session.SessionID] != s.session.Generation {
		t.Fatalf("pending generation not registered: %+v", pendingUC.registered)
	}
	if len(r.kinds) != 1 || r.kinds[0] != RespawnHandoffStage5 {
		t.Fatalf("respawn = %+v", r.kinds)
	}
	want := []supervisor.SupervisorSendKind{supervisor.SendKindHandoffEnd, supervisor.SendKindHandoffExit, supervisor.SendKindSessionStart, supervisor.SendKindParallelInstruction}
	if len(u.Send.(*rotationSender).calls) != len(want) {
		t.Fatalf("calls = %+v", u.Send.(*rotationSender).calls)
	}
	for i, call := range u.Send.(*rotationSender).calls {
		if call.Kind != want[i] {
			t.Fatalf("calls = %+v", u.Send.(*rotationSender).calls)
		}
	}
	if u.Send.(*rotationSender).calls[1].ConfirmTimeout != supervisor.SupervisorExitGrace {
		t.Fatalf("exit timeout = %v", u.Send.(*rotationSender).calls[1].ConfirmTimeout)
	}
	last := s.history[len(s.history)-1]
	if last.Rotation != nil || !last.PendingRemoteControl {
		t.Fatalf("last write = %+v", last)
	}
	completed, pending := 0, 0
	for _, e := range *events {
		if e.Kind == supervisor.EventKindRotateCompleted {
			completed++
		}
		if e.Kind == supervisor.EventKindRemoteControlPending {
			pending++
		}
	}
	if completed != 1 || pending != 1 {
		t.Fatalf("events = %+v", *events)
	}
}

func TestRotateSession_TwoHandoffsInOneSession(t *testing.T) {
	s, u, now, _ := rotationFixture()
	handoffs := 0
	u.Send.(*rotationSender).onSend = func(in SendInput) {
		if in.Kind == supervisor.SendKindHandoffEnd {
			handoffs++
		}
	}
	u.Inspect = func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		mtime := now.Add(time.Second)
		return supervisor.RotationCheckpointSnapshot{Exists: true, Mtime: &mtime, HeadHash: fmt.Sprintf("head-%d", handoffs)}, nil
	}
	u.Respawn = &rotationRespawn{store: s}
	for round := 1; round <= 2; round++ {
		if err := u.Start(context.Background(), s.session.SessionID); err != nil {
			t.Fatal(err)
		}
		if err := u.Step(context.Background(), s.session.SessionID); err != nil {
			t.Fatal(err)
		}
		if s.session.Rotation != nil || s.session.State != supervisor.SessionIdle || handoffs != round {
			t.Fatalf("round %d: session=%+v handoffs=%d", round, s.session, handoffs)
		}
		*now = now.Add(time.Second)
	}
}

func TestRotateSession_TwoHandoffsInOneSessionWithSessionStore(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	id := supervisor.SessionID("rotation-persisted-1")
	root := t.TempDir()
	sessions := store.NewSessionStore(root)
	initial := supervisor.ManagedSession{SessionID: id, ProjectID: "project", CLI: supervisor.CLIClaude, State: supervisor.SessionIdle, StateSince: now, Generation: 1, ChildPID: 10, StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	if err := sessions.Save(initial); err != nil {
		t.Fatal(err)
	}
	handoffs := 0
	sender := &rotationSender{onSend: func(in SendInput) {
		if in.Kind == supervisor.SendKindHandoffEnd {
			handoffs++
		}
	}}
	u := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, Sessions: sessions, Send: sender, Respawn: persistedRotationRespawn{sessions}, Now: func() time.Time { return now }, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle }, ChildAlive: func(supervisor.SessionID) bool { return true }, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }, IsStopped: func(supervisor.SessionID) bool { return false }, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		mtime := now.Add(time.Second)
		return supervisor.RotationCheckpointSnapshot{Exists: true, Mtime: &mtime, HeadHash: strings.Repeat(string(rune('a'+handoffs)), 64)}, nil
	}}
	for round := 1; round <= 2; round++ {
		if err := u.Start(context.Background(), id); err != nil {
			t.Fatalf("round %d Start: %v", round, err)
		}
		if err := u.Step(context.Background(), id); err != nil {
			t.Fatalf("round %d Step: %v", round, err)
		}
		got, err := store.NewSessionStore(root).Load(id)
		if err != nil || got.Rotation != nil || got.State != supervisor.SessionIdle || handoffs != round {
			t.Fatalf("round %d: session=%+v handoffs=%d error=%v", round, got, handoffs, err)
		}
		now = now.Add(time.Second)
	}
}

func TestRotateSession_SCN29_RecordsContainNoCheckpointOrSentText(t *testing.T) {
	s, u, _, events := rotationFixture()
	cwd := t.TempDir()
	oldBody := "private-checkpoint-before-rotation"
	newBody := "private-checkpoint-after-rotation"
	writeCheckpoint(t, cwd, oldBody)
	u.Inspect = nil
	u.CheckpointCWD = func(supervisor.ManagedSession) string { return cwd }
	u.Send.(*rotationSender).onSend = func(in SendInput) {
		if in.Kind == supervisor.SendKindHandoffEnd {
			writeCheckpoint(t, cwd, newBody)
		}
	}
	u.Respawn = &rotationRespawn{store: s}
	if err := u.Start(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.Rotation != nil {
		t.Fatalf("handoff did not finish: %+v", s.session.Rotation)
	}
	for _, record := range s.history {
		data, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{oldBody, newBody, "#end", "/exit", defaultParallelInstruction} {
			if strings.Contains(string(data), forbidden) {
				t.Fatalf("session JSON contains private text %q", forbidden)
			}
		}
	}
	data, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{oldBody, newBody, "#end", "/exit", defaultParallelInstruction} {
		if strings.Contains(string(data), forbidden) {
			t.Fatalf("event JSON contains private text %q", forbidden)
		}
	}
}

func TestRotateSession_SCN07_08_09_Stage3FailurePrecedence(t *testing.T) {
	for _, tc := range []struct {
		name     string
		after    supervisor.RotationCheckpointSnapshot
		observed supervisor.SupervisorSessionState
		want     supervisor.SupervisorRotateFailReason
	}{
		{"mtime", supervisor.RotationCheckpointSnapshot{}, supervisor.SessionIdle, supervisor.RotateFailMtimeNotUpdated},
		{"head", supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "old"}, supervisor.SessionIdle, supervisor.RotateFailHeadGenerationUnchanged},
		{"idle", supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "new"}, supervisor.SessionWorking, supervisor.RotateFailNotIdleAfterHandoff},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, u, now, _ := rotationFixture()
			started := *now
			issued := *now
			before := supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "old"}
			s.session.State = supervisor.SessionRotating
			s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateConfirmHandoff, StepStartedAt: &started, EndIssuedAt: &issued, CheckpointSnapshot: &before}
			mtime := now.Add(time.Second)
			after := tc.after
			if after.Exists {
				after.Mtime = &mtime
			}
			u.Inspect = func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) { return after, nil }
			u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return tc.observed }
			*now = now.Add(rotationStepTimeout)
			if err := u.Step(context.Background(), s.session.SessionID); err != nil {
				t.Fatal(err)
			}
			if s.session.Rotation.FailReason != tc.want || s.session.State != supervisor.SessionStopped || len(u.Send.(*rotationSender).calls) != 0 {
				t.Fatalf("session=%+v calls=%+v", s.session, u.Send.(*rotationSender).calls)
			}
		})
	}
}

func TestRotateSession_SCN11_12_13_14_16_StepFailures(t *testing.T) {
	for _, tc := range []struct {
		name       string
		step       supervisor.SupervisorRotateStep
		failSend   supervisor.SupervisorSendKind
		observed   supervisor.SupervisorSessionState
		respawnErr error
		want       supervisor.SupervisorRotateFailReason
	}{
		{"exit_timeout", supervisor.RotateSendExit, supervisor.SendKindHandoffExit, supervisor.SessionIdle, nil, supervisor.RotateFailExitTimeout},
		{"spawn_failed", supervisor.RotateRestart, "", supervisor.SessionIdle, errors.New("spawn"), supervisor.RotateFailSpawnFailed},
		{"start_unconfirmed", supervisor.RotateSendStart, supervisor.SendKindSessionStart, supervisor.SessionIdle, nil, supervisor.RotateFailSendNotConfirmed},
		{"parallel_unconfirmed", supervisor.RotateSendParallelInstruction, supervisor.SendKindParallelInstruction, supervisor.SessionIdle, nil, supervisor.RotateFailSendNotConfirmed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, u, now, _ := rotationFixture()
			s.session.State = supervisor.SessionRotating
			s.session.Rotation = &supervisor.RotationRecord{Step: tc.step, StepStartedAt: now}
			if tc.step == supervisor.RotateRestart {
				s.session.ChildPID = 0
			}
			u.Send.(*rotationSender).fail = tc.failSend
			u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return tc.observed }
			u.Respawn = &rotationRespawn{store: s, err: tc.respawnErr}
			if err := u.Step(context.Background(), s.session.SessionID); err != nil {
				t.Fatal(err)
			}
			if s.session.State != supervisor.SessionStopped || s.session.Rotation.FailReason != tc.want {
				t.Fatalf("session = %+v", s.session)
			}
			if s.session.PendingRemoteControl {
				t.Fatal("failed step registered /rc")
			}
			for _, call := range u.Send.(*rotationSender).calls {
				if call.Kind == supervisor.SendKindSessionStart && tc.step != supervisor.RotateSendStart ||
					call.Kind == supervisor.SendKindParallelInstruction && tc.step != supervisor.RotateSendParallelInstruction ||
					call.Kind == supervisor.SendKindRemoteControl {
					t.Fatalf("sent next step after %s failure: %+v", tc.step, u.Send.(*rotationSender).calls)
				}
			}
		})
	}
}

func TestRotateSession_SCN13_StartupTimeout(t *testing.T) {
	s, u, now, _ := rotationFixture()
	started := *now
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateRestart, StepStartedAt: &started}
	u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking }
	*now = now.Add(rotationStartupTimeout)
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionStopped || s.session.Rotation.FailReason != supervisor.RotateFailStartupTimeout || len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatalf("session = %+v", s.session)
	}
}

func TestRotateSession_SCN15_StartNotSettled(t *testing.T) {
	s, u, now, _ := rotationFixture()
	started := *now
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendStart, StepStartedAt: &started}
	s.session.LastSend = &supervisor.LastSendRecord{Kind: supervisor.SendKindSessionStart, Confirmed: true}
	u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking }
	*now = now.Add(rotationStepTimeout)
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.Rotation.FailReason != supervisor.RotateFailStartNotSettled || len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatalf("session = %+v", s.session)
	}
}

func TestRotateSession_SCN18_StopBeforeEachAction(t *testing.T) {
	for _, tc := range []struct {
		name string
		step supervisor.SupervisorRotateStep
	}{
		{"send_end", supervisor.RotateSendEnd}, {"send_exit", supervisor.RotateSendExit}, {"restart", supervisor.RotateRestart}, {"send_start", supervisor.RotateSendStart}, {"parallel", supervisor.RotateSendParallelInstruction},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, u, now, _ := rotationFixture()
			s.session.State = supervisor.SessionRotating
			s.session.Rotation = &supervisor.RotationRecord{Step: tc.step, StepStartedAt: now}
			stopped := true
			u.IsStopped = func(supervisor.SessionID) bool { return stopped }
			u.Respawn = &rotationRespawn{store: s}
			if err := u.Step(context.Background(), s.session.SessionID); err != nil {
				t.Fatal(err)
			}
			if s.session.State != supervisor.SessionStopped || s.session.Rotation.Step != tc.step || len(u.Send.(*rotationSender).calls) != 0 || len(u.Respawn.(*rotationRespawn).kinds) != 0 {
				t.Fatalf("session=%+v sends=%+v", s.session, u.Send.(*rotationSender).calls)
			}
		})
	}
}

func TestRotateSession_SCN36_StopDuringConfirmedStage3AdvancesOnly(t *testing.T) {
	s, u, now, events := rotationFixture()
	issued := *now
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateConfirmHandoff, StepStartedAt: now, EndIssuedAt: &issued, CheckpointSnapshot: &supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "old"}}
	mtime := now.Add(time.Second)
	u.Inspect = func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return supervisor.RotationCheckpointSnapshot{Exists: true, Mtime: &mtime, HeadHash: "new"}, nil
	}
	u.IsStopped = func(supervisor.SessionID) bool { return true }
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionStopped || s.session.Rotation.Step != supervisor.RotateSendExit || len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatalf("session = %+v", s.session)
	}
	if len(*events) != 1 || (*events)[0].Kind != supervisor.EventKindRotateStepDone {
		t.Fatalf("events = %+v", *events)
	}
}

func TestRotateSession_SCN30_32_ChildAbsentByStage(t *testing.T) {
	for _, tc := range []struct {
		name    string
		step    supervisor.SupervisorRotateStep
		stopped bool
	}{
		{"send_end", supervisor.RotateSendEnd, true}, {"confirm_handoff", supervisor.RotateConfirmHandoff, true}, {"send_start", supervisor.RotateSendStart, false}, {"parallel", supervisor.RotateSendParallelInstruction, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, u, now, _ := rotationFixture()
			s.session.State = supervisor.SessionRotating
			s.session.Rotation = &supervisor.RotationRecord{Step: tc.step, StepStartedAt: now}
			u.ChildAlive = func(supervisor.SessionID) bool { return false }
			if err := u.Step(context.Background(), s.session.SessionID); err != nil {
				t.Fatal(err)
			}
			if tc.stopped {
				if s.session.State != supervisor.SessionStopped || s.session.Rotation.FailReason != supervisor.RotateFailChildAbsent {
					t.Fatalf("session = %+v", s.session)
				}
			} else if s.session.State != supervisor.SessionRotating || s.session.Rotation.Step != supervisor.RotateRestart || s.session.Rotation.RespawnWaitUntil == nil {
				t.Fatalf("session = %+v", s.session)
			}
		})
	}
}

func TestRotateSession_SCN33_34_39_ResumeRecordedStage(t *testing.T) {
	s, u, now, events := rotationFixture()
	issued := *now
	mtime := now.Add(time.Second)
	s.session.State = supervisor.SessionRotating
	s.session.ClaudeSessionID = "claude-id"
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendEnd, EndIssuedAt: &issued, CheckpointSnapshot: &supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "old"}}
	u.ChildAlive = func(supervisor.SessionID) bool { return s.session.ChildPID == 20 }
	u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking }
	u.Respawn = &rotationRespawn{store: s}
	u.Inspect = func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return supervisor.RotationCheckpointSnapshot{Exists: true, Mtime: &mtime, HeadHash: "new"}, nil
	}
	if err := u.Resume(context.Background(), s.session); err != nil {
		t.Fatal(err)
	}
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionRotating || s.session.Rotation.Step != supervisor.RotateConfirmHandoff || len(u.Send.(*rotationSender).calls) != 0 || u.Respawn.(*rotationRespawn).kinds[0] != RespawnHandoffResume {
		t.Fatalf("session=%+v sends=%+v", s.session, u.Send.(*rotationSender).calls)
	}
	if s.session.Rotation.StepStartedAt == nil || !s.session.Rotation.StepStartedAt.Equal(*now) {
		t.Fatalf("restart time = %+v", s.session.Rotation.StepStartedAt)
	}
	if len(*events) < 2 || (*events)[1].Kind != supervisor.EventKindRotateResumed {
		t.Fatalf("events = %+v", *events)
	}
}

func TestRotateSession_SCN38_ExitTimeoutResumeRejected(t *testing.T) {
	s, u, now, _ := rotationFixture()
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendExit, StepStartedAt: now, FailReason: supervisor.RotateFailExitTimeout}
	if err := u.Resume(context.Background(), s.session); err == nil || err.Error() != "exit_process_alive" {
		t.Fatalf("resume = %v", err)
	}
	if len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatal("/exit resent")
	}
}

func TestRotateSession_SCN21_ReconcileDoesNotAutoResume(t *testing.T) {
	// The existing T4-10 reconcile path stops active sessions. This test pins the rotating case.
	now := time.Now().UTC()
	s := &recoveryReconcileStore{regressionSessionStore{session: supervisor.ManagedSession{SessionID: "rotation-reconcile", State: supervisor.SessionRotating, Generation: 2, ChildPID: 123, ChildStartedAt: &now, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateConfirmHandoff}}}}
	rows, err := (ReconcileOnStartupUseCase{Store: s, IsAlive: func(int) bool { return false }, Now: func() time.Time { return now }}).Execute()
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].State != supervisor.SessionStopped || rows[0].StopReason != supervisor.StopSupervisorRestarted || rows[0].PreviousState != supervisor.SessionRotating || rows[0].Rotation == nil || rows[0].Rotation.Step != supervisor.RotateConfirmHandoff {
		t.Fatalf("reconciled = %+v", rows[0])
	}
}

func TestRotateSession_SCN34_36_StopPausesStage3Clock(t *testing.T) {
	s, u, now, _ := rotationFixture()
	started := *now
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateConfirmHandoff, StepStartedAt: &started}
	stopped := true
	u.IsStopped = func(supervisor.SessionID) bool { return stopped }
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(rotationStepTimeout * 2)
	if s.session.Rotation.FailReason != "" || s.session.State != supervisor.SessionStopped {
		t.Fatalf("during STOP = %+v", s.session)
	}
	stopped = false
	s.session.State = supervisor.SessionRotating // StopControl clears STOP before invoking Resume.
	if err := u.Resume(context.Background(), s.session); err != nil {
		t.Fatal(err)
	}
	if s.session.Rotation.StepStartedAt == nil || !s.session.Rotation.StepStartedAt.Equal(*now) || s.session.Rotation.FailReason != "" {
		t.Fatalf("after resume = %+v", s.session.Rotation)
	}
}

func TestRotateSession_SCN32_BackoffAndLimit(t *testing.T) {
	s, u, now, _ := rotationFixture()
	s.session.State = supervisor.SessionRotating
	s.session.RestartCount = 1
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendStart, StepStartedAt: now}
	u.ChildAlive = func(supervisor.SessionID) bool { return false }
	r := &rotationRespawn{store: s}
	u.Respawn = r
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	wait := *s.session.Rotation.RespawnWaitUntil
	if !wait.Equal(now.Add(supervisor.RestartBackoff(1))) {
		t.Fatalf("wait = %v", wait)
	}
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if len(r.kinds) != 0 {
		t.Fatal("spawned before backoff")
	}
	*now = wait
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if len(r.kinds) != 1 || r.kinds[0] != RespawnHandoffStage5 {
		t.Fatalf("respawn = %+v", r.kinds)
	}
	s.session.ChildPID = 20
	s.session.RestartCount = supervisor.RestartMaxCount
	s.session.Rotation.Advance(supervisor.RotateSendStart, *now)
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionStopped || s.session.Rotation.FailReason != supervisor.RotateFailRestartLimit {
		t.Fatalf("limit = %+v", s.session)
	}
}

func TestRotateSession_SCN37_ResumeWaitsForFreshScreenBeforeResend(t *testing.T) {
	s, u, now, _ := rotationFixture()
	issued := *now
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendEnd, EndIssuedAt: &issued, CheckpointSnapshot: &supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "old"}}
	u.ResumeReady = func(supervisor.SessionID, time.Time) bool { return false }
	if err := u.Resume(context.Background(), s.session); err != nil {
		t.Fatal(err)
	}
	if len(u.Send.(*rotationSender).calls) != 0 || s.session.Rotation.EndIssuedAt == nil || !s.session.Rotation.EndIssuedAt.Equal(issued) {
		t.Fatalf("premature resend = %+v", s.session)
	}
	u.ResumeReady = func(supervisor.SessionID, time.Time) bool { return true }
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if len(u.Send.(*rotationSender).calls) != 1 {
		t.Fatalf("resends = %+v", u.Send.(*rotationSender).calls)
	}
}

func TestRotateSession_SCN28_OnlyTargetSessionChanges(t *testing.T) {
	s, u, _, _ := rotationFixture()
	other := supervisor.ManagedSession{SessionID: "other-session", State: supervisor.SessionIdle}
	s.others = map[supervisor.SessionID]supervisor.ManagedSession{other.SessionID: other}
	if err := u.Start(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	stored, err := s.Load(other.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(stored, other) || s.saves == 0 {
		t.Fatalf("other session = %+v", stored)
	}
}

func TestRotateSession_SCN36_StopAfterParallelConfirmationDoesNotResend(t *testing.T) {
	s, u, now, events := rotationFixture()
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendParallelInstruction, StepStartedAt: now}
	stopped := false
	u.IsStopped = func(supervisor.SessionID) bool { return stopped }
	u.Send.(*rotationSender).onSend = func(SendInput) {
		s.session.LastSend = &supervisor.LastSendRecord{Kind: supervisor.SendKindParallelInstruction, Confirmed: true}
		stopped = true
	}
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionStopped || s.session.Rotation == nil || len(u.Send.(*rotationSender).calls) != 1 {
		t.Fatalf("stopped = %+v", s.session)
	}
	stopped = false
	s.session.State = supervisor.SessionRotating
	if err := u.Resume(context.Background(), s.session); err != nil {
		t.Fatal(err)
	}
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.Rotation != nil || !s.session.PendingRemoteControl || len(u.Send.(*rotationSender).calls) != 1 {
		t.Fatalf("resumed = %+v", s.session)
	}
	stepDone := 0
	for _, e := range *events {
		if e.Kind == supervisor.EventKindRotateStepDone {
			stepDone++
		}
	}
	if stepDone != 1 {
		t.Fatalf("step_done count = %d", stepDone)
	}
}

func TestRotateSession_SCN20_31_ResumeAfterExitSkipsSecondExit(t *testing.T) {
	s, u, now, _ := rotationFixture()
	s.session.State = supervisor.SessionRotating
	s.session.ChildPID = 0
	s.session.LastSend = &supervisor.LastSendRecord{Kind: supervisor.SendKindHandoffExit, Confirmed: true}
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendExit, StepStartedAt: now}
	u.ChildAlive = func(supervisor.SessionID) bool { return false }
	u.Respawn = &rotationRespawn{store: s}
	if err := u.Resume(context.Background(), s.session); err != nil {
		t.Fatal(err)
	}
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if len(u.Send.(*rotationSender).calls) != 0 || len(u.Respawn.(*rotationRespawn).kinds) != 1 || s.session.Rotation.Step != supervisor.RotateRestart {
		t.Fatalf("resume = %+v", s.session)
	}
}

func TestRotateSession_SCN22_ExitBeforeCRStopsAsChildAbsent(t *testing.T) {
	s, u, now, _ := rotationFixture()
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendExit, StepStartedAt: now}
	u.ChildAlive = func(supervisor.SessionID) bool { return false }
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionStopped || s.session.Rotation.FailReason != supervisor.RotateFailChildAbsent || len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatalf("session = %+v", s.session)
	}
}

func TestRotateSession_SCN35_ObservedWorkingDoesNotChangeRotatingState(t *testing.T) {
	s, u, now, _ := rotationFixture()
	issued := *now
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateConfirmHandoff, StepStartedAt: now, EndIssuedAt: &issued}
	u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking }
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionRotating || s.session.Rotation.Step != supervisor.RotateConfirmHandoff {
		t.Fatalf("state = %+v", s.session)
	}
}

func TestRotateSession_SCN40_ResumeBeforeStopClearedHasNoSideEffects(t *testing.T) {
	s, u, now, _ := rotationFixture()
	s.session.State = supervisor.SessionStopped
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendEnd, StepStartedAt: now}
	u.IsStopped = func(supervisor.SessionID) bool { return true }
	if err := u.Resume(context.Background(), s.session); !errors.Is(err, ErrStopActive) {
		t.Fatalf("resume = %v", err)
	}
	if s.saves != 0 || len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatalf("side effects = %d %+v", s.saves, u.Send.(*rotationSender).calls)
	}
}

func TestRotateSession_SCN01_Stage2AcceptsMtimeWithoutWorking(t *testing.T) {
	s, u, now, _ := rotationFixture()
	updated := false
	u.Inspect = func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		if !updated {
			return supervisor.RotationCheckpointSnapshot{}, nil
		}
		mtime := now.Add(time.Second)
		return supervisor.RotationCheckpointSnapshot{Exists: true, Mtime: &mtime, HeadHash: "new"}, nil
	}
	if err := u.Start(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.Rotation.Step != supervisor.RotateSendEnd {
		t.Fatalf("step = %s", s.session.Rotation.Step)
	}
	updated = true
	u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState {
		return supervisor.SessionAwaitingPermission
	}
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.Rotation.Step != supervisor.RotateConfirmHandoff || len(u.Send.(*rotationSender).calls) != 1 {
		t.Fatalf("step = %s", s.session.Rotation.Step)
	}
}

func TestRotateSession_SCN19_ExecuteResumeRejectsStaleStep(t *testing.T) {
	s, u, now, _ := rotationFixture()
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateConfirmHandoff, StepStartedAt: now}
	stale := supervisor.RotateSendEnd
	if err := u.Execute(context.Background(), s.session.SessionID, &stale); err == nil {
		t.Fatal("stale resume accepted")
	}
	if s.saves != 0 || len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatal("stale resume changed session")
	}
}

func TestRotateSession_Stage2DeadlineStartsAtIssuedTime(t *testing.T) {
	s, u, now, _ := rotationFixture()
	started := now.Add(-supervisor.SupervisorSendConfirmTimeout - time.Second)
	issued := *now
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendEnd, StepStartedAt: &started, EndIssuedAt: &issued}
	u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle }
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionRotating {
		t.Fatalf("stage 2 expired before issue timeout: %+v", s.session)
	}
}

func TestRotateSession_Stage5DeadAfterSpawnUsesRestartLimit(t *testing.T) {
	s, u, now, _ := rotationFixture()
	s.session.State = supervisor.SessionRotating
	s.session.ChildPID = 0
	s.session.RestartCount = supervisor.RestartMaxCount
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateRestart, StepStartedAt: now, Stage5Started: true}
	u.ChildAlive = func(supervisor.SessionID) bool { return false }
	u.Respawn = &rotationRespawn{store: s}
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionStopped || s.session.Rotation.FailReason != supervisor.RotateFailRestartLimit {
		t.Fatalf("restart limit ignored: %+v", s.session)
	}
	if len(u.Respawn.(*rotationRespawn).kinds) != 0 {
		t.Fatal("respawned after limit")
	}
}

func TestRotateSession_SCN22_DeadChildDuringEndSendFailsChildAbsent(t *testing.T) {
	s, u, _, _ := rotationFixture()
	alive := true
	u.ChildAlive = func(supervisor.SessionID) bool { return alive }
	u.Send.(*rotationSender).fail = supervisor.SendKindHandoffEnd
	u.Send.(*rotationSender).onSend = func(SendInput) {
		alive = false
		s.session.SendGate.Pending = &supervisor.PendingSend{SendID: "pending", Kind: supervisor.SendKindHandoffEnd}
	}
	if err := u.Start(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionStopped || s.session.Rotation.FailReason != supervisor.RotateFailChildAbsent || s.session.SendGate.Pending != nil {
		t.Fatalf("dead child treated as send failure: %+v", s.session)
	}
}

func TestRotateSession_WorkerRunningCoversStartAndClearsAfterFailure(t *testing.T) {
	s, u, _, _ := rotationFixture()
	held := &heldRotationSender{started: make(chan struct{}, 1), release: make(chan struct{})}
	u.Send = held
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { done <- u.Execute(ctx, s.session.SessionID, nil) }()
	select {
	case <-held.started:
	case <-time.After(time.Second):
		t.Fatal("start did not reach sender")
	}
	if !u.IsRunning(s.session.SessionID) {
		t.Fatal("start not protected by running registration")
	}
	cancel()
	close(held.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not return")
	}
	if u.IsRunning(s.session.SessionID) {
		t.Fatal("running registration leaked")
	}
	other := supervisor.SessionID("no-session")
	if err := u.Execute(context.Background(), other, nil); err == nil || u.IsRunning(other) {
		t.Fatal("failed start retained running registration")
	}
}

func TestRotateSession_SCN36_RealStopKeepsEvaluatingConfirmedStage3(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	id := supervisor.SessionID("real-stop-36")
	issued := now
	mtime := now.Add(time.Second)
	records := &regressionSessionStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionRotating, ChildPID: 10, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateConfirmHandoff, StepStartedAt: &now, EndIssuedAt: &issued, CheckpointSnapshot: &supervisor.RotationCheckpointSnapshot{Exists: true, HeadHash: "old"}}}}
	stops := NewStopState(t.TempDir())
	control := NewStopControlUseCase(stops, regressionStopRepository{records}, nil, nil)
	if _, err := control.Stop(context.Background(), &id); err != nil {
		t.Fatal(err)
	}
	events := []store.Event{}
	u := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }, Sessions: records, Stop: stops, Send: &rotationSender{}, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle }, ChildAlive: func(supervisor.SessionID) bool { return true }, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return supervisor.RotationCheckpointSnapshot{Exists: true, Mtime: &mtime, HeadHash: "new"}, nil
	}, Now: func() time.Time { return now }, RecordStructured: func(e store.Event) error { events = append(events, e); return nil }}
	if err := u.Step(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	current, _ := records.Load(id)
	if current.State != supervisor.SessionStopped || current.PreviousState != supervisor.SessionRotating || current.Rotation.Step != supervisor.RotateSendExit || len(events) != 1 || events[0].Kind != supervisor.EventKindRotateStepDone {
		t.Fatalf("session=%+v events=%+v", current, events)
	}
	if len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatal("STOP allowed a new send")
	}
}

func TestRotateSession_SCN36_RealStopRecordsConfirmedStage7Once(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	id := supervisor.SessionID("real-stop-37")
	records := &regressionSessionStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionRotating, ChildPID: 10, LastSend: &supervisor.LastSendRecord{Kind: supervisor.SendKindParallelInstruction, Confirmed: true}, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateSendParallelInstruction, StepStartedAt: &now}}}
	stops := NewStopState(t.TempDir())
	control := NewStopControlUseCase(stops, regressionStopRepository{records}, nil, nil)
	if _, err := control.Stop(context.Background(), &id); err != nil {
		t.Fatal(err)
	}
	events := []store.Event{}
	u := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }, Sessions: records, Stop: stops, Send: &rotationSender{}, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle }, ChildAlive: func(supervisor.SessionID) bool { return true }, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return supervisor.RotationCheckpointSnapshot{}, nil
	}, RecordStructured: func(e store.Event) error { events = append(events, e); return nil }}
	for range 2 {
		if err := u.Step(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	current, _ := records.Load(id)
	if current.State != supervisor.SessionStopped || current.Rotation == nil || !current.Rotation.Stage7ConfirmedWhileStopped || len(events) != 1 || events[0].Kind != supervisor.EventKindRotateStepDone {
		t.Fatalf("session=%+v rotation=%+v events=%+v", current, current.Rotation, events)
	}
}

func TestRotateSession_NonStopReasonsDoNotConfirmStoppedRotation(t *testing.T) {
	for _, reason := range []supervisor.SupervisorStopReason{supervisor.StopRotateStepFailed, supervisor.StopSupervisorRestarted} {
		t.Run(string(reason), func(t *testing.T) {
			s, u, now, events := rotationFixture()
			s.session.State = supervisor.SessionStopped
			s.session.StopReason = reason
			s.session.PreviousState = supervisor.SessionRotating
			s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendEnd, EndIssuedAt: now}
			u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking }
			if err := u.Step(context.Background(), s.session.SessionID); err != nil {
				t.Fatal(err)
			}
			if s.session.Rotation.Step != supervisor.RotateSendEnd || len(*events) != 0 {
				t.Fatalf("non-STOP reason advanced: %+v events=%+v", s.session, *events)
			}
		})
	}
}

func TestRotateSession_StartAndCommitPhaseAreMutuallyExclusive(t *testing.T) {
	for range 20 {
		now := time.Date(2026, 10, 12, 3, 0, 0, 0, time.UTC)
		id := supervisor.SessionID("rotation-git-phase-1")
		sessions := store.NewSessionStore(t.TempDir())
		initial := supervisor.ManagedSession{SessionID: id, ProjectID: "alpha", CLI: supervisor.CLIClaude,
			Generation: 1, State: supervisor.SessionIdle, StateSince: now, StopScope: supervisor.StopScopeNone,
			ChildPID: 10, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
		if err := sessions.Save(initial); err != nil {
			t.Fatal(err)
		}
		rotation := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, Sessions: sessions, Send: &rotationSender{},
			Now: func() time.Time { return now }, IsStopped: func(supervisor.SessionID) bool { return false },
			Observed:    func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionIdle },
			ChildAlive:  func(supervisor.SessionID) bool { return true },
			ResumeReady: func(supervisor.SessionID, time.Time) bool { return true },
			Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
				return supervisor.RotationCheckpointSnapshot{}, nil
			},
		}
		start := make(chan struct{})
		startResult := make(chan error, 1)
		phaseResult := make(chan bool, 1)
		go func() { <-start; startResult <- rotation.Start(context.Background(), id) }()
		go func() {
			<-start
			ok, err := sessions.SetGitPhaseIfEligible(id, supervisor.GitPhaseEvaluating)
			if err != nil {
				t.Error(err)
			}
			phaseResult <- ok
		}()
		close(start)
		startErr := <-startResult
		phase := <-phaseResult
		current, err := sessions.Load(id)
		if err != nil {
			t.Fatal(err)
		}
		if phase && (startErr == nil || current.State == supervisor.SessionRotating) || !phase && startErr != nil {
			t.Fatalf("phase=%v startErr=%v state=%s", phase, startErr, current.State)
		}
	}
}

func TestRotateSession_SendEndPreservesGateIssuedAt(t *testing.T) {
	s, u, now, _ := rotationFixture()
	issuedByGate := now.Add(time.Second)
	u.Send.(*rotationSender).onSend = func(in SendInput) {
		if in.Kind != supervisor.SendKindHandoffEnd {
			return
		}
		s.session.Rotation.EndIssuedAt = &issuedByGate
	}
	if err := u.Start(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.Rotation.EndIssuedAt == nil || !s.session.Rotation.EndIssuedAt.Equal(issuedByGate) {
		t.Fatalf("end issued at = %v, want %v", s.session.Rotation.EndIssuedAt, issuedByGate)
	}
}

func TestRotateSession_SCN18_32_ChildExitDuringStage5BackoffThenStop(t *testing.T) {
	now := time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)
	id := supervisor.SessionID("backoff-stop-32")
	records := &regressionSessionStore{session: supervisor.ManagedSession{SessionID: id, State: supervisor.SessionRotating, ChildPID: 20, RestartCount: 1, Rotation: &supervisor.RotationRecord{Step: supervisor.RotateRestart, StepStartedAt: &now, Stage5Started: true}}}
	stops := NewStopState(t.TempDir())
	u := &RotateSessionUseCase{Wake: rotationWakeChannel, PendingRemoteControl: &PendingRemoteControlUseCase{}, ResumeReady: func(supervisor.SessionID, time.Time) bool { return true }, Sessions: records, Stop: stops, Send: &rotationSender{}, Respawn: &rotationRespawn{}, Observed: func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking }, ChildAlive: func(supervisor.SessionID) bool { return false }, Inspect: func(supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return supervisor.RotationCheckpointSnapshot{}, nil
	}, Now: func() time.Time { return now }}
	if err := u.Step(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	current, _ := records.Load(id)
	if current.State != supervisor.SessionRotating || current.Rotation.Step != supervisor.RotateRestart || current.Rotation.RespawnWaitUntil == nil {
		t.Fatalf("child exit did not enter backoff: %+v", current)
	}
	control := NewStopControlUseCase(stops, regressionStopRepository{records}, nil, nil)
	if _, err := control.Stop(context.Background(), &id); err != nil {
		t.Fatal(err)
	}
	now = now.Add(supervisor.RestartBackoff(1))
	if err := u.Step(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	current, _ = records.Load(id)
	if current.State != supervisor.SessionStopped || len(u.Respawn.(*rotationRespawn).kinds) != 0 {
		t.Fatalf("STOP during backoff allowed respawn: %+v", current)
	}
}

func TestRotateSession_FailureNotificationIncludesStepAndReason(t *testing.T) {
	s, u, _, _ := rotationFixture()
	u.Send.(*rotationSender).fail = supervisor.SendKindHandoffEnd
	var notice string
	u.Notify = func(_ supervisor.SupervisorEventKind, value string) { notice = value }
	if err := u.Start(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(notice, string(supervisor.RotateSendEnd)) || !strings.Contains(notice, string(supervisor.RotateFailSendNotConfirmed)) {
		t.Fatalf("notice omitted step or reason: %q", notice)
	}
}

func TestRotateSession_Stage6IdleWaitExpiresWithoutSend(t *testing.T) {
	s, u, now, _ := rotationFixture()
	started := *now
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendStart, StepStartedAt: &started}
	u.Observed = func(supervisor.SessionID) supervisor.SupervisorSessionState { return supervisor.SessionWorking }
	*now = now.Add(rotationStepTimeout)
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if s.session.State != supervisor.SessionStopped || s.session.Rotation.FailReason != supervisor.RotateFailStartNotSettled || len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatalf("idle timeout not applied: %+v", s.session)
	}
}

func TestRotateSession_SCN42_ResumeSpawnFailureStopsAtRecordedStage(t *testing.T) {
	for _, tc := range []struct {
		step  supervisor.SupervisorRotateStep
		setup func(*supervisor.ManagedSession, *time.Time)
	}{
		{step: supervisor.RotateSendEnd, setup: func(s *supervisor.ManagedSession, now *time.Time) {
			s.Rotation.EndIssuedAt = now
		}},
		{step: supervisor.RotateConfirmHandoff},
		{step: supervisor.RotateSendExit, setup: func(s *supervisor.ManagedSession, _ *time.Time) {
			s.LastSend = &supervisor.LastSendRecord{Kind: supervisor.SendKindHandoffExit, Confirmed: false}
		}},
	} {
		t.Run(string(tc.step), func(t *testing.T) {
			s, u, now, _ := rotationFixture()
			s.session.State = supervisor.SessionRotating
			s.session.ChildPID = 0
			s.session.ClaudeSessionID = "claude-id"
			s.session.Rotation = &supervisor.RotationRecord{Step: tc.step, StepStartedAt: now}
			if tc.setup != nil {
				tc.setup(&s.session, now)
			}
			u.ChildAlive = func(supervisor.SessionID) bool { return false }
			respawn := &rotationRespawn{store: s, err: errors.New("spawn failed")}
			u.Respawn = respawn
			if err := u.Resume(context.Background(), s.session); err != nil {
				t.Fatal(err)
			}
			if err := u.Step(context.Background(), s.session.SessionID); err != nil {
				t.Fatal(err)
			}
			if s.session.State != supervisor.SessionStopped || s.session.Rotation == nil || s.session.Rotation.Step != tc.step || s.session.Rotation.FailReason != supervisor.RotateFailSpawnFailed || s.session.Rotation.ResumeSpawn {
				t.Fatalf("resume spawn failure: %+v", s.session)
			}
			if len(u.Send.(*rotationSender).calls) != 0 {
				t.Fatalf("unexpected sends: %+v", u.Send.(*rotationSender).calls)
			}
			if len(respawn.kinds) != 1 || respawn.kinds[0] != RespawnHandoffResume {
				t.Fatalf("respawn kinds = %+v", respawn.kinds)
			}
		})
	}
}

func TestRotateSession_SCN40_ResumeStage4WaitsForFreshIdle(t *testing.T) {
	s, u, now, _ := rotationFixture()
	s.session.State = supervisor.SessionRotating
	s.session.Rotation = &supervisor.RotationRecord{Step: supervisor.RotateSendExit, StepStartedAt: now}
	u.Respawn = &rotationRespawn{store: s}
	ready := false
	u.ResumeReady = func(supervisor.SessionID, time.Time) bool { return ready }
	if err := u.Resume(context.Background(), s.session); err != nil {
		t.Fatal(err)
	}
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatal("resume sent /exit before fresh idle")
	}
	ready = true
	if err := u.Step(context.Background(), s.session.SessionID); err != nil {
		t.Fatal(err)
	}
	if len(u.Send.(*rotationSender).calls) != 1 || u.Send.(*rotationSender).calls[0].Kind != supervisor.SendKindHandoffExit {
		t.Fatalf("send after fresh idle = %+v", u.Send.(*rotationSender).calls)
	}
}

func TestRotateSession_ConcurrentStartAndRunSendEndOnce(t *testing.T) {
	s, u, _, _ := rotationFixture()
	id := s.session.SessionID
	held := &heldRotationSender{started: make(chan struct{}, 2), release: make(chan struct{})}
	u.Send = held
	startDone := make(chan error, 1)
	runDone := make(chan error, 1)
	go func() { startDone <- u.Start(context.Background(), id) }()
	select {
	case <-held.started:
	case <-time.After(time.Second):
		t.Fatal("first send did not start")
	}
	go func() { runDone <- u.Run(context.Background(), id) }()
	close(held.release)
	select {
	case <-startDone:
	case <-time.After(time.Second):
		t.Fatal("Start stayed blocked")
	}
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("Run stayed blocked")
	}
	select {
	case <-held.started:
		t.Fatal("concurrent Step sent #end twice")
	default:
	}
}

func TestRotateSession_MissingRequiredObservationFailsBeforeSend(t *testing.T) {
	s, u, _, _ := rotationFixture()
	u.Observed = nil
	u.Inspect = nil
	u.CheckpointCWD = nil
	if err := u.Start(context.Background(), s.session.SessionID); err == nil {
		t.Fatal("missing dependencies accepted")
	}
	if s.session.Rotation != nil || len(u.Send.(*rotationSender).calls) != 0 {
		t.Fatalf("started without observations: %+v", s.session)
	}
}

func TestRotateSession_MissingChildLivenessFailsClosed(t *testing.T) {
	s, u, _, _ := rotationFixture()
	u.ChildAlive = nil
	if err := u.Start(context.Background(), s.session.SessionID); err == nil || s.session.Rotation != nil {
		t.Fatalf("started without child liveness: err=%v session=%+v", err, s.session)
	}
}
