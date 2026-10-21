package usecase

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type remoteControlTestLog struct{ entries []store.SupervisorLogEntry }

func (l *remoteControlTestLog) Record(entry store.SupervisorLogEntry) error {
	l.entries = append(l.entries, entry)
	return nil
}

type failingRemoteControlTestLog struct{}

func (failingRemoteControlTestLog) Record(store.SupervisorLogEntry) error {
	return errors.New("log write failed")
}

func TestInitialRemoteControl_SCN0159ConfirmedResumeSkips(t *testing.T) {
	id, err := supervisor.NewSessionID("confirmed-1")
	if err != nil {
		t.Fatal(err)
	}
	s := supervisor.ManagedSession{SessionID: id, Generation: 2, State: supervisor.SessionIdle}
	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	record["remote_control_confirmed"] = true
	data, err = json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &s); err != nil {
		t.Fatal(err)
	}
	requests := 0
	var events []store.Event
	log := &remoteControlTestLog{}
	r := &InitialRemoteControl{Request: func(context.Context, SendInput) error { requests++; return nil }, RecordStructured: func(e store.Event) error { events = append(events, e); return nil }, OperationalLog: log}
	if err := r.OnStateChange(context.Background(), supervisor.SessionStarting, s); err != nil {
		t.Fatal(err)
	}
	if requests != 0 {
		t.Fatalf("confirmed resumed generation requested remote control %d times", requests)
	}
	if len(events) != 1 || events[0].Kind != supervisor.EventKindRemoteControlSkipped || events[0].Reason != store.EventReason(supervisor.RemoteControlSkipResumedAfterConfirmed) {
		t.Fatalf("skip events=%+v", events)
	}
	if len(log.entries) != 1 || log.entries[0].Level != store.LogLevelWarn || log.entries[0].Kind != supervisor.EventKindRemoteControlSkipped || log.entries[0].SessionID != id.String() {
		t.Fatalf("skip log=%+v", log.entries)
	}
	if err := r.OnStateChange(context.Background(), supervisor.SessionIdle, s); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || requests != 0 {
		t.Fatalf("repeated idle: events=%+v requests=%d", events, requests)
	}
}

func TestInitialRemoteControl_SCN0161UnconfirmedResumeRequests(t *testing.T) {
	id, _ := supervisor.NewSessionID("unconfirmed-1")
	for _, generation := range []int{1, 2} {
		s := supervisor.ManagedSession{SessionID: id, Generation: generation, State: supervisor.SessionIdle}
		if generation == 1 {
			s.RemoteControlConfirmed = true
		}
		requests := 0
		r := &InitialRemoteControl{Request: func(context.Context, SendInput) error { requests++; return nil }}
		if err := r.OnStateChange(context.Background(), supervisor.SessionStarting, s); err != nil {
			t.Fatal(err)
		}
		if requests != 1 {
			t.Fatalf("generation %d requests=%d", generation, requests)
		}
	}
}

func TestInitialRemoteControl_SCN0162HandoffResumeDoesNotRequestOrSkip(t *testing.T) {
	id, _ := supervisor.NewSessionID("handoff-resume-1")
	s := supervisor.ManagedSession{SessionID: id, Generation: 2, State: supervisor.SessionIdle, RemoteControlConfirmed: true}
	requests, skips := 0, 0
	r := &InitialRemoteControl{
		Request:          func(context.Context, SendInput) error { requests++; return nil },
		RecordStructured: func(store.Event) error { skips++; return nil },
	}
	if err := r.OnStateChange(context.Background(), supervisor.SessionRotating, s); err != nil {
		t.Fatal(err)
	}
	if err := r.OnStateChange(context.Background(), supervisor.SessionIdle, s); err != nil {
		t.Fatal(err)
	}
	if requests != 0 || skips != 0 {
		t.Fatalf("handoff_resume: requests=%d skips=%d", requests, skips)
	}
}

func TestInitialRemoteControl_SCN0162UnconfirmedAfterHandoffRecoveryRequests(t *testing.T) {
	id, _ := supervisor.NewSessionID("handoff-recovery-1")
	s := supervisor.ManagedSession{SessionID: id, Generation: 3, State: supervisor.SessionIdle}
	requests, skips := 0, 0
	r := &InitialRemoteControl{
		Request: func(_ context.Context, in SendInput) error {
			if in.Kind != supervisor.SendKindRemoteControl {
				t.Fatalf("kind=%s", in.Kind)
			}
			requests++
			return nil
		},
		RecordStructured: func(store.Event) error { skips++; return nil },
	}
	if err := r.OnStateChange(context.Background(), supervisor.SessionStarting, s); err != nil {
		t.Fatal(err)
	}
	if requests != 1 || skips != 0 {
		t.Fatalf("unconfirmed recovery: requests=%d skips=%d", requests, skips)
	}
}

func TestInitialRemoteControl_SCN0165SkipOncePerGeneration(t *testing.T) {
	id, _ := supervisor.NewSessionID("skip-generations-1")
	s := supervisor.ManagedSession{SessionID: id, Generation: 2, State: supervisor.SessionIdle, RemoteControlConfirmed: true}
	requests, skips := 0, 0
	r := &InitialRemoteControl{
		Request:          func(context.Context, SendInput) error { requests++; return nil },
		RecordStructured: func(store.Event) error { skips++; return nil },
	}
	for _, generation := range []int{2, 3} {
		s.Generation = generation
		if err := r.OnStateChange(context.Background(), supervisor.SessionStarting, s); err != nil {
			t.Fatal(err)
		}
		if err := r.OnStateChange(context.Background(), supervisor.SessionWorking, s); err != nil {
			t.Fatal(err)
		}
		if skips != generation-1 || requests != 0 {
			t.Fatalf("generation=%d skips=%d requests=%d", generation, skips, requests)
		}
	}
}

func TestInitialRemoteControlSkipEventFailureRetriesNextIdle(t *testing.T) {
	id, _ := supervisor.NewSessionID("skip-retry-1")
	s := supervisor.ManagedSession{SessionID: id, Generation: 2, State: supervisor.SessionIdle, RemoteControlConfirmed: true}
	writes, skips, requests := 0, 0, 0
	writeErr := errors.New("event write failed")
	r := &InitialRemoteControl{
		Request: func(context.Context, SendInput) error { requests++; return nil },
		RecordStructured: func(store.Event) error {
			writes++
			if writes == 1 {
				return writeErr
			}
			skips++
			return nil
		},
		OperationalLog: failingRemoteControlTestLog{},
	}
	if err := r.OnStateChange(context.Background(), supervisor.SessionStarting, s); !errors.Is(err, writeErr) {
		t.Fatalf("first idle error=%v", err)
	}
	if err := r.OnStateChange(context.Background(), supervisor.SessionWorking, s); err != nil {
		t.Fatal(err)
	}
	if err := r.OnStateChange(context.Background(), supervisor.SessionIdle, s); err != nil {
		t.Fatal(err)
	}
	if err := r.OnStateChange(context.Background(), supervisor.SessionWorking, s); err != nil {
		t.Fatal(err)
	}
	if writes != 2 || skips != 1 || requests != 0 {
		t.Fatalf("writes=%d skips=%d requests=%d", writes, skips, requests)
	}
}
