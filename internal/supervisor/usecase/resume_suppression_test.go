package usecase

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

type resumeSuppressionRepository struct{ sessions *pushTestSessions }

func (r resumeSuppressionRepository) List(context.Context) ([]supervisor.ManagedSession, error) {
	return []supervisor.ManagedSession{r.sessions.value}, nil
}
func (r resumeSuppressionRepository) Save(_ context.Context, s supervisor.ManagedSession) error {
	return r.sessions.Save(s)
}

func TestResumeSuppression_SCN19_ClearFailureAndPushState(t *testing.T) {
	id := supervisor.SessionID("resume-1")
	sessions := &pushTestSessions{value: supervisor.ManagedSession{
		SessionID: id, LastPushFailedHead: pushTestHead,
		LastFailureFingerprint: "failure-fingerprint", LastFailureReason: supervisor.FailureGitFailed,
		LastVerifiedFingerprint: "failed-fingerprint", LastVerifiedResult: "failed",
	}}
	push := &PushToAllowedRemoteUseCase{suppressed: map[supervisor.SessionID]string{id: pushTestHead, "other": pushTestBase}}
	if err := NewSessionResumeSuppression(sessions, push).Clear(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	s := sessions.value
	if s.LastPushFailedHead != "" || s.LastFailureFingerprint != "" || s.LastFailureReason != "" || s.LastVerifiedFingerprint != "" || s.LastVerifiedResult != "" {
		t.Fatalf("suppression remains: %+v", s)
	}
	if _, ok := push.suppressed[id]; ok || push.suppressed["other"] != pushTestBase {
		t.Fatalf("push suppression=%v", push.suppressed)
	}
}

func TestResumeSuppression_SCN36_PreservesPassedVerification(t *testing.T) {
	id := supervisor.SessionID("resume-2")
	sessions := &pushTestSessions{value: supervisor.ManagedSession{SessionID: id, LastVerifiedFingerprint: "passed-fingerprint", LastVerifiedResult: "passed"}}
	if err := NewSessionResumeSuppression(sessions, nil).Clear(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	if sessions.value.LastVerifiedFingerprint != "passed-fingerprint" || sessions.value.LastVerifiedResult != "passed" {
		t.Fatalf("passed verification changed: %+v", sessions.value)
	}
}

func TestResumeSuppression_SCN19_RetriesSameHeadAfterResume(t *testing.T) {
	push, git, sessions, events := pushFakeFixture(t)
	git.pushErr = errors.New("push rejected")
	pushRunFake(t, push, sessions)
	pushRunFake(t, push, sessions)
	if sessions.value.LastPushFailedHead != pushTestHead {
		t.Fatal("failed HEAD was not saved")
	}
	if err := NewSessionResumeSuppression(sessions, push).Clear(context.Background(), sessions.value.SessionID); err != nil {
		t.Fatal(err)
	}
	git.pushErr = nil
	pushRunFake(t, push, sessions)
	if strings.Count(strings.Join(git.calls, ","), "push") != 2 || (*events)[len(*events)-1].Kind != supervisor.EventKindPushSucceeded {
		t.Fatalf("calls=%v events=%+v", git.calls, *events)
	}
}

func TestStopControlResume_SCN19_ClearsSuppressionAfterResume(t *testing.T) {
	id := supervisor.SessionID("resume-3")
	sessions := &pushTestSessions{value: supervisor.ManagedSession{
		SessionID: id, State: supervisor.SessionStopped, StopScope: supervisor.StopScopeSession,
		StopReason: supervisor.StopSession, ClaudeSessionID: "claude-resume",
		LastPushFailedHead: pushTestHead, LastFailureFingerprint: "failure-fingerprint",
		LastFailureReason: supervisor.FailureGitFailed, LastVerifiedFingerprint: "failed-fingerprint", LastVerifiedResult: "failed",
	}}
	stops := NewStopState(t.TempDir())
	stops.SetSession(id, true)
	if err := stops.WriteSession(id); err != nil {
		t.Fatal(err)
	}
	push := &PushToAllowedRemoteUseCase{suppressed: map[supervisor.SessionID]string{id: pushTestHead}}
	u := NewStopControlUseCase(stops, resumeSuppressionRepository{sessions}, nil, nil)
	u.SetResumeSuppression(NewSessionResumeSuppression(sessions, push))
	result, err := u.Resume(context.Background(), &id, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Resumed) != 1 || result.Resumed[0] != id || sessions.value.State == supervisor.SessionStopped {
		t.Fatalf("resume result=%+v session=%+v", result, sessions.value)
	}
	if sessions.value.LastPushFailedHead != "" || sessions.value.LastFailureFingerprint != "" || sessions.value.LastFailureReason != "" || sessions.value.LastVerifiedFingerprint != "" || sessions.value.LastVerifiedResult != "" {
		t.Fatalf("suppression remains after resume: %+v", sessions.value)
	}
	if _, ok := push.suppressed[id]; ok {
		t.Fatal("push duplicate event suppression remains")
	}
}
