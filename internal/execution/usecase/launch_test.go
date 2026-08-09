package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

type launchRunnerFake struct {
	called bool
	got    execution.LaunchParams
	result *execution.LaunchedProcess
	err    error
}

func (f *launchRunnerFake) Launch(_ context.Context, p execution.LaunchParams) (*execution.LaunchedProcess, error) {
	f.called = true
	f.got = p
	return f.result, f.err
}

func TestLaunchWithPTYUseCaseRejectsNonResumableSession(t *testing.T) {
	fake := &launchRunnerFake{}
	_, err := NewLaunchWithPTYUseCase(fake).Execute(context.Background(), execution.LaunchParams{})
	if !errors.Is(err, domain.ErrSessionNotResumable) || fake.called {
		t.Fatalf("err=%v called=%t", err, fake.called)
	}
}

func TestLaunchWithPTYUseCaseDelegatesResult(t *testing.T) {
	want := &execution.LaunchedProcess{}
	fake := &launchRunnerFake{result: want, err: errors.New("runner error")}
	got, err := NewLaunchWithPTYUseCase(fake).Execute(context.Background(), execution.LaunchParams{AllowResume: true})
	if got != want || err != fake.err || !fake.called {
		t.Fatalf("got,err,called = %v,%v,%t", got, err, fake.called)
	}
}
