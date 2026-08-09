package usecase

import (
	"context"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

type LaunchWithPTYUseCase struct {
	runner execution.ProcessRunner
}

func NewLaunchWithPTYUseCase(runner execution.ProcessRunner) *LaunchWithPTYUseCase {
	return &LaunchWithPTYUseCase{runner: runner}
}

func (u *LaunchWithPTYUseCase) Execute(ctx context.Context, params execution.LaunchParams) (*execution.LaunchedProcess, error) {
	if !params.AllowResume {
		return nil, domain.ErrSessionNotResumable
	}
	return u.runner.Launch(ctx, params)
}
