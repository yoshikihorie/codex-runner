package usecase_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/contract"
	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	"github.com/yoshikihorie/codex-runner/internal/execution/usecase"
	"github.com/yoshikihorie/codex-runner/internal/metrics"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type externalStore struct{ store.TaskStore }
type externalWriter struct{ contract.ContractWriter }

func TestNewCheckStallUseCaseIsUsableFromExternalPackage(t *testing.T) {
	mutex := store.NewTaskMutex()
	liveness := execution.NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return false, nil }), func(domain.TaskID) string { return "lock" })
	ownership := execution.NewLifecycleOwnershipRegistry()
	uc := usecase.NewCheckStallUseCase(&externalStore{}, mutex, liveness, &externalWriter{}, domain.ClockFunc(func() time.Time { return time.Time{} }), &metrics.StalledTimeTracker{}, ownership, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	uc.Run(ctx)
}
