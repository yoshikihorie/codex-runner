package execution

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

type fakeTaskPlacementStore struct {
	mu           sync.Mutex
	policy       store.TaskPlacementPolicy
	plan         *store.TaskPlacementPlan
	deleted      []domain.TaskID
	err          error
	planCalls    int
	executeCalls int
}

func (s *fakeTaskPlacementStore) Plan(_ context.Context, policy store.TaskPlacementPolicy) (*store.TaskPlacementPlan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.policy = policy
	s.planCalls++
	return s.plan, s.err
}
func (s *fakeTaskPlacementStore) Execute(context.Context, *store.TaskPlacementPlan) ([]domain.TaskID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.executeCalls++
	return s.deleted, s.err
}

func (s *fakeTaskPlacementStore) calls() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.planCalls, s.executeCalls
}

type fakeEvictedTaskIndex struct{ ids []domain.TaskID }

func (i *fakeEvictedTaskIndex) ForgetEvicted(id domain.TaskID) { i.ids = append(i.ids, id) }

func taskPlacementID(t *testing.T, suffix string) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20200101-000000-abcd-" + suffix)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestEvictTaskPlacementPlanConvertsPolicyAndExecuteSynchronizesBeforeError(t *testing.T) {
	placement := &fakeTaskPlacementStore{plan: &store.TaskPlacementPlan{}}
	index := &fakeEvictedTaskIndex{}
	u, err := NewEvictTaskPlacementUseCase(placement, index, TaskPlacementEvictionPolicy{RetentionDays: 2, DiskBudgetMB: 3, Now: func() time.Time { return time.Unix(1, 0) }})
	if err != nil {
		t.Fatal(err)
	}
	result, err := u.Plan(context.Background())
	if err != nil || placement.policy.DiskBudgetBytes != 3_000_000 || placement.policy.RetentionDays != 2 {
		t.Fatalf("plan=%+v policy=%+v err=%v", result, placement.policy, err)
	}
	id := taskPlacementID(t, "cancelled")
	placement.deleted, placement.err = []domain.TaskID{id}, context.Canceled
	if err := u.Execute(context.Background(), result); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if len(index.ids) != 1 || index.ids[0] != id {
		t.Fatalf("forgot=%v", index.ids)
	}
}

func TestEvictTaskPlacementLogsBoundedStructuredFailures(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	placement := &fakeTaskPlacementStore{plan: &store.TaskPlacementPlan{}}
	index := &fakeEvictedTaskIndex{}
	var failures []error
	for n := 0; n < taskPlacementCleanupWarningDetailLimit+1; n++ {
		failures = append(failures, &store.TaskPlacementCleanupFailure{TaskID: taskPlacementID(t, fmt.Sprintf("failure-%03d", n)), Path: "candidate", Stage: "delete", Reason: "access_denied", Err: errors.New("denied")})
	}
	placement.err = errors.Join(failures...)
	u, err := NewEvictTaskPlacementUseCase(placement, index, TaskPlacementEvictionPolicy{RetentionDays: 1, DiskBudgetMB: 1, Now: time.Now}, logger)
	if err != nil {
		t.Fatal(err)
	}
	if err := u.Execute(context.Background(), TaskPlacementPlanResult{Plan: placement.plan}); err == nil {
		t.Fatal("expected failures")
	}
	logs := out.String()
	if got := strings.Count(logs, `"task_id"`); got != taskPlacementCleanupWarningDetailLimit {
		t.Fatalf("detail logs=%d", got)
	}
	if !strings.Contains(logs, "TASK_PLACEMENT_CLEANUP_FAILED") || !strings.Contains(logs, "error.taskPlacement.cleanupFailed") || !strings.Contains(logs, `"reasons"`) {
		t.Fatalf("missing structured cleanup log: %s", logs)
	}
}
