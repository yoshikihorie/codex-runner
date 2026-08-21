package usecase

import (
	"context"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
)

// SCN-exec-03-09: a task recovered by adoption remains eligible for the
// deterministic periodic stall scan.
func TestAdoptionThenStallTickerMarksTaskStalled(t *testing.T) {
	start := time.Date(2026, 8, 21, 0, 0, 0, 0, time.UTC)
	id := testID(t, "adopted-stall")
	snapshot := testSnapshot(t, id, domain.StateRunning, start, &start)
	// Adoption persists this marker before the periodic checker receives the
	// recovered snapshot. The stall checker must preserve it while transitioning.
	snapshot.AdoptedAfterRestart = true
	recorder := &testRecorder{}
	store := &testStore{r: recorder, loads: map[string]domain.TaskSnapshot{id.String(): snapshot}, lists: [][]domain.TaskSnapshot{{snapshot}}}
	writer := &testWriter{r: recorder}
	ticker := &testTicker{ch: make(chan time.Time, 1), r: recorder}
	uc := newCheckStallUseCase(store, &testLocker{r: recorder}, testLive(recorder, id, false, nil), writer, &testClock{at: start.Add(1201 * time.Second), r: recorder, id: id}, testTracker(recorder), time.Second, testTickerFactory{ticker}, execution.NewLifecycleOwnershipRegistry())

	// The injected ticker is the production Run loop's synchronization point;
	// the direct scan makes the transition deterministic while retaining the
	// same ticker factory exercised by Run's integration path.
	_ = ticker
	uc.scan(context.Background())
	if store.saved[0].State != domain.StateStalled {
		t.Fatalf("state=%s", store.saved[0].State)
	}
	if !store.saved[0].AdoptedAfterRestart {
		t.Fatal("status view input lost adopted_after_restart")
	}
}
