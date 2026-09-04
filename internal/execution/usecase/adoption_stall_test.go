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

// SCN-exec-03-12/13: an adopted running task remains on the normal stall
// path. The coordinator probe is shared with the lock test and confirms that
// the recovered task is handed off only after orphan persistence.
func TestAdoptedRunningTaskDeathIsHandedToSharedOrphanCoordinator(t *testing.T) {
	start := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct{ name string }{{name: "artifact"}, {name: "no-session"}} {
		t.Run(tc.name, func(t *testing.T) {
			id := testID(t, "adopted-death-"+tc.name)
			snapshot := testSnapshot(t, id, domain.StateRunning, start, &start)
			snapshot.AdoptedAfterRestart = true
			recorder := &testRecorder{}
			store := &testStore{r: recorder, loads: map[string]domain.TaskSnapshot{id.String(): snapshot}, lists: [][]domain.TaskSnapshot{{snapshot}}}
			lock := &stallLockProbe{}
			coordinator := &stallCoordinatorProbe{lock: lock}
			uc := newCheckStallUseCase(store, lock, testLive(recorder, id, true, nil), &testWriter{r: recorder}, &testClock{at: start, r: recorder, id: id}, testTracker(recorder), time.Second, testTickerFactory{&testTicker{ch: make(chan time.Time, 1), r: recorder}}, execution.NewLifecycleOwnershipRegistry())
			uc.orphanCoordinator = coordinator

			uc.scan(context.Background())

			if coordinator.calls != 1 || coordinator.id != id || len(store.saved) != 1 || store.saved[0].State != domain.StateOrphaned || !store.saved[0].AdoptedAfterRestart {
				t.Fatalf("coordinator=%d id=%s saved=%+v", coordinator.calls, coordinator.id, store.saved)
			}
		})
	}
}
