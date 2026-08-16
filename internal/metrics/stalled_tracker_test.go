package metrics

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func stalledTrackerTaskID(t *testing.T, value string) domain.TaskID {
	t.Helper()

	taskID, err := domain.NewTaskID(value)
	if err != nil {
		t.Fatal(err)
	}
	return taskID
}

func TestStalledTimeTracker_ZeroValueTracksFirstInterval(t *testing.T) {
	// Arrange
	var tracker StalledTimeTracker
	taskID := stalledTrackerTaskID(t, "impl-20260815-120000-abcd-first-interval")
	enteredAt := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	leftAt := enteredAt.Add(1250 * time.Millisecond)

	// Act
	tracker.EnterStalled(taskID, enteredAt)
	got := tracker.LeaveStalled(taskID, leftAt)

	// Assert
	if got != 1250 {
		t.Fatalf("LeaveStalled() = %d, want 1250", got)
	}
}

func TestStalledTimeTracker_AccumulatesSeparateIntervalsOnly(t *testing.T) {
	// Arrange
	var tracker StalledTimeTracker
	taskID := stalledTrackerTaskID(t, "impl-20260815-120001-abcd-separate-intervals")
	firstEnteredAt := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	firstLeftAt := firstEnteredAt.Add(200 * time.Millisecond)
	secondEnteredAt := firstLeftAt.Add(5 * time.Second)
	secondLeftAt := secondEnteredAt.Add(350 * time.Millisecond)

	// Act
	tracker.EnterStalled(taskID, firstEnteredAt)
	tracker.LeaveStalled(taskID, firstLeftAt)
	tracker.EnterStalled(taskID, secondEnteredAt)
	got := tracker.LeaveStalled(taskID, secondLeftAt)

	// Assert
	if got != 550 {
		t.Fatalf("LeaveStalled() total = %d, want 550", got)
	}
}

func TestStalledTimeTracker_DuplicateEnterPreservesOriginalStart(t *testing.T) {
	// Arrange
	var tracker StalledTimeTracker
	taskID := stalledTrackerTaskID(t, "impl-20260815-120002-abcd-duplicate-enter")
	firstEnteredAt := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	duplicateEnteredAt := firstEnteredAt.Add(500 * time.Millisecond)
	leftAt := firstEnteredAt.Add(900 * time.Millisecond)

	// Act
	tracker.EnterStalled(taskID, firstEnteredAt)
	tracker.EnterStalled(taskID, duplicateEnteredAt)
	got := tracker.LeaveStalled(taskID, leftAt)

	// Assert
	if got != 900 {
		t.Fatalf("LeaveStalled() = %d, want 900 from original start", got)
	}
}

func TestStalledTimeTracker_LeaveUntrackedOrInactiveReturnsCurrentTotal(t *testing.T) {
	// Arrange
	var tracker StalledTimeTracker
	untrackedTaskID := stalledTrackerTaskID(t, "impl-20260815-120003-abcd-untracked")
	inactiveTaskID := stalledTrackerTaskID(t, "impl-20260815-120004-abcd-inactive")
	enteredAt := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	leftAt := enteredAt.Add(400 * time.Millisecond)

	// Act
	untrackedGot := tracker.LeaveStalled(untrackedTaskID, leftAt)
	tracker.EnterStalled(inactiveTaskID, enteredAt)
	tracker.LeaveStalled(inactiveTaskID, leftAt)
	inactiveGot := tracker.LeaveStalled(inactiveTaskID, leftAt.Add(time.Second))

	// Assert
	if untrackedGot != 0 {
		t.Fatalf("LeaveStalled() for untracked task = %d, want 0", untrackedGot)
	}
	if inactiveGot != 400 {
		t.Fatalf("LeaveStalled() for inactive task = %d, want 400", inactiveGot)
	}
}

func TestStalledTimeTracker_TakeTotalRemovesOnlyRequestedTask(t *testing.T) {
	// Arrange
	var tracker StalledTimeTracker
	firstTaskID := stalledTrackerTaskID(t, "impl-20260815-120005-abcd-first-take")
	secondTaskID := stalledTrackerTaskID(t, "impl-20260815-120006-abcd-second-take")
	enteredAt := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)

	tracker.EnterStalled(firstTaskID, enteredAt)
	tracker.LeaveStalled(firstTaskID, enteredAt.Add(100*time.Millisecond))
	tracker.EnterStalled(secondTaskID, enteredAt)
	tracker.LeaveStalled(secondTaskID, enteredAt.Add(250*time.Millisecond))

	// Act
	firstGot := tracker.TakeTotal(firstTaskID)
	firstAgainGot := tracker.TakeTotal(firstTaskID)
	secondGot := tracker.TakeTotal(secondTaskID)

	// Assert
	if firstGot != 100 {
		t.Fatalf("first TakeTotal() = %d, want 100", firstGot)
	}
	if firstAgainGot != 0 {
		t.Fatalf("second TakeTotal() = %d, want 0", firstAgainGot)
	}
	if secondGot != 250 {
		t.Fatalf("TakeTotal() for remaining task = %d, want 250", secondGot)
	}
}

func TestStalledTimeTracker_ConcurrentTasksKeepTotalsSeparate(t *testing.T) {
	// Arrange
	var tracker StalledTimeTracker
	const taskCount = 8
	enteredAt := time.Date(2026, time.August, 15, 12, 0, 0, 0, time.UTC)
	start := make(chan struct{})
	results := make(chan stalledTrackerResult, taskCount)
	var waitGroup sync.WaitGroup

	for index := 0; index < taskCount; index++ {
		taskID := stalledTrackerTaskID(t, fmt.Sprintf("impl-20260815-1200%02d-abcd-concurrent", index+7))
		want := (index + 1) * 100
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			<-start
			tracker.EnterStalled(taskID, enteredAt)
			tracker.LeaveStalled(taskID, enteredAt.Add(time.Duration(want)*time.Millisecond))
			results <- stalledTrackerResult{taskID: taskID, got: tracker.TakeTotal(taskID), want: want}
		}()
	}

	// Act
	close(start)
	waitGroup.Wait()

	// Assert
	for index := 0; index < taskCount; index++ {
		result := <-results
		if result.got != result.want {
			t.Fatalf("TakeTotal() for %s = %d, want %d", result.taskID.String(), result.got, result.want)
		}
	}
}

type stalledTrackerResult struct {
	taskID domain.TaskID
	got    int
	want   int
}
