package recovery

import (
	"sync"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestPendingReconciliationSetAddAndMonotonicSignal(t *testing.T) {
	set := &PendingReconciliationSet{}
	taskID := pendingTestTaskID(t, "add")

	set.Add(taskID, false)
	set.Add(taskID, false)
	if entries := set.List(); len(entries) != 1 || entries[0].taskID != taskID || entries[0].signalSent {
		t.Fatalf("entries after duplicate false add = %#v", entries)
	}

	set.Add(taskID, true)
	set.Add(taskID, false)
	if entries := set.List(); len(entries) != 1 || !entries[0].signalSent {
		t.Fatalf("entries after monotonic update = %#v", entries)
	}
}

func TestPendingReconciliationSetRemoveAndListSnapshot(t *testing.T) {
	set := &PendingReconciliationSet{}
	taskID := pendingTestTaskID(t, "remove")
	set.Remove(taskID)
	set.Add(taskID, true)

	snapshot := set.List()
	snapshot[0].signalSent = false
	if entries := set.List(); len(entries) != 1 || !entries[0].signalSent {
		t.Fatalf("internal state mutated via returned snapshot = %#v", entries)
	}

	set.Remove(taskID)
	set.Remove(taskID)
	if entries := set.List(); len(entries) != 0 {
		t.Fatalf("entries after idempotent remove = %#v", entries)
	}

	set.Add(taskID, true)
	if entries := set.List(); len(entries) != 1 || !entries[0].signalSent {
		t.Fatalf("entries affected by snapshot mutation = %#v", entries)
	}
}

func TestPendingReconciliationSetRegister(t *testing.T) {
	set := &PendingReconciliationSet{}
	var registrar PendingRegistrar = set
	taskID := pendingTestTaskID(t, "register")
	if err := registrar.Register(taskID, true); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if entries := set.List(); len(entries) != 1 || entries[0].taskID != taskID || !entries[0].signalSent {
		t.Fatalf("entries after Register = %#v", entries)
	}
}

func TestPendingReconciliationSetClaimForSend(t *testing.T) {
	set := &PendingReconciliationSet{}
	missingID := pendingTestTaskID(t, "missing")
	if claimed, found := set.ClaimForSend(missingID); claimed || found {
		t.Fatalf("ClaimForSend(missing) = (%t, %t), want (false, false)", claimed, found)
	}

	unsentID := pendingTestTaskID(t, "unsent")
	set.Add(unsentID, false)
	if claimed, found := set.ClaimForSend(unsentID); !claimed || !found {
		t.Fatalf("first ClaimForSend = (%t, %t), want (true, true)", claimed, found)
	}
	if claimed, found := set.ClaimForSend(unsentID); claimed || !found {
		t.Fatalf("second ClaimForSend = (%t, %t), want (false, true)", claimed, found)
	}

	sentID := pendingTestTaskID(t, "sent")
	set.Add(sentID, true)
	if claimed, found := set.ClaimForSend(sentID); claimed || !found {
		t.Fatalf("ClaimForSend(sent) = (%t, %t), want (false, true)", claimed, found)
	}
}

func TestPendingReconciliationSetClaimForSendHasOneWinner(t *testing.T) {
	set := &PendingReconciliationSet{}
	taskID := pendingTestTaskID(t, "claim")
	set.Add(taskID, false)

	const claimants = 32
	start := make(chan struct{})
	results := make(chan bool, claimants)
	var wg sync.WaitGroup
	for range claimants {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claimed, found := set.ClaimForSend(taskID)
			if !found {
				t.Error("ClaimForSend unexpectedly did not find task")
			}
			results <- claimed
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	winners := 0
	for claimed := range results {
		if claimed {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("claim winners = %d, want 1", winners)
	}
}

func TestPendingReconciliationSetConcurrentOperations(t *testing.T) {
	set := &PendingReconciliationSet{}
	taskID := pendingTestTaskID(t, "concurrent")

	const workers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			switch worker % 5 {
			case 0:
				set.Add(taskID, worker%2 == 0)
			case 1:
				if err := set.Register(taskID, worker%2 == 0); err != nil {
					t.Errorf("Register() error = %v", err)
				}
			case 2:
				_ = set.List()
			case 3:
				set.Remove(taskID)
			case 4:
				set.ClaimForSend(taskID)
			}
		}(worker)
	}
	close(start)
	wg.Wait()
}

func pendingTestTaskID(t *testing.T, slug string) domain.TaskID {
	t.Helper()
	taskID, err := domain.NewTaskID("impl-20260810-120000-abcd-pending-" + slug)
	if err != nil {
		t.Fatalf("NewTaskID() error = %v", err)
	}
	return taskID
}
