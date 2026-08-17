package recovery

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestPendingRegistrarContract(t *testing.T) {
	set := &PendingReconciliationSet{}
	var registrar PendingRegistrar = set
	taskID := pendingTestTaskID(t, "contract")
	authority := pendingTestAuthority(taskID, 1)
	if err := registrar.Register(taskID, PendingSendUnsent, &authority); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	claim, outcome := registrar.ClaimForSend(taskID, authority)
	if outcome != ClaimAcquired || claim.Token == 0 {
		t.Fatalf("ClaimForSend() = (%#v, %v)", claim, outcome)
	}
	if !registrar.ReleaseSend(claim) {
		t.Fatal("ReleaseSend() = false")
	}
	claim, _ = registrar.ClaimForSend(taskID, authority)
	if !registrar.CompleteSend(claim) {
		t.Fatal("CompleteSend() = false")
	}

	secondID := pendingTestTaskID(t, "contract-invalidate")
	secondAuthority := pendingTestAuthority(secondID, 2)
	if err := registrar.Register(secondID, PendingSendUnsent, &secondAuthority); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	claim, _ = registrar.ClaimForSend(secondID, secondAuthority)
	if !registrar.InvalidateSend(claim) {
		t.Fatal("InvalidateSend() = false")
	}
}

func TestPendingRegisterValidationDoesNotMutate(t *testing.T) {
	taskID := pendingTestTaskID(t, "validation")
	otherID := pendingTestTaskID(t, "validation-other")
	authority := pendingTestAuthority(taskID, 1)
	for _, tc := range []struct {
		name        string
		disposition PendingSendDisposition
		authority   *ProcessSignalAuthority
		wantErr     string
	}{
		{"nil unsent", PendingSendUnsent, nil, "recovery: unsent pending entry requires process signal authority"},
		{"mismatched unsent", PendingSendUnsent, ptr(pendingTestAuthority(otherID, 1)), "recovery: process signal authority task ID does not match pending entry"},
		{"zero PID", PendingSendUnsent, ptr(pendingTestAuthority(taskID, 0)), "recovery: process signal authority requires a positive PID"},
		{"negative PID", PendingSendUnsent, ptr(pendingTestAuthority(taskID, -1)), "recovery: process signal authority requires a positive PID"},
		{"zero process start time", PendingSendUnsent, ptr(ProcessSignalAuthority{TaskID: taskID, PID: 1}), "recovery: process signal authority requires a non-zero process start time"},
		{"zero PID and process start time", PendingSendUnsent, ptr(ProcessSignalAuthority{TaskID: taskID}), "recovery: process signal authority requires a positive PID"},
		{"invalid", PendingSendDisposition(99), &authority, "recovery: invalid pending send disposition"},
	} {
		t.Run(tc.name+" missing", func(t *testing.T) {
			set := &PendingReconciliationSet{}
			if err := set.Register(taskID, tc.disposition, tc.authority); err == nil || err.Error() != tc.wantErr {
				t.Fatalf("Register() error = %v, want %q", err, tc.wantErr)
			}
			pendingRequireNoEntry(t, set, taskID)
		})
		t.Run(tc.name+" existing", func(t *testing.T) {
			set := pendingSetWithUnsent(t, taskID, authority)
			before := pendingOnlyEntry(t, set)
			if err := set.Register(taskID, tc.disposition, tc.authority); err == nil || err.Error() != tc.wantErr {
				t.Fatalf("Register() error = %v, want %q", err, tc.wantErr)
			}
			pendingRequireEntryEqual(t, before, pendingOnlyEntry(t, set))
		})
	}
}

func TestPendingRegisterAcceptsCompleteUnsentAuthority(t *testing.T) {
	taskID := pendingTestTaskID(t, "complete-unsent-authority")
	authority := pendingTestAuthority(taskID, 1)
	set := &PendingReconciliationSet{}
	if err := set.Register(taskID, PendingSendUnsent, &authority); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	entry := pendingOnlyEntry(t, set)
	if entry.state != pendingUnsent || !pendingTestAuthorityEqual(entry.authority, authority) {
		t.Fatalf("entry = %#v", entry)
	}
}

func TestPendingRegisterAcceptsNilForNonUnsent(t *testing.T) {
	for _, disposition := range []PendingSendDisposition{PendingSendSent, PendingSendConfirmOnly} {
		set := &PendingReconciliationSet{}
		taskID := pendingTestTaskID(t, "nil-non-unsent")
		if err := set.Register(taskID, disposition, nil); err != nil {
			t.Fatalf("Register(%v, nil) error = %v", disposition, err)
		}
	}
}

func TestPendingRegisterStateTransitions(t *testing.T) {
	taskID := pendingTestTaskID(t, "transitions")
	authority := pendingTestAuthority(taskID, 1)
	for _, tc := range []struct {
		name, initial string
		disposition   PendingSendDisposition
		want          pendingState
		wantErr       error
		wantCleared   bool
	}{
		{"missing unsent", "missing", PendingSendUnsent, pendingUnsent, nil, false},
		{"missing sent", "missing", PendingSendSent, pendingSent, nil, false},
		{"missing confirm", "missing", PendingSendConfirmOnly, pendingConfirmOnly, nil, false},
		{"unsent unsent", "unsent", PendingSendUnsent, pendingUnsent, nil, false},
		{"unsent sent", "unsent", PendingSendSent, pendingSent, nil, false},
		{"unsent confirm", "unsent", PendingSendConfirmOnly, pendingConfirmOnly, nil, true},
		{"confirm unsent", "confirm", PendingSendUnsent, pendingConfirmOnly, nil, false},
		{"confirm sent", "confirm", PendingSendSent, pendingSent, nil, false},
		{"confirm confirm", "confirm", PendingSendConfirmOnly, pendingConfirmOnly, nil, false},
		{"sent unsent", "sent", PendingSendUnsent, pendingSent, nil, false},
		{"sent sent", "sent", PendingSendSent, pendingSent, nil, false},
		{"sent confirm", "sent", PendingSendConfirmOnly, pendingSent, nil, false},
		{"claimed unsent", "claimed", PendingSendUnsent, pendingClaimed, ErrPendingClaimed, false},
		{"claimed sent", "claimed", PendingSendSent, pendingClaimed, ErrPendingClaimed, false},
		{"claimed confirm", "claimed", PendingSendConfirmOnly, pendingClaimed, ErrPendingClaimed, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set := pendingSetInState(t, taskID, authority, tc.initial)
			var before PendingEntry
			if tc.initial != "missing" {
				before = pendingOnlyEntry(t, set)
			}
			err := set.Register(taskID, tc.disposition, &authority)
			if !errors.Is(err, tc.wantErr) || (tc.wantErr == nil && err != nil) {
				t.Fatalf("Register() error = %v, want %v", err, tc.wantErr)
			}
			after := pendingOnlyEntry(t, set)
			if after.state != tc.want {
				t.Fatalf("state = %v, want %v", after.state, tc.want)
			}
			if tc.wantErr != nil {
				pendingRequireEntryEqual(t, before, after)
			}
			if tc.wantCleared && (after.authority != (ProcessSignalAuthority{}) || after.claimToken != 0) {
				t.Fatalf("confirm-only entry = %#v, want cleared authority and claim token", after)
			}
		})
	}
}

func TestPendingClaimForSend(t *testing.T) {
	missing := &PendingReconciliationSet{}
	if claim, outcome := missing.ClaimForSend(pendingTestTaskID(t, "missing"), ProcessSignalAuthority{}); claim != (SendClaim{}) || outcome != ClaimNotFound {
		t.Fatalf("ClaimForSend(missing) = (%#v, %v)", claim, outcome)
	}
	taskID := pendingTestTaskID(t, "claim")
	authority := pendingTestAuthority(taskID, 1)
	set := pendingSetWithUnsent(t, taskID, authority)
	claim, outcome := set.ClaimForSend(taskID, authority)
	if outcome != ClaimAcquired || claim.TaskID != taskID || claim.Token == 0 || !pendingTestAuthorityEqual(claim.Authority, authority) {
		t.Fatalf("ClaimForSend() = (%#v, %v)", claim, outcome)
	}
	if entry := pendingOnlyEntry(t, set); entry.state != pendingClaimed {
		t.Fatalf("state = %v, want claimed", entry.state)
	}
	for state, want := range map[string]ClaimOutcome{"sent": ClaimSent, "claimed": ClaimAlreadyClaimed, "confirm": ClaimConfirmOnly} {
		set := pendingSetInState(t, taskID, authority, state)
		if claim, outcome := set.ClaimForSend(taskID, authority); claim != (SendClaim{}) || outcome != want {
			t.Fatalf("ClaimForSend(%s) = (%#v, %v)", state, claim, outcome)
		}
	}
}

func TestPendingClaimForSendAuthorityMismatchBecomesConfirmOnly(t *testing.T) {
	taskID := pendingTestTaskID(t, "claim-mismatch-outcome")
	authority := pendingTestAuthority(taskID, 1)
	set := pendingSetWithUnsent(t, taskID, authority)
	if claim, outcome := set.ClaimForSend(taskID, pendingTestAuthority(taskID, 2)); claim != (SendClaim{}) || outcome != ClaimConfirmOnly {
		t.Fatalf("ClaimForSend() = (%#v, %v), want confirm-only", claim, outcome)
	}
	entry := pendingOnlyEntry(t, set)
	if entry.state != pendingConfirmOnly || entry.authority != (ProcessSignalAuthority{}) || entry.claimToken != 0 {
		t.Fatalf("entry = %#v, want cleared confirm-only entry", entry)
	}
}

func TestPendingRemoveClaimOwnershipAndTokenLifetime(t *testing.T) {
	taskID := pendingTestTaskID(t, "remove-claim")
	authority := pendingTestAuthority(taskID, 1)
	set := pendingSetWithUnsent(t, taskID, authority)
	claim, outcome := set.ClaimForSend(taskID, authority)
	if outcome != ClaimAcquired {
		t.Fatalf("ClaimForSend() outcome = %v", outcome)
	}
	for _, invalid := range []SendClaim{{}, {TaskID: pendingTestTaskID(t, "remove-claim-other"), Token: claim.Token}, {TaskID: taskID, Token: claim.Token + 1}} {
		if set.RemoveClaim(invalid) {
			t.Fatalf("RemoveClaim(%#v) = true", invalid)
		}
	}
	if !set.RemoveClaim(claim) {
		t.Fatal("RemoveClaim(valid claim) = false")
	}
	pendingRequireNoEntry(t, set, taskID)
	if err := set.Register(taskID, PendingSendUnsent, &authority); err != nil {
		t.Fatal(err)
	}
	next, nextOutcome := set.ClaimForSend(taskID, authority)
	if nextOutcome != ClaimAcquired || next.Token == claim.Token {
		t.Fatalf("reclaim = (%#v, %v), reused token", next, nextOutcome)
	}
}

func TestPendingClaimAuthorityIdentity(t *testing.T) {
	taskID := pendingTestTaskID(t, "identity")
	authority := pendingTestAuthority(taskID, 1)
	mismatches := []struct {
		name      string
		authority ProcessSignalAuthority
	}{
		{"task ID", pendingTestAuthority(pendingTestTaskID(t, "identity-other"), 1)},
		{"PID", pendingTestAuthority(taskID, 2)},
		{"process started at", ProcessSignalAuthority{TaskID: taskID, PID: authority.PID, ProcessStartedAt: authority.ProcessStartedAt.Add(time.Second)}},
	}
	for _, tc := range mismatches {
		t.Run("unsent mismatched "+tc.name+" becomes confirm-only", func(t *testing.T) {
			set := pendingSetWithUnsent(t, taskID, authority)
			if claim, outcome := set.ClaimForSend(taskID, tc.authority); claim != (SendClaim{}) || outcome != ClaimConfirmOnly {
				t.Fatalf("ClaimForSend(mismatch) = (%#v, %v)", claim, outcome)
			}
			entry := pendingOnlyEntry(t, set)
			if entry.state != pendingConfirmOnly || entry.authority != (ProcessSignalAuthority{}) || entry.claimToken != 0 {
				t.Fatalf("entry after mismatch = %#v", entry)
			}
			if claim, outcome := set.ClaimForSend(taskID, authority); claim != (SendClaim{}) || outcome != ClaimConfirmOnly {
				t.Fatalf("ClaimForSend(old authority) = (%#v, %v)", claim, outcome)
			}
		})
	}
	for _, state := range []string{"claimed", "sent", "confirm"} {
		t.Run(state+" mismatched authority is unchanged", func(t *testing.T) {
			set := pendingSetInState(t, taskID, authority, state)
			before := pendingOnlyEntry(t, set)
			if state == "claimed" && before.claimToken == 0 {
				t.Fatal("claimed entry token = 0")
			}
			if claim, outcome := set.ClaimForSend(taskID, mismatches[0].authority); claim != (SendClaim{}) || outcome == ClaimNotFound {
				t.Fatalf("ClaimForSend(mismatch) = (%#v, %v)", claim, outcome)
			}
			pendingRequireEntryEqual(t, before, pendingOnlyEntry(t, set))
		})
	}
	t.Run("released claim mismatch clears retained token", func(t *testing.T) {
		set := pendingSetWithUnsent(t, taskID, authority)
		claim, outcome := set.ClaimForSend(taskID, authority)
		if outcome != ClaimAcquired || claim.Token == 0 {
			t.Fatalf("ClaimForSend() = (%#v, %v)", claim, outcome)
		}
		if !set.ReleaseSend(claim) {
			t.Fatal("ReleaseSend() = false")
		}
		if entry := pendingOnlyEntry(t, set); entry.state != pendingUnsent || entry.claimToken == 0 {
			t.Fatalf("entry after release = %#v", entry)
		}
		if got, outcome := set.ClaimForSend(taskID, mismatches[0].authority); got != (SendClaim{}) || outcome != ClaimConfirmOnly {
			t.Fatalf("ClaimForSend(mismatch) = (%#v, %v)", got, outcome)
		}
		entry := pendingOnlyEntry(t, set)
		if entry.state != pendingConfirmOnly || entry.authority != (ProcessSignalAuthority{}) || entry.claimToken != 0 {
			t.Fatalf("entry after mismatch = %#v", entry)
		}
		if got, outcome := set.ClaimForSend(taskID, authority); got != (SendClaim{}) || outcome != ClaimConfirmOnly {
			t.Fatalf("ClaimForSend(old authority) = (%#v, %v)", got, outcome)
		}
	})
	parsed, err := time.Parse(time.RFC3339Nano, authority.ProcessStartedAt.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	equalButNotIdentical := authority
	equalButNotIdentical.ProcessStartedAt = parsed
	set := pendingSetWithUnsent(t, taskID, authority)
	if claim, outcome := set.ClaimForSend(taskID, equalButNotIdentical); outcome != ClaimAcquired || claim.Token == 0 {
		t.Fatalf("ClaimForSend(Equal time) = (%#v, %v)", claim, outcome)
	}
}

func TestPendingCompleteReleaseAndStaleClaims(t *testing.T) {
	taskID := pendingTestTaskID(t, "complete-release")
	authority := pendingTestAuthority(taskID, 1)
	set := pendingSetWithUnsent(t, taskID, authority)
	claim, _ := set.ClaimForSend(taskID, authority)
	if !set.ReleaseSend(claim) {
		t.Fatal("ReleaseSend() = false")
	}
	if entry := pendingOnlyEntry(t, set); entry.state != pendingUnsent || !pendingTestAuthorityEqual(entry.authority, authority) {
		t.Fatalf("entry after release = %#v", entry)
	}
	next, _ := set.ClaimForSend(taskID, authority)
	if next.Token == claim.Token {
		t.Fatal("reclaim reused token")
	}
	if set.CompleteSend(claim) || set.ReleaseSend(claim) {
		t.Fatal("stale claim succeeded")
	}
	if !set.CompleteSend(next) {
		t.Fatal("CompleteSend() = false")
	}
	if entry := pendingOnlyEntry(t, set); entry.state != pendingSent {
		t.Fatalf("state = %v", entry.state)
	}
	for _, invalid := range []SendClaim{{}, {TaskID: pendingTestTaskID(t, "wrong"), Token: next.Token}, {TaskID: taskID, Token: next.Token + 1}, next} {
		if set.CompleteSend(invalid) || set.ReleaseSend(invalid) {
			t.Fatalf("invalid claim %#v succeeded", invalid)
		}
	}
}

func TestPendingInvalidateSend(t *testing.T) {
	taskID := pendingTestTaskID(t, "invalidate")
	authority := pendingTestAuthority(taskID, 1)
	set := pendingSetWithUnsent(t, taskID, authority)
	claim, outcome := set.ClaimForSend(taskID, authority)
	if outcome != ClaimAcquired || claim.Token == 0 {
		t.Fatalf("ClaimForSend() = (%#v, %v)", claim, outcome)
	}
	claim.Authority = ProcessSignalAuthority{}
	if !set.InvalidateSend(claim) {
		t.Fatal("InvalidateSend() = false")
	}
	entry := pendingOnlyEntry(t, set)
	if entry.state != pendingConfirmOnly || entry.authority != (ProcessSignalAuthority{}) || entry.claimToken != 0 {
		t.Fatalf("entry after InvalidateSend() = %#v", entry)
	}
	if got, outcome := set.ClaimForSend(taskID, authority); got != (SendClaim{}) || outcome != ClaimConfirmOnly {
		t.Fatalf("ClaimForSend(old authority) = (%#v, %v)", got, outcome)
	}

	missing := &PendingReconciliationSet{}
	if missing.InvalidateSend(claim) {
		t.Fatal("InvalidateSend(missing) = true")
	}
	pendingRequireNoEntry(t, missing, taskID)

	for _, state := range []string{"unsent", "sent", "confirm"} {
		t.Run(state+" is unchanged", func(t *testing.T) {
			set := pendingSetInState(t, taskID, authority, state)
			before := pendingOnlyEntry(t, set)
			if set.InvalidateSend(claim) {
				t.Fatal("InvalidateSend() = true")
			}
			pendingRequireEntryEqual(t, before, pendingOnlyEntry(t, set))
		})
	}

	set = pendingSetWithUnsent(t, taskID, authority)
	claim, _ = set.ClaimForSend(taskID, authority)
	for _, invalid := range []SendClaim{
		{TaskID: taskID, Token: claim.Token + 1},
		{TaskID: pendingTestTaskID(t, "invalidate-wrong-task"), Token: claim.Token},
	} {
		before := pendingOnlyEntry(t, set)
		if set.InvalidateSend(invalid) {
			t.Fatalf("InvalidateSend(%#v) = true", invalid)
		}
		pendingRequireEntryEqual(t, before, pendingOnlyEntry(t, set))
	}
}

func TestPendingInvalidateSendRejectsMismatchedEntryTaskID(t *testing.T) {
	taskID := pendingTestTaskID(t, "invalidate-entry-task")
	authority := pendingTestAuthority(taskID, 1)
	set := pendingSetWithUnsent(t, taskID, authority)
	claim, _ := set.ClaimForSend(taskID, authority)
	entry := pendingOnlyEntry(t, set)
	entry.taskID = pendingTestTaskID(t, "invalidate-entry-task-other")
	set.entries[taskID] = entry
	before := pendingOnlyEntry(t, set)
	if set.InvalidateSend(claim) {
		t.Fatal("InvalidateSend() = true")
	}
	pendingRequireEntryEqual(t, before, pendingOnlyEntry(t, set))
}

func TestPendingRemoveOwnershipAndTokenLifetime(t *testing.T) {
	taskID := pendingTestTaskID(t, "remove")
	authority := pendingTestAuthority(taskID, 1)
	for _, state := range []string{"unsent", "sent", "confirm"} {
		set := pendingSetInState(t, taskID, authority, state)
		set.Remove(taskID)
		set.Remove(taskID)
		pendingRequireNoEntry(t, set, taskID)
	}
	set := pendingSetWithUnsent(t, taskID, authority)
	claim, _ := set.ClaimForSend(taskID, authority)
	before := pendingOnlyEntry(t, set)
	set.Remove(taskID)
	pendingRequireEntryEqual(t, before, pendingOnlyEntry(t, set))
	if !set.ReleaseSend(claim) {
		t.Fatal("ReleaseSend after Remove = false")
	}
	set.Remove(taskID)
	pendingRequireNoEntry(t, set, taskID)
	if err := set.Register(taskID, PendingSendUnsent, &authority); err != nil {
		t.Fatal(err)
	}
	next, _ := set.ClaimForSend(taskID, authority)
	if next.Token == claim.Token {
		t.Fatal("token reused after Remove")
	}

	set = pendingSetWithUnsent(t, taskID, authority)
	claim, _ = set.ClaimForSend(taskID, authority)
	set.Remove(taskID)
	if !set.CompleteSend(claim) {
		t.Fatal("CompleteSend after Remove = false")
	}
}

func TestPendingAuthorityCopyAndNonUnsentDiscard(t *testing.T) {
	taskID := pendingTestTaskID(t, "authority-copy")
	authority := pendingTestAuthority(taskID, 1)
	set := pendingSetWithUnsent(t, taskID, authority)
	authority.PID = 99
	authority.ProcessStartedAt = authority.ProcessStartedAt.Add(time.Hour)
	entry := pendingOnlyEntry(t, set)
	if entry.authority.PID == authority.PID {
		t.Fatal("Register retained authority pointer")
	}
	if claim, _ := set.ClaimForSend(taskID, entry.authority); claim.Token == 0 {
		t.Fatal("stored authority did not claim")
	}

	for _, initial := range []string{"missing", "unsent", "confirm"} {
		set := pendingSetInState(t, taskID, pendingTestAuthority(taskID, 2), initial)
		extra := pendingTestAuthority(pendingTestTaskID(t, "ignored"), 3)
		if err := set.Register(taskID, PendingSendSent, &extra); err != nil {
			t.Fatal(err)
		}
		if entry := pendingOnlyEntry(t, set); entry.state != pendingSent || entry.authority != (ProcessSignalAuthority{}) {
			t.Fatalf("sent entry = %#v", entry)
		}
	}
	for _, initial := range []string{"missing", "confirm"} {
		set := pendingSetInState(t, taskID, pendingTestAuthority(taskID, 2), initial)
		extra := pendingTestAuthority(pendingTestTaskID(t, "ignored-confirm"), 3)
		if err := set.Register(taskID, PendingSendConfirmOnly, &extra); err != nil {
			t.Fatal(err)
		}
		if entry := pendingOnlyEntry(t, set); entry.state != pendingConfirmOnly || entry.authority != (ProcessSignalAuthority{}) {
			t.Fatalf("confirm entry = %#v", entry)
		}
	}
}

func TestPendingConcurrentClaimsAndOperations(t *testing.T) {
	const workers = 32
	sharedID := pendingTestTaskID(t, "one-winner")
	sharedAuthority := pendingTestAuthority(sharedID, 1)
	sharedSet := pendingSetWithUnsent(t, sharedID, sharedAuthority)
	start := make(chan struct{})
	sharedClaims := make(chan SendClaim, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claim, _ := sharedSet.ClaimForSend(sharedID, sharedAuthority)
			sharedClaims <- claim
		}()
	}
	close(start)
	wg.Wait()
	close(sharedClaims)
	winners := 0
	for claim := range sharedClaims {
		if claim.Token != 0 {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("same-task claim winners = %d", winners)
	}

	set := &PendingReconciliationSet{}
	ids := make([]domain.TaskID, workers)
	authorities := make([]ProcessSignalAuthority, workers)
	for i := range workers {
		ids[i] = pendingTestTaskID(t, fmt.Sprintf("parallel-%02d", i))
		authorities[i] = pendingTestAuthority(ids[i], i+1)
		if err := set.Register(ids[i], PendingSendUnsent, &authorities[i]); err != nil {
			t.Fatal(err)
		}
	}
	start = make(chan struct{})
	claims := make(chan SendClaim, workers)
	wg = sync.WaitGroup{}
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			claim, outcome := set.ClaimForSend(ids[i], authorities[i])
			if outcome != ClaimAcquired || claim.Token == 0 {
				t.Errorf("claim %d = (%#v, %v)", i, claim, outcome)
				return
			}
			claims <- claim
		}(i)
	}
	close(start)
	wg.Wait()
	close(claims)
	seen := map[uint64]bool{}
	for claim := range claims {
		if seen[claim.Token] {
			t.Fatalf("duplicate token %d", claim.Token)
		}
		seen[claim.Token] = true
	}
	if len(seen) != workers {
		t.Fatalf("unique tokens = %d", len(seen))
	}

	set = pendingSetWithUnsent(t, ids[0], authorities[0])
	claim, _ := set.ClaimForSend(ids[0], authorities[0])
	start = make(chan struct{})
	results := make(chan bool, 3)
	wg = sync.WaitGroup{}
	for _, action := range []func(SendClaim) bool{set.CompleteSend, set.ReleaseSend, set.InvalidateSend} {
		wg.Add(1)
		go func(action func(SendClaim) bool) { defer wg.Done(); <-start; results <- action(claim) }(action)
	}
	close(start)
	wg.Wait()
	close(results)
	wins := 0
	for result := range results {
		if result {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("complete/release/invalidate winners = %d", wins)
	}
}

func TestPendingListSnapshotAndMixedConcurrentOperations(t *testing.T) {
	taskID := pendingTestTaskID(t, "snapshot")
	authority := pendingTestAuthority(taskID, 1)
	set := pendingSetWithUnsent(t, taskID, authority)
	snapshot := set.List()
	snapshot[0].authority.PID = 99
	snapshot[0].claimToken = 99
	if entry := pendingOnlyEntry(t, set); entry.authority.PID == 99 || entry.claimToken == 99 {
		t.Fatal("List leaked mutable internal entry")
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := range 32 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			switch worker % 7 {
			case 0:
				_ = set.Register(taskID, PendingSendUnsent, &authority)
			case 1:
				_ = set.List()
			case 2:
				set.Remove(taskID)
			case 3:
				claim, _ := set.ClaimForSend(taskID, authority)
				if claim.Token != 0 {
					set.CompleteSend(claim)
				}
			case 4:
				claim, _ := set.ClaimForSend(taskID, authority)
				if claim.Token != 0 {
					set.ReleaseSend(claim)
				}
			case 5:
				_ = set.Register(taskID, PendingSendConfirmOnly, nil)
			case 6:
				claim, _ := set.ClaimForSend(taskID, authority)
				if claim.Token != 0 {
					set.InvalidateSend(claim)
				}
			}
		}(worker)
	}
	close(start)
	wg.Wait()
}

func pendingSetWithUnsent(t *testing.T, taskID domain.TaskID, authority ProcessSignalAuthority) *PendingReconciliationSet {
	t.Helper()
	set := &PendingReconciliationSet{}
	if err := set.Register(taskID, PendingSendUnsent, &authority); err != nil {
		t.Fatal(err)
	}
	return set
}
func pendingSetInState(t *testing.T, taskID domain.TaskID, authority ProcessSignalAuthority, state string) *PendingReconciliationSet {
	t.Helper()
	set := &PendingReconciliationSet{}
	switch state {
	case "missing":
	case "unsent":
		if err := set.Register(taskID, PendingSendUnsent, &authority); err != nil {
			t.Fatal(err)
		}
	case "sent":
		if err := set.Register(taskID, PendingSendSent, nil); err != nil {
			t.Fatal(err)
		}
	case "confirm":
		if err := set.Register(taskID, PendingSendConfirmOnly, nil); err != nil {
			t.Fatal(err)
		}
	case "claimed":
		if err := set.Register(taskID, PendingSendUnsent, &authority); err != nil {
			t.Fatal(err)
		}
		if claim, outcome := set.ClaimForSend(taskID, authority); outcome != ClaimAcquired || claim.Token == 0 {
			t.Fatal("could not create claimed state")
		}
	default:
		t.Fatalf("unknown state %q", state)
	}
	return set
}
func pendingOnlyEntry(t *testing.T, set *PendingReconciliationSet) PendingEntry {
	t.Helper()
	entries := set.List()
	if len(entries) != 1 {
		t.Fatalf("entries = %#v, want one", entries)
	}
	return entries[0]
}
func pendingRequireNoEntry(t *testing.T, set *PendingReconciliationSet, taskID domain.TaskID) {
	t.Helper()
	for _, entry := range set.List() {
		if entry.taskID == taskID {
			t.Fatalf("unexpected entry %#v", entry)
		}
	}
}
func pendingRequireEntryEqual(t *testing.T, want, got PendingEntry) {
	t.Helper()
	if want != got {
		t.Fatalf("entry = %#v, want %#v", got, want)
	}
}
func pendingTestAuthority(taskID domain.TaskID, pid int) ProcessSignalAuthority {
	return ProcessSignalAuthority{TaskID: taskID, PID: pid, ProcessStartedAt: time.Date(2026, time.August, 10, 12, 0, pid, 0, time.Local)}
}
func pendingTestAuthorityEqual(a, b ProcessSignalAuthority) bool {
	return a.TaskID == b.TaskID && a.PID == b.PID && a.ProcessStartedAt.Equal(b.ProcessStartedAt)
}
func ptr(value ProcessSignalAuthority) *ProcessSignalAuthority { return &value }
func pendingTestTaskID(t *testing.T, slug string) domain.TaskID {
	t.Helper()
	taskID, err := domain.NewTaskID("impl-20260810-120000-abcd-pending-" + slug)
	if err != nil {
		t.Fatalf("NewTaskID() error = %v", err)
	}
	return taskID
}
