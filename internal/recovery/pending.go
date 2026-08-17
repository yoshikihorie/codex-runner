package recovery

import (
	"errors"
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

var ErrPendingClaimed = errors.New("recovery: pending entry is claimed for send")

type ProcessSignalAuthority struct {
	TaskID           domain.TaskID
	PID              int
	ProcessStartedAt time.Time
}

type SendClaim struct {
	TaskID    domain.TaskID
	Token     uint64
	Authority ProcessSignalAuthority
}

type PendingSendDisposition int

const (
	PendingSendUnsent PendingSendDisposition = iota
	PendingSendSent
	PendingSendConfirmOnly
)

type ClaimOutcome uint8

const (
	ClaimNotFound ClaimOutcome = iota
	ClaimAcquired
	ClaimAlreadyClaimed
	ClaimSent
	ClaimConfirmOnly
)

type pendingState int

const (
	pendingUnsent pendingState = iota
	pendingClaimed
	pendingSent
	pendingConfirmOnly
)

type PendingEntry struct {
	taskID     domain.TaskID
	state      pendingState
	authority  ProcessSignalAuthority
	claimToken uint64
}

type PendingRegistrar interface {
	Register(taskID domain.TaskID, disposition PendingSendDisposition, authority *ProcessSignalAuthority) error
	ClaimForSend(taskID domain.TaskID, authority ProcessSignalAuthority) (claim SendClaim, outcome ClaimOutcome)
	CompleteSend(claim SendClaim) bool
	ReleaseSend(claim SendClaim) bool
	InvalidateSend(claim SendClaim) bool
	RemoveClaim(claim SendClaim) bool
}

type PendingReconciliationSet struct {
	mu        sync.Mutex
	entries   map[domain.TaskID]PendingEntry
	nextToken uint64
}

var _ PendingRegistrar = (*PendingReconciliationSet)(nil)

func (s *PendingReconciliationSet) Register(taskID domain.TaskID, disposition PendingSendDisposition, authority *ProcessSignalAuthority) error {
	if err := validatePendingRegistration(taskID, disposition, authority); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.entries[taskID]
	if found && entry.state == pendingClaimed {
		return ErrPendingClaimed
	}
	if !found {
		if s.entries == nil {
			s.entries = make(map[domain.TaskID]PendingEntry)
		}
		s.entries[taskID] = pendingEntryForRegistration(taskID, disposition, authority)
		return nil
	}

	switch entry.state {
	case pendingSent:
		return nil
	case pendingUnsent:
		switch disposition {
		case PendingSendUnsent:
			entry.authority = *authority
		case PendingSendSent:
			entry.state = pendingSent
			entry.authority = ProcessSignalAuthority{}
			entry.claimToken = 0
		case PendingSendConfirmOnly:
			entry.state = pendingConfirmOnly
			entry.authority = ProcessSignalAuthority{}
			entry.claimToken = 0
		}
	case pendingConfirmOnly:
		switch disposition {
		case PendingSendSent:
			entry.state = pendingSent
		}
	}
	// Only the current claim owner may record a successful send via CompleteSend.
	s.entries[taskID] = entry
	return nil
}

func (s *PendingReconciliationSet) ClaimForSend(taskID domain.TaskID, authority ProcessSignalAuthority) (claim SendClaim, outcome ClaimOutcome) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.entries[taskID]
	if !found {
		return SendClaim{}, ClaimNotFound
	}
	switch entry.state {
	case pendingClaimed:
		return SendClaim{}, ClaimAlreadyClaimed
	case pendingSent:
		return SendClaim{}, ClaimSent
	case pendingConfirmOnly:
		return SendClaim{}, ClaimConfirmOnly
	}
	if !pendingAuthorityEqual(entry.authority, authority) {
		// Authority invalidation detected here fails closed by moving unsent to confirm-only, preventing future sends.
		entry.state = pendingConfirmOnly
		entry.authority = ProcessSignalAuthority{}
		entry.claimToken = 0
		s.entries[taskID] = entry
		return SendClaim{}, ClaimConfirmOnly
	}
	// A set instance cannot realistically issue 2^64 claims during its lifetime.
	s.nextToken++
	entry.claimToken = s.nextToken
	entry.state = pendingClaimed
	s.entries[taskID] = entry
	return SendClaim{TaskID: taskID, Token: entry.claimToken, Authority: entry.authority}, ClaimAcquired
}

func (s *PendingReconciliationSet) CompleteSend(claim SendClaim) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.entries[claim.TaskID]
	if !found || entry.state != pendingClaimed || entry.claimToken != claim.Token {
		return false
	}
	entry.state = pendingSent
	entry.authority = ProcessSignalAuthority{}
	s.entries[claim.TaskID] = entry
	return true
}

func (s *PendingReconciliationSet) ReleaseSend(claim SendClaim) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.entries[claim.TaskID]
	if !found || entry.state != pendingClaimed || entry.claimToken != claim.Token {
		return false
	}
	entry.state = pendingUnsent
	s.entries[claim.TaskID] = entry
	return true
}

func (s *PendingReconciliationSet) InvalidateSend(claim SendClaim) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.entries[claim.TaskID]
	if !found || entry.state != pendingClaimed || entry.claimToken != claim.Token || entry.taskID != claim.TaskID {
		return false
	}
	entry.state = pendingConfirmOnly
	entry.authority = ProcessSignalAuthority{}
	entry.claimToken = 0
	s.entries[claim.TaskID] = entry
	return true
}

func (s *PendingReconciliationSet) RemoveClaim(claim SendClaim) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, found := s.entries[claim.TaskID]
	if !found || entry.state != pendingClaimed || entry.taskID != claim.TaskID || entry.claimToken != claim.Token {
		return false
	}
	delete(s.entries, claim.TaskID)
	return true
}

func (s *PendingReconciliationSet) Remove(taskID domain.TaskID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, found := s.entries[taskID]; found && entry.state == pendingClaimed {
		// A claimed entry remains until its claim owner completes or releases it.
		return
	}
	delete(s.entries, taskID)
}

func (s *PendingReconciliationSet) List() []PendingEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries := make([]PendingEntry, 0, len(s.entries))
	for _, entry := range s.entries {
		entries = append(entries, entry)
	}
	return entries
}

func validatePendingRegistration(taskID domain.TaskID, disposition PendingSendDisposition, authority *ProcessSignalAuthority) error {
	if disposition < PendingSendUnsent || disposition > PendingSendConfirmOnly {
		return errors.New("recovery: invalid pending send disposition")
	}
	if disposition == PendingSendUnsent {
		if authority == nil {
			return errors.New("recovery: unsent pending entry requires process signal authority")
		}
		if authority.TaskID != taskID {
			return errors.New("recovery: process signal authority task ID does not match pending entry")
		}
		if authority.PID <= 0 {
			return errors.New("recovery: process signal authority requires a positive PID")
		}
		if authority.ProcessStartedAt.IsZero() {
			return errors.New("recovery: process signal authority requires a non-zero process start time")
		}
	}
	return nil
}

func pendingEntryForRegistration(taskID domain.TaskID, disposition PendingSendDisposition, authority *ProcessSignalAuthority) PendingEntry {
	entry := PendingEntry{taskID: taskID}
	switch disposition {
	case PendingSendUnsent:
		entry.state = pendingUnsent
		entry.authority = *authority
	case PendingSendSent:
		entry.state = pendingSent
	case PendingSendConfirmOnly:
		entry.state = pendingConfirmOnly
	}
	return entry
}

func pendingAuthorityEqual(left, right ProcessSignalAuthority) bool {
	return left.TaskID == right.TaskID && left.PID == right.PID && left.ProcessStartedAt.Equal(right.ProcessStartedAt)
}
