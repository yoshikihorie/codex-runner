package domain

import "errors"

var (
	ErrInvalidStateTransition = errors.New("invalid task state transition")
	ErrTaskAlreadyTerminal    = errors.New("task is already terminal")
	ErrSessionNotResumable    = errors.New("session is not resumable")
	ErrTaskNotFound           = errors.New("task not found")
	ErrPathLockConflict       = errors.New("path lock conflict")
	ErrContractWriteFailed    = errors.New("contract write failed")
)
