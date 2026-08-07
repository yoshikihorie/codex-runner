package domain

import "errors"

var (
	ErrInvalidStateTransition = errors.New("invalid task state transition")
	ErrTaskAlreadyTerminal    = errors.New("task is already terminal")
	ErrSessionNotResumable    = errors.New("session is not resumable")
)
