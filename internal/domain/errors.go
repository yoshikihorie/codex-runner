package domain

import "errors"

var (
	ErrInvalidStateTransition   = errors.New("invalid task state transition")
	ErrTaskAlreadyTerminal      = errors.New("task is already terminal")
	ErrSessionNotResumable      = errors.New("session is not resumable")
	ErrTaskNotFound             = errors.New("task not found")
	ErrPathLockConflict         = errors.New("path lock conflict")
	ErrQueueFull                = errors.New("queue full")
	ErrContractWriteFailed      = errors.New("contract write failed")
	ErrPTYAllocationFailed      = errors.New("PTY allocation failed")
	ErrChildProcessLaunchFailed = errors.New("child process launch failed")
)
