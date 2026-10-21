package usecase

import "errors"

var (
	ErrProjectNotFound          = errors.New("SUP_PROJECT_NOT_FOUND")
	ErrSessionNotFound          = errors.New("SUP_SESSION_NOT_FOUND")
	ErrScreenUnavailable        = errors.New("SUP_SCREEN_UNAVAILABLE")
	ErrSessionAlreadyRunning    = errors.New("SUP_SESSION_ALREADY_RUNNING")
	ErrSessionAlreadyExists     = ErrSessionAlreadyRunning
	ErrSessionLimitReached      = errors.New("SUP_SESSION_LIMIT_REACHED")
	ErrProjectCWDUnavailable    = errors.New("SUP_PROJECT_CWD_UNAVAILABLE")
	ErrProjectInvalidCWD        = ErrProjectCWDUnavailable
	ErrSpawnFailed              = errors.New("SUP_SPAWN_FAILED")
	ErrSessionIDProjectMismatch = errors.New("SUP_REQUEST_MALFORMED")
	ErrBillingGuardFailed       = errors.New("SUP_BILLING_GUARD_FAILED")
	ErrLaunchArgvInvalid        = ErrSpawnFailed
	ErrMaxSessionsInvalid       = errors.New("SUP_CONFIG_INVALID")
	ErrPTYSizeInvalid           = errors.New("SUP_CONFIG_INVALID pty_size")
)
