// Package detect classifies a reconstructed supervisor screen and structured observations.
package detect

import (
	"context"
	"time"

	supervisor "github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// QuietPeriod is copied from 10-shared/validation-rules.md (SUPERVISOR_IDLE_QUIET_SECONDS, PT-09).
const QuietPeriod = 5 * time.Second

type DetectedState string

const (
	StateIdle                   DetectedState = "idle"
	StateWorking                DetectedState = "working"
	StateAwaitingPermission     DetectedState = "awaiting_permission"
	StateAwaitingRecommendation DetectedState = "awaiting_recommendation"
	StateExited                 DetectedState = "exited"
	StateUndetermined           DetectedState = "undetermined"
)

type DetectionBasis = supervisor.DetectionBasis

const (
	BasisScreenPattern  DetectionBasis = supervisor.DetectionScreenPattern
	BasisOutputQuiet    DetectionBasis = supervisor.DetectionOutputQuiet
	BasisProcessExit    DetectionBasis = supervisor.DetectionProcessExit
	BasisStructuredInfo DetectionBasis = supervisor.DetectionStructuredInfo
)

type PermissionMode string

const (
	PermissionDefaultOrUnknown PermissionMode = "default_or_unknown"
	PermissionUnsafe           PermissionMode = "unsafe"
)

// StructuredInfo carries the accepted statusline observation.
type StructuredInfo struct {
	ObservedAt     time.Time
	UsedPercentage int
	SessionID      string
}

// DetectInput contains one observation of a managed CLI session. Percentage is valid only in [0,100].
type DetectInput struct {
	SessionID           supervisor.SessionID
	CLI                 supervisor.SupervisorCli
	ScreenRows          []string
	ScreenDim           [][]bool
	ScreenCells         [][]supervisor.Cell
	LastScreenChangeAt  time.Time
	ProcessExited       bool
	ExitCode            *int
	Structured          *StructuredInfo
	LastSendConfirmedAt *time.Time
	Now                 time.Time
	Patterns            *DetectorPatternSet
	PatternsByCLI       map[supervisor.SupervisorCli]DetectorPatternSet
}

// DetectResult contains a state and only the evidence used by its winning rule.
type DetectResult struct {
	State               DetectedState
	Basis               []DetectionBasis
	ProcessingIndicator bool
	ContextPercent      *int
	SessionID           string
	PermissionMode      PermissionMode
}

// Detector classifies one session observation.
type Detector interface {
	Detect(context.Context, DetectInput) (DetectResult, error)
}

func addBasis(dst []DetectionBasis, b DetectionBasis) []DetectionBasis {
	for _, v := range dst {
		if v == b {
			return dst
		}
	}
	return append(dst, b)
}
