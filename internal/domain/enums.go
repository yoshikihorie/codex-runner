package domain

type TaskState string

const (
	StateQueued      TaskState = "queued"
	StateStarting    TaskState = "starting"
	StateRunning     TaskState = "running"
	StateStalled     TaskState = "stalled"
	StateCompleted   TaskState = "completed"
	StateFailed      TaskState = "failed"
	StateTimeout     TaskState = "timeout"
	StateRecovering  TaskState = "recovering"
	StateRecovered   TaskState = "recovered"
	StateTimeoutLost TaskState = "timeout-lost"
	StateCancelling  TaskState = "cancelling"
	StateKilled      TaskState = "killed"
	StateAdopted     TaskState = "adopted"
	StateOrphaned    TaskState = "orphaned"
	StateLost        TaskState = "lost"
)

type ProtocolVerb string

const (
	ProtocolVerbSubmit ProtocolVerb = "submit"
	ProtocolVerbStatus ProtocolVerb = "status"
	ProtocolVerbCancel ProtocolVerb = "cancel"
	ProtocolVerbTail   ProtocolVerb = "tail"
	ProtocolVerbPing   ProtocolVerb = "ping"
)

type Subcommand string

const (
	SubcommandImpl     Subcommand = "impl"
	SubcommandReview   Subcommand = "review"
	SubcommandPlan     Subcommand = "plan"
	SubcommandResearch Subcommand = "research"
	SubcommandRead     Subcommand = "read"
	SubcommandStatus   Subcommand = "status"
	SubcommandLogs     Subcommand = "logs"
	SubcommandCancel   Subcommand = "cancel"
	SubcommandDoctor   Subcommand = "doctor"
	SubcommandCleanup  Subcommand = "cleanup"
	SubcommandStats    Subcommand = "stats"
)

type ExecutionRoute string

const (
	ExecutionRouteDaemon ExecutionRoute = "daemon"
	ExecutionRouteLegacy ExecutionRoute = "legacy"
)

type ExecutionRouteReason string

const (
	ExecutionRouteReasonNone              ExecutionRouteReason = "none"
	ExecutionRouteReasonConnectRefused    ExecutionRouteReason = "connect_refused"
	ExecutionRouteReasonConnectTimeout    ExecutionRouteReason = "connect_timeout"
	ExecutionRouteReasonPingTimeout       ExecutionRouteReason = "ping_timeout"
	ExecutionRouteReasonVersionUnknown    ExecutionRouteReason = "version_unknown"
	ExecutionRouteReasonStageDisabled     ExecutionRouteReason = "stage_disabled"
	ExecutionRouteReasonClientUnavailable ExecutionRouteReason = "client_unavailable"
)

type ExitCodeClass string

const (
	ExitCodeClassSuccess   ExitCodeClass = "success"
	ExitCodeClassFailure   ExitCodeClass = "failure"
	ExitCodeClassTimeout   ExitCodeClass = "timeout"
	ExitCodeClassCancelled ExitCodeClass = "cancelled"
)

type RecoveryOrigin string

const (
	RecoveryOriginTimeout RecoveryOrigin = "timeout"
	RecoveryOriginOrphan  RecoveryOrigin = "orphan"
)
const (
	ReasonNoOutput     = "no-output"
	ReasonAbnormalExit = "abnormal-exit-code"
)

func IsSubmittable(s Subcommand) bool {
	return s == SubcommandImpl || s == SubcommandReview || s == SubcommandPlan || s == SubcommandResearch || s == SubcommandRead
}
