package recovery

import (
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

// RecoverViaResumeInput は resume による最終回答の回収に必要な入力を表す。
type RecoverViaResumeInput struct {
	TaskID     domain.TaskID
	SessionRef *domain.SessionRef
	Origin     domain.RecoveryOrigin
	OccurredAt time.Time
}

// RecoverViaResumeOutput は resume による最終回答の回収結果を表す。
type RecoverViaResumeOutput struct {
	Succeeded          bool
	ExitCode           domain.ExitCode
	PartialOutputSaved bool
	FinalState         domain.TaskState
}
