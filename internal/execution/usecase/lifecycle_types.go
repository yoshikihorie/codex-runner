package usecase

import (
	"time"

	"github.com/yoshikihorie/codex-runner/internal/execution"
)

type TaskLifecycleInput struct {
	execution.TaskLaunchPayload
	TaskDirPath string
	Now         time.Time
}
