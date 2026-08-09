package recovery

import (
	"context"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type SlotReleaser interface {
	ReleaseAndAdvance(ctx context.Context, taskID domain.TaskID, now time.Time)
}

type SlotResetter interface {
	Reset(taskIDs []domain.TaskID)
}
