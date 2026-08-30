package execution

import (
	"context"
	"errors"
	"io/fs"
	"time"
)

var worktreeEvictionTickerFactory logTickerFactory = realLogTickerFactory{}

// Run periodically triggers automatic worktree eviction until ctx is cancelled.
func (uc *EvictWorkDirUseCase) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		return
	}
	ticker := worktreeEvictionTickerFactory.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case at := <-ticker.C():
			if ctx.Err() != nil {
				return
			}
			_, err := uc.Execute(ctx, EvictWorkDirInput{
				Trigger:    TriggerAutomatic,
				Force:      false,
				MaxAgeDays: 0,
				OccurredAt: at,
			}, nil)
			if err != nil && !errors.Is(err, fs.ErrNotExist) {
				uc.logger.Warn("worktree eviction scan failed", "error", err)
			}
		}
	}
}
