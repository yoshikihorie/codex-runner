package usecase

import (
	"sync"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

// Canonical value: SUPERVISOR_PERMISSION_REMIND_SECONDS in validation-rules.md.
const permissionRemindInterval = 900 * time.Second

type permissionVisit struct {
	since time.Time
	last  time.Time
}

// PermissionPendingUseCase records entry and periodic reminders; it has no
// input sender, so it cannot approve or reject a dialog.
type PermissionPendingUseCase struct {
	Record   func(supervisor.SessionID, supervisor.SupervisorEventKind) error
	Notify   func(supervisor.SupervisorEventKind, string)
	bindOnce sync.Once
	mu       sync.Mutex
	visits   map[supervisor.SessionID]permissionVisit
}

func (u *PermissionPendingUseCase) Bind(record func(supervisor.SessionID, supervisor.SupervisorEventKind) error, notify func(supervisor.SupervisorEventKind, string)) {
	u.bindOnce.Do(func() {
		if u.Record == nil {
			u.Record = record
		}
		if u.Notify == nil {
			u.Notify = notify
		}
	})
}

func (u *PermissionPendingUseCase) Observe(id supervisor.SessionID, state supervisor.SupervisorSessionState, since, now time.Time) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if state != supervisor.SessionAwaitingPermission {
		delete(u.visits, id)
		return nil
	}
	if u.visits == nil {
		u.visits = make(map[supervisor.SessionID]permissionVisit)
	}
	visit, exists := u.visits[id]
	if exists && visit.since.Equal(since) && now.Sub(visit.last) < permissionRemindInterval {
		return nil
	}
	if u.Record != nil {
		if err := u.Record(id, supervisor.EventKindPermissionPending); err != nil {
			return err
		}
	}
	u.visits[id] = permissionVisit{since: since, last: now}
	if u.Notify != nil {
		u.Notify(supervisor.EventKindPermissionPending, id.String())
	}
	return nil
}
