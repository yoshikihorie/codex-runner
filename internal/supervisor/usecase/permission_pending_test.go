package usecase

import (
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

func TestPermissionPending_SCN13_SCN14_EntryReminderAndReentry(t *testing.T) {
	now := time.Unix(1000, 0)
	var got []supervisor.SupervisorEventKind
	u := PermissionPendingUseCase{Record: func(_ supervisor.SessionID, kind supervisor.SupervisorEventKind) error {
		got = append(got, kind)
		return nil
	}}
	id := supervisor.SessionID("session")
	for _, at := range []time.Time{now, now.Add(time.Minute), now.Add(permissionRemindInterval - time.Second), now.Add(permissionRemindInterval), now.Add(permissionRemindInterval + time.Minute)} {
		if err := u.Observe(id, supervisor.SessionAwaitingPermission, now, at); err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 2 {
		t.Fatalf("notifications=%d", len(got))
	}
	if err := u.Observe(id, supervisor.SessionWorking, now, now.Add(2*permissionRemindInterval)); err != nil {
		t.Fatal(err)
	}
	if err := u.Observe(id, supervisor.SessionAwaitingPermission, now.Add(3*permissionRemindInterval), now.Add(3*permissionRemindInterval)); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("reentry notifications=%d", len(got))
	}
	for _, kind := range got {
		if kind != supervisor.EventKindPermissionPending {
			t.Fatalf("kind=%s", kind)
		}
	}
}
