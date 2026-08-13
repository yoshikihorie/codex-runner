package transport

import "github.com/yoshikihorie/codex-runner/internal/domain"

// TaskSnapshotProvider supplies the authoritative status snapshot for a task.
type TaskSnapshotProvider interface {
	Snapshot(taskID domain.TaskID) (domain.TaskSnapshot, error)
	QueuePosition(taskID domain.TaskID) (position int, found bool, err error)
}
