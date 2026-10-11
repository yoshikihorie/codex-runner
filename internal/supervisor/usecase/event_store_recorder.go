package usecase

import (
	"fmt"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type SupervisorOperationalLogger interface {
	Record(store.SupervisorLogEntry) error
}

// EventStoreRecorder connects usecase event writes to the durable EventStore and reports failures to the operational log.
type EventStoreRecorder struct {
	Events *store.EventStore
	Log    SupervisorOperationalLogger
}

func (r EventStoreRecorder) RecordEvent(id supervisor.SessionID, kind supervisor.SupervisorEventKind, reason supervisor.SupervisorStopReason, exit supervisor.SupervisorExitReason, rotate supervisor.SupervisorRotateFailReason, at time.Time) {
	if err := r.RecordEventChecked(id, kind, reason, exit, rotate, at); err != nil {
		r.warn(kind, err)
	}
}
func (r EventStoreRecorder) RecordEventChecked(id supervisor.SessionID, kind supervisor.SupervisorEventKind, reason supervisor.SupervisorStopReason, exit supervisor.SupervisorExitReason, rotate supervisor.SupervisorRotateFailReason, at time.Time) error {
	event := store.Event{Kind: kind, At: at}
	if _, err := supervisor.NewSessionID(id.String()); err == nil {
		event.SessionID = id.String()
	}
	if reason != "" && exit != "" || reason != "" && rotate != "" || exit != "" && rotate != "" {
		return fmt.Errorf("multiple event reasons supplied")
	}
	if reason != "" {
		event.Reason = store.EventReason(reason)
	} else if exit != "" {
		event.Reason = store.EventReason(exit)
	} else if rotate != "" {
		event.Reason = store.EventReason(rotate)
	}
	if r.Events == nil {
		return fmt.Errorf("event store is unavailable")
	}
	_, err := r.Events.Record(event)
	return err
}

type EventStoreNotifyRecorder struct{ Recorder EventStoreRecorder }

func (r EventStoreNotifyRecorder) RecordEvent(e EventRecord) {
	id, _ := supervisor.NewSessionID(e.SessionID)
	if err := r.Recorder.RecordEventChecked(id, e.Kind, "", "", "", time.Now()); err != nil {
		r.Recorder.warn(e.Kind, err)
	}
}
func (r EventStoreRecorder) warn(kind supervisor.SupervisorEventKind, err error) {
	if r.Log != nil {
		_ = r.Log.Record(store.SupervisorLogEntry{Level: store.LogLevelWarn, Kind: kind, Reason: store.LogReasonEventWriteFailed})
	}
}
func (r EventStoreRecorder) LogEventFailure(kind supervisor.SupervisorEventKind, err error) {
	r.warn(kind, err)
}
