package usecase

import (
	"bytes"
	"encoding/json"
	"reflect"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type SessionRecordStore interface {
	Load(supervisor.SessionID) (supervisor.ManagedSession, error)
	Save(supervisor.ManagedSession) error
}

// timeoutKillGrace is copied from specifications/10-shared/validation-rules.md (TIMEOUT_KILL_GRACE_SECONDS).
const timeoutKillGrace = 10 * time.Second

type SessionLocker = store.SessionLocker

var sessionLocker = &store.SharedSessionLocker

// UpdateSession is the serialized session-record write path. It saves only real changes.
func UpdateSession(store SessionRecordStore, id supervisor.SessionID, update func(*supervisor.ManagedSession) error) error {
	if updating, ok := store.(interface {
		UpdateSession(supervisor.SessionID, func(*supervisor.ManagedSession) error) error
	}); ok {
		return updating.UpdateSession(id, update)
	}
	return UpdateSessionWith(sessionLocker, id, func() (supervisor.ManagedSession, error) { return store.Load(id) }, store.Save, update)
}

func CreateSession(store interface {
	Save(supervisor.ManagedSession) error
}, session supervisor.ManagedSession) error {
	unlock := sessionLocker.Lock(session.SessionID)
	defer unlock()
	return store.Save(session)
}

func DeleteSession(store interface {
	Delete(supervisor.SessionID) error
}, id supervisor.SessionID) error {
	unlock := sessionLocker.Lock(id)
	defer unlock()
	return store.Delete(id)
}

func UpdateSessionPatch(locker *SessionLocker, id supervisor.SessionID, load func() (supervisor.ManagedSession, error), save func(supervisor.ManagedSession) error, before, after supervisor.ManagedSession) error {
	return UpdateSessionWith(locker, id, load, save, func(current *supervisor.ManagedSession) error {
		oldValue, newValue, currentValue := reflect.ValueOf(before), reflect.ValueOf(after), reflect.ValueOf(current).Elem()
		for i := 0; i < oldValue.NumField(); i++ {
			if !reflect.DeepEqual(oldValue.Field(i).Interface(), newValue.Field(i).Interface()) {
				currentValue.Field(i).Set(newValue.Field(i))
			}
		}
		return nil
	})
}

func UpdateSessionWith(locker *SessionLocker, id supervisor.SessionID, load func() (supervisor.ManagedSession, error), save func(supervisor.ManagedSession) error, update func(*supervisor.ManagedSession) error) error {
	unlock := locker.Lock(id)
	defer unlock()
	session, err := load()
	if err != nil {
		return err
	}
	before, err := json.Marshal(session)
	if err != nil {
		return err
	}
	if err = update(&session); err != nil {
		return err
	}
	after, err := json.Marshal(session)
	if err != nil {
		return err
	}
	if bytes.Equal(before, after) {
		return nil
	}
	return save(session)
}
