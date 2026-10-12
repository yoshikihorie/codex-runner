package usecase

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/launch"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type StartProject struct {
	ID                  string
	CLI                 supervisor.SupervisorCli
	CWD, BinaryPath     string
	Model               *string
	ParallelInstruction string
}
type StartSessionConfig struct {
	StateDir, CodexdPath string
	MaxSessions          int
	Columns, Rows        uint16
}
type StartSessionStore interface {
	SessionLoader
	SessionSaver
	Delete(supervisor.SessionID) error
	List() ([]supervisor.ManagedSession, error)
}
type PTYStarter interface {
	Start(executable string, argv, env []string, cwd string, cols, rows uint16) (ManagedPTY, error)
}
type ManagedPTY interface {
	Pid() int
	Close() error
	Kill(syscall.Signal) error
	Wait() (int, error)
}
type defaultPTYStarter struct{}

func (defaultPTYStarter) Start(executable string, argv, env []string, cwd string, cols, rows uint16) (ManagedPTY, error) {
	return proc.StartPTYSession(executable, argv, cwd, env, cols, rows)
}

type StartSessionUseCase struct {
	Config             StartSessionConfig
	Projects           map[string]StartProject
	Store              StartSessionStore
	Stop               *StopState
	Billing            launch.BillingGuard
	Starter            PTYStarter
	PTYs               *PTYRegistry
	Lock               func() (func(), error)
	Now                func() time.Time
	RecordStructured   func(store.Event) error
	RecordStartFailure func(string, store.SupervisorStartFailReason)
	Notify             func(supervisor.SupervisorEventKind, string)
	ParentEnv          func() []string
	CleanupGrace       time.Duration
}

var startSessionMutex sync.Mutex

func (u StartSessionUseCase) Execute(projectID string, id supervisor.SessionID) (ManagedPTY, error) {
	startSessionMutex.Lock()
	defer startSessionMutex.Unlock()
	if u.Lock != nil {
		unlock, err := u.Lock()
		if err != nil {
			return nil, err
		}
		defer unlock()
	}
	var pty ManagedPTY
	var startErr error
	start := func() error {
		pty, startErr = u.executeLocked(projectID, id)
		return startErr
	}
	if u.Stop != nil {
		if err := u.Stop.GuardStart(start); err != nil {
			return nil, err
		}
	} else if err := start(); err != nil {
		return nil, err
	}
	return pty, startErr
}

func (u StartSessionUseCase) executeLocked(projectID string, id supervisor.SessionID) (ManagedPTY, error) {
	if u.Store == nil {
		return nil, errors.New("session store is required")
	}
	project, ok := u.Projects[projectID]
	if !ok {
		return nil, ErrProjectNotFound
	}
	if _, err := supervisor.NewSessionID(id.String()); err != nil {
		return nil, err
	}
	if !strings.HasPrefix(id.String(), projectID+"-") {
		return nil, ErrSessionIDProjectMismatch
	}
	all, err := u.existingSessions()
	if err != nil {
		return nil, err
	}
	active := 0
	for _, s := range all {
		if s.SessionID == id {
			return nil, ErrSessionAlreadyExists
		}
		if s.State != supervisor.SessionExited && s.State != supervisor.SessionFailed {
			active++
			if s.ProjectID == projectID {
				return nil, ErrSessionAlreadyRunning
			}
		}
	}
	if u.Config.MaxSessions <= 0 {
		return nil, ErrMaxSessionsInvalid
	}
	if active >= u.Config.MaxSessions {
		return nil, ErrSessionLimitReached
	}
	preflight := launchPreflight{config: u.Config, billing: u.Billing, parentEnv: u.ParentEnv, validateModel: true, failure: func(reason store.SupervisorStartFailReason) { u.startFailure(projectID, reason) }}
	env, err := preflight.check(project)
	if err != nil {
		return nil, err
	}
	settingsPath, err := preflight.settings(id)
	if err != nil {
		return nil, err
	}
	cols, rows := u.Config.Columns, u.Config.Rows
	now := time.Now().UTC()
	if u.Now != nil {
		now = u.Now().UTC()
	}
	session := supervisor.ManagedSession{SessionID: id, ProjectID: projectID, CLI: project.CLI, Generation: 1, State: supervisor.SessionStarting, StateSince: now, StopScope: supervisor.StopScopeNone, RestartTimes: []time.Time{}, SendGate: supervisor.SendGateRecord{NextSeq: 1}}
	if err = CreateSession(u.Store, session); err != nil {
		_ = os.Remove(settingsPath)
		return nil, err
	}
	argv, err := launch.BuildLaunchArgv(string(project.CLI), project.BinaryPath, settingsPath, "")
	if err != nil {
		_ = DeleteSession(u.Store, id)
		_ = os.Remove(settingsPath)
		u.startFailure(projectID, store.StartFailLaunchArgvInvalid)
		return nil, fmt.Errorf("%w: %v", ErrLaunchArgvInvalid, err)
	}
	starter := u.Starter
	if starter == nil {
		starter = defaultPTYStarter{}
	}
	pty, err := starter.Start(project.BinaryPath, argv, env, project.CWD, cols, rows)
	if err != nil {
		_ = DeleteSession(u.Store, id)
		_ = os.Remove(settingsPath)
		u.startFailure(projectID, store.StartFailSpawnFailed)
		return nil, fmt.Errorf("%w: %v", ErrSpawnFailed, err)
	}
	started := time.Now().UTC()
	if u.Now != nil {
		started = u.Now().UTC()
	}
	session.ChildPID = pty.Pid()
	session.ChildStartedAt = &started
	ownedChildren.Register(id, pty.Pid())
	if err = UpdateSession(u.Store, id, func(current *supervisor.ManagedSession) error {
		current.ChildPID = session.ChildPID
		current.ChildStartedAt = session.ChildStartedAt
		return nil
	}); err != nil {
		ownedChildren.Remove(id, pty.Pid())
		grace := u.CleanupGrace
		if grace <= 0 {
			grace = timeoutKillGrace
		}
		cleanupDone := make(chan struct{})
		_ = pty.Close()
		go func() { _, _ = pty.Wait(); close(cleanupDone) }()
		select {
		case <-cleanupDone:
		case <-time.After(grace):
			_ = pty.Kill(syscall.SIGKILL)
			<-cleanupDone
		}
		_ = DeleteSession(u.Store, id)
		_ = os.Remove(settingsPath)
		return nil, fmt.Errorf("%w: %v", ErrSpawnFailed, err)
	}
	u.eventNotify(id, supervisor.EventKindSessionStarted)
	if u.PTYs != nil {
		if writer, ok := pty.(PTYWriter); ok {
			u.PTYs.Register(id, writer)
		}
	}
	return pty, nil
}
func (u StartSessionUseCase) existingSessions() ([]supervisor.ManagedSession, error) {
	if u.RecordStructured != nil || u.Notify != nil {
		bindQuarantine(u.Store, func(id supervisor.SessionID) { u.eventNotify(id, supervisor.EventKindStateFileCorrupt) })
	}
	if listing, ok := u.Store.(interface {
		ListWithCorrupt() ([]supervisor.ManagedSession, []supervisor.SessionID, error)
	}); ok {
		sessions, newly, err := listing.ListWithCorrupt()
		_ = newly
		return sessions, err
	}
	return u.Store.List()
}
func (u StartSessionUseCase) startFailure(projectID string, reason store.SupervisorStartFailReason) {
	if !reason.Valid() {
		return
	}
	if u.RecordStartFailure != nil {
		u.RecordStartFailure(projectID, reason)
	}
	// Notify accepts a session ID; start failures have only a project ID.
}

func billingStartFailureReason(err error) store.SupervisorStartFailReason {
	for _, reason := range []store.SupervisorStartFailReason{store.StartFailUserSettings, store.StartFailProjectSettings, store.StartFailProjectLocalSettings, store.StartFailClaudeJSON, store.StartFailManaged, store.StartFailCodexAuth, store.StartFailChildEnv} {
		if err.Error() == "SUP_BILLING_GUARD_FAILED: "+string(reason) {
			return reason
		}
	}
	return ""
}

func (u StartSessionUseCase) eventNotify(id supervisor.SessionID, kind supervisor.SupervisorEventKind) {
	if u.RecordStructured != nil {
		event := store.Event{SessionID: id.String(), Kind: kind}
		if kind == supervisor.EventKindSessionStarted {
			event.ToState = supervisor.SessionStarting
		}
		_ = u.RecordStructured(event)
	}
	if u.Notify != nil {
		u.Notify(kind, id.String())
	}
}
