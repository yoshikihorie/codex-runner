package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/config"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/gitops"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/launch"
	supervisorstore "github.com/yoshikihorie/codex-runner/internal/supervisor/store"
	supervisortransport "github.com/yoshikihorie/codex-runner/internal/supervisor/transport"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/usecase"
)

const (
	supervisorLockName   = "codexd-supervisor.lock" // FD-sup-01 §5.7.
	supervisorPTYColumns = 200                      // SUPERVISOR_PTY_COLUMNS_DEFAULT.
	supervisorPTYRows    = 50                       // SUPERVISOR_PTY_ROWS_DEFAULT.
)

type supervisorProjectDeps struct {
	CommitVerified *usecase.CommitVerifiedChangesUseCase
	Push           *usecase.PushToAllowedRemoteUseCase
	ObserveSession *usecase.ObserveSessionUseCase
}

type supervisorRuntime struct {
	config         config.SupervisorConfig
	ctx            context.Context
	cancel         context.CancelFunc
	sessions       usecase.SessionStoreAdapter
	events         *supervisorstore.EventStore
	recorder       usecase.EventStoreRecorder
	notifier       *usecase.NotifyUseCase
	stops          *usecase.StopState
	ptys           *usecase.PTYRegistry
	observed       *usecase.ObservedStateStore
	contextPercent *usecase.ObservedContextStore
	screens        *usecase.ObservedScreenStore
	pending        *usecase.PendingRemoteControlUseCase
	send           *usecase.SendInputUseCase
	auto           *usecase.AutoContinueUseCase
	control        *usecase.StopControlUseCase
	recovery       *usecase.RecoverSessionUseCase
	rotation       *usecase.RotateSessionUseCase
	start          *usecase.StartSessionUseCase
	stopSession    *usecase.StopSessionUseCase
	locker         *gitops.RepoLocker
	projects       map[string]config.SupervisorProject
	startProjects  map[string]usecase.StartProject
	projectMu      sync.Mutex
	projectCache   map[string]*supervisorProjectDeps
	startLock      sync.Mutex
	sequence       *supervisorstore.SessionSequence
	wakes          sync.Map
}

type supervisorSessionRepository struct{ store usecase.SessionStoreAdapter }

func (r supervisorSessionRepository) List(context.Context) ([]supervisor.ManagedSession, error) {
	return r.store.List()
}
func (r supervisorSessionRepository) Save(_ context.Context, session supervisor.ManagedSession) error {
	return r.store.Save(session)
}

type supervisorResumeSuppression struct{ runtime *supervisorRuntime }

func (s supervisorResumeSuppression) Clear(ctx context.Context, id supervisor.SessionID) error {
	session, err := s.runtime.sessions.Load(id)
	if err != nil {
		return err
	}
	deps := s.runtime.projectDeps(session.ProjectID)
	if deps == nil {
		return usecase.ErrProjectNotFound
	}
	return usecase.NewSessionResumeSuppression(s.runtime.sessions, deps.Push).Clear(ctx, id)
}

type supervisorRotationRespawner struct{ runtime *supervisorRuntime }

func (r supervisorRotationRespawner) Execute(ctx context.Context, id supervisor.SessionID, kind usecase.RespawnKind) error {
	session, err := r.runtime.sessions.Load(id)
	if err != nil {
		return err
	}
	deps := r.runtime.projectDeps(session.ProjectID)
	if deps == nil {
		return usecase.ErrProjectNotFound
	}
	return deps.ObserveSession.ObservedRespawn()(ctx, id, kind)
}

func newSupervisorRuntime(parent context.Context, cfg config.SupervisorConfig) (*supervisorRuntime, error) {
	ctx, cancel := context.WithCancel(parent)
	log, err := supervisorstore.NewSupervisorLog(cfg.StateDir, 0, 0)
	if err != nil {
		cancel()
		return nil, err
	}
	r := &supervisorRuntime{config: cfg, ctx: ctx, cancel: cancel, sessions: usecase.SessionStoreAdapter{Store: supervisorstore.NewSessionStore(cfg.StateDir)}, events: supervisorstore.NewEventStoreWithLogger(cfg.StateDir, log), stops: usecase.NewStopState(cfg.StateDir), ptys: &usecase.PTYRegistry{}, observed: &usecase.ObservedStateStore{}, contextPercent: &usecase.ObservedContextStore{}, screens: &usecase.ObservedScreenStore{}, locker: gitops.NewRepoLocker(), sequence: supervisorstore.NewSessionSequence(cfg.StateDir), projects: make(map[string]config.SupervisorProject), startProjects: make(map[string]usecase.StartProject), projectCache: make(map[string]*supervisorProjectDeps)}
	r.recorder = usecase.EventStoreRecorder{Events: r.events, Log: log}
	r.notifier = usecase.NewNotifyUseCase(cfg.NotifyCommand, nil, usecase.EventStoreNotifyRecorder{Recorder: r.recorder}, time.Now)
	record := r.recorder.RecordStructured
	notify := func(kind supervisor.SupervisorEventKind, id string) { _ = r.notifier.Notify(r.ctx, kind, id) }
	for _, p := range cfg.Projects {
		r.projects[p.ID] = p
		r.startProjects[p.ID] = usecase.StartProject{ID: p.ID, CLI: supervisor.SupervisorCli(p.CLI), CWD: p.CWD, BinaryPath: cfg.ClaudeBinaryPath, Model: p.Model, ParallelInstruction: p.ParallelInstruction}
	}
	instructions := make(map[string]string, len(r.projects))
	for id, project := range r.projects {
		instructions[id] = project.ParallelInstruction
	}
	r.send = &usecase.SendInputUseCase{Sessions: r.sessions, Screens: r.screens, PTYs: r.ptys, IsStopped: r.stops.IsStopped, StopState: r.stops, ProjectInstructions: instructions, OperationalLog: log, RecordStructured: record, Notify: notify}
	r.pending = &usecase.PendingRemoteControlUseCase{Sessions: r.sessions, Send: r.send, RecordStructured: record, Now: time.Now}
	r.auto = &usecase.AutoContinueUseCase{Extractor: supervisor.RecommendationExtractor{ObservedScreen: true}}
	r.auto.Bind(r.sessions, r.send, r.stops.IsStopped, record, notify)
	r.control = usecase.NewStopControlUseCase(r.stops, supervisorSessionRepository{r.sessions}, r.recorder, r.notifier)
	r.control.SetAutoContinue(r.auto)
	r.recovery = &usecase.RecoverSessionUseCase{Store: r.sessions, Stop: r.stops, LifetimeContext: r.ctx, Running: &sync.WaitGroup{}, RecordStructured: record, Notify: notify}
	r.recovery.Respawn = func(ctx context.Context, id supervisor.SessionID, kind usecase.RespawnKind) error {
		return (supervisorRotationRespawner{r}).Execute(ctx, id, kind)
	}
	r.rotation = &usecase.RotateSessionUseCase{Sessions: r.sessions, Send: r.send, Respawn: supervisorRotationRespawner{r}, Stop: r.stops, IsStopped: r.stops.IsStopped, Observed: func(id supervisor.SessionID) supervisor.SupervisorSessionState {
		value, _ := r.observed.Get(id)
		return supervisor.SupervisorSessionState(value)
	}, ChildAlive: func(id supervisor.SessionID) bool {
		session, err := r.sessions.Load(id)
		return err == nil && session.ChildPID > 0 && syscall.Kill(session.ChildPID, 0) == nil
	}, CheckpointCWD: func(s supervisor.ManagedSession) string { return r.projects[s.ProjectID].CWD }, Inspect: func(s supervisor.ManagedSession) (supervisor.RotationCheckpointSnapshot, error) {
		return (usecase.CheckpointInspector{CWD: r.projects[s.ProjectID].CWD}).Inspect()
	}, RecordStructured: record, Notify: notify, AutoContinue: r.auto, PendingRemoteControl: r.pending, ResumeReady: r.send.ResumeReady, Wake: r.rotationWake}
	r.control.SetRotationUseCase(r.rotation, r.ctx)
	r.control.SetResumeContinuations(nil, func(ctx context.Context, s supervisor.ManagedSession) error {
		deps := r.projectDeps(s.ProjectID)
		if deps == nil {
			return usecase.ErrProjectNotFound
		}
		return deps.ObserveSession.ResumeRecovery(ctx, s)
	})
	r.control.SetResumeSuppression(supervisorResumeSuppression{r})
	r.control.SetResumeMaintenance(usecase.NewSessionResumeMaintenance(r.sessions, func(ctx context.Context, project string) (string, error) {
		deps := r.projectDeps(project)
		if deps == nil {
			return "", usecase.ErrProjectNotFound
		}
		return deps.CommitVerified.CurrentGitConfigFingerprint(ctx)
	}))
	// NewStopControlUseCase supplies the process resume transition context.
	executable, err := os.Executable()
	if err != nil {
		r.close()
		return nil, err
	}
	startConfig := usecase.StartSessionConfig{StateDir: cfg.StateDir, CodexdPath: executable, MaxSessions: cfg.MaxSessions, Columns: supervisorPTYColumns, Rows: supervisorPTYRows}
	lock := func() (func(), error) { r.startLock.Lock(); return r.startLock.Unlock, nil }
	startFailure := func(project string, reason supervisorstore.SupervisorStartFailReason) {
		_ = r.recorder.RecordStartFailed(project, reason, time.Now().UTC())
	}
	r.start = &usecase.StartSessionUseCase{Config: startConfig, Projects: r.startProjects, Store: r.sessions, Stop: r.stops, Billing: launch.BillingGuard{}, PTYs: r.ptys, Lock: lock, Now: time.Now, RecordStructured: record, RecordStartFailure: startFailure, Notify: notify, GitConfigFingerprint: func(project string) string {
		deps := r.projectDeps(project)
		if deps == nil {
			return ""
		}
		value, err := deps.CommitVerified.CurrentGitConfigFingerprint(r.ctx)
		if err != nil {
			return ""
		}
		return value
	}}
	r.stopSession = &usecase.StopSessionUseCase{Sessions: r.sessions, PTYs: r.ptys, RecordStructured: record, Notify: notify}
	return r, nil
}

func (r *supervisorRuntime) projectDeps(projectID string) *supervisorProjectDeps {
	r.projectMu.Lock()
	defer r.projectMu.Unlock()
	if deps := r.projectCache[projectID]; deps != nil {
		return deps
	}
	p, ok := r.projects[projectID]
	if !ok {
		return nil
	}
	git := gitops.NewGitRunner(p.CWD, r.config.CommitAuthorName, r.config.CommitAuthorEmail, r.config.StateDir)
	git.SetPassSSHAuthSock(r.config.GitPassSSHAuthSock)
	record := r.recorder.RecordStructured
	notify := func(kind supervisor.SupervisorEventKind, id string) { _ = r.notifier.Notify(r.ctx, kind, id) }
	notifyID := func(kind supervisor.SupervisorEventKind, id supervisor.SessionID) { notify(kind, id.String()) }
	allow := make([]gitops.PushAllowlistEntry, len(r.config.PushAllowlist))
	for i, entry := range r.config.PushAllowlist {
		allow[i] = gitops.PushAllowlistEntry{Remote: entry.Remote, URL: entry.URL, Branch: entry.Branch}
	}
	push := &usecase.PushToAllowedRemoteUseCase{Git: git, Sessions: r.sessions, Projects: supervisorstore.NewProjectRecordStore(r.config.StateDir), Stop: r.stops, Allowlist: allow, ProtectedBranches: r.config.ProtectedBranches, Record: record, Notify: notifyID}
	commit := &usecase.CommitVerifiedChangesUseCase{Git: git, Verify: gitops.VerifyRunner{StateDir: r.config.StateDir}, Sessions: r.sessions, Projects: supervisorstore.NewProjectRecordStore(r.config.StateDir), Stop: r.stops, Locker: r.locker, CWD: p.CWD, OwnedPaths: p.OwnedPaths, VerifyCommand: p.VerifyCommand, VerifyCommandSHA256: p.VerifySHA256, Record: record, Notify: notifyID, AfterCommit: push.Run, Now: time.Now}
	respawn := &usecase.RespawnSessionUseCase{Config: r.start.Config, Projects: r.startProjects, Store: r.sessions, Stop: r.stops, Billing: launch.BillingGuard{}, PTYs: r.ptys, Lock: r.start.Lock, Now: time.Now, RecordStructured: record, RecordStartFailure: r.start.RecordStartFailure, Notify: notify}
	initial := &usecase.InitialRemoteControl{Request: func(ctx context.Context, input usecase.SendInput) error {
		_, err := r.send.Execute(ctx, input)
		return err
	}}
	permission := &usecase.PermissionPendingUseCase{}
	permission.Bind(func(id supervisor.SessionID, kind supervisor.SupervisorEventKind) error {
		return record(supervisorstore.Event{Kind: kind, SessionID: id.String()})
	}, notify)
	observer := &usecase.ObserveSessionUseCase{Detect: usecase.DetectSessionStateUseCase{StateDir: r.config.StateDir, Sessions: r.sessions, Detector: detect.NewCompositeDetector(detect.DefaultPatternSet()), Now: time.Now}, Sessions: r.sessions, Stop: r.stops, Recover: r.recovery, Respawn: respawn, SendInput: r.send, InitialRemoteControl: initial, Rotation: r.rotation, AutoContinue: r.auto, PermissionPending: permission, CommitVerified: commit, PlanRotation: usecase.PlanRotationUseCase{}, PendingRemoteControl: r.pending, SupervisorContext: r.ctx, RecordStructured: record, Observed: r.observed, ContextPercent: r.contextPercent, Notify: notify, Now: time.Now}
	deps := &supervisorProjectDeps{CommitVerified: commit, Push: push, ObserveSession: observer}
	r.projectCache[projectID] = deps
	return deps
}

func (r *supervisorRuntime) rotationWake(id supervisor.SessionID) <-chan struct{} {
	if value, ok := r.wakes.Load(id); ok {
		return value.(chan struct{})
	}
	ch := make(chan struct{}, 1)
	actual, loaded := r.wakes.LoadOrStore(id, ch)
	if loaded {
		return actual.(chan struct{})
	}
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.ctx.Done():
				return
			case <-ticker.C:
				select {
				case ch <- struct{}{}:
				default:
				}
			}
		}
	}()
	return ch
}

func (r *supervisorRuntime) close() {
	r.ptys.BeginShutdown()
	for _, pty := range r.ptys.Managed() {
		_ = pty.Close()
	}
	r.cancel()
	r.recovery.WaitForCompletion()
	for len(r.ptys.Managed()) != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	r.notifier.Close()
}

func runSupervise(parent context.Context, args []string, stderr io.Writer) int {
	flags := flag.NewFlagSet("supervise", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "")
	if flags.Parse(args) != nil || len(flags.Args()) != 0 {
		return 2
	}
	cfg, err := config.LoadSupervisorConfig(*configPath)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		_, _ = fmt.Fprintln(stderr, "SUP_CONFIG_INVALID HOME")
		return 1
	}
	if resolved, err := filepath.EvalSymlinks(home); err == nil {
		home = resolved
	}
	runDir := filepath.Join(home, ".claude", "run")
	if err := ensureManagedPrivateDir(runDir); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	if err := ensureSocketParent(filepath.Dir(cfg.SocketPath), runDir); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	lease, err := acquireDaemonInstanceLease(filepath.Join(runDir, supervisorLockName))
	if err != nil {
		_, _ = fmt.Fprintln(stderr, "SUP_INSTANCE_LOCK_HELD")
		return 1
	}
	defer lease.Unlock()
	if err := os.MkdirAll(cfg.StateDir, 0700); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	if err := os.Chmod(cfg.StateDir, 0700); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	ctx, stopSignals := signal.NotifyContext(parent, os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	r, err := newSupervisorRuntime(context.Background(), cfg)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	defer r.close()
	if err := r.sequence.MarkStarted(time.Now().UTC()); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	notify := func(kind supervisor.SupervisorEventKind, id string) { _ = r.notifier.Notify(r.ctx, kind, id) }
	if _, err := (usecase.ReconcileOnStartupUseCase{Store: usecase.ReconcileSessionStoreAdapter{Store: r.sessions.Store}, Stop: r.stops, RecordStructured: r.recorder.RecordStructured, Notify: notify}).Execute(); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	dispatch := r.newDispatch()
	server := &supervisortransport.Server{SocketPath: cfg.SocketPath, DefaultSocketPath: filepath.Join(runDir, "codexd-supervisor.sock"), LockHeld: true, Handler: dispatch}
	served := make(chan error, 1)
	go func() { served <- server.Serve(r.ctx) }()
	ready := time.NewTimer(5 * time.Second)
	defer ready.Stop()
	for {
		if info, err := os.Lstat(cfg.SocketPath); err == nil && info.Mode()&os.ModeSocket != 0 {
			if conn, dialErr := net.DialTimeout("unix", cfg.SocketPath, 50*time.Millisecond); dialErr == nil {
				_ = conn.Close()
				break
			}
		}
		select {
		case err := <-served:
			if err == nil && ctx.Err() != nil {
				return 0
			}
			_, _ = fmt.Fprintln(stderr, err)
			return 1
		case <-ready.C:
			_, _ = fmt.Fprintln(stderr, "SUP_CONFIG_PATH_CONFLICT socket_path")
			return 1
		case <-time.After(10 * time.Millisecond):
		}
	}
	ownedSocket, err := os.Lstat(cfg.SocketPath)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 1
	}
	defer func() {
		if _, err := os.Lstat(cfg.SocketPath); err == nil {
			_ = removeOwnedSocket(cfg.SocketPath, ownedSocket)
		}
	}()
	if err := r.recorder.RecordStructured(supervisorstore.Event{Kind: supervisor.EventKindSupervisorStarted, At: time.Now().UTC()}); err != nil {
		_, _ = fmt.Fprintln(stderr, err)
	}
	notify(supervisor.EventKindSupervisorStarted, "")
	if allowed, reason := launch.LunaImplAllowed(); !allowed {
		if err := r.recorder.RecordStructured(supervisorstore.Event{Kind: supervisor.EventKindLunaImplNotAllowed, Reason: supervisorstore.EventReason(reason), At: time.Now().UTC()}); err != nil {
			_, _ = fmt.Fprintln(stderr, err)
		}
		notify(supervisor.EventKindLunaImplNotAllowed, "")
	}
	select {
	case err := <-served:
		if err != nil {
			_, _ = fmt.Fprintln(stderr, err)
			return 1
		}
	case <-ctx.Done():
		r.close()
		if err := <-served; err != nil && !errors.Is(err, context.Canceled) {
			_, _ = fmt.Fprintln(stderr, err)
			return 1
		}
	}
	return 0
}

func (r *supervisorRuntime) newDispatch() *supervisortransport.Dispatch {
	return &supervisortransport.Dispatch{Sessions: r.sessions, Control: r.control, StartSession: r.start, StopSession: r.stopSession, Events: r.events, ObserveFor: func(project string) *usecase.ObserveSessionUseCase {
		deps := r.projectDeps(project)
		if deps == nil {
			return nil
		}
		return deps.ObserveSession
	}, NewSessionID: r.sequence.Next, SupervisorContext: r.ctx, Observed: r.observed, ContextPercent: r.contextPercent.Get, StartedAt: time.Now().UTC(), GlobalStopped: r.stops.IsGlobalStopped, ShuttingDown: r.ptys.IsShuttingDown}
}
