package usecase

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/launch"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/store"
)

type launchPreflight struct {
	config        StartSessionConfig
	billing       launch.BillingGuard
	parentEnv     func() []string
	failure       func(store.SupervisorStartFailReason)
	validateModel bool
}

func (p launchPreflight) check(project StartProject) ([]string, error) {
	st, err := os.Stat(project.CWD)
	if !filepath.IsAbs(project.CWD) || err != nil || !st.IsDir() {
		p.failure(store.StartFailProjectCWDUnavailable)
		return nil, ErrProjectCWDUnavailable
	}
	owner, ok := st.Sys().(*syscall.Stat_t)
	if !ok || owner.Uid != uint32(os.Getuid()) {
		p.failure(store.StartFailProjectCWDUnavailable)
		return nil, ErrProjectCWDUnavailable
	}
	st, err = os.Stat(project.BinaryPath)
	if !filepath.IsAbs(project.BinaryPath) || err != nil || st.IsDir() || st.Mode()&0111 == 0 {
		p.failure(store.StartFailSpawnFailed)
		return nil, ErrSpawnFailed
	}
	if p.validateModel {
		if err := launch.ValidateProjectModel(project.Model); err != nil {
			p.failure(store.StartFailLaunchArgvInvalid)
			return nil, err
		}
	}
	parent := os.Environ()
	if p.parentEnv != nil {
		parent = p.parentEnv()
	}
	env := proc.BuildSessionEnv(parent)
	if err := p.billing.Check(project.CWD, env); err != nil {
		reason := billingStartFailureReason(err)
		if reason.Valid() {
			p.failure(reason)
			return nil, fmt.Errorf("%w: %s", ErrBillingGuardFailed, reason)
		}
		return nil, ErrBillingGuardFailed
	}
	if p.config.StateDir == "" || !filepath.IsAbs(p.config.StateDir) {
		p.failure(store.StartFailSettingsGenerationFailed)
		return nil, errors.New("state directory must be absolute")
	}
	if p.config.Columns == 0 || p.config.Rows == 0 {
		p.failure(store.StartFailSpawnFailed)
		return nil, ErrPTYSizeInvalid
	}
	return env, nil
}

func (p launchPreflight) settings(id supervisor.SessionID) (string, error) {
	statusDir := filepath.Join(p.config.StateDir, "statusline")
	if err := os.MkdirAll(statusDir, 0700); err != nil {
		return "", err
	}
	settingsPath := filepath.Join(statusDir, id.String()+".settings.json")
	data, err := launch.BuildSettingsJSON(p.config.CodexdPath, id.String(), p.config.StateDir)
	if err != nil {
		p.failure(store.StartFailSettingsGenerationFailed)
		return "", err
	}
	if err := launch.WriteSettingsFile(settingsPath, data); err != nil {
		p.failure(store.StartFailSettingsGenerationFailed)
		return "", err
	}
	if err := os.Remove(filepath.Join(statusDir, id.String()+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	return settingsPath, nil
}
