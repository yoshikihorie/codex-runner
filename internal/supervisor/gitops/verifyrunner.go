package gitops

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/proc"
)

const verifyTimeout = 1800 * time.Second // SUPERVISOR_VERIFY_TIMEOUT_SECONDS
var ErrVerifyBinaryChanged = errors.New("verify_binary_changed")
var ErrVerifyTimeout = errors.New("verify_timeout")

type VerifyRunner struct {
	parentEnv []string
	StateDir  string
}

func (r VerifyRunner) Run(ctx context.Context, argv []string, expectedSHA256, cwd string) (int, error) {
	if len(argv) == 0 {
		return -1, fmt.Errorf("verify_exec_failed")
	}
	bin, err := proc.ValidateExecutable(argv[0])
	if err != nil {
		return -1, fmt.Errorf("verify_exec_failed")
	}
	matches, err := verifyBinaryMatches(bin, expectedSHA256)
	if err != nil {
		return -1, fmt.Errorf("verify_exec_failed")
	}
	if !matches {
		return -1, ErrVerifyBinaryChanged
	}
	parent := r.parentEnv
	if parent == nil {
		parent = os.Environ()
	}
	if r.StateDir == "" {
		return -1, fmt.Errorf("verify_exec_failed")
	}
	env, err := proc.GitOpsEnv(proc.GitEnvVerify, false, parent, r.StateDir)
	if err != nil {
		return -1, fmt.Errorf("verify_exec_failed")
	}
	ctx, cancel := context.WithTimeout(ctx, verifyTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, argv[1:]...)
	cmd.Dir = cwd
	cmd.Env = env
	if !filepath.IsAbs(cwd) {
		return -1, fmt.Errorf("verify_cwd_invalid")
	}
	if info, statErr := os.Stat(cwd); statErr != nil || !info.IsDir() {
		return -1, fmt.Errorf("verify_cwd_invalid")
	}
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = 2 * time.Second
	cmd.Cancel = func() error { return terminateProcessGroup(cmd.Process, timeoutKillGrace) }
	if err := cmd.Start(); err != nil {
		if ctx.Err() != nil {
			return -1, ctx.Err()
		}
		return -1, fmt.Errorf("verify_exec_failed")
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err = <-done:
		matches, checkErr := verifyBinaryMatches(bin, expectedSHA256)
		if checkErr != nil || !matches {
			return -1, ErrVerifyBinaryChanged
		}
		if err == nil {
			return 0, nil
		}
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode(), nil
		}
		return -1, fmt.Errorf("verify_exec_failed")
	case <-ctx.Done():
		_ = terminateProcessGroup(cmd.Process, timeoutKillGrace)
		<-done
		matches, checkErr := verifyBinaryMatches(bin, expectedSHA256)
		if checkErr != nil || !matches {
			return -1, ErrVerifyBinaryChanged
		}
		if ctx.Err() != context.DeadlineExceeded {
			return -1, ctx.Err()
		}
		return -1, ErrVerifyTimeout
	}
}

func verifyBinaryMatches(path, expectedSHA256 string) (bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return false, err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]) == strings.ToLower(expectedSHA256), nil
}
