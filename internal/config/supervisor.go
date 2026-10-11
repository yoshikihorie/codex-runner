package config

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"

	"github.com/yoshikihorie/codex-runner/internal/proc"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/textpolicy"

	"github.com/BurntSushi/toml"
)

const (
	supervisorDefaultMaxSessions         = 4                                                                                    // 10-shared/validation-rules.yaml: SUPERVISOR_MAX_SESSIONS_DEFAULT
	supervisorMaxSessionsMin             = 1                                                                                    // SUPERVISOR_MAX_SESSIONS_MIN
	supervisorMaxSessionsMax             = 8                                                                                    // SUPERVISOR_MAX_SESSIONS_MAX
	supervisorInstructionMaxLength       = 200                                                                                  // SUPERVISOR_INSTRUCTION_MAX_LENGTH
	supervisorPTYColumnsDefault          = 200                                                                                  // SUPERVISOR_PTY_COLUMNS_DEFAULT
	supervisorSlugMaxLength              = 40                                                                                   // SLUG_MAX_LENGTH
	supervisorDefaultParallelInstruction = "自動作業計画モード 並列で作業して。実装は ~/.claude/scripts/codex.sh impl --model gpt-6-luna を明示して委譲する" // SUPERVISOR_PARALLEL_INSTRUCTION_DEFAULT
)

var (
	supervisorSlugPattern       = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	supervisorRemotePattern     = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	supervisorBranchPattern     = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)
	supervisorProtectedDefaults = []string{"main", "master", "develop"} // SUPERVISOR_PROTECTED_BRANCHES_DEFAULT
	supervisorOwnsPath          = ownedByCurrentUserStrict
)

// SupervisorConfig contains the validated runtime configuration for the supervisor.
type SupervisorConfig struct {
	SocketPath         string
	StateDir           string
	ClaudeBinaryPath   string
	CodexBinaryPath    string
	MaxSessions        int
	NotifyCommand      string
	CommitAuthorName   string
	CommitAuthorEmail  string
	ProtectedBranches  []string
	GitPassSSHAuthSock bool
	PushAllowlist      []SupervisorPushAllow
	Projects           []SupervisorProject
}

// SupervisorPushAllow describes one allowed remote and branch pair.
type SupervisorPushAllow struct {
	Remote string `toml:"remote"`
	URL    string `toml:"url"`
	Branch string `toml:"branch"`
}

// SupervisorProject contains a validated project registration.
type SupervisorProject struct {
	ID                  string   `toml:"id"`
	CWD                 string   `toml:"cwd"`
	CLI                 string   `toml:"cli"`
	Model               *string  `toml:"model"`
	OwnedPaths          []string `toml:"owned_paths"`
	VerifyCommand       []string `toml:"verify_command"`
	VerifySHA256        string   `toml:"-"`
	ParallelInstruction string   `toml:"parallel_instruction"`
}

type supervisorProjectRaw struct {
	ID                  string   `toml:"id"`
	CWD                 string   `toml:"cwd"`
	CLI                 string   `toml:"cli"`
	Model               *string  `toml:"model"`
	OwnedPaths          []string `toml:"owned_paths"`
	VerifyCommand       []string `toml:"verify_command"`
	ParallelInstruction *string  `toml:"parallel_instruction"`
}
type supervisorRaw struct {
	SocketPath         *string                `toml:"socket_path"`
	StateDir           *string                `toml:"state_dir"`
	ClaudeBinaryPath   *string                `toml:"claude_binary_path"`
	CodexBinaryPath    *string                `toml:"codex_binary_path"`
	MaxSessions        *int                   `toml:"max_sessions"`
	NotifyCommand      *string                `toml:"notify_command"`
	CommitAuthorName   *string                `toml:"commit_author_name"`
	CommitAuthorEmail  *string                `toml:"commit_author_email"`
	ProtectedBranches  []string               `toml:"protected_branches"`
	GitPassSSHAuthSock *bool                  `toml:"git_pass_ssh_auth_sock"`
	PushAllowlist      []SupervisorPushAllow  `toml:"push_allowlist"`
	Projects           []supervisorProjectRaw `toml:"projects"`
}

// LoadSupervisorConfig reads and validates supervisor configuration from path.
// An empty path selects the default configuration location.
func LoadSupervisorConfig(path string) (SupervisorConfig, error) {
	if path == "" {
		home, err := validatedSupervisorHome()
		if err != nil {
			return SupervisorConfig{}, supervisorInvalid("HOME", "missing_required", err)
		}
		path = filepath.Join(home, ".claude", "codexd", "supervisor.toml")
	}
	if !filepath.IsAbs(path) {
		return SupervisorConfig{}, supervisorInvalid("config_path", "relative_path", nil)
	}
	configFile, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			home, homeErr := validatedSupervisorHome()
			if homeErr != nil {
				return SupervisorConfig{}, supervisorInvalid("HOME", "missing_required", homeErr)
			}
			_ = home
			return emptySupervisorConfig(), nil
		}
		return SupervisorConfig{}, supervisorInvalid("config_path", "file_unreadable", err)
	}
	defer configFile.Close()
	configInfo, err := configFile.Stat()
	if err != nil || !configInfo.Mode().IsRegular() || configInfo.Mode().Perm()&0444 == 0 {
		return SupervisorConfig{}, supervisorInvalid("config_path", "file_unreadable", err)
	}
	raw := supervisorRaw{}
	md, err := toml.DecodeReader(configFile, &raw)
	if err != nil {
		return SupervisorConfig{}, supervisorInvalid(tomlErrorKey(err), "type_mismatch", err)
	}
	for _, key := range md.Keys() {
		if !knownSupervisorKey(key.String()) {
			return SupervisorConfig{}, supervisorInvalid(key.String(), "unknown_key", nil)
		}
	}
	if keys := md.Undecoded(); len(keys) > 0 {
		return SupervisorConfig{}, supervisorInvalid(keys[0].String(), "unknown_key", nil)
	}
	c := SupervisorConfig{MaxSessions: supervisorDefaultMaxSessions, ProtectedBranches: append([]string(nil), supervisorProtectedDefaults...), GitPassSSHAuthSock: true, PushAllowlist: raw.PushAllowlist, Projects: make([]SupervisorProject, len(raw.Projects))}
	for i, rp := range raw.Projects {
		c.Projects[i] = SupervisorProject{ID: rp.ID, CWD: rp.CWD, CLI: rp.CLI, Model: rp.Model, OwnedPaths: rp.OwnedPaths, VerifyCommand: rp.VerifyCommand}
		if rp.ParallelInstruction != nil {
			c.Projects[i].ParallelInstruction = *rp.ParallelInstruction
		}
	}
	assign := func(dst *string, src *string) {
		if src != nil {
			*dst = *src
		}
	}
	assign(&c.SocketPath, raw.SocketPath)
	assign(&c.StateDir, raw.StateDir)
	assign(&c.ClaudeBinaryPath, raw.ClaudeBinaryPath)
	assign(&c.CodexBinaryPath, raw.CodexBinaryPath)
	assign(&c.CommitAuthorName, raw.CommitAuthorName)
	assign(&c.CommitAuthorEmail, raw.CommitAuthorEmail)
	assign(&c.NotifyCommand, raw.NotifyCommand)
	if raw.MaxSessions != nil {
		c.MaxSessions = *raw.MaxSessions
	}
	if raw.GitPassSSHAuthSock != nil {
		c.GitPassSSHAuthSock = *raw.GitPassSSHAuthSock
	}
	if len(c.Projects) > 0 && strings.TrimSpace(c.CommitAuthorName) == "" {
		return SupervisorConfig{}, supervisorInvalid("commit_author_name", "commit_author_missing", nil)
	}
	if len(c.Projects) > 0 && strings.TrimSpace(c.CommitAuthorEmail) == "" {
		return SupervisorConfig{}, supervisorInvalid("commit_author_email", "commit_author_missing", nil)
	}
	if strings.ContainsAny(c.CommitAuthorName, "\r\n") {
		return SupervisorConfig{}, supervisorInvalid("commit_author_name", "type_mismatch", nil)
	}
	if strings.ContainsAny(c.CommitAuthorEmail, "\r\n") {
		return SupervisorConfig{}, supervisorInvalid("commit_author_email", "type_mismatch", nil)
	}
	if c.MaxSessions < supervisorMaxSessionsMin || c.MaxSessions > supervisorMaxSessionsMax {
		return SupervisorConfig{}, supervisorInvalid("max_sessions", "out_of_range", nil)
	}
	if len(raw.ProtectedBranches) > 0 {
		for _, b := range raw.ProtectedBranches {
			if !validBranch(b) {
				return SupervisorConfig{}, supervisorInvalid("protected_branches", "type_mismatch", nil)
			}
			if !containsExactString(c.ProtectedBranches, b) {
				c.ProtectedBranches = append(c.ProtectedBranches, b)
			}
		}
	}
	if raw.NotifyCommand != nil && *raw.NotifyCommand == "" {
		return SupervisorConfig{}, supervisorInvalid("notify_command", "not_executable", nil)
	}
	if c.NotifyCommand != "" && c.NotifyCommand != "none" {
		if !filepath.IsAbs(c.NotifyCommand) {
			return SupervisorConfig{}, supervisorInvalid("notify_command", "relative_path", nil)
		}
		real, err := proc.ValidateExecutable(c.NotifyCommand)
		if err != nil {
			return SupervisorConfig{}, supervisorInvalid("notify_command", executableConfigReason(err), err)
		}
		c.NotifyCommand = real
	}
	for _, p := range []*string{&c.SocketPath, &c.StateDir} {
		if *p != "" && !filepath.IsAbs(*p) {
			key := "socket_path"
			if p == &c.StateDir {
				key = "state_dir"
			}
			return SupervisorConfig{}, supervisorInvalid(key, "relative_path", nil)
		}
	}
	home, homeErr := validatedSupervisorHome()
	if homeErr != nil || home == "" {
		return SupervisorConfig{}, supervisorInvalid("HOME", "missing_required", homeErr)
	}
	if c.SocketPath == "" {
		c.SocketPath = filepath.Join(home, ".claude", "run", "codexd-supervisor.sock")
	}
	if c.StateDir == "" {
		c.StateDir = filepath.Join(home, ".claude", "codexd", "supervisor")
	}
	for key, path := range map[string]*string{"socket_path": &c.SocketPath, "state_dir": &c.StateDir} {
		if filepath.Clean(*path) != *path {
			return SupervisorConfig{}, supervisorInvalid(key, "not_normalized", nil)
		}
		resolved, e := evalNearestPath(*path)
		if e != nil {
			return SupervisorConfig{}, supervisorInvalid(key, "file_unreadable", e)
		}
		if resolved != filepath.Clean(*path) {
			return SupervisorConfig{}, supervisorInvalid(key, "not_normalized", nil)
		}
		*path = resolved
		if info, e := os.Stat(*path); e == nil {
			if !info.IsDir() && key == "state_dir" {
				return SupervisorConfig{}, supervisorInvalid(key, "file_unreadable", nil)
			}
			if !supervisorOwnsPath(info) {
				return SupervisorConfig{}, supervisorInvalid(key, "not_owned", nil)
			}
		} else if !errors.Is(e, fs.ErrNotExist) {
			return SupervisorConfig{}, supervisorInvalid(key, "file_unreadable", e)
		}
	}
	if err := checkSupervisorPathConflicts(c); err != nil {
		return SupervisorConfig{}, err
	}
	if c.ClaudeBinaryPath != "" {
		if !filepath.IsAbs(c.ClaudeBinaryPath) {
			return SupervisorConfig{}, supervisorInvalid("claude_binary_path", "relative_path", nil)
		}
		real, err := filepath.EvalSymlinks(c.ClaudeBinaryPath)
		if err != nil {
			return SupervisorConfig{}, supervisorInvalid("claude_binary_path", "not_executable", err)
		}
		c.ClaudeBinaryPath = real
		if err := isExecutableFile(c.ClaudeBinaryPath); err != nil {
			return SupervisorConfig{}, supervisorInvalid("claude_binary_path", "not_executable", err)
		}
	}
	if c.CodexBinaryPath != "" {
		return SupervisorConfig{}, supervisorInvalid("codex_binary_path", "reserved_in_v1", nil)
	}
	projectIDs := make(map[string]struct{}, len(c.Projects))
	if len(c.Projects) > 0 && c.ClaudeBinaryPath == "" {
		return SupervisorConfig{}, supervisorInvalid("claude_binary_path", "missing_required", nil)
	}
	for i := range c.Projects {
		p := &c.Projects[i]
		if !supervisorSlugPattern.MatchString(p.ID) || len(p.ID) > supervisorSlugMaxLength {
			if p.ID == "" {
				return SupervisorConfig{}, supervisorInvalid("projects.id", "missing_required", nil)
			}
			return SupervisorConfig{}, supervisorInvalid("projects.id", "type_mismatch", nil)
		}
		if _, exists := projectIDs[p.ID]; exists {
			return SupervisorConfig{}, supervisorInvalid("projects.id", "duplicate_project_id", nil)
		}
		projectIDs[p.ID] = struct{}{}
		if p.CLI == "" {
			return SupervisorConfig{}, supervisorInvalid("projects.cli", "missing_required", nil)
		}
		if p.CLI == "codex" {
			return SupervisorConfig{}, supervisorInvalid("projects.cli", "cli_not_supported", nil)
		}
		if p.CLI != "claude" {
			return SupervisorConfig{}, supervisorInvalid("projects.cli", "cli_not_supported", nil)
		}
		if p.Model != nil {
			return SupervisorConfig{}, supervisorInvalid("projects.model", "model_not_allowed_for_claude", nil)
		}
		if p.CWD == "" {
			return SupervisorConfig{}, supervisorInvalid("projects.cwd", "missing_required", nil)
		}
		if !filepath.IsAbs(p.CWD) {
			return SupervisorConfig{}, supervisorInvalid("projects.cwd", "relative_path", nil)
		}
		resolved, e := filepath.EvalSymlinks(p.CWD)
		if e != nil {
			return SupervisorConfig{}, supervisorInvalid("projects.cwd", "file_unreadable", e)
		}
		if filepath.Clean(p.CWD) != p.CWD || resolved != p.CWD {
			return SupervisorConfig{}, supervisorInvalid("projects.cwd", "not_normalized", nil)
		}
		p.CWD = resolved
		info, e := os.Stat(resolved)
		if e != nil || !info.IsDir() {
			return SupervisorConfig{}, supervisorInvalid("projects.cwd", "file_unreadable", e)
		}
		if !supervisorOwnsPath(info) {
			return SupervisorConfig{}, supervisorInvalid("projects.cwd", "not_owned", nil)
		}
		repositoryRoot, inRepository := gitRepositoryRoot(resolved)
		for prior := 0; prior < i; prior++ {
			priorRoot, priorOK := gitRepositoryRoot(c.Projects[prior].CWD)
			if resolved == c.Projects[prior].CWD || inRepository && priorOK && priorRoot == repositoryRoot {
				return SupervisorConfig{}, supervisorInvalid("projects.cwd", "shared_repository", nil)
			}
		}
		if len(p.VerifyCommand) > 0 {
			bin := p.VerifyCommand[0]
			if !filepath.IsAbs(bin) {
				return SupervisorConfig{}, supervisorInvalid("projects.verify_command", "relative_path", nil)
			}
			real, e := proc.ValidateExecutable(bin)
			if e != nil {
				return SupervisorConfig{}, supervisorInvalid("projects.verify_command", executableConfigReason(e), e)
			}
			repository := resolved
			if inRepository {
				repository = repositoryRoot
			}
			if pathWithin(real, repository) {
				return SupervisorConfig{}, supervisorInvalid("projects.verify_command", "verify_command_inside_repo", nil)
			}
			b, e := os.ReadFile(real)
			if e != nil {
				return SupervisorConfig{}, supervisorInvalid("projects.verify_command", "file_unreadable", e)
			}
			sum := sha256.Sum256(b)
			p.VerifySHA256 = fmt.Sprintf("%x", sum)
		}
		if p.ParallelInstruction == "" {
			// TOML metadata below distinguishes omitted from explicitly empty.
			if raw.Projects[i].ParallelInstruction == nil {
				p.ParallelInstruction = supervisorDefaultParallelInstruction
			} else {
				return SupervisorConfig{}, supervisorInvalid("projects.parallel_instruction", "missing_required", nil)
			}
		}
		p.ParallelInstruction = normalizeInstructionWidth(p.ParallelInstruction)
		if e := validateParallelInstruction(p.ParallelInstruction); e != nil {
			return SupervisorConfig{}, e
		}
		for _, op := range p.OwnedPaths {
			if filepath.IsAbs(op) || op == "" {
				return SupervisorConfig{}, supervisorInvalid("projects.owned_paths", "relative_path", nil)
			}
			if containsPathTraversal(op) || filepath.Clean(op) != op {
				return SupervisorConfig{}, supervisorInvalid("projects.owned_paths", "not_normalized", nil)
			}
			resolvedOwned := filepath.Join(p.CWD, op)
			resolvedOwned, ownedErr := evalNearestPath(resolvedOwned)
			if ownedErr != nil || !pathWithin(resolvedOwned, p.CWD) {
				return SupervisorConfig{}, supervisorInvalid("projects.owned_paths", "symlink_escape", ownedErr)
			}
		}
	}
	allowlistEntries := make(map[SupervisorPushAllow]struct{}, len(c.PushAllowlist))
	for _, a := range c.PushAllowlist {
		if _, exists := allowlistEntries[a]; exists {
			return SupervisorConfig{}, supervisorInvalid("push_allowlist", "duplicate_allowlist_entry", nil)
		}
		allowlistEntries[a] = struct{}{}
		if !supervisorRemotePattern.MatchString(a.Remote) {
			return SupervisorConfig{}, supervisorInvalid("push_allowlist.remote", "type_mismatch", nil)
		}
		if a.URL == "" {
			return SupervisorConfig{}, supervisorInvalid("push_allowlist.url", "type_mismatch", nil)
		}
		if !validBranch(a.Branch) {
			return SupervisorConfig{}, supervisorInvalid("push_allowlist.branch", "type_mismatch", nil)
		}
		if containsExactString(c.ProtectedBranches, a.Branch) {
			return SupervisorConfig{}, supervisorInvalid("push_allowlist.branch", "protected_branch_in_allowlist", nil)
		}
	}
	return c, nil
}
func supervisorInvalid(key, reason string, err error) error {
	return &LoadError{Key: key, Reason: "SUP_CONFIG_INVALID " + reason, Err: err}
}
func isExecutableFile(p string) error {
	i, e := os.Stat(p)
	if e != nil {
		return e
	}
	if !i.Mode().IsRegular() || i.Mode().Perm()&0111 == 0 {
		return fmt.Errorf("not an executable file")
	}
	return nil
}
func validBranch(s string) bool {
	return s != "" && supervisorBranchPattern.MatchString(s) && !strings.Contains(s, "..") && !strings.Contains(s, "//") && !strings.HasSuffix(s, "/") && !strings.HasSuffix(s, ".")
}
func containsExactString(a []string, s string) bool {
	for _, v := range a {
		if v == s {
			return true
		}
	}
	return false
}
func pathWithin(path, root string) bool {
	r, e := filepath.Rel(root, path)
	return e == nil && (r == "." || r != ".." && !strings.HasPrefix(r, ".."+string(filepath.Separator)))
}

func containsPathTraversal(path string) bool {
	for _, component := range strings.Split(filepath.ToSlash(path), "/") {
		if component == ".." {
			return true
		}
	}
	return false
}

func gitRepositoryRoot(path string) (string, bool) {
	for current := path; ; current = filepath.Dir(current) {
		if info, err := os.Stat(filepath.Join(current, ".git")); err == nil {
			if info.IsDir() || info.Mode().IsRegular() {
				resolved, err := filepath.EvalSymlinks(current)
				return resolved, err == nil
			}
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", false
		}
	}
}

func validateParallelInstruction(s string) error {
	s = normalizeInstructionWidth(s)
	if s == "" || strings.ContainsRune("!/#", []rune(s)[0]) {
		return supervisorInvalid("projects.parallel_instruction", "instruction_prefix", nil)
	}
	if strings.ContainsFunc(s, isInstructionControlOrFormat) {
		return supervisorInvalid("projects.parallel_instruction", "instruction_control_char", nil)
	}
	if len([]rune(s)) > supervisorInstructionMaxLength || textpolicy.DisplayWidth(s) > supervisorPTYColumnsDefault-10 {
		return supervisorInvalid("projects.parallel_instruction", "instruction_too_wide", nil)
	}
	if !textpolicy.ValidateParallelInstruction(s, supervisorInstructionMaxLength, supervisorPTYColumnsDefault-10) {
		return supervisorInvalid("projects.parallel_instruction", "instruction_forbidden", nil)
	}
	return nil
}

func normalizeInstructionWidth(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 0xff01 && r <= 0xff5e {
			return r - 0xfee0
		}
		if r == 0x3000 {
			return ' '
		}
		return r
	}, s)
}

func isInstructionControlOrFormat(r rune) bool {
	return r < 0x20 || r >= 0x7f && r <= 0x9f || r >= 0x200b && r <= 0x200f ||
		r >= 0x202a && r <= 0x202e || r >= 0x2060 && r <= 0x2064 || r == 0xfeff
}

func validatedSupervisorHome() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil || home == "" || !filepath.IsAbs(home) {
		if err == nil {
			err = fmt.Errorf("HOME must be an absolute path")
		}
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(home)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func tomlErrorKey(err error) string {
	message := err.Error()
	if start := strings.LastIndex(message, `last key "`); start >= 0 {
		keyText := message[start+len(`last key "`):]
		if end := strings.IndexByte(keyText, '"'); end >= 0 && keyText[:end] != "" {
			return keyText[:end]
		}
	}
	if start := strings.Index(message, " (last key "); start >= 0 {
		keyText := strings.Trim(message[start+len(" (last key "):], ")\" ")
		if keyText != "" {
			return keyText
		}
	}
	return "config_path"
}

func ownedByCurrentUserStrict(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && int(stat.Uid) == os.Getuid()
}
func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && (int(stat.Uid) == os.Getuid() || stat.Uid == 0)
}
func executableConfigReason(err error) string {
	var validationErr *proc.ExecutableValidationError
	if !errors.As(err, &validationErr) {
		return "file_unreadable"
	}
	switch validationErr.Reason {
	case "not_owned":
		return "not_owned"
	case "not_absolute":
		return "relative_path"
	case "not_found":
		if errors.Is(validationErr.Err, fs.ErrNotExist) {
			return "not_executable"
		}
		return "file_unreadable"
	default:
		return "not_executable"
	}
}

func checkSupervisorPathConflicts(c SupervisorConfig) error {
	home, err := validatedSupervisorHome()
	if err != nil {
		return supervisorInvalid("HOME", "missing_required", err)
	}
	bases := []string{filepath.Join(home, ".claude", "run", "codexd.sock"), filepath.Join(home, ".claude", "run", "codexd.lock"), filepath.Join(home, ".claude", "run", "codexd-supervisor.lock"), filepath.Join(home, ".claude", "run", "path-locks.lock"), filepath.Join(home, ".claude", "run", "log-eviction.lock"), filepath.Join(home, ".claude", "run", "path-locks"), filepath.Join(home, ".claude", "codexd", "config.toml"), filepath.Join(home, ".claude", "codexd", "supervisor.toml"), defaultTaskPlacementRoot, filepath.Join(home, ".codex-worktrees-cli"), filepath.Join(home, ".claude", "logs")}
	paths := []struct{ key, path string }{{"socket_path", c.SocketPath}, {"state_dir", c.StateDir}}
	for _, entry := range paths {
		actual, e := evalNearestPath(entry.path)
		if e != nil {
			return supervisorInvalid(entry.key, "file_unreadable", e)
		}
		if entry.key == "state_dir" && pathWithin(actual, filepath.Join(home, ".claude", "run")) {
			return supervisorPathConflict("state_dir", "under_run_dir")
		}
		for _, base := range bases {
			resolved, e := evalNearestPath(base)
			if e != nil {
				continue
			}
			if actual == resolved {
				return supervisorPathConflict(entry.key, "same_path")
			}
			if pathWithin(actual, resolved) {
				return supervisorPathConflict(entry.key, "contained_by")
			}
			if pathWithin(resolved, actual) {
				return supervisorPathConflict(entry.key, "contains")
			}
		}
	}
	if pathWithin(c.SocketPath, c.StateDir) || pathWithin(c.StateDir, c.SocketPath) {
		if c.SocketPath == c.StateDir {
			return supervisorPathConflict("state_dir", "same_path")
		}
		if pathWithin(c.SocketPath, c.StateDir) {
			return supervisorPathConflict("socket_path", "contained_by")
		}
		return supervisorPathConflict("socket_path", "contains")
	}
	return nil
}

func supervisorPathConflict(key, reason string) error {
	return &LoadError{Key: key, Reason: "SUP_CONFIG_PATH_CONFLICT " + reason}
}

func evalNearestPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	suffix := []string{}
	current := abs
	for {
		if _, err = os.Lstat(current); err == nil {
			resolved, e := filepath.EvalSymlinks(current)
			if e != nil {
				return "", e
			}
			for i := len(suffix) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, suffix[i])
			}
			return filepath.Clean(resolved), nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", err
		}
		suffix = append(suffix, filepath.Base(current))
		current = parent
	}
}
func knownSupervisorKey(key string) bool {
	if strings.HasPrefix(key, "projects.") {
		field := key[strings.LastIndex(key, ".")+1:]
		return field == "id" || field == "cwd" || field == "cli" || field == "model" || field == "owned_paths" || field == "verify_command" || field == "parallel_instruction"
	}
	if strings.HasPrefix(key, "push_allowlist.") {
		field := key[strings.LastIndex(key, ".")+1:]
		return field == "remote" || field == "url" || field == "branch"
	}
	for _, k := range []string{"socket_path", "state_dir", "claude_binary_path", "codex_binary_path", "max_sessions", "notify_command", "commit_author_name", "commit_author_email", "protected_branches", "git_pass_ssh_auth_sock", "push_allowlist", "projects"} {
		if key == k {
			return true
		}
	}
	return false
}

func emptySupervisorConfig() SupervisorConfig {
	home, err := validatedSupervisorHome()
	if err != nil {
		return SupervisorConfig{}
	}
	return SupervisorConfig{SocketPath: filepath.Join(home, ".claude", "run", "codexd-supervisor.sock"), StateDir: filepath.Join(home, ".claude", "codexd", "supervisor"), MaxSessions: supervisorDefaultMaxSessions, ProtectedBranches: append([]string(nil), supervisorProtectedDefaults...), GitPassSSHAuthSock: true, Projects: []SupervisorProject{}}
}
