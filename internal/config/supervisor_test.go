package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadSupervisorConfigRejectsInvalid(t *testing.T) {
	project := t.TempDir()
	inside := filepath.Join(project, "verify")
	if err := os.WriteFile(inside, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "verify")
	if err := os.WriteFile(outside, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, extra, reason string
	}{
		{"unknown key", "unknown = 1\n", "unknown_key"},
		{"relative socket", "socket_path = \"relative\"\n", "relative_path"},
		{"missing author", "", "commit_author_missing"},
		{"codex cli", strings.Replace(projectBlock(project, ""), `cli = "claude"`, `cli = "codex"`, 1), "cli_not_supported"},
		{"claude model", projectBlock(project, `model = "x"`), "model_not_allowed_for_claude"},
		{"max sessions range", "max_sessions = 0\n", "out_of_range"},
		{"relative cwd", projectBlock("relative", ""), "relative_path"},
		{"verify command inside repo", projectBlockWithVerify(project, inside), "verify_command_inside_repo"},
		{"verify command missing", projectBlockWithVerify(project, filepath.Join(project, "missing")), "not_executable"},
		{"bad push remote", "[[push_allowlist]]\nremote = \"bad remote\"\nurl = \"https://example.test/r\"\nbranch = \"feature\"\n", "type_mismatch"},
		{"push protected branch", "[[push_allowlist]]\nremote = \"origin\"\nurl = \"https://example.test/r\"\nbranch = \"main\"\n", "protected_branch_in_allowlist"},
		{"instruction prefix", projectBlock(project, `parallel_instruction = "#start"`), "instruction_prefix"},
		{"instruction too long", projectBlock(project, `parallel_instruction = "`+strings.Repeat("a", supervisorInstructionMaxLength+1)+`"`), "instruction_too_wide"},
		{"instruction forbidden", projectBlock(project, `parallel_instruction = "please deploy this"`), "instruction_forbidden"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", canonicalTestPath(t, t.TempDir()))
			body := validSupervisorBase(t)
			if tc.name == "missing author" {
				body = strings.ReplaceAll(body, "commit_author_name = \"Test Author\"\n", "") + projectBlock(t.TempDir(), "")
			} else {
				body = addRootSetting(body, tc.extra)
			}
			if err := loadSupervisorText(t, body); err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("error=%v, want reason %q", err, tc.reason)
			}
		})
	}
}

func TestValidBranchRejectsRefsAndSpecialNames(t *testing.T) {
	for _, branch := range []string{"refs/heads/x", "refs/tags/x", "HEAD", "has\ttab", "has\x01control", "has\x7fdel"} {
		if validBranch(branch) {
			t.Errorf("accepted %q", branch)
		}
	}
	for _, branch := range []string{"topic", "release/v2"} {
		if !validBranch(branch) {
			t.Errorf("rejected %q", branch)
		}
	}
}

func TestLoadSupervisorConfigRejectsAmbiguousBranchAndInvalidRemoteNames(t *testing.T) {
	for _, remote := range []string{"a/b", "o$rigin", "orígin", "o#r", "tab\tname", ".", "..", "-bad"} {
		t.Run("remote_"+remote, func(t *testing.T) {
			t.Setenv("HOME", canonicalTestPath(t, t.TempDir()))
			body := addRootSetting(validSupervisorBase(t), "[[push_allowlist]]\nremote = \""+remote+"\"\nurl = \"https://example.test/other\"\nbranch = \"other\"\n")
			if err := loadSupervisorText(t, body); err == nil || !strings.Contains(err.Error(), "type_mismatch") {
				t.Fatalf("error=%v", err)
			}
		})
	}
	t.Setenv("HOME", canonicalTestPath(t, t.TempDir()))
	body := validSupervisorBase(t) + "[[push_allowlist]]\nremote = \"backup\"\nurl = \"https://example.test/other\"\nbranch = \"feature\"\n"
	if err := loadSupervisorText(t, body); err == nil || !strings.Contains(err.Error(), "duplicate_allowlist_entry") {
		t.Fatalf("error=%v, want duplicate_allowlist_entry", err)
	}
}

func TestSupervisorConfigRejectsEveryCanonicalReason(t *testing.T) {
	home := canonicalTestPath(t, t.TempDir())
	t.Setenv("HOME", home)
	project := t.TempDir()
	verify := filepath.Join(t.TempDir(), "verify")
	if err := os.WriteFile(verify, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	type testCase struct {
		word, key, category string
		body                func() string
	}
	root := validSupervisorBase(t)
	projectBody := func(extra string) string { return root + projectBlock(project, extra) }
	conflict := func(socket, state string) string {
		return addRootSetting(root, "socket_path = \""+socket+"\"\nstate_dir = \""+state+"\"\n")
	}
	conflictHome := filepath.Join(home, ".claude", "run", "codexd.sock")
	cases := []testCase{
		{"unknown_key", "unknown", "SUP_CONFIG_INVALID", func() string { return addRootSetting(root, "unknown = 1\n") }},
		{"type_mismatch", "max_sessions", "SUP_CONFIG_INVALID", func() string { return addRootSetting(root, "max_sessions = \"many\"\n") }},
		{"out_of_range", "max_sessions", "SUP_CONFIG_INVALID", func() string { return addRootSetting(root, "max_sessions = 0\n") }},
		{"missing_required", "claude_binary_path", "SUP_CONFIG_INVALID", func() string {
			return strings.Replace(root, "claude_binary_path = \"/bin/sh\"\n", "", 1) + projectBlock(project, "")
		}},
		{"relative_path", "projects.cwd", "SUP_CONFIG_INVALID", func() string { return root + projectBlock("relative", "") }},
		{"not_normalized", "projects.cwd", "SUP_CONFIG_INVALID", func() string {
			return root + "[[projects]]\nid = \"test-project\"\ncwd = \"" + project + "/.\"\ncli = \"claude\"\nowned_paths = [\"src\"]\n"
		}},
		{"not_executable", "projects.verify_command", "SUP_CONFIG_INVALID", func() string {
			f := filepath.Join(t.TempDir(), "noexec")
			_ = os.WriteFile(f, []byte("text"), 0o600)
			return root + projectBlockWithVerify(project, f)
		}},
		{"not_owned", "socket_path", "SUP_CONFIG_INVALID", func() string { return root }},
		{"symlink_escape", "projects.owned_paths", "SUP_CONFIG_INVALID", func() string {
			outside := t.TempDir()
			_ = os.Symlink(outside, filepath.Join(project, "escape"))
			return root + strings.Replace(projectBlock(project, ""), "owned_paths = [\"src\"]", "owned_paths = [\"escape\"]", 1)
		}},
		{"file_unreadable", "socket_path", "SUP_CONFIG_INVALID", func() string {
			p := filepath.Join(t.TempDir(), "link")
			_ = os.Symlink(filepath.Join(t.TempDir(), "absent"), p)
			return addRootSetting(root, "socket_path = \""+p+"\"\n")
		}},
		{"cli_not_supported", "projects.cli", "SUP_CONFIG_INVALID", func() string {
			return root + strings.Replace(projectBlock(project, ""), `cli = "claude"`, `cli = "codex"`, 1)
		}},
		{"model_not_allowed_for_claude", "projects.model", "SUP_CONFIG_INVALID", func() string { return projectBody("model = \"x\"\n") }},
		{"reserved_in_v1", "codex_binary_path", "SUP_CONFIG_INVALID", func() string { return addRootSetting(root, "codex_binary_path = \"/bin/sh\"\n") }},
		{"instruction_prefix", "projects.parallel_instruction", "SUP_CONFIG_INVALID", func() string { return projectBody("parallel_instruction = \"#bad\"\n") }},
		{"instruction_control_char", "projects.parallel_instruction", "SUP_CONFIG_INVALID", func() string { return projectBody("parallel_instruction = \"bad\\nline\"\n") }},
		{"instruction_too_wide", "projects.parallel_instruction", "SUP_CONFIG_INVALID", func() string { return projectBody("parallel_instruction = \"" + strings.Repeat("界", 100) + "\"\n") }},
		{"instruction_forbidden", "projects.parallel_instruction", "SUP_CONFIG_INVALID", func() string { return projectBody("parallel_instruction = \"please deploy this\"\n") }},
		{"protected_branch_in_allowlist", "push_allowlist.branch", "SUP_CONFIG_INVALID", func() string {
			return addRootSetting(root, "[[push_allowlist]]\nremote = \"origin\"\nurl = \"https://example.test/r\"\nbranch = \"main\"\n")
		}},
		{"duplicate_project_id", "projects.id", "SUP_CONFIG_INVALID", func() string { return root + projectBlock(project, "") + projectBlock(project, "") }},
		{"duplicate_allowlist_entry", "push_allowlist", "SUP_CONFIG_INVALID", func() string {
			return root + "[[push_allowlist]]\nremote = \"origin\"\nurl = \"https://example.test/r\"\nbranch = \"feature\"\n"
		}},
		{"verify_command_inside_repo", "projects.verify_command", "SUP_CONFIG_INVALID", func() string {
			_ = os.Mkdir(filepath.Join(project, ".git"), 0o700)
			in := filepath.Join(project, "verify")
			_ = os.WriteFile(in, []byte("#!/bin/sh\n"), 0o700)
			return root + projectBlockWithVerify(project, in)
		}},
		{"shared_repository", "projects.cwd", "SUP_CONFIG_INVALID", func() string {
			repo := t.TempDir()
			_ = os.Mkdir(filepath.Join(repo, ".git"), 0o700)
			a := filepath.Join(repo, "a")
			b := filepath.Join(repo, "b")
			_ = os.Mkdir(a, 0o700)
			_ = os.Mkdir(b, 0o700)
			return root + projectBlock(a, "") + strings.Replace(projectBlock(b, ""), "id = \"test-project\"", "id = \"second-project\"", 1)
		}},
		{"commit_author_missing", "commit_author_name", "SUP_CONFIG_INVALID", func() string {
			return strings.Replace(root, "commit_author_name = \"Test Author\"\n", "", 1) + projectBlock(project, "")
		}},
		{"same_path", "socket_path", "SUP_CONFIG_PATH_CONFLICT", func() string { return conflict(conflictHome, conflictHome) }},
		{"contains", "socket_path", "SUP_CONFIG_PATH_CONFLICT", func() string {
			return conflict(filepath.Join(home, "container"), filepath.Join(home, "container", "child"))
		}},
		{"contained_by", "socket_path", "SUP_CONFIG_PATH_CONFLICT", func() string {
			return conflict(filepath.Join(home, "container", "child"), filepath.Join(home, "container"))
		}},
		{"under_run_dir", "state_dir", "SUP_CONFIG_PATH_CONFLICT", func() string {
			return conflict(filepath.Join(home, "socket"), filepath.Join(home, ".claude", "run", "state"))
		}},
	}
	seen := make(map[string]bool, len(cases))
	for _, tc := range cases {
		t.Run(tc.word, func(t *testing.T) {
			body := tc.body()
			if tc.word == "not_owned" {
				// A root-owned path cannot be reliably created in an unprivileged test; ownership is verified via the replaceable predicate.
				old := supervisorOwnsPath
				supervisorOwnsPath = func(os.FileInfo) bool { return false }
				t.Cleanup(func() { supervisorOwnsPath = old })
				p := filepath.Join(home, ".claude", "codexd", "owned-socket")
				_ = os.MkdirAll(filepath.Dir(p), 0o700)
				_ = os.WriteFile(p, nil, 0o600)
				body = addRootSetting(body, "socket_path = \""+p+"\"\n")
			}
			err := loadSupervisorText(t, body)
			var loadErr *LoadError
			if !errors.As(err, &loadErr) || !strings.Contains(err.Error(), tc.category) || !strings.Contains(err.Error(), tc.word) || loadErr.Key != tc.key {
				t.Fatalf("error=%v, want category=%s reason=%s key=%s", err, tc.category, tc.word, tc.key)
			}
			seen[tc.word] = true
		})
	}
	words := []string{"unknown_key", "type_mismatch", "out_of_range", "missing_required", "relative_path", "not_normalized", "not_executable", "not_owned", "symlink_escape", "file_unreadable", "cli_not_supported", "model_not_allowed_for_claude", "reserved_in_v1", "instruction_prefix", "instruction_control_char", "instruction_too_wide", "instruction_forbidden", "protected_branch_in_allowlist", "duplicate_project_id", "duplicate_allowlist_entry", "verify_command_inside_repo", "shared_repository", "commit_author_missing", "same_path", "contains", "contained_by", "under_run_dir"}
	for _, word := range words {
		if !seen[word] {
			t.Errorf("canonical reason %q had no executed case", word)
		}
	}
}

func TestSupervisorConfigRejectsStickyWritableNotifyParent(t *testing.T) {
	t.Setenv("HOME", canonicalTestPath(t, t.TempDir()))
	dir := filepath.Join(t.TempDir(), "sticky")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o1777); err != nil {
		t.Fatal(err)
	}
	command := filepath.Join(dir, "notify")
	if err := os.WriteFile(command, []byte("#!/bin/sh\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	body := validSupervisorBase(t)
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "notify_command = ") {
			body = strings.Replace(body, line, "notify_command = \""+command+"\"", 1)
			break
		}
	}
	if err := loadSupervisorText(t, body); err == nil || !strings.Contains(err.Error(), "not_executable") {
		t.Fatalf("error=%v, want not_executable", err)
	}
}

func TestSupervisorConfigRejectsProbeCases(t *testing.T) {
	home := canonicalTestPath(t, t.TempDir())
	t.Setenv("HOME", home)
	for _, tc := range []struct{ name, setting, want string }{
		{"relative notify command", "notify_command = \"relative-notify\"\n", "notify_command"},
		{"case variant key", "Socket_Path = \"relative\"\n", "unknown_key"},
		{"state path collision", "state_dir = \"/private/tmp/codex-tasks/sup\"\n", "SUP_CONFIG_PATH_CONFLICT"},
		{"lock path collision", "socket_path = \"" + filepath.Join(home, ".claude/run/codexd-supervisor.lock") + "\"\n", "SUP_CONFIG_PATH_CONFLICT"},
		{"unnormalized state path", "state_dir = \"" + home + "/.claude/codexd/supervisor/../supervisor\"\n", "not_normalized"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := addRootSetting(validSupervisorBase(t), tc.setting)
			err := loadSupervisorText(t, body)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want %s", err, tc.want)
			}
		})
	}
}

func TestLoadSupervisorConfigDefaultsAndAllKeys(t *testing.T) {
	home := canonicalTestPath(t, t.TempDir())
	t.Setenv("HOME", home)
	project := t.TempDir()
	verify := filepath.Join(t.TempDir(), "verify")
	if err := os.WriteFile(verify, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(home, ".claude", "codexd", "supervisor.toml")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	body := validSupervisorBase(t)
	body = addRootSetting(body, "socket_path = \""+filepath.Join(home, ".claude", "run", "codexd-supervisor.sock")+"\"\nstate_dir = \""+filepath.Join(home, ".claude", "codexd", "supervisor")+"\"\nprotected_branches = [\"release\"]\n")
	if err := os.WriteFile(path, []byte(body+projectBlockWithVerify(project, verify)), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadSupervisorConfig("")
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxSessions != supervisorDefaultMaxSessions || len(c.ProtectedBranches) != 4 || c.SocketPath != filepath.Join(home, ".claude", "run", "codexd-supervisor.sock") || c.StateDir != filepath.Join(home, ".claude", "codexd", "supervisor") || !c.GitPassSSHAuthSock || c.ClaudeBinaryPath != "/bin/sh" || !filepath.IsAbs(c.NotifyCommand) || c.NotifyCommand == "none" {
		t.Fatalf("defaults not applied: %+v", c)
	}
	if len(c.Projects) != 1 || len(c.Projects[0].VerifySHA256) != 64 || c.Projects[0].CLI != "claude" || c.Projects[0].CWD != canonicalTestPath(t, project) || len(c.Projects[0].OwnedPaths) != 1 || c.Projects[0].OwnedPaths[0] != "src" || c.Projects[0].ParallelInstruction != supervisorDefaultParallelInstruction {
		t.Fatalf("all-key config not resolved: %+v", c.Projects)
	}
}

func TestLoadSupervisorConfigRejectsPathConflicts(t *testing.T) {
	home := canonicalTestPath(t, t.TempDir())
	t.Setenv("HOME", home)
	for _, path := range []string{
		filepath.Join(home, ".claude", "run", "codexd.sock"),
		filepath.Join(home, ".claude", "run", "codexd.lock"),
		filepath.Join(home, ".claude", "run", "path-locks.lock"),
		filepath.Join(home, ".claude", "run", "log-eviction.lock"),
		filepath.Join(home, ".claude", "run", "path-locks"),
		filepath.Join(home, ".claude", "codexd", "config.toml"),
		filepath.Join(home, ".claude", "codexd", "supervisor.toml"),
		filepath.Join(string(filepath.Separator), "private", "tmp", "codex-tasks"),
		filepath.Join(home, ".codex-worktrees-cli"),
		filepath.Join(home, ".claude", "logs"),
	} {
		t.Run(path, func(t *testing.T) {
			body := addRootSetting(validSupervisorBase(t), "socket_path = \""+path+"\"\n")
			err := loadSupervisorText(t, body)
			if err == nil || !strings.Contains(err.Error(), "SUP_CONFIG_PATH_CONFLICT") {
				t.Fatalf("error=%v, want SUP_CONFIG_PATH_CONFLICT", err)
			}
		})
	}
}

func TestLoadSupervisorConfigRejectsSharedRepository(t *testing.T) {
	t.Setenv("HOME", canonicalTestPath(t, t.TempDir()))
	repo := t.TempDir()
	if err := os.Mkdir(filepath.Join(repo, ".git"), 0o700); err != nil {
		t.Fatal(err)
	}
	first := filepath.Join(repo, "one")
	second := filepath.Join(repo, "two")
	if err := os.Mkdir(first, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(second, 0o700); err != nil {
		t.Fatal(err)
	}
	body := validSupervisorBase(t) + projectBlock(first, "") + strings.Replace(projectBlock(second, ""), "id = \"test-project\"", "id = \"second-project\"", 1)
	err := loadSupervisorText(t, body)
	if err == nil || !strings.Contains(err.Error(), "shared_repository") {
		t.Fatalf("error=%v, want shared_repository", err)
	}
}

func validSupervisorBase(t *testing.T) string {
	t.Helper()
	notify := filepath.Join(t.TempDir(), "notify")
	if err := os.WriteFile(notify, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	return "claude_binary_path = \"/bin/sh\"\nnotify_command = \"" + notify + "\"\n" +
		"commit_author_name = \"Test Author\"\ncommit_author_email = \"test@example.test\"\n" +
		"[[push_allowlist]]\nremote = \"origin\"\nurl = \"https://example.test/r\"\nbranch = \"feature\"\n"
}

func addRootSetting(body, setting string) string {
	if setting == "" {
		return body
	}
	if strings.HasPrefix(setting, "[[projects]]") {
		return body + setting
	}
	if strings.HasPrefix(setting, "[[push_allowlist]]") {
		return body + setting
	}
	return strings.Replace(body, "[[push_allowlist]]", setting+"[[push_allowlist]]", 1)
}

func projectBlock(cwd, extra string) string {
	if resolved, err := filepath.EvalSymlinks(cwd); err == nil {
		cwd = resolved
	}
	return projectBlockWithVerify(cwd, "") + extra + "\n"
}
func projectBlockWithVerify(cwd, verify string) string {
	if resolved, err := filepath.EvalSymlinks(cwd); err == nil {
		cwd = resolved
	}
	if verify != "" {
		if resolved, err := filepath.EvalSymlinks(verify); err == nil {
			verify = resolved
		}
	}
	block := "[[projects]]\nid = \"test-project\"\ncwd = \"" + cwd + "\"\ncli = \"claude\"\nowned_paths = [\"src\"]\n"
	if verify != "" {
		block += "verify_command = [\"" + verify + "\"]\n"
	}
	return block
}

func canonicalTestPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func loadSupervisorText(t *testing.T, body string) error {
	t.Helper()
	path := filepath.Join(t.TempDir(), "supervisor.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadSupervisorConfig(path)
	return err
}

func TestMissingSupervisorConfigWaitsWithNoProjects(t *testing.T) {
	home := canonicalTestPath(t, t.TempDir())
	t.Setenv("HOME", home)
	c, err := LoadSupervisorConfig("")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Projects) != 0 {
		t.Fatalf("projects=%d", len(c.Projects))
	}
}

func TestLoadSupervisorConfigRejectsMissingHome(t *testing.T) {
	old, had := os.LookupEnv("HOME")
	if err := os.Unsetenv("HOME"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv("HOME", old)
		} else {
			_ = os.Unsetenv("HOME")
		}
	})
	if _, err := LoadSupervisorConfig(filepath.Join(t.TempDir(), "supervisor.toml")); err == nil {
		t.Fatal("configuration accepted without HOME")
	}
}

func TestParallelInstructionRejectsControlAndAcceptsDefault(t *testing.T) {
	if err := validateParallelInstruction(supervisorDefaultParallelInstruction); err != nil {
		t.Fatalf("default instruction rejected: %v", err)
	}
	for _, value := range []string{"line\nbreak", "tab\tcontrol", "!command", "/command", "#command", "open a PR", "ｄｅｐｌｏｙ", "git push origin +main"} {
		if err := validateParallelInstruction(value); err == nil {
			t.Errorf("accepted %q", value)
		}
	}
}

func TestSupervisorMeasuredValidationFailures(t *testing.T) {
	home := canonicalTestPath(t, t.TempDir())
	t.Setenv("HOME", home)
	project := canonicalTestPath(t, t.TempDir())
	allowed := map[string]bool{
		"unknown_key": true, "type_mismatch": true, "out_of_range": true, "missing_required": true,
		"relative_path": true, "not_normalized": true, "not_executable": true, "not_owned": true,
		"symlink_escape": true, "file_unreadable": true, "cli_not_supported": true,
		"model_not_allowed_for_claude": true, "reserved_in_v1": true, "instruction_prefix": true,
		"instruction_control_char": true, "instruction_too_wide": true, "instruction_forbidden": true,
		"protected_branch_in_allowlist": true, "duplicate_project_id": true, "duplicate_allowlist_entry": true,
		"verify_command_inside_repo": true, "shared_repository": true, "commit_author_missing": true,
	}
	check := func(name, body, key, reason string) {
		t.Helper()
		t.Run(name, func(t *testing.T) {
			err := loadSupervisorText(t, body)
			var loadErr *LoadError
			if !errors.As(err, &loadErr) || loadErr.Key != key || !strings.Contains(loadErr.Reason, reason) {
				t.Fatalf("error=%v, want key=%q reason=%q", err, key, reason)
			}
			if !allowed[reason] {
				t.Fatalf("noncanonical reason returned: %q", reason)
			}
		})
	}
	root := validSupervisorBase(t)
	if err := loadSupervisorText(t, addRootSetting(root, "protected_branches = []\n")); err != nil {
		t.Fatalf("empty protected branches should retain defaults: %v", err)
	}
	t.Setenv("HOME", "relative-home")
	if _, err := LoadSupervisorConfig(""); err == nil || !strings.Contains(err.Error(), "missing_required") || !strings.Contains(err.Error(), "HOME") {
		t.Fatal("accepted relative HOME")
	}
	t.Setenv("HOME", home)
	if err := loadSupervisorText(t, root+projectBlock("/usr", "")); err == nil || !strings.Contains(err.Error(), "not_owned") {
		t.Errorf("cwd outside the user's ownership was accepted: %v", err)
	}
	check("missing id", root+strings.Replace(projectBlock(project, ""), "id = \"test-project\"\n", "", 1), "projects.id", "missing_required")
	check("missing cli", root+strings.Replace(projectBlock(project, ""), "cli = \"claude\"\n", "", 1), "projects.cli", "missing_required")
	check("owned traversal", root+strings.Replace(projectBlock(project, ""), "owned_paths = [\"src\"]", "owned_paths = [\"../x\"]", 1), "projects.owned_paths", "not_normalized")
	for _, value := range []string{"\u0085", "\u202e", "\u200b"} {
		if err := validateParallelInstruction("ok" + value); err == nil || !strings.Contains(err.Error(), "instruction_control_char") {
			t.Errorf("validateParallelInstruction(%q) error=%v", value, err)
		}
	}
	if err := validateParallelInstruction("！command"); err == nil || !strings.Contains(err.Error(), "instruction_prefix") {
		t.Errorf("full-width prefix error=%v", err)
	}
	// Same working directory is invalid even when it is outside a git repository.
	first := projectBlock(project, "")
	second := strings.Replace(projectBlock(project, ""), "id = \"test-project\"", "id = \"second-project\"", 1)
	check("same non-git cwd", root+first+second, "projects.cwd", "shared_repository")
}

func TestSupervisorFileAndFieldErrorsUseCanonicalKeys(t *testing.T) {
	home := canonicalTestPath(t, t.TempDir())
	t.Setenv("HOME", home)
	root := validSupervisorBase(t)
	for _, mode := range []string{"directory", "mode_000"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "supervisor.toml")
			if mode == "directory" {
				if err := os.Mkdir(path, 0o700); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := os.WriteFile(path, []byte(root), 0o000); err != nil {
					t.Fatal(err)
				}
			}
			_, err := LoadSupervisorConfig(path)
			var loadErr *LoadError
			if !errors.As(err, &loadErr) || loadErr.Key != "config_path" || !strings.Contains(loadErr.Reason, "file_unreadable") {
				t.Fatalf("error=%v, want file_unreadable at config_path", err)
			}
		})
	}
	stateFile := filepath.Join(home, "state-file")
	if err := os.WriteFile(stateFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	err := loadSupervisorText(t, addRootSetting(root, "state_dir = \""+stateFile+"\"\n"))
	if err == nil || !strings.Contains(err.Error(), `key "state_dir": SUP_CONFIG_INVALID file_unreadable`) {
		t.Fatalf("regular-file state_dir error=%v", err)
	}
	err = loadSupervisorText(t, addRootSetting(root, "socket_path = 42\n"))
	var loadErr *LoadError
	if !errors.As(err, &loadErr) || loadErr.Key != "socket_path" || !strings.Contains(loadErr.Reason, "type_mismatch") {
		t.Fatalf("wrong socket_path type error=%v", err)
	}
}
