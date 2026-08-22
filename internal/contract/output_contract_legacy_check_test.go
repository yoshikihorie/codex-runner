//go:build legacycheck

package contract_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func TestOutputContractLegacyCheck(t *testing.T) {
	dir := os.Getenv("CODEX_GOLDEN_LEGACY_TASK_DIR")
	if dir == "" || !filepath.IsAbs(dir) {
		t.Fatal("CODEX_GOLDEN_LEGACY_TASK_DIR must be an absolute completed task directory")
	}
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		t.Fatalf("legacy task directory is invalid: %v", err)
	}
	id, err := domain.NewTaskID(filepath.Base(dir))
	if err != nil || id.String() != filepath.Base(dir) {
		t.Fatal("legacy task directory basename is not a valid task ID")
	}
	_, inputErr := os.Lstat(filepath.Join(dir, "input.txt"))
	_, combinedErr := os.Lstat(filepath.Join(dir, "combined-prompt.md"))
	_, recoveredErr := os.Lstat(filepath.Join(dir, "recovered-after-timeout"))
	_, partialErr := os.Lstat(filepath.Join(dir, "partial-output.md"))
	scenario := ""
	if inputErr == nil || combinedErr == nil {
		scenario = "review-normal"
	} else if recoveredErr == nil {
		scenario = "research-recovered"
	} else if partialErr == nil {
		scenario = "research-recovery-failed"
	} else {
		b, e := os.ReadFile(filepath.Join(dir, "exit-code"))
		if e == nil && strings.TrimSpace(string(b)) == "0" {
			scenario = "research-normal"
		}
	}
	if scenario == "" {
		b, _ := os.ReadFile(filepath.Join(dir, "exit-code"))
		t.Skipf("legacy task %s is outside the four scenarios (exit-code=%s)", id.String(), strings.TrimSpace(string(b)))
	}
	m, err := loadManifest(scenario)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(id.String(), m.SubcommandFamily+"-") {
		t.Fatalf("task ID %s does not match %s", id.String(), m.SubcommandFamily)
	}
	baseline := map[string][]byte{}
	for _, f := range m.Files {
		if f.Class == "once" {
			if b, e := os.ReadFile(filepath.Join(dir, f.Name)); e == nil {
				baseline[f.Name] = b
			}
		}
	}
	if err := verifyOutputDirectory(dir, m, baseline); err != nil {
		t.Fatal(err)
	}
}
