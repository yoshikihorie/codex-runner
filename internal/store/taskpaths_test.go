package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

func pathTaskID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260806-120000-a1b2-paths")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestTaskPathsResolveContractFileNames(t *testing.T) {
	root, id := t.TempDir(), pathTaskID(t)
	p, err := newTaskPaths(root, id)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"task": p.taskJSON(), "events": p.eventsJSONL(), "prompt": p.promptMD(), "input": p.inputTXT(), "combined": p.combinedPromptMD(), "last": p.lastMessageMD(), "exit": p.exitCode(), "stdout": p.stdoutLog(), "stderr": p.stderrLog(), "partial": p.partialOutputMD(), "recovered": p.recoveredAfterTimeout(), "adopted": p.adoptedAfterRestart()}
	for name, got := range want {
		if filepath.Dir(got) != filepath.Join(root, id.String()) {
			t.Errorf("%s path = %q", name, got)
		}
	}
	if filepath.Base(p.taskJSON()) != "task.json" || filepath.Base(p.eventsJSONL()) != "events.jsonl" || filepath.Base(p.promptMD()) != "prompt.md" || filepath.Base(p.inputTXT()) != "input.txt" || filepath.Base(p.combinedPromptMD()) != "combined-prompt.md" || filepath.Base(p.lastMessageMD()) != "last-message.md" || filepath.Base(p.exitCode()) != "exit-code" || filepath.Base(p.stdoutLog()) != "stdout.log" || filepath.Base(p.stderrLog()) != "stderr.log" || filepath.Base(p.partialOutputMD()) != "partial-output.md" || filepath.Base(p.recoveredAfterTimeout()) != "recovered-after-timeout" || filepath.Base(p.adoptedAfterRestart()) != "adopted-after-restart" {
		t.Fatal("contract filenames differ from layout")
	}
}

func TestNewTaskPathsRejectsZeroID(t *testing.T) {
	if _, err := newTaskPaths(t.TempDir(), domain.TaskID{}); err == nil {
		t.Fatal("accepted zero TaskID")
	}
}

func TestOpenTaskDirRejectsSymlinkedTaskDir(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, taskDirPerm); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if f, err := openTaskDir(link); err == nil || f != nil {
		t.Fatalf("openTaskDir symlink = (%v, %v)", f, err)
	}
}

func TestTaskPathsRejectsZeroTaskID(t *testing.T) {
	root, id := t.TempDir(), domain.TaskID{}
	for name, path := range map[string]func(string, domain.TaskID) (string, error){"events": EventsJSONLPath, "prompt": PromptMDPath, "input": InputTXTPath, "combined": CombinedPromptMDPath, "exit": ExitCodePath, "stdout": StdoutLogPath, "stderr": StderrLogPath, "partial": PartialOutputMDPath, "recovered": RecoveredAfterTimeoutPath, "adopted": AdoptedAfterRestartPath} {
		if _, err := path(root, id); err == nil {
			t.Errorf("%s accepted zero TaskID", name)
		}
	}
}
