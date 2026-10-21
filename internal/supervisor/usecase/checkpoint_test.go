package usecase

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
)

func TestCheckpointInspector_SCN0441_UndeterminableBlocksComparison(t *testing.T) {
	cwd := t.TempDir()
	path := writeCheckpoint(t, cwd, "old")
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(t.TempDir(), "outside"), path); err != nil {
		t.Fatal(err)
	}
	before, err := (CheckpointInspector{CWD: cwd}).Inspect()
	if err == nil || !before.Undeterminable || before.Exists || before.Mtime != nil || before.HeadHash != "" {
		t.Fatalf("snapshot=%+v err=%v", before, err)
	}
	issued := time.Now().UTC()
	updated := issued.Add(time.Second)
	after := supervisor.RotationCheckpointSnapshot{Exists: true, Mtime: &updated, HeadHash: "new"}
	if CheckpointUpdated(before, after, issued) {
		t.Fatal("undeterminable snapshot counted as updated")
	}
}

func TestCheckpointInspector_SCN0410_NewCheckpointSatisfiesHeadChange(t *testing.T) {
	cwd := t.TempDir()
	inspector := CheckpointInspector{CWD: cwd}
	before, err := inspector.Inspect()
	if err != nil || before.Exists {
		t.Fatalf("before=%+v err=%v", before, err)
	}
	issued := time.Now().Add(-time.Second)
	writeCheckpoint(t, cwd, "new generation")
	after, err := inspector.Inspect()
	if err != nil || !CheckpointUpdated(before, after, issued) {
		t.Fatalf("new checkpoint did not confirm handoff: after=%+v err=%v", after, err)
	}
}

func writeCheckpoint(t *testing.T, cwd, body string) string {
	t.Helper()
	path := filepath.Join(cwd, checkpointRelativePath)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestCheckpointInspector_SCN0407_SnapshotAndCompare(t *testing.T) {
	cwd := t.TempDir()
	i := CheckpointInspector{CWD: cwd}
	missing, err := i.Inspect()
	if err != nil || missing.Exists {
		t.Fatalf("missing: %+v %v", missing, err)
	}
	path := writeCheckpoint(t, cwd, "old\n")
	before, err := i.Inspect()
	if err != nil || !before.Exists || before.Mtime == nil || before.HeadHash == "" {
		t.Fatalf("before: %+v %v", before, err)
	}
	if !CheckpointUpdated(missing, before, time.Time{}) {
		t.Fatal("new file must count")
	}
	if CheckpointUpdated(before, before, *before.Mtime) {
		t.Fatal("unchanged checkpoint")
	}
	if err := os.WriteFile(path, []byte("new\n"), 0600); err != nil {
		t.Fatal(err)
	}
	later := before.Mtime.Add(time.Second)
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatal(err)
	}
	after, err := i.Inspect()
	if err != nil || !CheckpointUpdated(before, after, *before.Mtime) {
		t.Fatalf("updated: %+v %v", after, err)
	}
	if after.HeadHash == before.HeadHash {
		t.Fatal("head unchanged")
	}
}

func TestCheckpointInspector_SCN0429_MarkerBoundariesAndNoContent(t *testing.T) {
	cwd := t.TempDir()
	i := CheckpointInspector{CWD: cwd}
	secret := "SECRET_CHECKPOINT_BODY"
	writeCheckpoint(t, cwd, "prefix\n<!-- CHECKPOINT-GEN --> \r\nfirst\n<!-- CHECKPOINT-GEN -->\n"+secret+"\n")
	first, err := i.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	writeCheckpoint(t, cwd, "prefix changed\n<!-- CHECKPOINT-GEN -->\nfirst\n<!-- CHECKPOINT-GEN -->\nother\n")
	second, err := i.Inspect()
	if err != nil || first.HeadHash != second.HeadHash {
		t.Fatalf("first generation changed: %v", err)
	}
	writeCheckpoint(t, cwd, "<!-- CHECKPOINT-GEN -->X\nfirst\n")
	third, err := i.Inspect()
	if err != nil || third.HeadHash == first.HeadHash {
		t.Fatalf("invalid marker accepted: %v", err)
	}
	writeCheckpoint(t, cwd, "X<!-- CHECKPOINT-GEN -->\nfirst\n")
	fourth, err := i.Inspect()
	if err != nil || fourth.HeadHash == first.HeadHash {
		t.Fatalf("marker with prefix accepted: %v", err)
	}
	writeCheckpoint(t, cwd, strings.Repeat(secret, checkpointMaxBytes+1))
	_, err = i.Inspect()
	if err == nil || strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), cwd) {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestCheckpointInspector_SCN0408_NoMarkerUsesFirstLines(t *testing.T) {
	cwd := t.TempDir()
	i := CheckpointInspector{CWD: cwd}
	base := strings.Repeat("same\n", checkpointHeadLineCount)
	writeCheckpoint(t, cwd, base+"old tail\n")
	first, err := i.Inspect()
	if err != nil {
		t.Fatal(err)
	}
	writeCheckpoint(t, cwd, base+"new tail\n")
	second, err := i.Inspect()
	if err != nil || second.HeadHash != first.HeadHash {
		t.Fatalf("tail changed head: %v", err)
	}
	writeCheckpoint(t, cwd, "new head\n"+base)
	third, err := i.Inspect()
	if err != nil || third.HeadHash == first.HeadHash {
		t.Fatalf("head not changed: %v", err)
	}
}

func TestCheckpointInspector_SCN0429_RejectUnsafePaths(t *testing.T) {
	cwd := t.TempDir()
	path := writeCheckpoint(t, cwd, "safe")
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, path); err != nil {
		t.Fatal(err)
	}
	if _, err := (CheckpointInspector{CWD: cwd}).Inspect(); err == nil || strings.Contains(err.Error(), outside) {
		t.Fatalf("symlink: %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := (CheckpointInspector{CWD: cwd}).Inspect(); err == nil {
		t.Fatal("directory accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Dir(outside), filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	if _, err := (CheckpointInspector{CWD: cwd}).Inspect(); err == nil || strings.Contains(err.Error(), cwd) {
		t.Fatalf("escaping parent: %v", err)
	}
}
