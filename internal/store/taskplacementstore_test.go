package store

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

var placementNow = time.Date(2026, 1, 1, 0, 0, 0, 0, time.Local)

func placementStore(t *testing.T) (*TaskPlacementFileStore, string) {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	s, err := NewTaskPlacementFileStore(root)
	if err != nil {
		t.Fatal(err)
	}
	return s, root
}
func placementDir(t *testing.T, root, suffix string, lock bool) (string, string) {
	t.Helper()
	n := "impl-20200101-000000-abcd-" + suffix
	d := filepath.Join(root, n)
	if err := os.Mkdir(d, 0700); err != nil {
		t.Fatal(err)
	}
	if lock {
		if err := os.WriteFile(filepath.Join(d, taskPlacementLockName), nil, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return n, d
}
func placementPlan(t *testing.T, s *TaskPlacementFileStore) *TaskPlacementPlan {
	t.Helper()
	p, e := s.Plan(context.Background(), TaskPlacementPolicy{RetentionDays: 1, DiskBudgetBytes: 1000000, Now: placementNow})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}
func unchanged(t *testing.T, path string, want []byte) {
	t.Helper()
	got, e := os.ReadFile(path)
	if e != nil || !bytes.Equal(got, want) {
		t.Fatalf("changed %s: %q %v", path, got, e)
	}
}

func unchangedFileIdentity(t *testing.T, path string, want []byte, before os.FileInfo) {
	t.Helper()
	unchanged(t, path, want)
	after, err := os.Stat(path)
	if err != nil || !os.SameFile(before, after) {
		t.Fatalf("identity changed for %s: before=%v after=%v err=%v", path, before, after, err)
	}
}

func writePlacementState(t *testing.T, dir string, state domain.TaskState) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "task.json"), []byte(`{"state":"`+string(state)+`"}`), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestTaskPlacementPlanSkipsDirectoriesWithoutTaskLock(t *testing.T) { // N03
	s, r := placementStore(t)
	_, d := placementDir(t, r, "no-lock", false)
	f := filepath.Join(d, "task.json")
	if e := os.WriteFile(f, []byte(`{"state":"completed"}`), 0600); e != nil {
		t.Fatal(e)
	}
	if n := len(placementPlan(t, s).Candidates); n != 0 {
		t.Fatalf("N03 candidates=%d", n)
	}
	unchanged(t, f, []byte(`{"state":"completed"}`))
}
func TestTaskPlacementExecuteDeletesOnlyLockBackedCandidate(t *testing.T) {
	s, r := placementStore(t)
	n, d := placementDir(t, r, "terminal", true)
	if e := os.WriteFile(filepath.Join(d, "x"), []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	got, e := s.Execute(context.Background(), placementPlan(t, s))
	if e != nil || len(got) != 1 || got[0].String() != n {
		t.Fatalf("deleted=%v err=%v", got, e)
	}
	if _, err := os.Lstat(d); !os.IsNotExist(err) {
		t.Fatalf("placement remains after reported deletion: %v", err)
	}
}

func TestTaskPlacementPlanAddsRecentCandidatesForBudgetOverage(t *testing.T) {
	s, root := placementStore(t)
	name := "impl-20251231-000000-abcd-budget"
	dir := filepath.Join(root, name)
	if err := os.Mkdir(dir, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, taskPlacementLockName), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "payload"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	plan, err := s.Plan(context.Background(), TaskPlacementPolicy{RetentionDays: 2, DiskBudgetBytes: 1, Now: placementNow})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plan.Close() })
	if len(plan.Candidates) != 1 || plan.Candidates[0].TaskID.String() != name || plan.Candidates[0].retention {
		t.Fatalf("budget candidate=%+v", plan.Candidates)
	}
}

func TestTaskPlacementDepthUsesChildOfPlacementAsDepthOne(t *testing.T) {
	t.Run("depth 64 is eligible and removable", func(t *testing.T) {
		s, root := placementStore(t)
		name, dir := placementDir(t, root, "depth64", true)
		for i := 0; i < taskPlacementMaxDepth; i++ {
			dir = filepath.Join(dir, "d")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
		}
		got, err := s.Execute(context.Background(), placementPlan(t, s))
		if err != nil || len(got) != 1 || got[0].String() != name {
			t.Fatalf("deleted=%v err=%v", got, err)
		}
	})
	t.Run("depth 65 rejects before marker creation", func(t *testing.T) {
		s, root := placementStore(t)
		_, dir := placementDir(t, root, "depth65", true)
		for i := 0; i <= taskPlacementMaxDepth; i++ {
			dir = filepath.Join(dir, "d")
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
		}
		if candidates := placementPlan(t, s).Candidates; len(candidates) != 0 {
			t.Fatalf("candidates=%v", candidates)
		}
		if _, err := os.Lstat(filepath.Join(filepath.Dir(dir), taskPlacementMarkerName)); !os.IsNotExist(err) {
			t.Fatalf("marker=%v", err)
		}
	})
}

func TestTaskPlacementExecuteDoesNotUnlinkReplacementLock(t *testing.T) {
	s, root := placementStore(t)
	_, dir := placementDir(t, root, "lock-identity", true)
	plan := placementPlan(t, s)
	original := taskPlacementUnlinkAt
	taskPlacementUnlinkAt = func(fd int, name string, flags int) error {
		if name == taskPlacementMarkerName {
			if err := original(fd, taskPlacementLockName, 0); err != nil {
				return err
			}
			newFD, err := taskPlacementOpenAt(fd, taskPlacementLockName, syscall.O_RDWR|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW, 0600)
			if err != nil {
				return err
			}
			return syscall.Close(newFD)
		}
		return original(fd, name, flags)
	}
	t.Cleanup(func() { taskPlacementUnlinkAt = original })
	got, err := s.Execute(context.Background(), plan)
	if len(got) != 0 || err == nil {
		t.Fatalf("deleted=%v err=%v", got, err)
	}
	if info, err := os.Lstat(filepath.Join(dir, taskPlacementLockName)); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("replacement lock missing: info=%v err=%v", info, err)
	}
}

func TestTaskPlacementNegativeSafety(t *testing.T) {
	t.Run("N01 held lock", func(t *testing.T) {
		s, r := placementStore(t)
		_, d := placementDir(t, r, "held", true)
		x := filepath.Join(d, "x")
		_ = os.WriteFile(x, []byte("safe"), 0600)
		f, e := os.Open(filepath.Join(d, taskPlacementLockName))
		if e != nil {
			t.Fatal(e)
		}
		defer f.Close()
		if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
			t.Fatal(e)
		}
		if len(placementPlan(t, s).Candidates) != 0 {
			t.Fatal("N01 candidate")
		}
		unchanged(t, x, []byte("safe"))
	})
	t.Run("N02 revived", func(t *testing.T) {
		s, r := placementStore(t)
		_, d := placementDir(t, r, "revive", true)
		j := filepath.Join(d, "task.json")
		_ = os.WriteFile(j, []byte(`{"state":"completed"}`), 0600)
		p := placementPlan(t, s)
		_ = os.WriteFile(j, []byte(`{"state":"running"}`), 0600)
		got, e := s.Execute(context.Background(), p)
		if e != nil || len(got) != 0 {
			t.Fatalf("N02 %v %v", got, e)
		}
		unchanged(t, j, []byte(`{"state":"running"}`))
	})
	t.Run("N04 reserve window", func(t *testing.T) {
		s, r := placementStore(t)
		_, d := placementDir(t, r, "reserve", false)
		if len(placementPlan(t, s).Candidates) != 0 {
			t.Fatal("N04 candidate")
		}
		if _, e := os.Stat(d); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("N05 corrupt snapshot", func(t *testing.T) {
		s, r := placementStore(t)
		_, d := placementDir(t, r, "corrupt", true)
		j := filepath.Join(d, "task.json")
		_ = os.WriteFile(j, []byte("{"), 0600)
		if len(placementPlan(t, s).Candidates) != 0 {
			t.Fatal("N05 candidate")
		}
		unchanged(t, j, []byte("{"))
	})
	t.Run("N06 root survives", func(t *testing.T) {
		s, r := placementStore(t)
		placementDir(t, r, "root", true)
		_, e := s.Execute(context.Background(), placementPlan(t, s))
		if e != nil {
			t.Fatal(e)
		}
		if i, e := os.Stat(r); e != nil || !i.IsDir() {
			t.Fatal(e)
		}
	})
	t.Run("N07 N11 external symlink target", func(t *testing.T) {
		s, r := placementStore(t)
		_, d := placementDir(t, r, "link", true)
		o := filepath.Join(t.TempDir(), "s")
		_ = os.WriteFile(o, []byte("safe"), 0600)
		if e := os.Symlink(o, filepath.Join(d, "link")); e != nil {
			t.Fatal(e)
		}
		_, e := s.Execute(context.Background(), placementPlan(t, s))
		if e != nil {
			t.Fatal(e)
		}
		unchanged(t, o, []byte("safe"))
	})
	t.Run("N08 unsafe names", func(t *testing.T) {
		for _, n := range []string{"", ".", "..", "a/b"} {
			if safeEntryName(n) {
				t.Fatal(n)
			}
		}
	})
	t.Run("N09 N10 root and candidate symlink", func(t *testing.T) {
		target := t.TempDir()
		o := filepath.Join(target, "s")
		_ = os.WriteFile(o, []byte("safe"), 0600)
		root := filepath.Join(t.TempDir(), "root")
		_ = os.Symlink(target, root)
		s, e := NewTaskPlacementFileStore(root)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = s.Plan(context.Background(), TaskPlacementPolicy{RetentionDays: 1, Now: placementNow}); e == nil {
			t.Fatal("N09")
		}
		unchanged(t, o, []byte("safe"))
	})
	t.Run("N12 invalid root modes and missing root", func(t *testing.T) {
		r := t.TempDir()
		_ = os.Chmod(r, 0755)
		s, _ := NewTaskPlacementFileStore(r)
		if _, e := s.Plan(context.Background(), TaskPlacementPolicy{RetentionDays: 1, Now: placementNow}); e == nil {
			t.Fatal("N12 mode")
		}
		s, _ = NewTaskPlacementFileStore(filepath.Join(t.TempDir(), "none"))
		p, e := s.Plan(context.Background(), TaskPlacementPolicy{RetentionDays: 1, Now: placementNow})
		if e != nil || len(p.Candidates) != 0 {
			t.Fatal(e)
		}
	})
	t.Run("N16 cancelled", func(t *testing.T) {
		s, r := placementStore(t)
		_, d := placementDir(t, r, "cancel", true)
		p := placementPlan(t, s)
		c, cancel := context.WithCancel(context.Background())
		cancel()
		if _, e := s.Execute(c, p); e == nil {
			t.Fatal("N16")
		}
		if _, e := os.Lstat(filepath.Join(d, taskPlacementMarkerName)); !os.IsNotExist(e) {
			t.Fatal(e)
		}
	})
	t.Run("N18 legacy", func(t *testing.T) {
		s, r := placementStore(t)
		_, d := placementDir(t, r, "legacy", false)
		x := filepath.Join(d, "x")
		_ = os.WriteFile(x, []byte("legacy"), 0600)
		if len(placementPlan(t, s).Candidates) != 0 {
			t.Fatal("N18")
		}
		unchanged(t, x, []byte("legacy"))
	})
	t.Run("N21 N22 lock replacement", func(t *testing.T) {
		s, r := placementStore(t)
		_, d := placementDir(t, r, "replace", true)
		p := placementPlan(t, s)
		_ = os.Remove(filepath.Join(d, taskPlacementLockName))
		_ = os.Mkdir(filepath.Join(d, taskPlacementLockName), 0700)
		got, e := s.Execute(context.Background(), p)
		if e == nil || len(got) != 0 {
			t.Fatal(got, e)
		}
	})
	t.Run("N23 marker directory", func(t *testing.T) {
		s, r := placementStore(t)
		_, d := placementDir(t, r, "marker", true)
		_ = os.Mkdir(filepath.Join(d, taskPlacementMarkerName), 0700)
		got, e := s.Execute(context.Background(), placementPlan(t, s))
		if e == nil || len(got) != 0 {
			t.Fatal(got, e)
		}
	})
	t.Run("N25 chmod child", func(t *testing.T) {
		s, r := placementStore(t)
		_, d := placementDir(t, r, "chmod", true)
		c := filepath.Join(d, "c")
		_ = os.Mkdir(c, 0500)
		_ = os.WriteFile(filepath.Join(c, "x"), []byte("x"), 0600)
		got, e := s.Execute(context.Background(), placementPlan(t, s))
		if e != nil || len(got) != 1 {
			t.Fatal(got, e)
		}
	})
	t.Run("N26 depth limit", func(t *testing.T) {
		s, r := placementStore(t)
		_, d := placementDir(t, r, "deep", true)
		for i := 0; i <= taskPlacementMaxDepth; i++ {
			d = filepath.Join(d, "d")
			if e := os.Mkdir(d, 0700); e != nil {
				t.Fatal(e)
			}
		}
		if len(placementPlan(t, s).Candidates) != 0 {
			t.Fatal("N26")
		}
		if _, e := os.Stat(d); e != nil {
			t.Fatal(e)
		}
	})
	t.Run("N29 external deletion", func(t *testing.T) {
		s, r := placementStore(t)
		n, d := placementDir(t, r, "gone", true)
		p := placementPlan(t, s)
		_ = os.RemoveAll(d)
		got, e := s.Execute(context.Background(), p)
		if e != nil || len(got) != 1 || got[0].String() != n {
			t.Fatal(got, e)
		}
	})
}

func TestTaskPlacementExecuteSkipsCandidateSwappedForExternalSymlink(t *testing.T) { // N10
	s, root := placementStore(t)
	name, candidate := placementDir(t, root, "swap", true)
	plan := placementPlan(t, s)
	external := t.TempDir()
	sentinel := filepath.Join(external, "sentinel")
	contents := []byte("outside bytes must not change")
	if err := os.WriteFile(sentinel, contents, 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(candidate); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(root, name)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Execute(context.Background(), plan)
	if err == nil || len(got) != 0 {
		t.Fatalf("N10 deleted=%v err=%v", got, err)
	}
	unchangedFileIdentity(t, sentinel, contents, before)
}

func TestTaskPlacementExecuteNeverWritesOutsideTempRoot(t *testing.T) { // N14
	s, root := placementStore(t)
	name, candidate := placementDir(t, root, "outside-write", true)
	plan := placementPlan(t, s)
	external := t.TempDir()
	sentinel := filepath.Join(external, "sentinel")
	contents := []byte("outside root")
	if err := os.WriteFile(sentinel, contents, 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(sentinel)
	if err != nil {
		t.Fatal(err)
	}
	externalDir, err := os.Open(external)
	if err != nil {
		t.Fatal(err)
	}
	defer externalDir.Close()
	originalOpenAt, originalUnlinkAt := taskPlacementOpenAt, taskPlacementUnlinkAt
	t.Cleanup(func() {
		taskPlacementOpenAt, taskPlacementUnlinkAt = originalOpenAt, originalUnlinkAt
	})
	writesOutside := 0
	taskPlacementOpenAt = func(dirFD int, entry string, flags int, mode uint32) (int, error) {
		if dirFD == int(externalDir.Fd()) && flags&(syscall.O_WRONLY|syscall.O_RDWR|syscall.O_CREAT) != 0 {
			writesOutside++
		}
		return originalOpenAt(dirFD, entry, flags, mode)
	}
	taskPlacementUnlinkAt = func(dirFD int, entry string, flags int) error {
		if dirFD == int(externalDir.Fd()) {
			writesOutside++
		}
		return originalUnlinkAt(dirFD, entry, flags)
	}
	if err := os.RemoveAll(candidate); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(root, name)); err != nil {
		t.Fatal(err)
	}
	got, err := s.Execute(context.Background(), plan)
	if err == nil || len(got) != 0 || writesOutside != 0 {
		t.Fatalf("N14 deleted=%v writesOutside=%d err=%v", got, writesOutside, err)
	}
	unchangedFileIdentity(t, sentinel, contents, before)
}

func TestTaskPlacementExecuteSkipsReplacedRoot(t *testing.T) { // N13
	s, root := placementStore(t)
	placementDir(t, root, "original", true)
	plan := placementPlan(t, s)
	parent := filepath.Dir(root)
	old := root + "-old"
	if err := os.Rename(root, old); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	_, replacement := placementDir(t, root, "replacement", true)
	sentinel := filepath.Join(replacement, "sentinel")
	if err := os.WriteFile(sentinel, []byte("replacement root"), 0600); err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(root) != parent {
		t.Fatal("unexpected root parent")
	}
	got, err := s.Execute(context.Background(), plan)
	if err == nil || len(got) != 0 {
		t.Fatalf("N13 deleted=%v err=%v", got, err)
	}
	unchanged(t, sentinel, []byte("replacement root"))
}

func TestTaskPlacementPlanRejectsDifferentOwner(t *testing.T) { // N12
	s, root := placementStore(t)
	_, dir := placementDir(t, root, "owner", true)
	sentinel := filepath.Join(dir, "sentinel")
	if err := os.WriteFile(sentinel, []byte("owner protected"), 0600); err != nil {
		t.Fatal(err)
	}
	originalFstat := taskPlacementFstat
	taskPlacementFstat = func(fd int, stat *syscall.Stat_t) error {
		if err := originalFstat(fd, stat); err != nil {
			return err
		}
		stat.Uid++
		return nil
	}
	t.Cleanup(func() { taskPlacementFstat = originalFstat })
	if _, err := s.Plan(context.Background(), TaskPlacementPolicy{RetentionDays: 1, DiskBudgetBytes: 1000000, Now: placementNow}); err == nil {
		t.Fatal("N12 owner mismatch was accepted")
	}
	unchanged(t, sentinel, []byte("owner protected"))
}

func TestTaskPlacementNonTerminalStatesAreNeverCandidates(t *testing.T) { // N19
	states := []domain.TaskState{domain.StateQueued, domain.StateStarting, domain.StateRunning, domain.StateStalled, domain.StateTimeout, domain.StateRecovering, domain.StateCancelling, domain.StateAdopted, domain.StateOrphaned}
	for _, state := range states {
		t.Run(string(state), func(t *testing.T) {
			s, root := placementStore(t)
			_, dir := placementDir(t, root, string(state), true)
			writePlacementState(t, dir, state)
			if candidates := placementPlan(t, s).Candidates; len(candidates) != 0 {
				t.Fatalf("N19 state=%s candidates=%v", state, candidates)
			}
			unchanged(t, filepath.Join(dir, "task.json"), []byte(`{"state":"`+string(state)+`"}`))
		})
	}
}

func TestTaskPlacementExecuteDoesNotFollowReplacedLock(t *testing.T) { // N22
	s, root := placementStore(t)
	_, dir := placementDir(t, root, "lock-link", true)
	plan := placementPlan(t, s)
	external := filepath.Join(t.TempDir(), "external-lock")
	contents := []byte("external death lease")
	if err := os.WriteFile(external, contents, 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(external)
	if err != nil {
		t.Fatal(err)
	}
	lock := filepath.Join(dir, taskPlacementLockName)
	if err := os.Remove(lock); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, lock); err != nil {
		t.Fatal(err)
	}
	got, err := s.Execute(context.Background(), plan)
	if err == nil || len(got) != 0 {
		t.Fatalf("N22 deleted=%v err=%v", got, err)
	}
	if target, err := os.Readlink(lock); err != nil || target != external {
		t.Fatalf("N22 lock link=%q err=%v", target, err)
	}
	unchangedFileIdentity(t, external, contents, before)
}

func TestTaskPlacementExecuteDoesNotFollowExistingMarkerSymlink(t *testing.T) { // N24
	s, root := placementStore(t)
	_, dir := placementDir(t, root, "marker-link", true)
	external := filepath.Join(t.TempDir(), "marker-target")
	contents := []byte("marker target is immutable")
	if err := os.WriteFile(external, contents, 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(external)
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, taskPlacementMarkerName)
	if err := os.Symlink(external, marker); err != nil {
		t.Fatal(err)
	}
	got, err := s.Execute(context.Background(), placementPlan(t, s))
	if err == nil || len(got) != 0 {
		t.Fatalf("N24 deleted=%v err=%v", got, err)
	}
	if target, err := os.Readlink(marker); err != nil || target != external {
		t.Fatalf("N24 marker link=%q err=%v", target, err)
	}
	unchangedFileIdentity(t, external, contents, before)
}

func TestTaskPlacementPlanSkipsMalformedAndUnreadableSizedEntriesButContinues(t *testing.T) { // N15
	s, root := placementStore(t)
	if err := os.Mkdir(filepath.Join(root, "not-a-task"), 0700); err != nil {
		t.Fatal(err)
	}
	_, deep := placementDir(t, root, "too-deep", true)
	for i := 0; i <= taskPlacementMaxDepth; i++ {
		deep = filepath.Join(deep, "d")
		if err := os.Mkdir(deep, 0700); err != nil {
			t.Fatal(err)
		}
	}
	name, _ := placementDir(t, root, "valid-after-errors", true)
	plan := placementPlan(t, s)
	if len(plan.Candidates) != 1 || plan.Candidates[0].TaskID.String() != name {
		t.Fatalf("N15 candidates=%v", plan.Candidates)
	}
	if len(plan.Failures) == 0 || plan.CurrentBytes >= 0 || plan.ProjectedBytes >= 0 {
		t.Fatalf("N15 failures=%v current=%d projected=%d", plan.Failures, plan.CurrentBytes, plan.ProjectedBytes)
	}
}

func TestTaskPlacementDeletesAtMaximumSupportedDepth(t *testing.T) { // N26
	s, root := placementStore(t)
	name, dir := placementDir(t, root, "maximum-depth", true)
	for i := 0; i < taskPlacementMaxDepth; i++ {
		dir = filepath.Join(dir, "d")
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Execute(context.Background(), placementPlan(t, s))
	if err != nil || len(got) != 1 || got[0].String() != name {
		t.Fatalf("N26 deleted=%v err=%v", got, err)
	}
}

func TestTaskPlacementConcurrentExecuteUsesOneDeathLease(t *testing.T) { // N17
	s, root := placementStore(t)
	name, _ := placementDir(t, root, "concurrent", true)
	plan := placementPlan(t, s)
	results := make(chan []domain.TaskID, 2)
	errs := make(chan error, 2)
	var start sync.WaitGroup
	start.Add(1)
	for i := 0; i < 2; i++ {
		go func() {
			start.Wait()
			deleted, err := s.Execute(context.Background(), plan)
			results <- deleted
			errs <- err
		}()
	}
	start.Done()
	deleted := 0
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
		for _, id := range <-results {
			if id.String() != name {
				t.Fatalf("unexpected task id %s", id)
			}
			deleted++
		}
	}
	if deleted != 1 {
		t.Fatalf("N17 deleted=%d, want one death-lease holder", deleted)
	}
}

func TestTaskPlacementExecuteRevivedSaveRetainsPlacement(t *testing.T) { // N20
	s, root := placementStore(t)
	taskName, dir := placementDir(t, root, "save-race", true)
	plan := placementPlan(t, s)
	originalUnlinkAt := taskPlacementUnlinkAt
	taskPlacementUnlinkAt = func(fd int, name string, flags int) error {
		if name == taskPlacementMarkerName {
			return originalUnlinkAt(fd, name, flags)
		}
		if name == taskName && flags == atRemoveDir {
			return syscall.ENOTEMPTY
		}
		return originalUnlinkAt(fd, name, flags)
	}
	t.Cleanup(func() { taskPlacementUnlinkAt = originalUnlinkAt })
	got, err := s.Execute(context.Background(), plan)
	if err == nil || len(got) != 0 {
		t.Fatalf("N20 deleted=%v err=%v", got, err)
	}
	if _, err := os.Lstat(filepath.Join(dir, taskPlacementLockName)); !os.IsNotExist(err) {
		t.Fatalf("N20 lock=%v, want absent after final directory removal failure", err)
	}
	if info, err := os.Lstat(filepath.Join(dir, taskPlacementMarkerName)); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("N20 marker=%v err=%v, want regular marker", info, err)
	}
}

func TestTaskPlacementFailureKeepsMarkerAndLockAndContinues(t *testing.T) { // N27, N30
	s, root := placementStore(t)
	_, failed := placementDir(t, root, "marker-directory", true)
	if err := os.Mkdir(filepath.Join(failed, taskPlacementMarkerName), 0700); err != nil {
		t.Fatal(err)
	}
	name, _ := placementDir(t, root, "after-failure", true)
	got, err := s.Execute(context.Background(), placementPlan(t, s))
	if err == nil || len(got) != 1 || got[0].String() != name {
		t.Fatalf("N27/N30 deleted=%v err=%v", got, err)
	}
	if info, err := os.Lstat(filepath.Join(failed, taskPlacementMarkerName)); err != nil || !info.IsDir() {
		t.Fatalf("N27 marker changed: info=%v err=%v", info, err)
	}
	if _, err := os.Lstat(filepath.Join(failed, taskPlacementLockName)); err != nil {
		t.Fatalf("N27 lock changed: %v", err)
	}
}

func TestTaskPlacementFailureToCreateMarkerOrChangePermissionsRetainsLease(t *testing.T) { // N25, N27
	t.Run("marker open EACCES", func(t *testing.T) {
		s, root := placementStore(t)
		_, dir := placementDir(t, root, "marker-eacces", true)
		originalOpenAt := taskPlacementOpenAt
		taskPlacementOpenAt = func(dirFD int, name string, flags int, mode uint32) (int, error) {
			if name == taskPlacementMarkerName && flags&syscall.O_CREAT != 0 {
				return -1, syscall.EACCES
			}
			return originalOpenAt(dirFD, name, flags, mode)
		}
		t.Cleanup(func() { taskPlacementOpenAt = originalOpenAt })
		got, err := s.Execute(context.Background(), placementPlan(t, s))
		if err == nil || len(got) != 0 {
			t.Fatalf("N25 deleted=%v err=%v", got, err)
		}
		if _, err := os.Lstat(filepath.Join(dir, taskPlacementLockName)); err != nil {
			t.Fatalf("N25 lock was removed: %v", err)
		}
	})
	t.Run("Fchmod failure", func(t *testing.T) {
		s, root := placementStore(t)
		_, dir := placementDir(t, root, "fchmod-failure", true)
		originalFchmod := removeTreeFchmod
		removeTreeFchmod = func(int, uint32) error { return syscall.EACCES }
		t.Cleanup(func() { removeTreeFchmod = originalFchmod })
		got, err := s.Execute(context.Background(), placementPlan(t, s))
		if err == nil || len(got) != 0 {
			t.Fatalf("N25 deleted=%v err=%v", got, err)
		}
		if _, err := os.Lstat(filepath.Join(dir, taskPlacementMarkerName)); err != nil {
			t.Fatalf("N25 marker was not retained: %v", err)
		}
		if _, err := os.Lstat(filepath.Join(dir, taskPlacementLockName)); err != nil {
			t.Fatalf("N25 lock was removed: %v", err)
		}
	})
}

func TestTaskPlacementExecuteReturnsCompletedIDsBeforeCancellation(t *testing.T) { // N16
	s, root := placementStore(t)
	first, _ := placementDir(t, root, "cancel-first", true)
	_, _ = placementDir(t, root, "cancel-second", true)
	plan := placementPlan(t, s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	originalUnlinkAt := taskPlacementUnlinkAt
	taskPlacementUnlinkAt = func(fd int, name string, flags int) error {
		err := originalUnlinkAt(fd, name, flags)
		if err == nil && name == first && flags == atRemoveDir {
			cancel()
		}
		return err
	}
	t.Cleanup(func() { taskPlacementUnlinkAt = originalUnlinkAt })
	deleted, err := s.Execute(ctx, plan)
	if !errors.Is(err, context.Canceled) || len(deleted) != 1 || deleted[0].String() != first {
		t.Fatalf("N16 deleted=%v err=%v", deleted, err)
	}
}

func TestTaskPlacementExecuteRecordsFileDescriptorExhaustion(t *testing.T) { // SCN-23, N26
	s, root := placementStore(t)
	name, _ := placementDir(t, root, "emfile", true)
	plan := placementPlan(t, s)
	originalOpenAt := taskPlacementOpenAt
	taskPlacementOpenAt = func(fd int, entry string, flags int, mode uint32) (int, error) {
		if entry == name && flags&syscall.O_DIRECTORY != 0 {
			return -1, syscall.EMFILE
		}
		return originalOpenAt(fd, entry, flags, mode)
	}
	t.Cleanup(func() { taskPlacementOpenAt = originalOpenAt })
	deleted, err := s.Execute(context.Background(), plan)
	if len(deleted) != 0 || !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("SCN-23 deleted=%v err=%v", deleted, err)
	}
}
