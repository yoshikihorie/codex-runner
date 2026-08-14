package execution

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type createWorktreeRecorder struct {
	calls       int
	source      string
	destination string
	err         error
}

func (r *createWorktreeRecorder) Create(_ context.Context, source, destination string) error {
	r.calls++
	r.source, r.destination = source, destination
	return r.err
}

func TestCreateWorktreeUseCaseValidatesConstructorAndInputs(t *testing.T) {
	root := t.TempDir()
	creator := &createWorktreeRecorder{}
	for _, tc := range []struct {
		name string
		root string
	}{
		{"nil creator", root},
		{"empty root", ""},
		{"relative root", "relative"},
		{"unnormalized root", root + string(filepath.Separator) + "."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			candidate := WorktreeCreator(creator)
			if tc.name == "nil creator" {
				candidate = nil
			}
			if _, err := NewCreateWorktreeUseCase(candidate, tc.root); err == nil {
				t.Fatal("constructor accepted invalid input")
			}
		})
	}
	uc, err := NewCreateWorktreeUseCase(creator, root)
	if err != nil {
		t.Fatal(err)
	}
	id, err := domain.NewTaskID("impl-20260812-120000-a1b2-worktree")
	if err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, in := range []CreateWorktreeInput{
		{TaskID: id, SourceWorkingDir: root},
		{SourceWorkingDir: root},
		{TaskID: id},
		{TaskID: id, SourceWorkingDir: "relative"},
		{TaskID: id, SourceWorkingDir: root + string(filepath.Separator) + "."},
	} {
		ctx := context.Background()
		if in.TaskID == id && in.SourceWorkingDir == root {
			ctx = cancelled
		}
		if _, err := uc.Execute(ctx, in); err == nil {
			t.Fatalf("Execute(%+v) accepted invalid input", in)
		}
	}
	if creator.calls != 0 {
		t.Fatal("invalid input called creator")
	}
}

func TestCreateWorktreeUseCaseDerivesDestinationAndRejectsUnsafePaths(t *testing.T) {
	root := filepath.Join(t.TempDir(), "worktrees")
	source := t.TempDir()
	creator := &createWorktreeRecorder{}
	uc, err := NewCreateWorktreeUseCase(creator, root)
	if err != nil {
		t.Fatal(err)
	}
	id, err := domain.NewTaskID("impl-20260812-120000-a1b2-derived")
	if err != nil {
		t.Fatal(err)
	}
	out, err := uc.Execute(context.Background(), CreateWorktreeInput{TaskID: id, SourceWorkingDir: source})
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, id.String())
	if out.WorkingDir != want || creator.calls != 1 || creator.source != source || creator.destination != want || !filepath.IsAbs(out.WorkingDir) || filepath.Clean(out.WorkingDir) != out.WorkingDir {
		t.Fatalf("output=%+v recorder=%+v", out, creator)
	}
	link := filepath.Join(t.TempDir(), "source-link")
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.Execute(context.Background(), CreateWorktreeInput{TaskID: id, SourceWorkingDir: link}); err == nil {
		t.Fatal("symlink source accepted")
	}
}

const testWorktreeTaskID = "impl-20260808-120000-abcd-cleanup"

type fakeWorktreeStore struct {
	paths    []string
	changes  map[string]bool
	mtime    map[string]time.Time
	links    map[string]bool
	errs     map[string]error
	removed  []string
	onRemove func(string)
}

func (s *fakeWorktreeStore) ListTopLevel(string) ([]string, error) { return s.paths, s.errs["list"] }
func (s *fakeWorktreeStore) IsSymlink(path string) (bool, error) {
	return s.links[path], s.errs["link:"+path]
}
func (s *fakeWorktreeStore) HasGitChanges(path string) (bool, error) {
	return s.changes[path], s.errs["git:"+path]
}
func (s *fakeWorktreeStore) ModTime(path string) (time.Time, error) {
	return s.mtime[path], s.errs["mtime:"+path]
}
func (s *fakeWorktreeStore) AgeDays(path string, now time.Time) (int, error) {
	if err := s.errs["age:"+path]; err != nil {
		return 0, err
	}
	return int(now.Sub(s.mtime[path]).Hours() / 24), nil
}
func (s *fakeWorktreeStore) Remove(path string) error {
	s.removed = append(s.removed, path)
	if s.onRemove != nil {
		s.onRemove(path)
	}
	return s.errs["remove:"+path]
}

func newTestUseCase(t *testing.T, store *fakeWorktreeStore, liveness func(string) (bool, error)) (*EvictWorkDirUseCase, string) {
	t.Helper()
	root := t.TempDir()
	locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(liveness), func(id domain.TaskID) string { return id.String() })
	uc, err := NewEvictWorkDirUseCase(store, locks, root)
	if err != nil {
		t.Fatal(err)
	}
	return uc, root
}

func validInput(trigger string) EvictWorkDirInput {
	return EvictWorkDirInput{Trigger: trigger, OccurredAt: time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)}
}

func TestEvictWorkDirValidation(t *testing.T) {
	store := &fakeWorktreeStore{errs: map[string]error{}}
	uc, _ := newTestUseCase(t, store, func(string) (bool, error) { return true, nil })
	for _, in := range []EvictWorkDirInput{{Trigger: "bad", OccurredAt: time.Now()}, {Trigger: TriggerAutomatic, Force: true, OccurredAt: time.Now()}, {Trigger: TriggerExplicit}, {Trigger: TriggerExplicit, MaxAgeDays: 366, OccurredAt: time.Now()}} {
		if _, _, err := uc.Plan(context.Background(), in); err == nil {
			t.Fatalf("Plan(%+v) error = nil", in)
		}
	}
	in := validInput(TriggerExplicit)
	if _, err := uc.Execute(context.Background(), in, nil); err == nil {
		t.Fatal("explicit Execute nil confirmation error = nil")
	}
	if len(store.removed) != 0 {
		t.Fatal("invalid Execute removed a path")
	}
	in = validInput(TriggerAutomatic)
	if _, err := uc.Execute(context.Background(), in, []string{}); err == nil {
		t.Fatal("automatic Execute confirmed paths error = nil")
	}
}

func TestEvictWorkDirPlanAndExecute(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	uc, root := newTestUseCase(t, store, func(string) (bool, error) { return true, nil })
	path := filepath.Join(root, testWorktreeTaskID)
	store.paths = []string{path}
	store.mtime[path] = validInput(TriggerAutomatic).OccurredAt
	candidates, skipped, err := uc.Plan(context.Background(), validInput(TriggerAutomatic))
	if err != nil || len(candidates) != 1 || len(skipped) != 0 {
		t.Fatalf("Plan() = %#v, %#v, %v", candidates, skipped, err)
	}
	out, err := uc.Execute(context.Background(), validInput(TriggerAutomatic), nil)
	if err != nil || len(out.Candidates) != 1 || len(out.Deleted) != 1 || len(store.removed) != 1 {
		t.Fatalf("Execute() = %#v, %v", out, err)
	}
}

func TestEvictWorkDirSkipsAndExplicitRechecks(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	calls := 0
	uc, root := newTestUseCase(t, store, func(string) (bool, error) { calls++; return calls == 1, nil })
	path := filepath.Join(root, testWorktreeTaskID)
	store.paths = []string{path}
	store.mtime[path] = validInput(TriggerExplicit).OccurredAt.Add(-8 * 24 * time.Hour)
	plan, _, err := uc.Plan(context.Background(), validInput(TriggerExplicit))
	if err != nil || len(plan) != 1 {
		t.Fatalf("Plan = %#v, %v", plan, err)
	}
	out, err := uc.Execute(context.Background(), validInput(TriggerExplicit), []string{path, path, filepath.Join(root, "child", testWorktreeTaskID)})
	if err != nil || len(out.Candidates) != 0 || len(out.Deleted) != 0 || len(out.Skipped) != 1 || out.Skipped[0].Reason != WorktreeSkipStillAlive {
		t.Fatalf("Execute = %#v, %v", out, err)
	}
}

func TestEvictWorkDirExplicitRecheckSkipsRevivedCandidateAndDeletesOtherCandidate(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	root := t.TempDir()
	revived := filepath.Join(root, testWorktreeTaskID)
	deletable := filepath.Join(root, "impl-20260808-120001-abcd-cleanup")
	calls := map[string]int{}
	locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(id string) (bool, error) {
		calls[id]++
		return id != testWorktreeTaskID || calls[id] == 1, nil
	}), func(id domain.TaskID) string { return id.String() })
	uc, err := NewEvictWorkDirUseCase(store, locks, root)
	if err != nil {
		t.Fatal(err)
	}
	store.paths = []string{revived, deletable}
	store.mtime[revived], store.mtime[deletable] = validInput(TriggerExplicit).OccurredAt, validInput(TriggerExplicit).OccurredAt
	plan, _, err := uc.Plan(context.Background(), validInput(TriggerExplicit))
	if err != nil || len(plan) != 2 {
		t.Fatalf("Plan() = %#v, %v", plan, err)
	}
	out, err := uc.Execute(context.Background(), validInput(TriggerExplicit), []string{revived, deletable})
	if err != nil || len(out.Candidates) != 1 || len(out.Deleted) != 1 || out.Deleted[0] != deletable || len(out.Skipped) != 1 || out.Skipped[0] != (WorktreeSkipped{Path: revived, Reason: WorktreeSkipStillAlive}) {
		t.Fatalf("Execute() = %#v, %v", out, err)
	}
}

func TestEvictWorkDirLivenessErrors(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	uc, root := newTestUseCase(t, store, func(string) (bool, error) { return false, domain.ErrTaskNotFound })
	path := filepath.Join(root, testWorktreeTaskID)
	store.paths, store.mtime[path] = []string{path}, time.Now()
	candidates, _, err := uc.Plan(context.Background(), validInput(TriggerAutomatic))
	if err != nil || len(candidates) != 1 {
		t.Fatalf("not found = %#v, %v", candidates, err)
	}
	locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return false, errors.New("io") }), func(id domain.TaskID) string { return id.String() })
	uc, err = NewEvictWorkDirUseCase(store, locks, root)
	if err != nil {
		t.Fatal(err)
	}
	candidates, _, err = uc.Plan(context.Background(), validInput(TriggerAutomatic))
	if err != nil || len(candidates) != 0 {
		t.Fatalf("io error = %#v, %v", candidates, err)
	}
}

func TestEvictWorkDirAutomaticSkipsOldChangedWorktreeWithoutForce(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	uc, root := newTestUseCase(t, store, func(string) (bool, error) { return true, nil })
	path := filepath.Join(root, testWorktreeTaskID)
	input := validInput(TriggerAutomatic)
	store.paths = []string{path}
	store.changes[path] = true
	store.mtime[path] = input.OccurredAt.Add(-(WorktreeRetentionDaysDefault + 1) * 24 * time.Hour)
	out, err := uc.Execute(context.Background(), input, nil)
	if err != nil || len(out.Candidates) != 0 || len(out.Deleted) != 0 || len(out.Skipped) != 1 || out.Skipped[0] != (WorktreeSkipped{Path: path, Reason: WorktreeSkipHasGitChanges}) {
		t.Fatalf("Execute() = %#v, %v", out, err)
	}
}

func TestEvictWorkDirForceAndRetentionBoundaries(t *testing.T) {
	now := validInput(TriggerExplicit).OccurredAt
	for _, tc := range []struct {
		name       string
		elapsed    time.Duration
		candidates int
		skipped    int
	}{
		{"below threshold", 7*24*time.Hour - time.Second, 0, 1},
		{"at threshold", 7 * 24 * time.Hour, 1, 0},
		{"above threshold", 7*24*time.Hour + time.Second, 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
			uc, root := newTestUseCase(t, store, func(string) (bool, error) { return true, nil })
			path := filepath.Join(root, testWorktreeTaskID)
			store.paths, store.changes[path], store.mtime[path] = []string{path}, true, now.Add(-tc.elapsed)
			candidates, skipped, err := uc.Plan(context.Background(), EvictWorkDirInput{Trigger: TriggerExplicit, Force: true, MaxAgeDays: 7, OccurredAt: now})
			if err != nil || len(candidates) != tc.candidates || len(skipped) != tc.skipped {
				t.Fatalf("Plan() = %#v, %#v, %v", candidates, skipped, err)
			}
		})
	}
}

func TestEvictWorkDirForceDeletesOldChangedWorktree(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	uc, root := newTestUseCase(t, store, func(string) (bool, error) { return true, nil })
	path := filepath.Join(root, testWorktreeTaskID)
	store.changes[path] = true
	store.mtime[path] = validInput(TriggerExplicit).OccurredAt.Add(-8 * 24 * time.Hour)
	out, err := uc.Execute(context.Background(), EvictWorkDirInput{Trigger: TriggerExplicit, Force: true, OccurredAt: validInput(TriggerExplicit).OccurredAt}, []string{path})
	if err != nil || len(out.Deleted) != 1 || out.Deleted[0] != path {
		t.Fatalf("Execute() = %#v, %v", out, err)
	}
}

func TestEvictWorkDirForceIncludesYoungCleanWorktree(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	uc, root := newTestUseCase(t, store, func(string) (bool, error) { return true, nil })
	path := filepath.Join(root, testWorktreeTaskID)
	store.paths = []string{path}
	store.mtime[path] = validInput(TriggerExplicit).OccurredAt.Add(-time.Second)
	candidates, skipped, err := uc.Plan(context.Background(), EvictWorkDirInput{Trigger: TriggerExplicit, Force: true, MaxAgeDays: 7, OccurredAt: validInput(TriggerExplicit).OccurredAt})
	if err != nil || len(candidates) != 1 || len(skipped) != 0 {
		t.Fatalf("Plan() = %#v, %#v, %v", candidates, skipped, err)
	}
}

func TestEvictWorkDirDefaultRetentionAppliesSevenDays(t *testing.T) {
	now := validInput(TriggerExplicit).OccurredAt
	for _, tc := range []struct {
		name       string
		elapsed    time.Duration
		candidates int
	}{
		{"six days", 6 * 24 * time.Hour, 0},
		{"seven days", 7 * 24 * time.Hour, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
			uc, root := newTestUseCase(t, store, func(string) (bool, error) { return true, nil })
			path := filepath.Join(root, testWorktreeTaskID)
			store.paths = []string{path}
			store.changes[path], store.mtime[path] = true, now.Add(-tc.elapsed)
			candidates, _, err := uc.Plan(context.Background(), EvictWorkDirInput{Trigger: TriggerExplicit, Force: true, OccurredAt: now})
			if err != nil || len(candidates) != tc.candidates {
				t.Fatalf("Plan() = %#v, %v", candidates, err)
			}
		})
	}
}

func TestEvictWorkDirSkipsSymlinkInvalidTaskIDAndSymlinkError(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	uc, root := newTestUseCase(t, store, func(string) (bool, error) { return true, nil })
	link := filepath.Join(root, testWorktreeTaskID)
	invalid := filepath.Join(root, "not-a-task-id")
	broken := filepath.Join(root, "impl-20260808-120001-abcd-cleanup")
	store.paths = []string{link, invalid, broken}
	store.links[link] = true
	store.errs["link:"+broken] = errors.New("lstat failed")
	_, skipped, err := uc.Plan(context.Background(), validInput(TriggerAutomatic))
	if err != nil || len(skipped) != 3 {
		t.Fatalf("Plan() = %#v, %v", skipped, err)
	}
	want := []WorktreeSkipReason{WorktreeSkipSymlink, WorktreeSkipInvalidTaskID, WorktreeSkipSymlinkCheckFailed}
	for i, reason := range want {
		if skipped[i].Reason != reason {
			t.Fatalf("Skipped[%d] = %#v, want %q", i, skipped[i], reason)
		}
	}
}

func TestEvictWorkDirSymlinkErrorDoesNotStopOtherCandidates(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	uc, root := newTestUseCase(t, store, func(string) (bool, error) { return true, nil })
	broken := filepath.Join(root, testWorktreeTaskID)
	deletable := filepath.Join(root, "impl-20260808-120001-abcd-cleanup")
	store.paths = []string{broken, deletable}
	store.errs["link:"+broken] = errors.New("lstat failed")
	store.mtime[deletable] = validInput(TriggerAutomatic).OccurredAt
	out, err := uc.Execute(context.Background(), validInput(TriggerAutomatic), nil)
	if err != nil || len(out.Deleted) != 1 || out.Deleted[0] != deletable || len(out.Skipped) != 1 || out.Skipped[0].Reason != WorktreeSkipSymlinkCheckFailed {
		t.Fatalf("Execute() = %#v, %v", out, err)
	}
}

func TestEvictWorkDirLogsLivenessAndSymlinkErrors(t *testing.T) {
	var logs bytes.Buffer
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	root := t.TempDir()
	brokenSymlinkCheck := filepath.Join(root, testWorktreeTaskID)
	livenessFailed := filepath.Join(root, "impl-20260808-120001-abcd-cleanup")
	locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return false, errors.New("lock read failed") }), func(id domain.TaskID) string { return id.String() })
	uc, err := NewEvictWorkDirUseCase(store, locks, root, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	store.paths = []string{brokenSymlinkCheck, livenessFailed}
	store.errs["link:"+brokenSymlinkCheck] = errors.New("lstat failed")
	store.mtime[livenessFailed] = validInput(TriggerAutomatic).OccurredAt
	candidates, skipped, err := uc.Plan(context.Background(), validInput(TriggerAutomatic))
	if err != nil || len(candidates) != 0 || len(skipped) != 1 || skipped[0] != (WorktreeSkipped{Path: brokenSymlinkCheck, Reason: WorktreeSkipSymlinkCheckFailed}) {
		t.Fatalf("Plan() = %#v, %#v, %v", candidates, skipped, err)
	}
	for _, want := range []string{"check worktree symlink", brokenSymlinkCheck, "lstat failed", "check worktree liveness", livenessFailed, "lock read failed"} {
		if !bytes.Contains(logs.Bytes(), []byte(want)) {
			t.Fatalf("logs missing %q: %q", want, logs.String())
		}
	}
}

func TestEvictWorkDirRemoveFailureContinuesAndLogs(t *testing.T) {
	var logs bytes.Buffer
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	root := t.TempDir()
	locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(id domain.TaskID) string { return id.String() })
	uc, err := NewEvictWorkDirUseCase(store, locks, root, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	failed := filepath.Join(root, testWorktreeTaskID)
	success := filepath.Join(root, "impl-20260808-120001-abcd-cleanup")
	store.paths = []string{failed, success}
	store.mtime[failed], store.mtime[success] = validInput(TriggerAutomatic).OccurredAt, validInput(TriggerAutomatic).OccurredAt
	store.errs["remove:"+failed] = errors.New("permission denied")
	out, err := uc.Execute(context.Background(), validInput(TriggerAutomatic), nil)
	if err != nil || len(out.Deleted) != 1 || out.Deleted[0] != success || len(out.Skipped) != 1 || out.Skipped[0].Reason != WorktreeSkipRemoveFailed {
		t.Fatalf("Execute() = %#v, %v", out, err)
	}
	if !bytes.Contains(logs.Bytes(), []byte("remove worktree")) || !bytes.Contains(logs.Bytes(), []byte(failed)) {
		t.Fatalf("remove failure log = %q", logs.String())
	}
}

func TestEvictWorkDirLogsAgeErrorAndRejectedPath(t *testing.T) {
	var logs bytes.Buffer
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	root := t.TempDir()
	locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(id domain.TaskID) string { return id.String() })
	uc, err := NewEvictWorkDirUseCase(store, locks, root, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ageFailed := filepath.Join(root, testWorktreeTaskID)
	store.paths = []string{ageFailed, filepath.Join(root, "child", testWorktreeTaskID)}
	store.errs["age:"+ageFailed] = errors.New("stat failed")
	_, _, err = uc.Plan(context.Background(), validInput(TriggerAutomatic))
	if err != nil || !bytes.Contains(logs.Bytes(), []byte("compute worktree age")) || !bytes.Contains(logs.Bytes(), []byte("reject worktree path outside root")) {
		t.Fatalf("Plan() error/logs = %v, %q", err, logs.String())
	}
}

func TestEvictWorkDirRejectsInvalidConfirmedPathShapes(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	uc, root := newTestUseCase(t, store, func(string) (bool, error) { return true, nil })
	direct := filepath.Join(root, testWorktreeTaskID)
	store.mtime[direct] = validInput(TriggerExplicit).OccurredAt
	for _, tc := range []struct {
		name string
		path string
		want int
	}{
		{"direct child", direct, 1},
		{"grandchild", filepath.Join(root, "child", testWorktreeTaskID), 0},
		{"outside absolute path", filepath.Join(t.TempDir(), testWorktreeTaskID), 0},
		{"unnormalized parent traversal", root + string(filepath.Separator) + "child" + string(filepath.Separator) + ".." + string(filepath.Separator) + testWorktreeTaskID, 0},
		{"root", root, 0},
		{"empty", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := uc.Execute(context.Background(), validInput(TriggerExplicit), []string{tc.path})
			if err != nil || len(out.Candidates) != tc.want {
				t.Fatalf("Execute(%q) = %#v, %v", tc.path, out, err)
			}
		})
	}
}

func TestEvictWorkDirContextCancellationAndListFailure(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	uc, root := newTestUseCase(t, store, func(string) (bool, error) { return true, nil })
	path := filepath.Join(root, testWorktreeTaskID)
	store.paths, store.mtime[path] = []string{path}, validInput(TriggerAutomatic).OccurredAt
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := uc.Plan(ctx, validInput(TriggerAutomatic)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Plan() error = %v, want context cancellation", err)
	}
	if _, err := uc.Execute(ctx, validInput(TriggerAutomatic), nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context cancellation", err)
	}
	store.errs["list"] = errors.New("readdir failed")
	if _, _, err := uc.Plan(context.Background(), validInput(TriggerAutomatic)); err == nil {
		t.Fatal("Plan() ListTopLevel error = nil")
	}
}

func TestEvictWorkDirCheckLivenessIntegration(t *testing.T) {
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	calls := 0
	uc, root := newTestUseCase(t, store, func(string) (bool, error) { calls++; return calls > 1, nil })
	path := filepath.Join(root, testWorktreeTaskID)
	store.paths, store.mtime[path] = []string{path}, validInput(TriggerExplicit).OccurredAt
	plan, _, err := uc.Plan(context.Background(), validInput(TriggerExplicit))
	if err != nil || len(plan) != 0 {
		t.Fatalf("Plan() = %#v, %v", plan, err)
	}
	out, err := uc.Execute(context.Background(), validInput(TriggerExplicit), []string{path})
	if err != nil || len(out.Deleted) != 1 || calls != 2 {
		t.Fatalf("Execute() = %#v, calls=%d, %v", out, calls, err)
	}
}

func TestEvictWorkDirHoldsDeathLeaseAndWritesMarkerBeforeRemove(t *testing.T) {
	id, err := domain.NewTaskID("impl-20260814-120001-a1b2-deathlease")
	if err != nil {
		t.Fatal(err)
	}
	taskDir := filepath.Join(taskPlacementRoot, id.String())
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(taskDir) })
	lockPath := filepath.Join(taskDir, "task.lock")
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	root := t.TempDir()
	path := filepath.Join(root, id.String())
	store.mtime[path] = validInput(TriggerExplicit).OccurredAt
	store.onRemove = func(string) {
		if _, err := os.Lstat(worktreeEvictionMarkerPath(id)); err != nil {
			t.Errorf("marker unavailable during Remove: %v", err)
		}
		other, err := os.OpenFile(lockPath, os.O_RDWR, 0)
		if err != nil {
			t.Errorf("open competing lock: %v", err)
			return
		}
		defer other.Close()
		if err := syscall.Flock(int(other.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
			t.Errorf("death lease error during Remove = %v, want EWOULDBLOCK", err)
		}
	}
	locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(domain.TaskID) string { return lockPath })
	uc, err := NewEvictWorkDirUseCase(store, locks, root)
	if err != nil {
		t.Fatal(err)
	}
	out, err := uc.Execute(context.Background(), validInput(TriggerExplicit), []string{path})
	if err != nil || len(out.Deleted) != 1 {
		t.Fatalf("Execute() = %#v, %v", out, err)
	}
	other, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := syscall.Flock(int(other.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("death lease remained held after Execute: %v", err)
	}
}

func TestEvictWorkDirSkipsCandidateWhenDeathLeaseIsHeld(t *testing.T) {
	id, err := domain.NewTaskID("impl-20260814-120003-a1b2-heldlease")
	if err != nil {
		t.Fatal(err)
	}
	taskDir := filepath.Join(taskPlacementRoot, id.String())
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(taskDir) })
	lockPath := filepath.Join(taskDir, "task.lock")
	holder, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer holder.Close()
	if err := syscall.Flock(int(holder.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}

	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	root := t.TempDir()
	path := filepath.Join(root, id.String())
	store.mtime[path] = validInput(TriggerExplicit).OccurredAt
	locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(domain.TaskID) string { return lockPath })
	uc, err := NewEvictWorkDirUseCase(store, locks, root)
	if err != nil {
		t.Fatal(err)
	}

	out, err := uc.Execute(context.Background(), validInput(TriggerExplicit), []string{path})
	if err != nil || len(store.removed) != 0 || len(out.Deleted) != 0 || len(out.Skipped) != 1 || out.Skipped[0].Reason != WorktreeSkipStillAlive {
		t.Fatalf("Execute() = %#v, removed=%v, err=%v", out, store.removed, err)
	}
}

func TestEvictWorkDirRetainsMarkerAndReleasesDeathLeaseAfterRemoveFailure(t *testing.T) {
	id, err := domain.NewTaskID("impl-20260814-120004-a1b2-removefail")
	if err != nil {
		t.Fatal(err)
	}
	taskDir := filepath.Join(taskPlacementRoot, id.String())
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(taskDir) })
	lockPath := filepath.Join(taskDir, "task.lock")
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	root := t.TempDir()
	path := filepath.Join(root, id.String())
	store.mtime[path] = validInput(TriggerExplicit).OccurredAt
	store.errs["remove:"+path] = errors.New("remove failed")
	locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(domain.TaskID) string { return lockPath })
	uc, err := NewEvictWorkDirUseCase(store, locks, root)
	if err != nil {
		t.Fatal(err)
	}

	out, err := uc.Execute(context.Background(), validInput(TriggerExplicit), []string{path})
	if err != nil || len(out.Deleted) != 0 || len(out.Skipped) != 1 || out.Skipped[0].Reason != WorktreeSkipRemoveFailed {
		t.Fatalf("Execute() = %#v, %v", out, err)
	}
	if _, err := os.Lstat(worktreeEvictionMarkerPath(id)); err != nil {
		t.Fatalf("marker was not retained after remove failure: %v", err)
	}
	other, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := syscall.Flock(int(other.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatalf("death lease remained held after remove failure: %v", err)
	}
}

func TestEvictWorkDirHandlesMissingDeathLockTaskDirectoryStates(t *testing.T) {
	tests := []struct {
		name       string
		taskIDText string
		setup      func(t *testing.T, taskDir string)
		wantDelete bool
		wantMarker bool
	}{
		{
			name:       "task directory is absent",
			taskIDText: "impl-20260814-120007-a1b2-taskdirabsent",
			wantDelete: true,
		},
		{
			name:       "only task lock is absent",
			taskIDText: "impl-20260814-120008-a1b2-lockabsent",
			setup: func(t *testing.T, taskDir string) {
				t.Helper()
				if err := os.MkdirAll(taskDir, 0o700); err != nil {
					t.Fatal(err)
				}
			},
			wantDelete: true,
			wantMarker: true,
		},
		{
			name:       "task directory is symbolic link",
			taskIDText: "impl-20260814-120009-a1b2-taskdirlink",
			setup: func(t *testing.T, taskDir string) {
				t.Helper()
				if err := os.Symlink(t.TempDir(), taskDir); err != nil {
					t.Fatal(err)
				}
			},
		},
		{
			name:       "task directory is regular file",
			taskIDText: "impl-20260814-120010-a1b2-taskdirfile",
			setup: func(t *testing.T, taskDir string) {
				t.Helper()
				if err := os.WriteFile(taskDir, nil, 0o600); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := domain.NewTaskID(tt.taskIDText)
			if err != nil {
				t.Fatal(err)
			}
			taskDir := filepath.Join(taskPlacementRoot, id.String())
			if err := os.RemoveAll(taskDir); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(taskDir) })
			if tt.setup != nil {
				tt.setup(t, taskDir)
			}

			store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
			root := t.TempDir()
			path := filepath.Join(root, id.String())
			store.mtime[path] = validInput(TriggerExplicit).OccurredAt
			locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(domain.TaskID) string { return filepath.Join(taskDir, "task.lock") })
			uc, err := NewEvictWorkDirUseCase(store, locks, root)
			if err != nil {
				t.Fatal(err)
			}

			out, err := uc.Execute(context.Background(), validInput(TriggerExplicit), []string{path})
			if err != nil || (len(out.Deleted) == 1) != tt.wantDelete || len(out.Skipped) != 0 {
				t.Fatalf("Execute() = %#v, err=%v", out, err)
			}
			_, markerErr := os.Lstat(worktreeEvictionMarkerPath(id))
			if (markerErr == nil) != tt.wantMarker {
				t.Fatalf("marker error = %v, want marker=%t", markerErr, tt.wantMarker)
			}
			if tt.wantMarker {
				info, err := os.Stat(worktreeEvictionMarkerPath(id))
				if err != nil || info.Mode().Perm() != 0o600 {
					t.Fatalf("marker info = (%v, %v), want mode 600", info, err)
				}
				if data, err := os.ReadFile(worktreeEvictionMarkerPath(id)); err != nil || len(data) != 0 {
					t.Fatalf("marker data = %q, err=%v", data, err)
				}
			}
		})
	}
}

func TestEvictWorkDirDoesNotRemoveWhenMarkerWriteFails(t *testing.T) {
	id, err := domain.NewTaskID("impl-20260814-120005-a1b2-markerfail")
	if err != nil {
		t.Fatal(err)
	}
	taskDir := filepath.Join(taskPlacementRoot, id.String())
	if err := os.MkdirAll(taskDir, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(taskDir) })
	lockPath := filepath.Join(taskDir, "task.lock")
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	originalWriteAtomic := writeAtomic
	t.Cleanup(func() { writeAtomic = originalWriteAtomic })
	writeAtomic = func(string, []byte, os.FileMode) error { return errors.New("marker write failed") }
	var logOutput bytes.Buffer
	store := &fakeWorktreeStore{changes: map[string]bool{}, mtime: map[string]time.Time{}, links: map[string]bool{}, errs: map[string]error{}}
	root := t.TempDir()
	path := filepath.Join(root, id.String())
	store.mtime[path] = validInput(TriggerExplicit).OccurredAt
	locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), func(domain.TaskID) string { return lockPath })
	uc, err := NewEvictWorkDirUseCase(store, locks, root, slog.New(slog.NewTextHandler(&logOutput, nil)))
	if err != nil {
		t.Fatal(err)
	}

	out, err := uc.Execute(context.Background(), validInput(TriggerExplicit), []string{path})
	if err != nil || len(store.removed) != 0 || len(out.Deleted) != 0 || len(out.Skipped) != 0 {
		t.Fatalf("Execute() = %#v, removed=%v, err=%v", out, store.removed, err)
	}
	if !bytes.Contains(logOutput.Bytes(), []byte(id.String())) || !bytes.Contains(logOutput.Bytes(), []byte(worktreeEvictionMarkerPath(id))) || !bytes.Contains(logOutput.Bytes(), []byte("marker write failed")) {
		t.Fatalf("marker write failure log = %q", logOutput.String())
	}
}
