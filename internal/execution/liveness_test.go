package execution

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/store"
)

func testTaskID(t *testing.T) domain.TaskID {
	t.Helper()
	id, err := domain.NewTaskID("impl-20260807-120000-abcd-liveness")
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func testResolver(root string) lockPathResolver {
	return func(id domain.TaskID) string {
		return filepath.Join(root, id.String(), "task.lock")
	}
}

func makeTaskDirectory(t *testing.T, root string, id domain.TaskID) string {
	t.Helper()
	dir := filepath.Join(root, id.String())
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestAcquireForChildCreatesLockedFile(t *testing.T) {
	dir := t.TempDir()
	f, err := AcquireForChild(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	info, err := os.Stat(filepath.Join(dir, "task.lock"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("permissions = %o, want 600", info.Mode().Perm())
	}

	other, err := os.OpenFile(filepath.Join(dir, "task.lock"), os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := syscall.Flock(int(other.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("second exclusive lock error = %v, want EWOULDBLOCK", err)
	}
}

func TestAcquireForChildRejectsExistingPaths(t *testing.T) {
	tests := []struct {
		name  string
		setup func(t *testing.T, lockPath string)
	}{
		{"regular file", func(t *testing.T, lockPath string) {
			t.Helper()
			if err := os.WriteFile(lockPath, []byte("existing"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
		{"symbolic link", func(t *testing.T, lockPath string) {
			t.Helper()
			if err := os.Symlink(filepath.Join(filepath.Dir(lockPath), "missing"), lockPath); err != nil {
				t.Fatal(err)
			}
		}},
		{"existing lock file", func(t *testing.T, lockPath string) {
			t.Helper()
			f, err := os.Create(lockPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			lockPath := filepath.Join(dir, "task.lock")
			tt.setup(t, lockPath)
			f, err := AcquireForChild(dir)
			if err == nil {
				_ = f.Close()
				t.Fatal("AcquireForChild unexpectedly succeeded")
			}
			if tt.name == "regular file" {
				contents, readErr := os.ReadFile(lockPath)
				if readErr != nil || string(contents) != "existing" {
					t.Fatalf("existing file changed: contents=%q err=%v", contents, readErr)
				}
			}
		})
	}
}

func TestAcquireForChildClosesFileWhenFlockFails(t *testing.T) {
	original := flockFunc
	t.Cleanup(func() { flockFunc = original })
	var captured *os.File
	flockFunc = func(f *os.File, _ int) error {
		captured = f
		return syscall.EWOULDBLOCK
	}

	f, err := AcquireForChild(t.TempDir())
	if f != nil || err == nil {
		t.Fatalf("result = (%v, %v), want (nil, error)", f, err)
	}
	if captured == nil {
		t.Fatal("flock did not receive the opened file")
	}
	if _, err := captured.Stat(); err == nil {
		t.Fatal("opened file was not closed after flock failure")
	}
}

func TestAcquireExistingForChildLocksOnlyExistingFile(t *testing.T) {
	dir := t.TempDir()
	lockPath := filepath.Join(dir, "task.lock")
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := AcquireExistingForChild(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	other, err := os.OpenFile(lockPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := syscall.Flock(int(other.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); !errors.Is(err, syscall.EWOULDBLOCK) {
		t.Fatalf("second lock error = %v, want EWOULDBLOCK", err)
	}
}

func TestAcquireExistingForChildDoesNotCreateLock(t *testing.T) {
	dir := t.TempDir()
	if _, err := AcquireExistingForChild(dir); err == nil {
		t.Fatal("AcquireExistingForChild unexpectedly created a lock")
	}
	if _, err := os.Stat(filepath.Join(dir, "task.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("lock stat error = %v, want not exist", err)
	}
}

func TestAcquireExistingForChildClosesFileWhenFlockFails(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "task.lock"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	original := flockFunc
	t.Cleanup(func() { flockFunc = original })
	var captured *os.File
	flockFunc = func(f *os.File, _ int) error { captured = f; return syscall.EWOULDBLOCK }
	f, err := AcquireExistingForChild(dir)
	if f != nil || !errors.Is(err, syscall.EWOULDBLOCK) || captured == nil {
		t.Fatalf("result = (%v, %v), captured = %v", f, err, captured)
	}
	if _, err := captured.Stat(); err == nil {
		t.Fatal("file remained open after flock error")
	}
}

func TestAcquireForChildPropagatesMissingDirectoryAndRejectsSecondCall(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	if _, err := AcquireForChild(missing); err == nil {
		t.Fatal("missing directory did not return an error")
	}

	dir := t.TempDir()
	f, err := AcquireForChild(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := AcquireForChild(dir); err == nil {
		t.Fatal("second call unexpectedly succeeded")
	}
}

func TestCheckLivenessUseCaseResults(t *testing.T) {
	id := testTaskID(t)
	root := t.TempDir()
	resolver := testResolver(root)
	useCase := NewCheckLivenessUseCase(domain.LivenessLockFunc(store.TryAcquireLiveness), resolver)

	t.Run("unheld is dead", func(t *testing.T) {
		dir := makeTaskDirectory(t, root, id)
		f, err := os.OpenFile(filepath.Join(dir, "task.lock"), os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		dead, err := useCase.Execute(context.Background(), id)
		if !dead || err != nil {
			t.Fatalf("result = (%t, %v), want (true, nil)", dead, err)
		}
	})

	t.Run("held is not dead", func(t *testing.T) {
		heldID, err := domain.NewTaskID("impl-20260807-120003-abcd-held")
		if err != nil {
			t.Fatal(err)
		}
		dir := makeTaskDirectory(t, root, heldID)
		f, err := AcquireForChild(dir)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		dead, err := useCase.Execute(context.Background(), heldID)
		if dead || err != nil {
			t.Fatalf("result = (%t, %v), want (false, nil)", dead, err)
		}
	})

	t.Run("missing file", func(t *testing.T) {
		missingID, err := domain.NewTaskID("impl-20260807-120001-abcd-missing-file")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(filepath.Join(root, missingID.String()), 0o700); err != nil {
			t.Fatal(err)
		}
		dead, err := useCase.Execute(context.Background(), missingID)
		if dead || !errors.Is(err, domain.ErrTaskNotFound) {
			t.Fatalf("result = (%t, %v), want (false, ErrTaskNotFound)", dead, err)
		}
	})

	t.Run("missing directory", func(t *testing.T) {
		missingID, err := domain.NewTaskID("impl-20260807-120002-abcd-missing-directory")
		if err != nil {
			t.Fatal(err)
		}
		dead, err := useCase.Execute(context.Background(), missingID)
		if dead || !errors.Is(err, domain.ErrTaskNotFound) {
			t.Fatalf("result = (%t, %v), want (false, ErrTaskNotFound)", dead, err)
		}
	})
}

func TestCheckLivenessUseCaseFailsClosed(t *testing.T) {
	id := testTaskID(t)
	useCase := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) {
		return false, syscall.EIO
	}), func(domain.TaskID) string { return "unused" })
	dead, err := useCase.Execute(context.Background(), id)
	if dead || err == nil || !errors.Is(err, syscall.EIO) || errors.Is(err, domain.ErrTaskNotFound) {
		t.Fatalf("result = (%t, %v), want fail-closed EIO", dead, err)
	}
}

func TestCheckLivenessUseCasePermissionFailureIsNotNotFound(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("permission checks are not reliable for root")
	}
	id := testTaskID(t)
	root := t.TempDir()
	dir := makeTaskDirectory(t, root, id)
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	useCase := NewCheckLivenessUseCase(domain.LivenessLockFunc(store.TryAcquireLiveness), testResolver(root))
	dead, err := useCase.Execute(context.Background(), id)
	if dead || err == nil || errors.Is(err, domain.ErrTaskNotFound) {
		t.Fatalf("result = (%t, %v), want non-not-found error", dead, err)
	}
}

func TestLockPathResolvers(t *testing.T) {
	id := testTaskID(t)
	root := t.TempDir()
	var received string
	useCase := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(path string) (bool, error) { received = path; return true, nil }), testResolver(root))
	if dead, err := useCase.Execute(context.Background(), id); !dead || err != nil {
		t.Fatalf("result = (%t, %v)", dead, err)
	}
	if want := filepath.Join(root, id.String(), "task.lock"); received != want {
		t.Fatalf("path = %q, want %q", received, want)
	}
	if want := filepath.Join(taskPlacementRoot, id.String(), "task.lock"); DefaultLockPathResolver(id) != want {
		t.Fatalf("default path = %q, want %q", DefaultLockPathResolver(id), want)
	}
}

func TestCheckLivenessUseCaseLeavesUnheldLockUnchanged(t *testing.T) {
	id := testTaskID(t)
	root := t.TempDir()
	dir := makeTaskDirectory(t, root, id)
	lockPath := filepath.Join(dir, "task.lock")
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	useCase := NewCheckLivenessUseCase(domain.LivenessLockFunc(store.TryAcquireLiveness), testResolver(root))
	for range 3 {
		dead, err := useCase.Execute(context.Background(), id)
		if !dead || err != nil {
			t.Fatalf("result = (%t, %v), want (true, nil)", dead, err)
		}
	}
	if info, err := os.Stat(lockPath); err != nil || !info.Mode().IsRegular() {
		t.Fatalf("lock state = (%v, %v)", info, err)
	}
	if contents, err := os.ReadFile(lockPath); err != nil || len(contents) != 0 {
		t.Fatalf("lock contents = %q, err = %v", contents, err)
	}
}

func TestAcquireForChildAndCheckLiveness(t *testing.T) {
	id := testTaskID(t)
	root := t.TempDir()
	dir := makeTaskDirectory(t, root, id)
	f, err := AcquireForChild(dir)
	if err != nil {
		t.Fatal(err)
	}
	useCase := NewCheckLivenessUseCase(domain.LivenessLockFunc(store.TryAcquireLiveness), testResolver(root))
	if dead, err := useCase.Execute(context.Background(), id); dead || err != nil {
		t.Fatalf("held result = (%t, %v)", dead, err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if dead, err := useCase.Execute(context.Background(), id); !dead || err != nil {
		t.Fatalf("released result = (%t, %v)", dead, err)
	}
}

func TestCheckLivenessUseCaseConcurrentQueries(t *testing.T) {
	id := testTaskID(t)
	root := t.TempDir()
	dir := makeTaskDirectory(t, root, id)
	f, err := AcquireForChild(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	useCase := NewCheckLivenessUseCase(domain.LivenessLockFunc(store.TryAcquireLiveness), testResolver(root))
	const callers = 8
	results := make(chan struct {
		dead bool
		err  error
	}, callers)
	var start sync.WaitGroup
	start.Add(1)
	for range callers {
		go func() {
			start.Wait()
			dead, err := useCase.Execute(context.Background(), id)
			results <- struct {
				dead bool
				err  error
			}{dead, err}
		}()
	}
	start.Done()
	for range callers {
		result := <-results
		if result.dead || result.err != nil {
			t.Fatalf("concurrent result = (%t, %v)", result.dead, result.err)
		}
	}
	if contents, err := os.ReadFile(filepath.Join(dir, "task.lock")); err != nil || len(contents) != 0 {
		t.Fatalf("lock contents = %q, err = %v", contents, err)
	}
}

func TestChildInheritanceLiveness(t *testing.T) {
	if os.Getenv("GO_WANT_LIVENESS_HELPER") == "1" {
		select {}
	}
	id := testTaskID(t)
	root := t.TempDir()
	dir := makeTaskDirectory(t, root, id)
	f, err := AcquireForChild(dir)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestChildInheritanceLiveness$")
	cmd.Env = append(os.Environ(), "GO_WANT_LIVENESS_HELPER=1")
	cmd.ExtraFiles = []*os.File{f}
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatal(err)
	}
	useCase := NewCheckLivenessUseCase(domain.LivenessLockFunc(store.TryAcquireLiveness), testResolver(root))
	if dead, err := useCase.Execute(context.Background(), id); dead || err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("child-held result = (%t, %v)", dead, err)
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child unexpectedly exited cleanly")
	}
	if dead, err := useCase.Execute(context.Background(), id); !dead || err != nil {
		t.Fatalf("child-exited result = (%t, %v)", dead, err)
	}
}

func TestLivenessImplementationDoesNotCompareProcessIdentifiers(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate test source")
	}
	root := filepath.Dir(filepath.Dir(thisFile))
	paths := []string{
		filepath.Join(filepath.Dir(thisFile), "liveness.go"),
		filepath.Join(root, "store", "flocklock.go"),
		filepath.Join(root, "domain", "liveness.go"),
	}
	for _, path := range paths {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		source := string(contents)
		for _, forbidden := range []string{"Get" + "pid", "Process." + "Pid"} {
			if strings.Contains(source, forbidden) {
				t.Fatalf("process identifier comparison found in %s", path)
			}
		}
		if path == paths[0] && !strings.Contains(source, "Flock") {
			t.Fatalf("liveness implementation does not use flock: %s", path)
		}
	}
}

func TestCheckLivenessUseCaseTranslatesWrappedNotExist(t *testing.T) {
	useCase := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) {
		return false, &fs.PathError{Op: "open", Path: "missing", Err: fs.ErrNotExist}
	}), func(domain.TaskID) string { return "unused" })
	dead, err := useCase.Execute(context.Background(), testTaskID(t))
	if dead || !errors.Is(err, domain.ErrTaskNotFound) {
		t.Fatalf("result = (%t, %v)", dead, err)
	}
}
