package execution

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
)

type worktreeEvictionTicker struct {
	ch      chan time.Time
	stopped bool
}

func (t *worktreeEvictionTicker) C() <-chan time.Time { return t.ch }
func (t *worktreeEvictionTicker) Stop()               { t.stopped = true }

type worktreeEvictionTickerFactoryFake struct {
	ticker    *worktreeEvictionTicker
	intervals []time.Duration
}

func (f *worktreeEvictionTickerFactoryFake) NewTicker(interval time.Duration) logTicker {
	f.intervals = append(f.intervals, interval)
	return f.ticker
}

type worktreeEvictionStore struct {
	roots  []string
	errs   []error
	calls  int
	listed chan struct{}
}

func (s *worktreeEvictionStore) ListTopLevel(root string) ([]string, error) {
	s.roots = append(s.roots, root)
	if s.listed != nil {
		s.listed <- struct{}{}
	}
	var err error
	if s.calls < len(s.errs) {
		err = s.errs[s.calls]
	}
	s.calls++
	return nil, err
}
func (*worktreeEvictionStore) IsSymlink(string) (bool, error)         { return false, nil }
func (*worktreeEvictionStore) HasGitChanges(string) (bool, error)     { return false, nil }
func (*worktreeEvictionStore) ModTime(string) (time.Time, error)      { return time.Time{}, nil }
func (*worktreeEvictionStore) AgeDays(string, time.Time) (int, error) { return 0, nil }
func (*worktreeEvictionStore) Remove(string) error                    { return nil }

func newWorktreeEvictionRunUseCase(t *testing.T, store WorktreeStore, root string, logger *slog.Logger) *EvictWorkDirUseCase {
	t.Helper()
	locks := NewCheckLivenessUseCase(domain.LivenessLockFunc(func(string) (bool, error) { return true, nil }), DefaultLockPathResolver)
	uc, err := NewEvictWorkDirUseCase(store, locks, root, logger)
	if err != nil {
		t.Fatal(err)
	}
	return uc
}

func withWorktreeEvictionTickerFactory(t *testing.T, factory logTickerFactory) {
	t.Helper()
	previous := worktreeEvictionTickerFactory
	worktreeEvictionTickerFactory = factory
	t.Cleanup(func() { worktreeEvictionTickerFactory = previous })
}

func TestEvictWorkDirRunUsesConfiguredRootAndAutomaticInput(t *testing.T) {
	for _, root := range []string{filepath.Join(t.TempDir(), "daemon-one"), filepath.Join(t.TempDir(), "daemon-two")} {
		t.Run(root, func(t *testing.T) {
			store := &worktreeEvictionStore{listed: make(chan struct{}, 1)}
			ticker := &worktreeEvictionTicker{ch: make(chan time.Time, 1)}
			factory := &worktreeEvictionTickerFactoryFake{ticker: ticker}
			withWorktreeEvictionTickerFactory(t, factory)
			uc := newWorktreeEvictionRunUseCase(t, store, root, slog.Default())
			at := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() { uc.Run(ctx, 23*time.Second); close(done) }()
			ticker.ch <- at
			select {
			case <-store.listed:
			case <-time.After(time.Second):
				t.Fatal("Run did not execute on tick")
			}
			cancel()
			<-done

			if len(store.roots) != 1 || store.roots[0] != root {
				t.Fatalf("ListTopLevel roots=%#v, want %#v", store.roots, []string{root})
			}
			if len(factory.intervals) != 1 || factory.intervals[0] != 23*time.Second {
				t.Fatalf("ticker intervals=%#v", factory.intervals)
			}
			if !ticker.stopped {
				t.Fatal("ticker was not stopped")
			}
		})
	}
}

func TestEvictWorkDirRunSkipsNonPositiveIntervalAndCancelledContext(t *testing.T) {
	store := &worktreeEvictionStore{}
	ticker := &worktreeEvictionTicker{ch: make(chan time.Time, 1)}
	factory := &worktreeEvictionTickerFactoryFake{ticker: ticker}
	withWorktreeEvictionTickerFactory(t, factory)
	uc := newWorktreeEvictionRunUseCase(t, store, t.TempDir(), slog.Default())
	uc.Run(context.Background(), 0)
	if len(factory.intervals) != 0 || len(store.roots) != 0 {
		t.Fatalf("non-positive interval started work: intervals=%#v roots=%#v", factory.intervals, store.roots)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	uc.Run(ctx, time.Second)
	if len(factory.intervals) != 1 || !ticker.stopped || len(store.roots) != 0 {
		t.Fatalf("cancelled Run state: intervals=%#v stopped=%t roots=%#v", factory.intervals, ticker.stopped, store.roots)
	}
}

func TestEvictWorkDirRunIgnoresMissingRootAndWarnsForOtherErrors(t *testing.T) {
	var logs bytes.Buffer
	store := &worktreeEvictionStore{errs: []error{fs.ErrNotExist, errors.New("list failed")}, listed: make(chan struct{}, 2)}
	ticker := &worktreeEvictionTicker{ch: make(chan time.Time, 2)}
	factory := &worktreeEvictionTickerFactoryFake{ticker: ticker}
	withWorktreeEvictionTickerFactory(t, factory)
	uc := newWorktreeEvictionRunUseCase(t, store, t.TempDir(), slog.New(slog.NewTextHandler(&logs, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { uc.Run(ctx, time.Second); close(done) }()
	ticker.ch <- time.Now()
	<-store.listed
	ticker.ch <- time.Now()
	<-store.listed
	cancel()
	<-done
	if got := bytes.Count(logs.Bytes(), []byte("worktree eviction scan failed")); got != 1 {
		t.Fatalf("warning count=%d logs=%q", got, logs.String())
	}
}
