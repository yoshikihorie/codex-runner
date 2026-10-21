package usecase

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/supervisor"
	"github.com/yoshikihorie/codex-runner/internal/supervisor/detect"
)

func TestObservedContextStore(t *testing.T) {
	var store ObservedContextStore
	id := supervisor.SessionID("context-store-1")
	if store.Get(id) != nil {
		t.Fatal("zero value should be unknown")
	}
	value := 61
	store.Set(id, &value)
	value = 12
	if got := store.Get(id); got == nil || *got != 61 {
		t.Fatalf("stored value = %v", got)
	}
	got := store.Get(id)
	*got = 10
	if next := store.Get(id); next == nil || *next != 61 {
		t.Fatalf("Get leaked pointer: %v", next)
	}
	store.Set(id, nil)
	if store.Get(id) != nil {
		t.Fatal("nil should be unknown")
	}
	store.Set(id, &value)
	store.Delete(id)
	if store.Get(id) != nil {
		t.Fatal("Delete retained value")
	}
}

func TestObservedContextStoreConcurrent(t *testing.T) {
	var store ObservedContextStore
	id := supervisor.SessionID("context-concurrent-1")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(value int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				store.Set(id, &value)
				_ = store.Get(id)
				store.Delete(id)
			}
		}(i)
	}
	wg.Wait()
}

func TestObserveSessionContextLifecycle(t *testing.T) {
	s, sessions, _ := observeSendFixture(t, supervisor.SessionIdle)
	observed := &ObservedContextStore{}
	percent := 61
	at := s.StateSince.Add(time.Second)
	detector := &constantDetector{result: detect.DetectResult{State: detect.StateWorking, ContextPercent: &percent}}
	var stale atomic.Bool
	u := ObserveSessionUseCase{Detect: DetectSessionStateUseCase{Detector: detector}, Sessions: sessions, ContextPercent: observed, RotationStatuslineAt: func(supervisor.SessionID) *time.Time {
		if stale.Load() {
			old := s.StateSince.Add(-time.Second)
			return &old
		}
		return &at
	}, Interval: time.Millisecond}
	p := newObserveSendPTY()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- u.Run(ctx, s, p, supervisor.NewVirtualTerminal(80, 24)) }()
	waitContext := func(want *int) {
		t.Helper()
		deadline := time.After(time.Second)
		for {
			got := observed.Get(s.SessionID)
			if (want == nil && got == nil) || (want != nil && got != nil && *got == *want) {
				return
			}
			select {
			case <-deadline:
				t.Fatalf("context = %v, want %v", got, want)
			default:
				time.Sleep(time.Millisecond)
			}
		}
	}
	waitContext(&percent)
	stale.Store(true)
	waitContext(nil)
	// The stored observation must disappear when the child exits.
	p.exits <- 0
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not exit")
	}
	if got := observed.Get(s.SessionID); got != nil {
		t.Fatalf("exit retained context %v", got)
	}
	cancel()
	_ = p.Close()
}
