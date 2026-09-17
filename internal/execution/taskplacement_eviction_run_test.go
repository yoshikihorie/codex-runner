package execution

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/store"
)

type fakeTaskPlacementTicker struct{ ch chan time.Time }

func (t *fakeTaskPlacementTicker) C() <-chan time.Time { return t.ch }
func (*fakeTaskPlacementTicker) Stop()                 {}

type fakeTaskPlacementTickerFactory struct{ ticker *fakeTaskPlacementTicker }

func (f fakeTaskPlacementTickerFactory) NewTicker(time.Duration) logTicker { return f.ticker }

func TestEvictTaskPlacementRunPlansAtStartupAndExecutesOnlyOnTicks(t *testing.T) {
	placement := &fakeTaskPlacementStore{plan: &store.TaskPlacementPlan{}}
	u, err := NewEvictTaskPlacementUseCase(placement, &fakeEvictedTaskIndex{}, TaskPlacementEvictionPolicy{RetentionDays: 1, DiskBudgetMB: 1, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	ticker := &fakeTaskPlacementTicker{ch: make(chan time.Time, 1)}
	original := taskPlacementEvictionTickerFactory
	taskPlacementEvictionTickerFactory = fakeTaskPlacementTickerFactory{ticker: ticker}
	t.Cleanup(func() { taskPlacementEvictionTickerFactory = original })
	ctx, cancel := context.WithCancel(context.Background())
	var done sync.WaitGroup
	done.Add(1)
	go func() { defer done.Done(); u.Run(ctx, time.Second) }()
	deadline := time.Now().Add(time.Second)
	for {
		planCalls, _ := placement.calls()
		if planCalls != 0 || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	planCalls, executeCalls := placement.calls()
	if planCalls != 1 || executeCalls != 0 {
		t.Fatal("startup plan did not run")
	}
	if len(placement.deleted) != 0 {
		t.Fatal("startup executed")
	}
	ticker.ch <- time.Now()
	deadline = time.Now().Add(time.Second)
	for {
		_, executeCalls = placement.calls()
		if executeCalls != 0 || !time.Now().Before(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	planCalls, executeCalls = placement.calls()
	if planCalls != 2 || executeCalls != 1 {
		t.Fatalf("plan=%d execute=%d", planCalls, executeCalls)
	}
	cancel()
	done.Wait()
}
