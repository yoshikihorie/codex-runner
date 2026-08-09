package usecase

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/yoshikihorie/codex-runner/internal/domain"
	"github.com/yoshikihorie/codex-runner/internal/execution"
	transportusecase "github.com/yoshikihorie/codex-runner/internal/transport/usecase"
)

func TestPingRemainsResponsiveWhileQueueMutexIsHeld(t *testing.T) {
	queue, registry, mutex := execution.NewTaskQueue(), execution.NewActiveTaskRegistry(), &sync.Mutex{}
	admit := NewAdmitTaskUseCase(queue, registry, mutex, 1, 1)
	if _, err := admit.Execute(context.Background(), testAdmissionInput(t, domain.SubcommandImpl, "ping-active")); err != nil {
		t.Fatal(err)
	}
	if _, err := admit.Execute(context.Background(), testAdmissionInput(t, domain.SubcommandImpl, "ping-waiting")); err != nil {
		t.Fatal(err)
	}
	locked, unlock, lockerDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var unlockOnce sync.Once
	go func() {
		mutex.Lock()
		close(locked)
		<-unlock
		mutex.Unlock()
		close(lockerDone)
	}()
	t.Cleanup(func() {
		unlockOnce.Do(func() { close(unlock) })
		select {
		case <-lockerDone:
		case <-time.After(time.Second):
			t.Error("locker goroutine did not exit")
		}
	})
	select {
	case <-locked:
	case <-time.After(time.Second):
		t.Fatal("locker goroutine did not acquire mutex")
	}
	result := make(chan error, 1)
	pingDone := make(chan struct{})
	go func() { _, err := (&transportusecase.PingUseCase{}).Execute(context.Background()); result <- err }()
	t.Cleanup(func() {
		select {
		case <-pingDone:
		default:
			go func() { <-result; close(pingDone) }()
		}
		select {
		case <-pingDone:
		case <-time.After(time.Second):
			t.Error("ping goroutine did not exit")
		}
	})
	// Cleanup runs in LIFO order, so release the queue mutex before waiting for Ping.
	t.Cleanup(func() { unlockOnce.Do(func() { close(unlock) }) })
	select {
	case err := <-result:
		close(pingDone)
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("ping blocked on queue mutex")
	}
	unlockOnce.Do(func() { close(unlock) })
}
