package transport

import (
	"log/slog"
	"sync"
	"time"
)

// ShutdownFinalizer completes the transport portion of daemon shutdown.
type ShutdownFinalizer interface {
	// Finalize waits for in-flight connections for gracePeriod, then closes tail sessions.
	Finalize(gracePeriod time.Duration)
}

type shutdownFinalizer struct {
	wg             *sync.WaitGroup
	tailConns      *tailConnRegistry
	acceptLoopDone <-chan struct{}
}

// NewShutdownFinalizer creates the shutdown finalizer shared with Serve.
func NewShutdownFinalizer(wg *sync.WaitGroup, tailConns *tailConnRegistry, acceptLoopDone <-chan struct{}) ShutdownFinalizer {
	return &shutdownFinalizer{wg: wg, tailConns: tailConns, acceptLoopDone: acceptLoopDone}
}

func (f *shutdownFinalizer) Finalize(gracePeriod time.Duration) {
	<-f.acceptLoopDone
	waited := make(chan struct{})
	go func() {
		f.wg.Wait()
		close(waited)
	}()

	timer := time.NewTimer(gracePeriod)
	defer timer.Stop()
	select {
	case <-waited:
	case <-timer.C:
		slog.Warn("shutdown grace period elapsed; forcing remaining connections closed")
	}
	f.tailConns.closeAll()
}
