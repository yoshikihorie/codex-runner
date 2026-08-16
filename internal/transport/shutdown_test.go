package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type shutdownTestConn struct {
	closed chan struct{}
	once   sync.Once
	err    error
}

func newShutdownTestConn(err error) *shutdownTestConn {
	return &shutdownTestConn{closed: make(chan struct{}), err: err}
}

func (c *shutdownTestConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (c *shutdownTestConn) Write([]byte) (int, error)        { return 0, net.ErrClosed }
func (c *shutdownTestConn) LocalAddr() net.Addr              { return &net.UnixAddr{Name: "local", Net: "unix"} }
func (c *shutdownTestConn) RemoteAddr() net.Addr             { return &net.UnixAddr{Name: "remote", Net: "unix"} }
func (c *shutdownTestConn) SetDeadline(time.Time) error      { return nil }
func (c *shutdownTestConn) SetReadDeadline(time.Time) error  { return nil }
func (c *shutdownTestConn) SetWriteDeadline(time.Time) error { return nil }
func (c *shutdownTestConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.err
}

func TestShutdownFinalizerWaitsForAcceptLoopBeforeWaitingForConnections(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	registry := NewTailConnRegistry()
	acceptLoopDone := make(chan struct{})
	finalizer := NewShutdownFinalizer(&wg, registry, acceptLoopDone)
	finished := make(chan struct{})
	go func() {
		finalizer.Finalize(time.Second)
		close(finished)
	}()
	select {
	case <-finished:
		t.Fatal("Finalize returned before accept loop completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(acceptLoopDone)
	select {
	case <-finished:
		t.Fatal("Finalize returned before active connection completed")
	case <-time.After(50 * time.Millisecond):
	}
	wg.Done()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("Finalize did not return after active connection completed")
	}
}

func TestTailConnRegistryCloseAllCancelsAndClosesEveryRegisteredConnection(t *testing.T) {
	registry := NewTailConnRegistry()
	first := newShutdownTestConn(nil)
	second := newShutdownTestConn(errors.New("close failed"))
	firstCtx, firstCancel := context.WithCancel(context.Background())
	secondCtx, secondCancel := context.WithCancel(context.Background())
	if !registry.add(first, firstCancel) || !registry.add(second, secondCancel) {
		t.Fatal("registry rejected active tail connection")
	}
	registry.closeAll()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		conn *shutdownTestConn
	}{
		{"first", firstCtx, first},
		{"second", secondCtx, second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			select {
			case <-tc.ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("tail context was not cancelled")
			}
			select {
			case <-tc.conn.closed:
			case <-time.After(time.Second):
				t.Fatal("tail connection was not closed")
			}
		})
	}

	rejected := newShutdownTestConn(nil)
	rejectedCtx, rejectedCancel := context.WithCancel(context.Background())
	if registry.add(rejected, rejectedCancel) {
		t.Fatal("registry accepted a tail connection after shutdown")
	}
	select {
	case <-rejectedCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("rejected tail context was not cancelled")
	}
	select {
	case <-rejected.closed:
	case <-time.After(time.Second):
		t.Fatal("rejected tail connection was not closed")
	}
}

func TestShutdownFinalizerClosesTailConnectionsWhenGracePeriodExpires(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	registry := NewTailConnRegistry()
	conn := newShutdownTestConn(nil)
	tailCtx, tailCancel := context.WithCancel(context.Background())
	if !registry.add(conn, tailCancel) {
		t.Fatal("registry rejected active tail connection")
	}
	acceptLoopDone := make(chan struct{})
	close(acceptLoopDone)
	finished := make(chan struct{})
	go func() {
		NewShutdownFinalizer(&wg, registry, acceptLoopDone).Finalize(time.Millisecond)
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("Finalize did not return after grace period")
	}
	select {
	case <-tailCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("tail context was not cancelled")
	}
	select {
	case <-conn.closed:
	case <-time.After(time.Second):
		t.Fatal("tail connection was not closed")
	}
	wg.Done()
}
