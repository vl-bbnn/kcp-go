package kcp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type originalWorkerPacketGate struct {
	closed       chan struct{}
	closeOnce    sync.Once
	readEntered  chan struct{}
	readOnce     sync.Once
	readRelease  chan struct{}
	writeEntered chan struct{}
	writeOnce    sync.Once
	writeRelease chan struct{}
	writeErr     error
	closes       atomic.Int64
	packets      atomic.Int64
}

func (g *originalWorkerPacketGate) ReadFrom([]byte) (int, net.Addr, error) {
	<-g.closed
	g.readOnce.Do(func() { close(g.readEntered) })
	if g.readRelease != nil {
		<-g.readRelease
	}
	return 0, &net.UDPAddr{}, io.EOF
}
func (g *originalWorkerPacketGate) WriteTo(p []byte, _ net.Addr) (int, error) {
	g.packets.Add(1)
	if g.writeRelease != nil {
		g.writeOnce.Do(func() { close(g.writeEntered) })
		<-g.writeRelease
	}
	if g.writeErr != nil {
		return 0, g.writeErr
	}
	return len(p), nil
}
func (g *originalWorkerPacketGate) Close() error {
	g.closeOnce.Do(func() { g.closes.Add(1); close(g.closed) })
	return nil
}
func (g *originalWorkerPacketGate) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(192, 0, 2, 1), Port: 1}
}
func (g *originalWorkerPacketGate) SetDeadline(time.Time) error      { return nil }
func (g *originalWorkerPacketGate) SetReadDeadline(time.Time) error  { return nil }
func (g *originalWorkerPacketGate) SetWriteDeadline(time.Time) error { return nil }
func originalWorkerSession(t *testing.T, readHeld, writeHeld bool) (*UDPSession, *originalWorkerPacketGate) {
	t.Helper()
	g := &originalWorkerPacketGate{closed: make(chan struct{}), readEntered: make(chan struct{}), writeEntered: make(chan struct{})}
	if readHeld {
		g.readRelease = make(chan struct{})
	}
	if writeHeld {
		g.writeRelease = make(chan struct{})
	}
	s, e := NewConn3(23, g.LocalAddr(), nil, 0, 0, g)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		for _, ch := range []chan struct{}{g.readRelease, g.writeRelease} {
			if ch != nil {
				select {
				case <-ch:
				default:
					close(ch)
				}
			}
		}
		_ = s.Close()
		_ = g.Close()
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if e := s.WaitWorkersContext(ctx); e != nil {
			t.Error("original worker cleanup", e)
		}
	})
	return s, g
}
func originalWorkerWait(t *testing.T, desc string, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !ready() {
		if !time.Now().Before(deadline) {
			t.Fatal(desc)
		}
		time.Sleep(time.Millisecond)
	}
}
func TestSessionWorkerLifetimeOriginalReaderReturn(t *testing.T) {
	s, g := originalWorkerSession(t, true, false)
	var observations atomic.Int64
	if e := s.ObserveWorkersDone(func() { observations.Add(1) }); e != nil {
		t.Fatal(e)
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	_ = g.Close()
	select {
	case <-g.readEntered:
	case <-time.After(time.Second):
		t.Fatal("original terminal read not reached")
	}
	select {
	case <-s.die:
	default:
		t.Fatal("actual die not closed")
	}
	select {
	case <-s.WorkersDone():
		t.Fatal("held original reader certified complete")
	default:
	}
	ctx, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	e := s.WaitWorkersContext(ctx)
	stop()
	if !errors.Is(e, context.DeadlineExceeded) || observations.Load() != 0 {
		t.Fatal("held original worker certified", e, observations.Load())
	}
	close(g.readRelease)
	ctx, stop = context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if e := s.WaitWorkersContext(ctx); e != nil {
		t.Fatal(e)
	}
	originalWorkerWait(t, "original observer not called", func() bool { return observations.Load() == 1 })
	s.workers.mu.Lock()
	callback := s.workers.callback
	pending := s.workers.scheduledUpdates
	s.workers.mu.Unlock()
	if callback != nil || pending != 0 {
		t.Fatal("completed callback or actual scheduled task retained")
	}
	if e := s.ObserveWorkersDone(func() { observations.Add(100) }); e == nil {
		t.Fatal("second original observer admitted")
	}
	err, _ := s.socketReadError.Load().(error)
	if !errors.Is(err, io.EOF) {
		t.Fatal("original terminal read error lost", err)
	}
}
func TestSessionWorkerLifetimeOriginalPostProcessReturn(t *testing.T) {
	s, g := originalWorkerSession(t, false, true)
	raw := errors.New("original raw DATA write failure")
	g.writeErr = raw
	s.SetNoDelay(1, 10, 2, 1)
	payload := []byte("original worker output")
	if n, e := s.Write(payload); e != nil || n != len(payload) {
		t.Fatal(n, e)
	}
	select {
	case <-g.writeEntered:
	case <-time.After(time.Second):
		t.Fatal("actual original postprocessor write not reached")
	}
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	_ = g.Close()
	ctx, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	e := s.WaitWorkersContext(ctx)
	stop()
	if !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("held original postprocessor certified", e)
	}
	close(g.writeRelease)
	ctx, stop = context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if e := s.WaitWorkersContext(ctx); e != nil {
		t.Fatal(e)
	}
	err, _ := s.socketWriteError.Load().(error)
	if !errors.Is(err, raw) || g.packets.Load() == 0 {
		t.Fatal("actual raw packet/error lost", err, g.packets.Load())
	}
}
func TestSessionWorkerLifetimeOriginalScheduledUpdateReturn(t *testing.T) {
	s, g := originalWorkerSession(t, false, false)
	s.SetNoDelay(1, 500, 2, 1)
	originalWorkerWait(t, "original delayed update not scheduled", func() bool {
		s.workers.mu.Lock()
		defer s.workers.mu.Unlock()
		return s.workers.scheduledUpdates == 1 && time.Until(s.workers.nextUpdate) > 200*time.Millisecond
	})
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	_ = g.Close()
	originalWorkerWait(t, "original read/post workers did not return", func() bool {
		s.workers.mu.Lock()
		defer s.workers.mu.Unlock()
		return s.workers.returned&(sessionReadWorker|sessionPostWorker) == sessionReadWorker|sessionPostWorker
	})
	ctx, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	e := s.WaitWorkersContext(ctx)
	stop()
	if !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("original future update certified before its scheduled call returned", e)
	}
	ctx, stop = context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if e := s.WaitWorkersContext(ctx); e != nil {
		t.Fatal(e)
	}
	s.workers.mu.Lock()
	pending, returned := s.workers.scheduledUpdates, s.workers.returned
	s.workers.mu.Unlock()
	if pending != 0 || returned != sessionAllWorkers {
		t.Fatal("original scheduler completion missing", pending, returned)
	}
}
func TestSessionWorkerLifetimeReadyCallerCauseAndLateObserver(t *testing.T) {
	s, g := originalWorkerSession(t, false, false)
	_ = s.Close()
	_ = g.Close()
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	if e := s.WaitWorkersContext(ctx); e != nil {
		t.Fatal(e)
	}
	stop()
	var observations atomic.Int64
	if e := s.ObserveWorkersDone(func() { observations.Add(1) }); e != nil || observations.Load() != 1 {
		t.Fatal("late original observer", e)
	}
	cause := errors.New("original worker join caller cause")
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(cause)
	if e := s.WaitWorkersContext(ctx); !errors.Is(e, context.Canceled) || !errors.Is(e, cause) {
		t.Fatal("ready original worker hid caller cancellation", e)
	}
	ctx2, stop2 := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer stop2()
	if e := s.WaitWorkersContext(ctx2); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("ready original worker hid expired caller", e)
	}
	if e := (&UDPSession{}).WaitWorkersContext(context.Background()); !errors.Is(e, ErrSessionWorkersUnavailable) {
		t.Fatal("synthetic zero session certified", e)
	}
}
func TestSessionWorkerLifetimeListenerDoesNotOwnSharedReader(t *testing.T) {
	g := &originalWorkerPacketGate{closed: make(chan struct{}), readEntered: make(chan struct{}), writeEntered: make(chan struct{})}
	l := &Listener{}
	s := newUDPSession(23, 0, 0, l, g, false, g.LocalAddr(), nil)
	t.Cleanup(func() { _ = g.Close(); _ = s.Close() })
	if e := s.Close(); e != nil {
		t.Fatal(e)
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if e := s.WaitWorkersContext(ctx); e != nil {
		t.Fatal(e)
	}
	if g.closes.Load() != 0 {
		t.Fatal("session worker join closed shared listener socket")
	}
	select {
	case <-g.readEntered:
		t.Fatal("session created a shared listener reader")
	default:
	}
}
