package kcp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func nativeFixture2654(t *testing.T) (*UDPSession, *nativePacket2654) {
	t.Helper()
	a, b := net.Pipe()
	raw := &nativePacket2654{Conn: a, readEntered: make(chan struct{}), writeEntered: make(chan struct{}), allowRead: make(chan struct{}), allowWrite: make(chan struct{})}
	close(raw.allowRead)
	close(raw.allowWrite)
	s, err := NewConn3(1, b.LocalAddr(), nil, 0, 0, raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = s.Close()
		_ = raw.Close()
		_ = b.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.WaitClosed(ctx)
	})
	awaitNative2654(t, raw.readEntered, "real original native read entry")
	return s, raw
}

func TestActual2654NativeKCPListenerAndReplacementKeepIndependentOwnership(t *testing.T) {
	l, err := ListenWithOptions("127.0.0.1:0", nil, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_ = l.SetReadDeadline(time.Now().Add(2 * time.Second))
	type pair struct{ client, server *UDPSession }
	newPair := func(value byte) pair {
		c, err := DialWithOptions(l.Addr().String(), nil, 0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Write([]byte{value}); err != nil {
			t.Fatal(err)
		}
		s, err := l.AcceptKCP()
		if err != nil {
			t.Fatal(err)
		}
		_ = s.SetReadDeadline(time.Now().Add(time.Second))
		buf := []byte{0}
		if _, err := io.ReadFull(s, buf); err != nil || buf[0] != value {
			t.Fatalf("actual listener session data: %v %v", buf, err)
		}
		t.Cleanup(func() {
			_ = c.Close()
			_ = s.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = c.WaitClosed(ctx)
			_ = s.WaitClosed(ctx)
		})
		return pair{client: c, server: s}
	}
	original, other := newPair(1), newPair(2)
	_ = original.server.Close()
	_ = original.client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := original.server.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
	if err := original.client.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
	if original.server.NativeWorkerSnapshot().ClientReadOwned || !original.client.NativeWorkerSnapshot().ClientReadOwned {
		t.Fatal("listener-global monitor was falsely assigned as per-session client readLoop")
	}
	if other.server.NativeWorkerSnapshot().Sealed || other.client.NativeWorkerSnapshot().Sealed {
		t.Fatal("foreign listener session sealed")
	}
	if _, err := other.client.Write([]byte{3}); err != nil {
		t.Fatal(err)
	}
	buf := []byte{0}
	if _, err := io.ReadFull(other.server, buf); err != nil || buf[0] != 3 {
		t.Fatal("foreign session stopped carrying actual native data")
	}
	_ = newPair(4) // Shared listener still accepts a distinct replacement.
}

func TestActual2654NativeKCPQueuedCallbackAndForeignSchedulerStayReal(t *testing.T) {
	s, raw := nativeFixture2654(t)
	foreign, foreignRaw := nativeFixture2654(t)
	// One extra actual queued invocation exercises the same production birth
	// and callback epilogue path, on the unchanged shared scheduler.
	s.scheduleOwnedUpdate(time.Now().Add(200 * time.Millisecond))
	before := s.NativeWorkerSnapshot()
	if len(before.Active) < 3 {
		t.Fatal("actual native workers not registered")
	}
	_ = s.Close()
	_ = raw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := s.WaitClosed(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued original callback was discarded: %v", err)
	}
	cancel()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
	after := s.NativeWorkerSnapshot()
	if !after.Sealed || after.Started != after.Exited || len(after.Active) != 0 || after.Generation != before.Generation || after.ExitDigest == "" {
		t.Fatalf("native epilogues missing: %+v", after)
	}
	if foreign.NativeWorkerSnapshot().Sealed {
		t.Fatal("original Close sealed foreign/replacement lifetime")
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	if err := foreign.WaitClosed(ctx2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("foreign original worker was declared retired")
	}
	// Shared scheduler still runs an unrelated concrete callback after retirement.
	sharedRan := make(chan struct{})
	SystemTimedSched.Put(func() { close(sharedRan) }, time.Now())
	awaitNative2654(t, sharedRan, "shared scheduler stopped")
	_ = foreign.Close()
	_ = foreignRaw.Close()
}

type nativeCloseGate2654 struct {
	*nativePacket2654
	entered, release chan struct{}
	once             sync.Once
	err              error
}

func (c *nativeCloseGate2654) Close() error {
	c.once.Do(func() { _ = c.Conn.Close(); close(c.entered) })
	<-c.release
	return c.err
}

func TestActual2654NativeKCPCloseOwnerReturnAndNativeErrorAreNotFlags(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	raw := &nativePacket2654{Conn: a, readEntered: make(chan struct{}), writeEntered: make(chan struct{}), allowRead: make(chan struct{}), allowWrite: make(chan struct{})}
	close(raw.allowRead)
	close(raw.allowWrite)
	nativeErr := errors.New("original native owned PacketConn Close failure")
	gate := &nativeCloseGate2654{nativePacket2654: raw, entered: make(chan struct{}), release: make(chan struct{}), err: nativeErr}
	var once sync.Once
	release := func() { once.Do(func() { close(gate.release) }) }
	defer release()
	s := newUDPSession(1, 0, 0, nil, gate, true, b.LocalAddr(), nil)
	done := make(chan error, 1)
	go func() { done <- s.Close() }()
	awaitNative2654(t, gate.entered, "original native owner Close entry")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	if err := s.WaitClosed(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("die flag was mistaken for Close owner return: %v", err)
	}
	cancel()
	release()
	select {
	case err := <-done:
		if !errors.Is(err, nativeErr) {
			t.Fatal("legacy native Close return changed")
		}
	case <-time.After(time.Second):
		t.Fatal("Close owner did not return")
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.WaitClosed(ctx); !errors.Is(err, nativeErr) {
		t.Fatalf("native error hidden after join: %v", err)
	}
	if err := s.Close(); err == nil {
		t.Fatal("legacy duplicate Close error was normalized")
	}
	if err := s.WaitClosed(ctx); !errors.Is(err, nativeErr) {
		t.Fatal("duplicate Close replaced original native error")
	}
	if err := s.WaitClosed(context.Background()); err == nil {
		t.Fatal("unbounded observer accepted")
	}
	if err := s.WaitClosed(nil); err == nil {
		t.Fatal("nil observer accepted")
	}
}
