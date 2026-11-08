package kcp

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type nativePacket2654 struct {
	net.Conn
	readEntered, writeEntered, allowRead, allowWrite chan struct{}
	rOnce, wOnce                                     sync.Once
}

func (c *nativePacket2654) ReadFrom(p []byte) (int, net.Addr, error) {
	c.rOnce.Do(func() { close(c.readEntered) })
	n, e := c.Conn.Read(p)
	<-c.allowRead
	return n, c.Conn.RemoteAddr(), e
}
func (c *nativePacket2654) WriteTo(p []byte, _ net.Addr) (int, error) {
	c.wOnce.Do(func() { close(c.writeEntered) })
	n, e := c.Conn.Write(p)
	<-c.allowWrite
	return n, e
}

func awaitNative2654(t *testing.T, ch <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(label)
	}
}

func TestActual2654NativeKCPNeedsRealPostProcessANDReadLoopJoin(t *testing.T) {
	a, b := net.Pipe()
	defer b.Close()
	raw := &nativePacket2654{Conn: a, readEntered: make(chan struct{}), writeEntered: make(chan struct{}), allowRead: make(chan struct{}), allowWrite: make(chan struct{})}
	var rOnce, wOnce sync.Once
	releaseRead := func() { rOnce.Do(func() { close(raw.allowRead) }) }
	releaseWrite := func() { wOnce.Do(func() { close(raw.allowWrite) }) }
	s, err := NewConn3(1, b.LocalAddr(), nil, 0, 0, raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); _ = raw.Close(); releaseRead(); releaseWrite() })
	awaitNative2654(t, raw.readEntered, "actual native KCP client readLoop did not enter native ReadFrom")
	if _, err := s.Write([]byte("actual pending native KCP data")); err != nil {
		t.Fatal(err)
	}
	awaitNative2654(t, raw.writeEntered, "actual native KCP postProcess did not enter native WriteTo")
	join, ok := any(s).(interface{ WaitClosed(context.Context) error })
	if !ok {
		t.Fatal("actual pinned KCP has no original postProcess/readLoop/scheduled-callback join")
	}
	_ = s.Close()
	_ = raw.Close()
	checkHeld := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		if e := join.WaitClosed(ctx); !errors.Is(e, context.DeadlineExceeded) {
			t.Fatalf("native KCP workers falsely retired: %v", e)
		}
	}
	checkHeld()
	releaseRead()
	checkHeld()
	releaseWrite()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if e := join.WaitClosed(ctx); e != nil {
		t.Fatal(e)
	}
	if s.WaitSnd() == 0 {
		t.Fatal("unacknowledged original KCP tail was cleared to manufacture retirement")
	}
}
