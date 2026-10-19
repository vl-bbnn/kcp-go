package kcp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type retirementReaderFunc func([]byte) (int, error)

func (f retirementReaderFunc) Read(b []byte) (int, error) { return f(b) }

func retirementScopePair(t *testing.T) (*UDPSession, *UDPSession, net.PacketConn, net.PacketConn) {
	t.Helper()
	a, e := net.ListenPacket("udp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	b, e := net.ListenPacket("udp4", "127.0.0.1:0")
	if e != nil {
		_ = a.Close()
		t.Fatal(e)
	}
	left, e := NewConn3(4407, b.LocalAddr(), nil, 0, 0, a)
	if e != nil {
		_ = a.Close()
		_ = b.Close()
		t.Fatal(e)
	}
	right, e := NewConn3(4407, a.LocalAddr(), nil, 0, 0, b)
	if e != nil {
		_ = left.Close()
		_ = a.Close()
		_ = b.Close()
		t.Fatal(e)
	}
	for _, s := range []*UDPSession{left, right} {
		s.SetNoDelay(1, 10, 2, 1)
		s.SetWindowSize(32, 32)
		s.SetACKNoDelay(true)
	}
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
		_ = a.Close()
		_ = b.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for _, s := range []*UDPSession{left, right} {
			if e := s.WaitWorkersContext(ctx); e != nil {
				t.Error("original workers not joined", e)
			}
		}
	})
	return left, right, a, b
}
func retirementScopeWait(t *testing.T, label string, f func() bool) {
	t.Helper()
	end := time.Now().Add(time.Second)
	for !f() {
		if !time.Now().Before(end) {
			t.Fatal(label)
		}
		time.Sleep(time.Millisecond)
	}
}
func TestRetirementReadContextExpiredApplicationDeadlinePreservesRealPayload(t *testing.T) {
	left, right, _, _ := retirementScopePair(t)
	expired := time.Now().Add(-time.Second)
	if e := left.SetReadDeadline(expired); e != nil {
		t.Fatal(e)
	}
	_, e := left.Read(make([]byte, 1))
	var timeout net.Error
	if !errors.As(e, &timeout) || !timeout.Timeout() {
		t.Fatal("original application timeout lost", e)
	}
	payload := bytes.Repeat([]byte("original-bounded-retirement-body-"), 8192)
	sent := make(chan error, 1)
	go func() { _, e := right.Write(payload); sent <- e }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got := make([]byte, len(payload))
	offset := 0
	for offset < len(got) {
		end := offset + 733
		if end > len(got) {
			end = len(got)
		}
		n, e := left.ReadRetirementContext(ctx, got[offset:end])
		offset += n
		if e != nil {
			t.Fatal("actual receive", offset, e)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("original bytes changed")
	}
	select {
	case e := <-sent:
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	retirementScopeWait(t, "real pending ACK did not clear", func() bool { return right.WaitSnd() == 0 })
	if d, ok := left.ReadDeadlineSnapshot(); !ok || !d.Equal(expired) {
		t.Fatal("application deadline mutated", d, ok)
	}
	// Model only an already delivered ordered EOF, after full bytes and real ACK.
	left.SignalReadEOF()
	if n, e := left.ReadRetirementContext(ctx, make([]byte, 1)); n != 0 || e != io.EOF {
		t.Fatal("original EOF lost", n, e)
	}
}
func TestRetirementReadContextConcurrentApplicationDeadlineRemainsEffective(t *testing.T) {
	left, right, _, _ := retirementScopePair(t)
	deadline := time.Now().Add(30 * time.Millisecond)
	_ = left.SetReadDeadline(deadline)
	application := make(chan error, 1)
	go func() { _, e := left.Read(make([]byte, 1)); application <- e }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	retired := make(chan error, 1)
	payload := []byte("after-application-reader-return")
	go func() {
		got := make([]byte, len(payload))
		_, e := io.ReadFull(retirementReaderFunc(func(b []byte) (int, error) { return left.ReadRetirementContext(ctx, b) }), got)
		if e == nil && !bytes.Equal(got, payload) {
			e = errors.New("payload mismatch")
		}
		retired <- e
	}()
	select {
	case e := <-application:
		var timeout net.Error
		if !errors.As(e, &timeout) || !timeout.Timeout() {
			t.Fatal("active original read deadline lost", e)
		}
	case <-ctx.Done():
		t.Fatal("original reader not joined")
	}
	if _, e := right.Write(payload); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-retired:
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if d, ok := left.ReadDeadlineSnapshot(); !ok || !d.Equal(deadline) {
		t.Fatal("shared deadline changed")
	}
}
func TestRetirementReadContextRequiresBoundedScope(t *testing.T) {
	s := &UDPSession{}
	for _, ctx := range []context.Context{nil, context.Background()} {
		if n, e := s.ReadRetirementContext(ctx, make([]byte, 1)); n != 0 || e != errUnboundedRetirementRead {
			t.Fatal("unbounded admitted", n, e)
		}
	}
}
func TestRetirementReadContextCanceledScope(t *testing.T) {
	left, _, _, _ := retirementScopePair(t)
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	stop()
	if n, e := left.ReadRetirementContext(ctx, make([]byte, 1)); n != 0 || !errors.Is(e, context.Canceled) {
		t.Fatal(n, e)
	}
}
func TestRetirementReadContextExpiredScopeDoesNotConsumeQueuedPayload(t *testing.T) {
	left, right, _, _ := retirementScopePair(t)
	payload := []byte("queued-original-data")
	if _, e := right.Write(payload); e != nil {
		t.Fatal(e)
	}
	retirementScopeWait(t, "payload not queued", func() bool { left.mu.Lock(); defer left.mu.Unlock(); return left.kcp.PeekSize() > 0 })
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if n, e := left.ReadRetirementContext(ctx, make([]byte, 64)); n != 0 || !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("expired close scope consumed data", n, e)
	}
	_ = left.SetReadDeadline(time.Now().Add(time.Second))
	got := make([]byte, len(payload))
	if _, e := io.ReadFull(left, got); e != nil || !bytes.Equal(got, payload) {
		t.Fatal("queued data consumed or lost", e)
	}
}
func TestRetirementReadContextScopeNotRenewedByApplicationWakeups(t *testing.T) {
	left, _, _, _ := retirementScopePair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	stop := make(chan struct{})
	joined := make(chan time.Time, 1)
	go func() {
		last := time.Time{}
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				joined <- last
				return
			case <-tick.C:
				last = time.Now().Add(time.Hour)
				_ = left.SetReadDeadline(last)
			}
		}
	}()
	_, e := left.ReadRetirementContext(ctx, make([]byte, 1))
	close(stop)
	last := <-joined
	if !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("bounded scope lost", e)
	}
	if d, ok := left.ReadDeadlineSnapshot(); last.IsZero() || !ok || !d.Equal(last) {
		t.Fatal("ordinary deadline setter affected", d, ok)
	}
}
func TestRetirementReadContextOriginalSocketErrorPreserved(t *testing.T) {
	left, _, packet, _ := retirementScopePair(t)
	_ = packet.Close()
	select {
	case <-left.chSocketReadError:
	case <-time.After(time.Second):
		t.Fatal("actual packet reader not returned")
	}
	raw := left.socketReadError.Load().(error)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if n, e := left.ReadRetirementContext(ctx, make([]byte, 1)); n != 0 || e != raw {
		t.Fatal("actual socket error filtered", n, e, raw)
	}
}
func TestRetirementReadContextOriginalSessionClosePreserved(t *testing.T) {
	left, _, _, _ := retirementScopePair(t)
	_ = left.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if n, e := left.ReadRetirementContext(ctx, make([]byte, 1)); n != 0 || !errors.Is(e, io.ErrClosedPipe) {
		t.Fatal("session close lost", n, e)
	}
}
