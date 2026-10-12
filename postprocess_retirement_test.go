package kcp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type reviewHeldWatermark struct {
	watermarkTestPacketConn
	calls   atomic.Int64
	started chan struct{}
	release chan struct{}
}

func (c *reviewHeldWatermark) WriteTo(p []byte, _ net.Addr) (int, error) { return len(p), nil }
func (c *reviewHeldWatermark) WriteToReceipt(ctx context.Context, _ []byte, _ net.Addr) error {
	if c.calls.Add(1) == 1 {
		close(c.started)
		select {
		case <-c.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
func reviewWaitActualPostProcess(t *testing.T, s *UDPSession) {
	t.Helper()
	cap := time.Now().Add(time.Second)
	for {
		s.postProcessingAdmission.Lock()
		sealed := s.postProcessingSealed
		s.postProcessingAdmission.Unlock()
		if sealed {
			return
		}
		if !time.Now().Before(cap) {
			t.Fatal("actual postprocessor failed to terminate")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestPostProcessRetirementDrainsAdmittedRequests(t *testing.T) {
	c := &reviewHeldWatermark{started: make(chan struct{}), release: make(chan struct{})}
	s, e := NewConn3(31, &net.UDPAddr{}, nil, 0, 0, c)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- s.FlushReceiveWatermark(ctx) }()
	select {
	case <-c.started:
	case <-ctx.Done():
		t.Fatal("no real admitted raw watermark")
	}
	for i := 0; i < 16; i++ {
		buf := defaultBufferPool.Get()[:1]
		buf[0] = 1
		if e = s.enqueuePostProcessing(buf, false); e != nil {
			t.Fatal(e)
		}
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 64; i++ {
		if e = s.FlushReceiveWatermark(ctx); !errors.Is(e, io.ErrClosedPipe) {
			t.Fatalf("post-close error=%v", e)
		}
	}
	select {
	case e = <-first:
		if !errors.Is(e, io.ErrClosedPipe) {
			t.Fatalf("retired admitted producer error=%v", e)
		}
	case <-ctx.Done():
		t.Fatal("admitted producer leaked")
	}
	close(c.release)
	reviewWaitActualPostProcess(t, s)
	if n := len(s.chPostProcessing); n != 0 {
		t.Fatalf("retirement queue=%d", n)
	}
	if n := c.calls.Load(); n != 1 {
		t.Fatalf("post-close watermark reached writer: calls=%d", n)
	}
}
func TestReceiveWatermarkFullQueueWakesOnConsumerProgress(t *testing.T) {
	c := &reviewHeldWatermark{started: make(chan struct{}), release: make(chan struct{})}
	s, e := NewConn3(37, &net.UDPAddr{}, nil, 0, 0, c)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() { first <- s.FlushReceiveWatermark(ctx) }()
	select {
	case <-c.started:
	case <-ctx.Done():
		t.Fatal("no held first watermark")
	}
	for i := 0; i < cap(s.chPostProcessing); i++ {
		buf := defaultBufferPool.Get()[:1]
		buf[0] = 1
		if e = s.enqueuePostProcessing(buf, false); e != nil {
			t.Fatal(e)
		}
	}
	if len(s.chPostProcessing) != cap(s.chPostProcessing) {
		t.Fatal("actual queue not full")
	}
	second := make(chan error, 1)
	go func() { second <- s.FlushReceiveWatermark(ctx) }()
	select {
	case e = <-second:
		t.Fatalf("full queue did not wait: %v", e)
	case <-time.After(20 * time.Millisecond):
	}
	close(c.release)
	for _, done := range []chan error{first, second} {
		select {
		case e = <-done:
			if e != nil {
				t.Fatal(e)
			}
		case <-ctx.Done():
			t.Fatal("freed-capacity waiter did not wake")
		}
	}
	if c.calls.Load() != 2 {
		t.Fatal("not two actual raw watermark receipts")
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	reviewWaitActualPostProcess(t, s)
	if len(s.chPostProcessing) != 0 {
		t.Fatal("full-queue test leaked accepted requests")
	}
}
func TestReceiveWatermarkAfterCloseDoesNotOrphanRequests(t *testing.T) {
	s, e := NewConn3(19, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}, nil, 0, 0, watermarkTestPacketConn{})
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Close(); e != nil {
		t.Fatal(e)
	}
	cap := time.Now().Add(time.Second)
	for {
		s.postProcessingAdmission.Lock()
		sealed := s.postProcessingSealed
		s.postProcessingAdmission.Unlock()
		if sealed {
			break
		}
		if !time.Now().Before(cap) {
			t.Fatal("actual postProcess did not retire")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 128; i++ {
		if e := s.FlushReceiveWatermark(ctx); !errors.Is(e, io.ErrClosedPipe) {
			t.Fatalf("closed watermark error=%v", e)
		}
	}
	if n := len(s.chPostProcessing); n != 0 {
		t.Fatalf("closed session orphaned postprocessing requests after actual worker exit: pending=%d", n)
	}
}
