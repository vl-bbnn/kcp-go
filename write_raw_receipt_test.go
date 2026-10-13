package kcp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type rawWriteTestConn struct {
	watermarkTestPacketConn
	mu       sync.Mutex
	packets  [][]byte
	callback func(context.Context, []byte) error
}

func (c *rawWriteTestConn) WriteToReceipt(ctx context.Context, p []byte, _ net.Addr) error {
	c.mu.Lock()
	c.packets = append(c.packets, append([]byte(nil), p...))
	f := c.callback
	c.mu.Unlock()
	if f != nil {
		return f(ctx, p)
	}
	return nil
}
func rawWriteSession(c *rawWriteTestConn) *UDPSession {
	return &UDPSession{conn: c, remote: &net.UDPAddr{}, kcp: NewKCP(11, func([]byte, int) {}), chPostProcessing: make(chan *sendRequest, 4), chPostProcessingAvailable: make(chan struct{}, 1), chWriteEvent: make(chan struct{}, 1), chWriteSealed: make(chan struct{}), chSocketWriteError: make(chan struct{}), die: make(chan struct{})}
}
func rawWriteWorker(t *testing.T, s *UDPSession) {
	t.Helper()
	go s.postProcess()
	t.Cleanup(func() {
		if !s.isClosed() {
			close(s.die)
		}
	})
}
func TestRawWriteReceiptOwnsOriginalPUSHWithoutWINSOrACK(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	rawWriteWorker(t, s)
	payload := []byte("original control payload")
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	n, e := s.WriteWithRawReceipt(ctx, payload)
	if e != nil || n != len(payload) {
		t.Fatal(n, e)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.packets) != 1 {
		t.Fatal("unexpected emitted packet count", len(c.packets))
	}
	p := c.packets[0]
	if p[4] != IKCP_CMD_PUSH || !bytes.Equal(p[IKCP_OVERHEAD:], payload) {
		t.Fatal("did not receipt the actual original PUSH")
	}
	if s.WaitSnd() != 1 {
		t.Fatal("raw receipt fabricated a remote ACK")
	}
}
func TestRawWriteReceiptRetainsActualFailureAndShortWrite(t *testing.T) {
	for _, cause := range []error{errors.New("actual raw writer failure"), io.ErrShortWrite} {
		t.Run(cause.Error(), func(t *testing.T) {
			c := &rawWriteTestConn{callback: func(context.Context, []byte) error { return cause }}
			s := rawWriteSession(c)
			rawWriteWorker(t, s)
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if _, e := s.WriteWithRawReceipt(ctx, []byte("original")); !errors.Is(e, cause) {
				t.Fatal("raw failure discarded", e)
			}
		})
	}
}
func TestRawWriteReceiptAdmittedWriterOutcomeSurvivesLaterReaderClose(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	c.callback = func(context.Context, []byte) error { close(s.die); return nil }
	go s.postProcess()
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if n, e := s.WriteWithRawReceipt(ctx, []byte("final")); n != 5 || e != nil {
		t.Fatal("successful original writer replaced by later reader close", n, e)
	}
}
func TestRawWriteReceiptOriginalContextAndEarlierWriteDeadline(t *testing.T) {
	for _, writeDeadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "context", true: "write-deadline"}[writeDeadline], func(t *testing.T) {
			c := &rawWriteTestConn{callback: func(ctx context.Context, _ []byte) error { <-ctx.Done(); return ctx.Err() }}
			s := rawWriteSession(c)
			rawWriteWorker(t, s)
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if writeDeadline {
				s.wd.Store(time.Now().Add(20 * time.Millisecond))
			} else {
				stop()
				ctx, stop = context.WithTimeout(context.Background(), 20*time.Millisecond)
				defer stop()
			}
			start := time.Now()
			if _, e := s.WriteWithRawReceipt(ctx, []byte("final")); !errors.Is(e, context.DeadlineExceeded) {
				t.Fatal(e)
			}
			if time.Since(start) > 200*time.Millisecond {
				t.Fatal("original deadline extended")
			}
		})
	}
}
func TestRawWriteReceiptCanceledBeforeAdmissionAndOversize(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	ctx, stop := context.WithCancel(context.Background())
	stop()
	if n, e := s.WriteWithRawReceipt(ctx, []byte("never")); n != 0 || !errors.Is(e, context.Canceled) {
		t.Fatal(n, e)
	}
	if n, e := s.WriteWithRawReceipt(context.Background(), make([]byte, int(s.kcp.mss)+1)); n != 0 || e == nil {
		t.Fatal(n, e)
	}
	if s.WaitSnd() != 0 || len(c.packets) != 0 {
		t.Fatal("unadmitted payload sent")
	}
}
func TestRawWriteReceiptFullQueueRetainsOriginalEntryAndCap(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	s.chPostProcessing = make(chan *sendRequest, 1)
	original := &sendRequest{buffer: []byte("original occupied slot")}
	s.chPostProcessing <- original
	ctx, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, e := s.WriteWithRawReceipt(ctx, []byte("final")); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	if len(s.chPostProcessing) != 1 || <-s.chPostProcessing != original {
		t.Fatal("original queue modified")
	}
	if len(c.packets) != 0 {
		t.Fatal("full queue fabricated a raw receipt")
	}
}
func TestRawWriteReceiptClosedBeforeAdmissionCannotSucceed(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	close(s.die)
	if n, e := s.WriteWithRawReceipt(context.Background(), []byte("never")); n != 0 || !errors.Is(e, io.ErrClosedPipe) {
		t.Fatal(n, e)
	}
}
func TestRawWriteReceiptConcurrentOriginalPUSHes(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	s.kcp.NoDelay(1, 1, 2, 1)
	rawWriteWorker(t, s)
	errs := make(chan error, 2)
	for _, p := range [][]byte{[]byte("first"), []byte("second")} {
		go func(p []byte) {
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			n, e := s.WriteWithRawReceipt(ctx, p)
			if e == nil && n != len(p) {
				e = io.ErrShortWrite
			}
			errs <- e
		}(p)
	}
	for i := 0; i < 2; i++ {
		if e := <-errs; e != nil {
			t.Fatal(e)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.packets) != 2 || s.WaitSnd() != 2 {
		t.Fatal("concurrent packet/ACK ownership", len(c.packets), s.WaitSnd())
	}
}

func TestRawWriteReceiptCannotCertifyOnlyAnUnrelatedACK(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	s.kcp.rmt_wnd = 0
	s.kcp.acklist = append(s.kcp.acklist, ackItem{sn: 55, ts: 1})
	rawWriteWorker(t, s)
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, e := s.WriteWithRawReceipt(ctx, []byte("actual original write")); e == nil {
		t.Fatal("unrelated ACK falsely certified original PUSH")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.packets) != 0 {
		t.Fatal("non-PUSH packet promoted into successful receipt")
	}
}
func TestRawWriteReceiptRestoresOriginalOutputCallback(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	s.kcp.NoDelay(1, 1, 2, 1)
	count := 0
	s.kcp.output = func([]byte, int) { count++ }
	rawWriteWorker(t, s)
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, e := s.WriteWithRawReceipt(ctx, []byte("owned")); e != nil {
		t.Fatal(e)
	}
	s.mu.Lock()
	s.kcp.Send([]byte("ordinary"))
	s.kcp.flush(IKCP_FLUSH_FULL)
	s.mu.Unlock()
	if count != 1 {
		t.Fatal("original ordinary output callback changed", count)
	}
}

func TestRawWriteReceiptOriginalPUSHSequenceWrap(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	s.kcp.snd_una = ^uint32(0)
	s.kcp.snd_nxt = ^uint32(0)
	rawWriteWorker(t, s)
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, e := s.WriteWithRawReceipt(ctx, []byte("wrap")); e != nil {
		t.Fatal(e)
	}
	if s.kcp.snd_nxt != 0 || s.WaitSnd() != 1 {
		t.Fatal("sequence wrap or real ACK state changed")
	}
}

// A busy session is owned by an earlier operation. The caller's original cap
// must still bound waiting for that lock, before accepting any new KCP bytes.
func TestRawWriteReceiptSessionMutexRetainsOriginalCap(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	s.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	type outcome struct {
		n   int
		err error
	}
	done := make(chan outcome, 1)
	go func() { n, e := s.WriteWithRawReceipt(ctx, []byte("original bounded control")); done <- outcome{n, e} }()
	var got outcome
	exceeded := false
	select {
	case got = <-done:
	case <-time.After(150 * time.Millisecond):
		exceeded = true
	}
	s.mu.Unlock()
	if exceeded {
		select {
		case got = <-done:
		case <-time.After(400 * time.Millisecond):
			t.Fatal("writer owner failed to join after releasing only the fixture lock")
		}
		t.Error("session mutex wait exceeded the already expired original context")
	}
	if got.n != 0 || !errors.Is(got.err, context.DeadlineExceeded) {
		t.Errorf("deadline before KCP admission: n=%d err=%v; want zero bytes and original deadline", got.n, got.err)
	}
	if s.WaitSnd() != 0 {
		t.Errorf("expired call admitted %d KCP segments after the lock was released", s.WaitSnd())
	}
	if len(c.packets) != 0 {
		t.Fatal("expired call fabricated raw writer outcome")
	}
}

func TestRawWriteReceiptPostProcessingMutexRetainsOriginalCap(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	s.postProcessingAdmission.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := s.WriteWithRawReceipt(ctx, []byte("bounded original")); done <- e }()
	var got error
	exceeded := false
	select {
	case got = <-done:
	case <-time.After(150 * time.Millisecond):
		exceeded = true
	}
	s.postProcessingAdmission.Unlock()
	if exceeded {
		select {
		case got = <-done:
		case <-time.After(400 * time.Millisecond):
			t.Fatal("admission owner failed to join after fixture lock release")
		}
		t.Error("postprocessor admission mutex bypassed original deadline")
	}
	if !errors.Is(got, context.DeadlineExceeded) {
		t.Fatal(got)
	}
	if len(s.chPostProcessing) != 0 || len(c.packets) != 0 {
		t.Fatal("expired call admitted raw packets")
	}
}

// Failure to emit a new payload must not erase unrelated protocol output that
// the synchronous flush already removed from KCP's pending ACK list.
func TestRawWriteReceiptPreservesExistingACKWhenTargetPUSHCannotEmit(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	s.kcp.rmt_wnd = 0
	s.kcp.acklist = append(s.kcp.acklist, ackItem{sn: 55, ts: 1})
	ackOutputs := 0
	s.kcp.output = func(p []byte, n int) {
		if n >= IKCP_OVERHEAD && p[4] == IKCP_CMD_ACK {
			ackOutputs++
		}
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, e := s.WriteWithRawReceipt(ctx, []byte("new control")); e == nil {
		t.Fatal("non-PUSH falsely certified as original payload receipt")
	}
	if ackOutputs != 1 {
		t.Fatalf("failed new write discarded existing ACK: outputs=%d pending=%d", ackOutputs, len(s.kcp.acklist))
	}
	if len(c.packets) != 0 {
		t.Fatal("ordinary ACK promoted to successful original raw receipt")
	}
}

func TestRawWriteReceiptStreamTailOwnsAllOriginalBytes(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	s.kcp.NoDelay(1, 1, 2, 1)
	s.kcp.stream = 1
	prefix := bytes.Repeat([]byte("x"), int(s.kcp.mss)-1)
	if e := s.kcp.Send(prefix); e != 0 {
		t.Fatal(e)
	}
	rawWriteWorker(t, s)
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	n, e := s.WriteWithRawReceipt(ctx, []byte("AB"))
	if n != 2 || e != nil {
		t.Fatal(n, e)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.packets) != 2 {
		t.Fatal("stream split omitted packet", len(c.packets))
	}
	if !bytes.Equal(c.packets[0][IKCP_OVERHEAD:], append(prefix, 'A')) || !bytes.Equal(c.packets[1][IKCP_OVERHEAD:], []byte("B")) {
		t.Fatal("actual original stream bytes not covered")
	}
	if s.WaitSnd() != 2 {
		t.Fatal("real remote ACK fabricated")
	}
}
func TestRawWriteReceiptLaterReaderCloseCannotEraseActualWriterFailure(t *testing.T) {
	cause := errors.New("actual original PUSH failure")
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	c.callback = func(context.Context, []byte) error { close(s.die); return cause }
	go s.postProcess()
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, e := s.WriteWithRawReceipt(ctx, []byte("final")); !errors.Is(e, cause) {
		t.Fatal("later reader close replaced actual writer failure", e)
	}
}

// The original caller is canceled before the raw writer publishes its nil
// outcome. A ready receipt cannot certify completion outside that scope.
func TestRawWriteReceiptCallerCancellationBeforeWriterCompletionRemainsFailure(t *testing.T) {
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.callback = func(context.Context, []byte) error { cancel(); return nil }
	rawWriteWorker(t, s)
	if _, e := s.WriteWithRawReceipt(ctx, []byte("original canceled control")); !errors.Is(e, context.Canceled) {
		t.Fatalf("expired original caller falsely certified by ready nil writer receipt: %v", e)
	}
}

func TestRawWriteReceiptCanceledCallerRetainsActualWriterError(t *testing.T) {
	cause := errors.New("actual failed original writer")
	c := &rawWriteTestConn{}
	s := rawWriteSession(c)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.callback = func(context.Context, []byte) error { cancel(); return cause }
	rawWriteWorker(t, s)
	_, e := s.WriteWithRawReceipt(ctx, []byte("failed canceled original"))
	if !errors.Is(e, context.Canceled) {
		t.Fatal("original caller cancellation lost", e)
	}
	// A context-first return may precede the still-owned writer's result;
	// once returned, it remains failure and cannot claim successful delivery.
	if e == nil {
		t.Fatal("failure falsely certified")
	}
}
