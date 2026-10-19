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

func handoffEnable(t *testing.T, s *UDPSession) {
	t.Helper()
	if e := s.EnableApplicationReadHandoff(); e != nil {
		t.Fatal(e)
	}
}
func handoffActive(s *UDPSession) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applicationReads.active
}
func handoffContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), time.Second)
}
func handoffTimeout(t *testing.T, e error) {
	t.Helper()
	var n net.Error
	if !errors.As(e, &n) || !n.Timeout() || errors.Is(e, ErrApplicationReadsRetired) {
		t.Fatal("real timeout lost", e)
	}
}

func TestApplicationReadHandoffRejectsLateEnable(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	_ = s.SetReadDeadline(time.Now().Add(-time.Second))
	_, e := s.Read(make([]byte, 1))
	handoffTimeout(t, e)
	if e := s.EnableApplicationReadHandoff(); e != errReadHandoffStarted {
		t.Fatal("late activation admitted", e)
	}
}
func TestApplicationReadHandoffRejectsEnableAfterScopedRead(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	s.SignalReadEOF()
	ctx, cancel := handoffContext(t)
	defer cancel()
	if _, e := s.ReadRetirementContext(ctx, make([]byte, 1)); e != io.EOF {
		t.Fatal(e)
	}
	if e := s.EnableApplicationReadHandoff(); e != errReadHandoffStarted {
		t.Fatal(e)
	}
}
func TestApplicationReadHandoffBroadcastsAndJoinsEveryBlockedReader(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	handoffEnable(t, s)
	_ = s.SetReadDeadline(time.Now().Add(time.Hour))
	const count = 16
	results := make(chan error, count)
	for i := 0; i < count; i++ {
		go func() { _, e := s.Read(make([]byte, 1)); results <- e }()
	}
	retirementScopeWait(t, "ordinary reads not admitted", func() bool { return handoffActive(s) == count })
	ctx, cancel := handoffContext(t)
	defer cancel()
	if e := s.SealApplicationReadsContext(ctx); e != nil {
		t.Fatal(e)
	}
	if handoffActive(s) != 0 {
		t.Fatal("seal returned before readers joined")
	}
	for i := 0; i < count; i++ {
		select {
		case e := <-results:
			if e != ErrApplicationReadsRetired {
				t.Fatal(e)
			}
		case <-ctx.Done():
			t.Fatal("reader did not return")
		}
	}
	if e := s.SealApplicationReadsContext(ctx); e != nil {
		t.Fatal("repeated seal", e)
	}
	if _, e := s.Read(make([]byte, 1)); e != ErrApplicationReadsRetired {
		t.Fatal("new ordinary read admitted", e)
	}
}
func TestApplicationReadHandoffRetainsOriginalLaterDeadlineSetter(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	handoffEnable(t, s)
	_ = s.SetReadDeadline(time.Now().Add(time.Hour))
	done := make(chan error, 1)
	go func() { _, e := s.Read(make([]byte, 1)); done <- e }()
	retirementScopeWait(t, "read not started", func() bool { return handoffActive(s) == 1 })
	deadline := time.Now().Add(20 * time.Millisecond)
	_ = s.SetReadDeadline(deadline)
	select {
	case e := <-done:
		handoffTimeout(t, e)
	case <-time.After(time.Second):
		t.Fatal("ordinary setter wakeup lost")
	}
	if d, _ := s.ReadDeadlineSnapshot(); !d.Equal(deadline) {
		t.Fatal("deadline mutated")
	}
}
func TestApplicationReadHandoffRejectsScopedOwnershipBeforeJoin(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	handoffEnable(t, s)
	ctx, cancel := handoffContext(t)
	defer cancel()
	if _, e := s.ReadRetirementContext(ctx, make([]byte, 1)); e != errReadHandoffNotJoined {
		t.Fatal("unsealed scoped read admitted", e)
	}
	// Reserve an ordinary read ownership slot independently of scheduling to
	// prove the join barrier cannot admit a competing scoped receive owner.
	g, e := s.beginApplicationRead()
	if e != nil {
		t.Fatal(e)
	}
	result := make(chan error, 1)
	go func() { result <- s.SealApplicationReadsContext(ctx) }()
	retirementScopeWait(t, "gate not sealed", func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.applicationReads.sealed })
	if _, e := s.ReadRetirementContext(ctx, make([]byte, 1)); e != errReadHandoffNotJoined {
		t.Fatal("active ownership admitted", e)
	}
	if _, e := s.Read(make([]byte, 1)); e != ErrApplicationReadsRetired {
		t.Fatal("sealed read admitted", e)
	}
	s.finishApplicationRead(g)
	if e := <-result; e != nil {
		t.Fatal(e)
	}
}
func TestApplicationReadHandoffPreservesQueuedTailAndActualACK(t *testing.T) {
	left, right, _, _ := retirementScopePair(t)
	handoffEnable(t, left)
	payload := bytes.Repeat([]byte("actual-original-KCP-tail-"), 8192)
	sent := make(chan error, 1)
	go func() { _, e := right.Write(payload); sent <- e }()
	retirementScopeWait(t, "no original queued bytes", func() bool { left.mu.Lock(); defer left.mu.Unlock(); return left.kcp.PeekSize() > 0 })
	ctx, cancel := handoffContext(t)
	defer cancel()
	if e := left.SealApplicationReadsContext(ctx); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 5; i++ {
		if n, e := left.Read(make([]byte, 1)); n != 0 || e != ErrApplicationReadsRetired {
			t.Fatal("sealed read consumed queued tail", n, e)
		}
	}
	got := make([]byte, len(payload))
	offset := 0
	for offset < len(got) {
		end := offset + 701
		if end > len(got) {
			end = len(got)
		}
		n, e := left.ReadRetirementContext(ctx, got[offset:end])
		offset += n
		if e != nil {
			t.Fatal(offset, e)
		}
	}
	if !bytes.Equal(got, payload) {
		t.Fatal("original payload differs")
	}
	select {
	case e := <-sent:
		if e != nil {
			t.Fatal(e)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	retirementScopeWait(t, "real pending ACK not cleared", func() bool { return right.WaitSnd() == 0 })
}
func TestApplicationReadHandoffScopedOwnerUnaffectedByLaterSetters(t *testing.T) {
	left, right, _, _ := retirementScopePair(t)
	handoffEnable(t, left)
	_ = left.SetReadDeadline(time.Now().Add(time.Hour))
	done := make(chan error, 1)
	go func() { _, e := left.Read(make([]byte, 1)); done <- e }()
	retirementScopeWait(t, "read not started", func() bool { return handoffActive(left) == 1 })
	ctx, cancel := handoffContext(t)
	defer cancel()
	if e := left.SealApplicationReadsContext(ctx); e != nil {
		t.Fatal(e)
	}
	if e := <-done; e != ErrApplicationReadsRetired {
		t.Fatal(e)
	}
	stop := make(chan struct{})
	last := make(chan time.Time, 1)
	go func() {
		var d time.Time
		for {
			select {
			case <-stop:
				last <- d
				return
			default:
				d = time.Now().Add(-time.Second)
				_ = left.SetReadDeadline(d)
			}
		}
	}()
	payload := bytes.Repeat([]byte("post-join-original-payload"), 2048)
	sent := make(chan error, 1)
	go func() { _, e := right.Write(payload); sent <- e }()
	got := make([]byte, len(payload))
	_, e := io.ReadFull(retirementReaderFunc(func(b []byte) (int, error) { return left.ReadRetirementContext(ctx, b) }), got)
	close(stop)
	deadline := <-last
	if e != nil || !bytes.Equal(got, payload) {
		t.Fatal("post-handoff deadline wakeups consumed receive ownership", e)
	}
	if deadline.IsZero() {
		t.Fatal("concurrent setter not observed")
	}
	if d, _ := left.ReadDeadlineSnapshot(); !d.Equal(deadline) {
		t.Fatal("setter value modified")
	}
	if e := <-sent; e != nil {
		t.Fatal(e)
	}
}
func TestApplicationReadHandoffPreservesExpiredDeadlineAtSeal(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	handoffEnable(t, s)
	expired := time.Now().Add(-time.Second)
	_ = s.SetReadDeadline(expired)
	ctx, cancel := handoffContext(t)
	defer cancel()
	if e := s.SealApplicationReadsContext(ctx); e != nil {
		t.Fatal(e)
	}
	_, e := s.Read(make([]byte, 1))
	handoffTimeout(t, e)
	future := time.Now().Add(time.Hour)
	_ = s.SetReadDeadline(future)
	_, e = s.Read(make([]byte, 1))
	handoffTimeout(t, e)
	if d, _ := s.ReadDeadlineSnapshot(); !d.Equal(future) {
		t.Fatal("application deadline altered by gate")
	}
}
func TestApplicationReadHandoffPreservesOriginalSocketError(t *testing.T) {
	s, _, packet, _ := retirementScopePair(t)
	handoffEnable(t, s)
	_ = packet.Close()
	select {
	case <-s.chSocketReadError:
	case <-time.After(time.Second):
		t.Fatal("actual socket read did not fail")
	}
	raw := s.socketReadError.Load().(error)
	ctx, cancel := handoffContext(t)
	defer cancel()
	if e := s.SealApplicationReadsContext(ctx); e != nil {
		t.Fatal(e)
	}
	if n, e := s.Read(make([]byte, 1)); n != 0 || e != raw {
		t.Fatal("actual published error masked", n, e, raw)
	}
	if n, e := s.ReadRetirementContext(ctx, make([]byte, 1)); n != 0 || e != raw {
		t.Fatal("retirement socket error masked", n, e, raw)
	}
}
func TestApplicationReadHandoffPreservesOriginalSessionClose(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	handoffEnable(t, s)
	_ = s.Close()
	ctx, cancel := handoffContext(t)
	defer cancel()
	if e := s.SealApplicationReadsContext(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Read(make([]byte, 1)); !errors.Is(e, io.ErrClosedPipe) {
		t.Fatal(e)
	}
	if _, e := s.ReadRetirementContext(ctx, make([]byte, 1)); !errors.Is(e, io.ErrClosedPipe) {
		t.Fatal(e)
	}
}
func TestApplicationReadHandoffPreservesEOFAndQueuedPayload(t *testing.T) {
	left, right, _, _ := retirementScopePair(t)
	handoffEnable(t, left)
	payload := []byte("queued-before-ordered-EOF")
	if _, e := right.Write(payload); e != nil {
		t.Fatal(e)
	}
	retirementScopeWait(t, "not queued", func() bool { left.mu.Lock(); defer left.mu.Unlock(); return left.kcp.PeekSize() > 0 })
	left.SignalReadEOF()
	ctx, cancel := handoffContext(t)
	defer cancel()
	if e := left.SealApplicationReadsContext(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e := left.Read(make([]byte, 1)); e != ErrApplicationReadsRetired {
		t.Fatal("tail bypassed", e)
	}
	got := make([]byte, len(payload))
	if _, e := io.ReadFull(retirementReaderFunc(func(b []byte) (int, error) { return left.ReadRetirementContext(ctx, b) }), got); e != nil || !bytes.Equal(got, payload) {
		t.Fatal("tail lost", e)
	}
	if _, e := left.ReadRetirementContext(ctx, make([]byte, 1)); e != io.EOF {
		t.Fatal("real EOF lost", e)
	}
}
func TestApplicationReadHandoffPreservesAlreadyEmptyEOF(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	handoffEnable(t, s)
	s.SignalReadEOF()
	ctx, cancel := handoffContext(t)
	defer cancel()
	if e := s.SealApplicationReadsContext(ctx); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Read(make([]byte, 1)); e != io.EOF {
		t.Fatal(e)
	}
}
func TestApplicationReadHandoffRequiresEnabledBoundedLiveScope(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	ctx, cancel := handoffContext(t)
	defer cancel()
	if e := s.SealApplicationReadsContext(ctx); e != errReadHandoffNotEnabled {
		t.Fatal(e)
	}
	handoffEnable(t, s)
	for _, bad := range []context.Context{nil, context.Background()} {
		if e := s.SealApplicationReadsContext(bad); e != errUnboundedRetirementRead {
			t.Fatal(e)
		}
	}
	dead, stop := context.WithCancel(ctx)
	stop()
	if e := s.SealApplicationReadsContext(dead); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	s.mu.Lock()
	sealed := s.applicationReads.sealed
	s.mu.Unlock()
	if sealed {
		t.Fatal("invalid scope mutated gate")
	}
}
func TestApplicationReadHandoffDoesNotRenewCloseBudget(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	handoffEnable(t, s)
	g, e := s.beginApplicationRead()
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	deadline, _ := ctx.Deadline()
	released := make(chan struct{})
	go func() { time.Sleep(20 * time.Millisecond); s.finishApplicationRead(g); close(released) }()
	if e := s.SealApplicationReadsContext(ctx); e != nil {
		t.Fatal(e)
	}
	<-released
	if _, e := s.ReadRetirementContext(ctx, make([]byte, 1)); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("original close scope lost", e)
	}
	if d, _ := ctx.Deadline(); !d.Equal(deadline) {
		t.Fatal("original close deadline changed")
	}
	if time.Since(deadline) > 500*time.Millisecond {
		t.Fatal("close budget renewed")
	}
}
func TestApplicationReadHandoffReadSetterSealRace(t *testing.T) {
	for i := 0; i < 32; i++ {
		s, right, a, b := retirementScopePair(t)
		handoffEnable(t, s)
		_ = s.SetReadDeadline(time.Now().Add(time.Hour))
		done := make(chan error, 1)
		go func() { _, e := s.Read(make([]byte, 1)); done <- e }()
		retirementScopeWait(t, "read not admitted", func() bool { return handoffActive(s) == 1 })
		ctx, cancel := handoffContext(t)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() { defer wg.Done(); _ = s.SetReadDeadline(time.Now().Add(-time.Millisecond)) }()
		if e := s.SealApplicationReadsContext(ctx); e != nil {
			cancel()
			t.Fatal(e)
		}
		wg.Wait()
		select {
		case e := <-done:
			if e != ErrApplicationReadsRetired {
				handoffTimeout(t, e)
			}
		case <-ctx.Done():
			t.Fatal("original Read did not return")
		}
		if handoffActive(s) != 0 {
			t.Fatal("reader leaked")
		}
		cancel()
		_ = s.Close()
		_ = right.Close()
		_ = a.Close()
		_ = b.Close()
	}
}
