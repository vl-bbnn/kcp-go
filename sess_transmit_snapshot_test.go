package kcp

import (
	"bytes"
	"encoding/binary"
	"sync"
	"testing"
)

func transmitPushObservation(t *testing.T, packets [][]byte) (segments, payload uint64) {
	t.Helper()
	for _, p := range packets {
		for len(p) > 0 {
			if len(p) < IKCP_OVERHEAD {
				t.Fatal("short original output")
			}
			n := int(binary.LittleEndian.Uint32(p[20:24]))
			if len(p) < IKCP_OVERHEAD+n {
				t.Fatal("short segment")
			}
			if p[4] == IKCP_CMD_PUSH {
				segments++
				payload += uint64(n)
			}
			p = p[IKCP_OVERHEAD+n:]
		}
	}
	return
}
func TestTransmitSnapshotClassifiesActualPUSHBranches(t *testing.T) {
	for _, kind := range []string{"initial-only", "fast", "early", "timeout", "selectively-acked"} {
		t.Run(kind, func(t *testing.T) {
			var packets [][]byte
			k := NewKCP(42, func(p []byte, n int) { packets = append(packets, bytes.Clone(p[:n])) })
			k.NoDelay(1, 10, 2, 1)
			s := &UDPSession{kcp: k}
			data := []byte("count this original segment without recording payload")
			if k.Send(data) != 0 {
				t.Fatal("send rejected")
			}
			k.flush(IKCP_FLUSH_FULL)
			seg, ok := k.snd_buf.Peek()
			if !ok {
				t.Fatal("missing in-flight original segment")
			}
			seg.resendts = currentMs() + 60000
			switch kind {
			case "fast":
				seg.fastack = 2
			case "early":
				seg.fastack = 1
			case "timeout":
				seg.fastack = 0
				seg.resendts = currentMs() - 1
			case "selectively-acked":
				seg.acked = 1
			}
			k.flush(IKCP_FLUSH_FULL)
			snap, ok := s.TryTransmitSnapshot()
			if !ok {
				t.Fatal("snapshot unavailable")
			}
			st := snap.Stats
			n := uint64(len(data))
			want := TransmitStats{InitialSegments: 1, InitialPayloadBytes: n}
			switch kind {
			case "fast":
				want.FastSegments = 1
				want.FastPayloadBytes = n
			case "early":
				want.EarlySegments = 1
				want.EarlyPayloadBytes = n
			case "timeout":
				want.TimeoutSegments = 1
				want.TimeoutPayloadBytes = n
			}
			if st != want {
				t.Fatalf("wrong branch: got=%+v want=%+v", st, want)
			}
			ns, nb := transmitPushObservation(t, packets)
			if ns != st.InitialSegments+st.FastSegments+st.EarlySegments+st.TimeoutSegments || nb != st.InitialPayloadBytes+st.FastPayloadBytes+st.EarlyPayloadBytes+st.TimeoutPayloadBytes {
				t.Fatal("counters differ from actual encoded PUSH output")
			}
		})
	}
}
func TestTransmitSnapshotCountsSegmentsNotDatagramsAndExcludesACK(t *testing.T) {
	var packets [][]byte
	k := NewKCP(42, func(p []byte, n int) { packets = append(packets, bytes.Clone(p[:n])) })
	k.NoDelay(1, 10, 2, 1)
	s := &UDPSession{kcp: k}
	for i := 0; i < 3; i++ {
		if k.Send([]byte("hello")) != 0 {
			t.Fatal("send")
		}
	}
	k.flush(IKCP_FLUSH_FULL)
	a, ok := s.TryTransmitSnapshot()
	if !ok || a.Stats.InitialSegments != 3 || a.Stats.InitialPayloadBytes != 15 || len(packets) != 1 {
		t.Fatal("packet/segment aggregation", a, len(packets))
	}
	k.ack_push(7, currentMs())
	k.flush(IKCP_FLUSH_ACKONLY)
	b, ok := s.TryTransmitSnapshot()
	if !ok || a != b {
		t.Fatal("ACK changed PUSH counters")
	}
	n, sz := transmitPushObservation(t, packets)
	if n != 3 || sz != 15 {
		t.Fatal(n, sz)
	}
}
func TestTransmitSnapshotBusyUnknownIsolationAndCopy(t *testing.T) {
	a := &UDPSession{kcp: NewKCP(1, func([]byte, int) {}), die: make(chan struct{})}
	b := &UDPSession{kcp: NewKCP(2, func([]byte, int) {})}
	a.kcp.transmitStats.InitialSegments = 4
	a.mu.Lock()
	busy, ok := a.TryTransmitSnapshot()
	a.mu.Unlock()
	if ok || busy != (TransmitSnapshot{}) {
		t.Fatal("busy invented counters")
	}
	x, ok := a.TryTransmitSnapshot()
	if !ok || x.Stats.InitialSegments != 4 {
		t.Fatal(x)
	}
	x.Stats.InitialSegments = 99
	y, ok := a.TryTransmitSnapshot()
	if !ok || y.Stats.InitialSegments != 4 {
		t.Fatal("mutable copy")
	}
	z, ok := b.TryTransmitSnapshot()
	if !ok || z.Stats != (TransmitStats{}) {
		t.Fatal("cross-session counters")
	}
	close(a.die)
	z, ok = a.TryTransmitSnapshot()
	if !ok || !z.Closed || z.Stats.InitialSegments != 4 {
		t.Fatal("closed snapshot lost history")
	}
	var nilSession *UDPSession
	if _, ok := nilSession.TryTransmitSnapshot(); ok {
		t.Fatal("nil not unknown")
	}
}
func TestTransmitSnapshotConcurrentOriginalFlush(t *testing.T) {
	k := NewKCP(1, func([]byte, int) {})
	k.NoDelay(1, 10, 2, 1)
	s := &UDPSession{kcp: k}
	k.Send([]byte("original"))
	k.flush(IKCP_FLUSH_FULL)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var last uint64
			for j := 0; j < 1000; j++ {
				v, ok := s.TryTransmitSnapshot()
				if ok {
					if v.Stats.FastSegments < last {
						t.Error("non-monotonic original counter")
					}
					last = v.Stats.FastSegments
				}
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		s.mu.Lock()
		seg, ok := k.snd_buf.Peek()
		if !ok {
			s.mu.Unlock()
			t.Fatal("original queue absent")
		}
		seg.fastack = 2
		k.flush(IKCP_FLUSH_FULL)
		s.mu.Unlock()
	}
	wg.Wait()
	v, ok := s.TryTransmitSnapshot()
	if !ok || v.Stats.InitialSegments != 1 || v.Stats.FastSegments != 1000 {
		t.Fatal(v, ok)
	}
}
