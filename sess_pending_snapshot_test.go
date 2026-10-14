package kcp

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func pendingTestSession(output output_callback) *UDPSession {
	k := NewKCP(9876, output)
	k.NoDelay(1, 10, 2, 1)
	return &UDPSession{kcp: k, die: make(chan struct{})}
}

func TestPendingSnapshotBusyIsUnknownAndNonblocking(t *testing.T) {
	s := pendingTestSession(func([]byte, int) {})
	s.mu.Lock()
	started := time.Now()
	x, ok := s.TryPendingSnapshot()
	s.mu.Unlock()
	if ok || x != (PendingSnapshot{}) || time.Since(started) > time.Second {
		t.Fatal("busy inspection waited or invented state")
	}
}

func TestPendingSnapshotQueueBoundAndNoMutation(t *testing.T) {
	outputs := 0
	s := pendingTestSession(func([]byte, int) { outputs++ })
	for i := 0; i < 80; i++ {
		if s.kcp.Send([]byte("private test data")) != 0 {
			t.Fatal("Send")
		}
	}
	x, ok := s.TryPendingSnapshot()
	if !ok || x.WaitSnd != 80 || x.SendQueue != 80 || x.SendBuffer != 0 || x.Count != 64 || x.Omitted != 16 {
		t.Fatalf("bad bounded snapshot %+v", x)
	}
	for _, v := range x.Segments {
		if !v.Queued || v.SequenceAssigned || v.Transmissions != 0 || v.Command != 0 || v.DataBytes != 17 {
			t.Fatalf("invented transmitted state %+v", v)
		}
	}
	y, _ := s.TryPendingSnapshot()
	if x != y || s.kcp.WaitSnd() != 80 || outputs != 0 {
		t.Fatal("inspection mutated state or emitted output")
	}
	// Caller mutations cannot reach the live queue or later observations.
	x.Segments[0].DataBytes = 999
	z, _ := s.TryPendingSnapshot()
	if z != y {
		t.Fatal("snapshot alias")
	}
	allocs := testing.AllocsPerRun(100, func() { _, _ = s.TryPendingSnapshot() })
	if allocs != 0 {
		t.Fatal("inspection allocated", allocs)
	}
}

// Feed only actual peer-generated ACKs. Drop the first DATA segment, accept
// the second, and inspect accepted selective-ACK state before filling the gap.
func TestPendingSnapshotActualSelectiveACKAndCumulativeRecovery(t *testing.T) {
	var packets, acks [][]byte
	s := pendingTestSession(func(p []byte, n int) { packets = append(packets, append([]byte(nil), p[:n]...)) })
	peer := NewKCP(9876, func(p []byte, n int) { acks = append(acks, append([]byte(nil), p[:n]...)) })
	peer.NoDelay(1, 10, 2, 1)
	body := bytes.Repeat([]byte{42}, int(s.kcp.mss)*2)
	if s.kcp.Send(body) != 0 {
		t.Fatal("Send")
	}
	s.kcp.flush(IKCP_FLUSH_FULL)
	var data [][]byte
	for _, p := range packets {
		for len(p) >= IKCP_OVERHEAD {
			n := IKCP_OVERHEAD + int(binary.LittleEndian.Uint32(p[20:]))
			if n > len(p) {
				t.Fatal("invalid actual packet")
			}
			if p[4] == IKCP_CMD_PUSH {
				data = append(data, append([]byte(nil), p[:n]...))
			}
			p = p[n:]
		}
	}
	if len(data) != 2 {
		t.Fatal("expected two real DATA segments", len(data))
	}
	if peer.Input(data[1], IKCP_PACKET_REGULAR, false) != 0 {
		t.Fatal("peer DATA input")
	}
	peer.flush(IKCP_FLUSH_ACKONLY)
	for _, a := range acks {
		if s.kcp.Input(a, IKCP_PACKET_REGULAR, false) != 0 {
			t.Fatal("sender ACK input")
		}
	}
	x, ok := s.TryPendingSnapshot()
	if !ok || x.WaitSnd != 2 || x.SendBuffer != 2 || x.SendQueue != 0 || x.SendUNA != 0 || x.SendNext != 2 || x.Segments[0].ACKed != 0 || x.Segments[1].ACKed != 1 || x.Segments[1].DataBytes != 0 || x.Segments[0].DataBytes == 0 {
		t.Fatalf("accepted SACK not distinguished %+v", x)
	}
	acks = nil
	if peer.Input(data[0], IKCP_PACKET_REGULAR, false) != 0 {
		t.Fatal("gap input")
	}
	peer.flush(IKCP_FLUSH_ACKONLY)
	for _, a := range acks {
		if s.kcp.Input(a, IKCP_PACKET_REGULAR, false) != 0 {
			t.Fatal("cumulative ACK input")
		}
	}
	y, _ := s.TryPendingSnapshot()
	if y.WaitSnd != 0 || y.SendUNA != 2 {
		t.Fatalf("genuine cumulative ACK not applied %+v", y)
	}
	got := make([]byte, len(body))
	if peer.Recv(got) != len(body) || !bytes.Equal(got, body) {
		t.Fatal("DATA not delivered intact")
	}
}

func TestPendingSnapshotActualUDPConcurrentDeliveryAndClose(t *testing.T) {
	a, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer a.Close()
	b, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	sender, e := NewConn3(9876, b.LocalAddr(), nil, 0, 0, a)
	if e != nil {
		t.Fatal(e)
	}
	defer sender.Close()
	receiver, e := NewConn3(9876, a.LocalAddr(), nil, 0, 0, b)
	if e != nil {
		t.Fatal(e)
	}
	defer receiver.Close()
	sender.SetNoDelay(1, 10, 2, 1)
	receiver.SetNoDelay(1, 10, 2, 1)
	sender.SetDeadline(time.Now().Add(5 * time.Second))
	receiver.SetDeadline(time.Now().Add(5 * time.Second))
	body := bytes.Repeat([]byte("actual UDP body"), 2048)
	done := make(chan error, 1)
	go func() {
		got := make([]byte, len(body))
		_, e := io.ReadFull(receiver, got)
		if e == nil && !bytes.Equal(got, body) {
			e = io.ErrUnexpectedEOF
		}
		done <- e
	}()
	written := make(chan error, 1)
	go func() { _, e := sender.Write(body); written <- e }()
	deadline := time.Now().Add(5 * time.Second)
	samples := 0
	for {
		if x, ok := sender.TryPendingSnapshot(); ok {
			samples++
			if x.WaitSnd != x.SendBuffer+x.SendQueue || x.Count+x.Omitted != x.WaitSnd || x.Count > 64 {
				t.Fatal("incoherent observation")
			}
		}
		select {
		case e := <-written:
			if e != nil {
				t.Fatal(e)
			}
			goto writtenDone
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("Write did not finish")
		}
	}
writtenDone:
	if e := <-done; e != nil {
		t.Fatal(e)
	}
	for {
		x, ok := sender.TryPendingSnapshot()
		if ok {
			samples++
			if x.WaitSnd == 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("actual ACK not received")
		}
		time.Sleep(time.Millisecond)
	}
	if samples == 0 {
		t.Fatal("no observations")
	}
	if e := sender.Close(); e != nil {
		t.Fatal(e)
	}
	x, ok := sender.TryPendingSnapshot()
	if !ok || !x.Closed {
		t.Fatal("closed state absent")
	}
}
