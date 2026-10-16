package kcp

import (
	"bytes"
	"sync"
	"testing"
)

func TestACKRetransmitPolicyPreservesDefaultAndRTO(t *testing.T) {
	for _, kind := range []string{"default-fast", "default-early", "disabled-fast", "disabled-early", "disabled-RTO", "reenabled-fast"} {
		t.Run(kind, func(t *testing.T) {
			var packets [][]byte
			k := NewKCP(42, func(p []byte, n int) { packets = append(packets, bytes.Clone(p[:n])) })
			k.NoDelay(1, 10, 2, 1)
			s := &UDPSession{kcp: k}
			body := []byte("every original pending byte survives a policy switch")
			if k.Send(body) != 0 {
				t.Fatal("send")
			}
			k.flush(IKCP_FLUSH_FULL)
			seg, _ := k.snd_buf.Peek()
			seg.resendts = currentMs() + 60000
			if kind != "default-fast" && kind != "default-early" {
				s.SetACKTriggeredRetransmit(false)
			}
			if kind == "reenabled-fast" {
				s.SetACKTriggeredRetransmit(true)
			}
			seg.fastack = 2
			if kind == "default-early" || kind == "disabled-early" {
				seg.fastack = 1
			}
			if kind == "disabled-RTO" {
				seg.resendts = currentMs() - 1
			}
			k.flush(IKCP_FLUSH_FULL)
			snap, ok := s.TryTransmitSnapshot()
			if !ok {
				t.Fatal("snapshot")
			}
			st := snap.Stats
			wantFast, wantEarly, wantRTO := uint64(0), uint64(0), uint64(0)
			if kind == "default-fast" || kind == "reenabled-fast" {
				wantFast = 1
			}
			if kind == "default-early" {
				wantEarly = 1
			}
			if kind == "disabled-RTO" {
				wantRTO = 1
			}
			if st.InitialSegments != 1 || st.InitialPayloadBytes != uint64(len(body)) || st.FastSegments != wantFast || st.EarlySegments != wantEarly || st.TimeoutSegments != wantRTO {
				t.Fatalf("kind=%s stats=%+v", kind, st)
			}
			n, payload := transmitPushObservation(t, packets)
			if n != 1+wantFast+wantEarly+wantRTO || payload != n*uint64(len(body)) {
				t.Fatal("actual PUSH differs")
			}
			before := st
			k.ack_push(99, currentMs())
			k.flush(IKCP_FLUSH_ACKONLY)
			after, _ := s.TryTransmitSnapshot()
			if before != after.Stats || k.WaitSnd() != 1 {
				t.Fatal("ACK output lost data or changed PUSH counters")
			}
		})
	}
}
func TestACKRetransmitPolicyPreservesPendingAndTimers(t *testing.T) {
	k := NewKCP(43, func([]byte, int) {})
	k.NoDelay(1, 10, 2, 1)
	s := &UDPSession{kcp: k}
	body := []byte("unacknowledged original body")
	k.Send(body)
	k.flush(IKCP_FLUSH_FULL)
	seg, _ := k.snd_buf.Peek()
	stamp, rto, rx, snd, rcv := seg.resendts, seg.rto, k.rx_rto, k.snd_wnd, k.rcv_wnd
	seg.fastack = 7
	s.SetACKTriggeredRetransmit(false)
	if !k.ackTriggeredRetransmitDisabled || seg.fastack != 0 || seg.resendts != stamp || seg.rto != rto || k.rx_rto != rx || k.snd_wnd != snd || k.rcv_wnd != rcv || k.WaitSnd() != 1 || !bytes.Equal(seg.data, body) {
		t.Fatal("policy changed original pending/timer/window state")
	}
	seg.fastack = 3
	s.SetACKTriggeredRetransmit(false)
	if seg.fastack != 3 {
		t.Fatal("idempotent setter changed history")
	}
	s.SetACKTriggeredRetransmit(true)
	if k.ackTriggeredRetransmitDisabled || seg.fastack != 0 {
		t.Fatal("reenable boundary")
	}
}
func TestACKRetransmitPolicyConcurrentSetterSnapshot(t *testing.T) {
	s := &UDPSession{kcp: NewKCP(44, func([]byte, int) {})}
	var wg sync.WaitGroup
	for i := range 128 {
		wg.Add(1)
		go func(i int) { defer wg.Done(); s.SetACKTriggeredRetransmit(i%2 == 0); s.TryTransmitSnapshot() }(i)
	}
	wg.Wait()
}
