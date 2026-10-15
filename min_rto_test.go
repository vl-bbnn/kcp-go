package kcp

import "testing"

func TestMinRTOBoundsAndRTTUpdates(t *testing.T) {
	s := &UDPSession{kcp: NewKCP(1, func([]byte, int) {})}
	s.SetNoDelay(1, 10, 2, 1)
	for _, invalid := range []uint32{0, IKCP_RTO_MAX + 1, ^uint32(0)} {
		before := s.GetRTO()
		if s.SetMinRTO(invalid) || s.GetRTO() != before || s.kcp.rx_minrto != IKCP_RTO_NDL {
			t.Fatalf("invalid floor %d changed timing", invalid)
		}
	}
	if !s.SetMinRTO(400) || s.GetRTO() != 400 {
		t.Fatal("floor not applied before first transmission")
	}
	for i := 0; i < 100; i++ {
		s.kcp.update_ack(40)
		if s.GetRTO() < 400 {
			t.Fatal("RTT sampling violated floor")
		}
	}
	s.kcp.update_ack(1200)
	if s.GetRTO() <= 400 {
		t.Fatal("floor prevented adapting to a slower path")
	}
	before := s.GetRTO()
	if !s.SetMinRTO(30) || s.GetRTO() != before {
		t.Fatal("lowering floor discarded current RTT estimate")
	}
	s.SetNoDelay(0, 10, 2, 1)
	if s.kcp.rx_minrto != IKCP_RTO_MIN {
		t.Fatal("SetNoDelay no longer restores its documented timing")
	}
}
