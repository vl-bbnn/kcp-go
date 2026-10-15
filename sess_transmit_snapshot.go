package kcp

// TransmitStats counts scheduled PUSH segments for one original KCP session.
// PayloadBytes excludes KCP headers, FEC, encryption and packet encapsulation.
// These are encoding-attempt counters, not successful socket writes, delivered
// bytes, lost packets, or unique application bytes acknowledged by the peer.
// Multiple segments may share one output datagram. No payload is retained.
type TransmitStats struct {
	InitialSegments, InitialPayloadBytes uint64
	FastSegments, FastPayloadBytes       uint64
	EarlySegments, EarlyPayloadBytes     uint64
	TimeoutSegments, TimeoutPayloadBytes uint64
}

// TransmitSnapshot is a bounded copy independent of mutable session state.
type TransmitSnapshot struct {
	Closed bool
	Stats  TransmitStats
}

// TryTransmitSnapshot never waits, flushes, invokes a callback, or changes a
// deadline. False means unknown, not zero transmissions. All runtime KCP
// mutations and these scalar counters use the existing session mutex.
func (s *UDPSession) TryTransmitSnapshot() (TransmitSnapshot, bool) {
	var out TransmitSnapshot
	if s == nil || !s.mu.TryLock() {
		return out, false
	}
	defer s.mu.Unlock()
	if s.kcp == nil {
		return out, false
	}
	out.Closed = s.isClosed()
	out.Stats = s.kcp.transmitStats
	return out, true
}
