package kcp

// SetACKTriggeredRetransmit changes only duplicate-ACK fast/early retransmission
// on this exact session. Timer retransmission, ACK generation/processing, RTO,
// windows and deadlines stay unchanged. Defaults remain enabled. A caller using
// independent unordered paths can disable it for bulk DATA without treating
// path reordering as loss. This is not a delivery or loss guarantee.
func (s *UDPSession) SetACKTriggeredRetransmit(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	disabled := !enabled
	if s.kcp.ackTriggeredRetransmitDisabled == disabled {
		return
	}
	s.kcp.ackTriggeredRetransmitDisabled = disabled
	// Do not carry duplicate ACK history across an explicit policy boundary.
	// Resend timestamps and every pending original byte remain untouched.
	for segment := range s.kcp.snd_buf.ForEach {
		segment.fastack = 0
	}
}

// ACKTriggeredRetransmitEnabled reports this exact session's current policy.
func (s *UDPSession) ACKTriggeredRetransmitEnabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.kcp.ackTriggeredRetransmitDisabled
}
