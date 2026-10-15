package kcp

// SetMinRTO sets the retransmission timeout floor in milliseconds. Configure it
// after SetNoDelay and before sending data: SetNoDelay restores its own floor,
// and already scheduled retransmissions are not rescheduled by this method.
// This only changes local timing; packet format and peer compatibility are
// unchanged. Zero and values above the protocol maximum are rejected.
func (s *UDPSession) SetMinRTO(milliseconds uint32) bool {
	if milliseconds == 0 || milliseconds > IKCP_RTO_MAX {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kcp.rx_minrto = milliseconds
	s.kcp.rx_rto = max(s.kcp.rx_rto, milliseconds)
	return true
}
