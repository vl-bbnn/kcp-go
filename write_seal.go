package kcp

// SealWriteAdmission rejects new or window-blocked writes without discarding
// accepted sender data or stopping packet input. Safe under concurrent Close.
func (s *UDPSession) SealWriteAdmission() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.writeSealed {
		s.writeSealed = true
		close(s.chWriteSealed)
	}
}
