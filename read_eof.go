package kcp

// SignalReadEOF closes only the application receive direction after the packet
// adapter has consumed all packets preceding an ordered peer FIN. Input keeps
// accepting real packets and generating real ACKs until Close.
func (s *UDPSession) SignalReadEOF() {
	if !s.readEOF.Swap(true) {
		close(s.chReadEOF)
	}
}
