package kcp

import "time"

// ReadDeadlineSnapshot observes the deadline used by Read without consuming a
// notification, resetting a timer, or changing the operation's deadline. The
// boolean distinguishes an unset atomic value from an explicitly cleared one.
// A concurrent SetDeadline or SetReadDeadline may change it after this snapshot.
func (s *UDPSession) ReadDeadlineSnapshot() (time.Time, bool) {
	deadline, observed := s.rd.Load().(time.Time)
	return deadline, observed
}
