package kcp

// MaxPendingSnapshotSegments bounds inspection work regardless of queue size.
const MaxPendingSnapshotSegments = 64

// PendingSegmentMetadata contains no payload or references to mutable state.
// Queued segments have no assigned sequence/command yet. DataBytes is the
// currently retained length, which becomes zero after an accepted selective ACK
// recycles a segment; it is not the original payload length.
type PendingSegmentMetadata struct {
	Queued, SequenceAssigned                     bool
	Sequence, UNA, Transmissions, FastACK, ACKed uint32
	Command, Fragment                            uint8
	DataBytes                                    int
}

// PendingSnapshot is a coherent observation of one session's KCP state.
// WaitSnd counts entries, including retained selectively ACKed entries. A nonzero
// count alone does not establish unacknowledged payload. Only the first Count
// entries are populated, send buffer first, then unscheduled send queue.
type PendingSnapshot struct {
	Closed                                                          bool
	State, SendUNA, SendNext, ReceiveNext, RemoteWindow             uint32
	SendBuffer, SendQueue, ReceiveBuffer, ReceiveQueue, PendingACKs int
	WaitSnd, Count, Omitted                                         int
	Segments                                                        [MaxPendingSnapshotSegments]PendingSegmentMetadata
}

// TryPendingSnapshot returns unavailable immediately when the session mutex is
// busy. It never waits for a writer, invokes callbacks, flushes, acknowledges,
// changes a deadline, or exposes payload. The caller must treat unavailable as
// unknown, never as an empty/ACKed queue. Inspection uses no heap allocation and
// visits at most MaxPendingSnapshotSegments entries plus two stop checks.
func (s *UDPSession) TryPendingSnapshot() (PendingSnapshot, bool) {
	var out PendingSnapshot
	if !s.mu.TryLock() {
		return out, false
	}
	defer s.mu.Unlock()
	k := s.kcp
	out.Closed = s.isClosed()
	out.State, out.SendUNA, out.SendNext, out.ReceiveNext, out.RemoteWindow = k.state, k.snd_una, k.snd_nxt, k.rcv_nxt, k.rmt_wnd
	out.SendBuffer, out.SendQueue = k.snd_buf.Len(), k.snd_queue.Len()
	out.ReceiveBuffer, out.ReceiveQueue, out.PendingACKs = k.rcv_buf.Len(), k.rcv_queue.Len(), len(k.acklist)
	out.WaitSnd = out.SendBuffer + out.SendQueue
	visit := func(queued bool, seg *segment) bool {
		if out.Count == len(out.Segments) {
			return false
		}
		out.Segments[out.Count] = PendingSegmentMetadata{Queued: queued, SequenceAssigned: !queued, Sequence: seg.sn, UNA: seg.una, Transmissions: seg.xmit, FastACK: seg.fastack, ACKed: seg.acked, Command: seg.cmd, Fragment: seg.frg, DataBytes: len(seg.data)}
		out.Count++
		return true
	}
	k.snd_buf.ForEach(func(seg *segment) bool { return visit(false, seg) })
	k.snd_queue.ForEach(func(seg *segment) bool { return visit(true, seg) })
	out.Omitted = out.WaitSnd - out.Count
	return out, true
}
