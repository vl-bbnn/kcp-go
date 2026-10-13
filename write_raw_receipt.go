package kcp

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// WriteWithRawReceipt sends one MSS-bounded write through the existing KCP,
// FEC and encryption pipeline and waits for its actual packet writers. It
// does not append WINS, wait for a remote ACK, or change ordinary Write.
// A nonnil error remains failure even when n reports locally queued bytes.
func (s *UDPSession) WriteWithRawReceipt(ctx context.Context, b []byte) (n int, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if e := ctx.Err(); e != nil {
		return 0, e
	}
	if _, ok := s.conn.(receiptPacketConn); !ok {
		return 0, errors.New("kcp: write raw receipt unsupported")
	}
	if wd, ok := s.wd.Load().(time.Time); ok && !wd.IsZero() {
		if current, ok := ctx.Deadline(); !ok || wd.Before(current) {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, wd)
			defer cancel()
		}
	}
	for {
		if e := ctx.Err(); e != nil {
			return 0, e
		}
		select {
		case <-s.chSocketWriteError:
			return 0, s.socketWriteError.Load().(error)
		case <-s.die:
			return 0, io.ErrClosedPipe
		default:
		}
		if e := s.lockRawReceiptContext(ctx, &s.mu); e != nil {
			return 0, e
		}
		// Context or closure may become ready at the same instant as the lock.
		// Do not accept any new bytes after the caller's original cap expires.
		if e := ctx.Err(); e != nil {
			s.mu.Unlock()
			return 0, e
		}
		select {
		case <-s.chSocketWriteError:
			s.mu.Unlock()
			return 0, s.socketWriteError.Load().(error)
		case <-s.die:
			s.mu.Unlock()
			return 0, io.ErrClosedPipe
		default:
		}
		if s.writeSealed {
			s.mu.Unlock()
			return 0, net.ErrClosed
		}
		if len(b) == 0 || len(b) > int(s.kcp.mss) {
			s.mu.Unlock()
			return 0, errors.New("kcp: raw receipt write requires one nonempty MSS-bounded payload")
		}
		if s.kcp.WaitSnd() >= int(s.kcp.snd_wnd) {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-s.chWriteEvent:
				continue
			case <-s.chWriteSealed:
				return 0, net.ErrClosed
			case <-s.chSocketWriteError:
				return 0, s.socketWriteError.Load().(error)
			case <-s.die:
				return 0, io.ErrClosedPipe
			}
		}
		// Capture only this synchronous flush under the existing KCP lock. All
		// captured buffers are either admitted once or returned once; output is
		// restored before unlock, and no network I/O runs inside KCP callbacks.
		original := s.kcp.output
		var packets [][]byte
		var targetSN uint32
		targetSeen := false
		s.kcp.output = func(p []byte, size int) {
			if size >= IKCP_OVERHEAD {
				buf := defaultBufferPool.Get()[:size+s.headerSize]
				copy(buf[s.headerSize:], p[:size])
				packets = append(packets, buf)
				for offset := 0; offset+IKCP_OVERHEAD <= size; {
					length := int(binary.LittleEndian.Uint32(p[offset+20 : offset+24]))
					if offset+IKCP_OVERHEAD+length > size {
						break
					}
					if p[offset+4] == IKCP_CMD_PUSH && binary.LittleEndian.Uint32(p[offset+12:offset+16]) == targetSN {
						targetSeen = true
					}
					offset += IKCP_OVERHEAD + length
				}
			}
		}
		sent := s.kcp.Send(b)
		if sent == 0 {
			targetSN = s.kcp.snd_nxt + uint32(s.kcp.snd_queue.Len()) - 1
			s.kcp.flush(IKCP_FLUSH_FULL)
			if !targetSeen {
				s.kcp.flush(IKCP_FLUSH_FULL)
			}
			n = len(b)
			atomic.AddUint64(&DefaultSnmp.BytesSent, uint64(n))
		}
		s.kcp.output = original
		if sent != 0 {
			s.mu.Unlock()
			return 0, errors.New("kcp: raw receipt payload admission failed")
		}
		if !targetSeen {
			// flush has already consumed ACKs/probes and advanced retransmit state.
			// Preserve those ordinary outputs through the restored callback exactly
			// once. They cannot certify the new payload and carry no new receipt.
			for _, buf := range packets {
				original(buf[s.headerSize:], len(buf)-s.headerSize)
				defaultBufferPool.Put(buf)
			}
			s.mu.Unlock()
			return n, errors.New("kcp: raw receipt write did not emit its original PUSH")
		}
		receipts := make([]*watermarkReceipt, 0, len(packets))
		for i, buf := range packets {
			receipt := &watermarkReceipt{ctx: ctx, done: make(chan error, 1)}
			req := acquireSendRequest(buf, false)
			req.receipt = receipt
			e := s.admitRawWriteReceipt(ctx, req)
			if e != nil {
				releaseSendRequest(req)
				defaultBufferPool.Put(buf)
				for _, rest := range packets[i+1:] {
					defaultBufferPool.Put(rest)
				}
				s.mu.Unlock()
				return n, e
			}
			receipts = append(receipts, receipt)
		}
		// Admission precedes unlock so an updater cannot send the original PUSH
		// through the ordinary queue before its own receipt packet is admitted.
		s.mu.Unlock()
		var joined error
		for _, receipt := range receipts {
			// An admitted writer owns its outcome even if the inbound reader ends.
			// The original bounded context remains the cap; no new timeout starts.
			select {
			case e := <-receipt.done:
				joined = errors.Join(joined, e)
			case <-ctx.Done():
				select {
				case e := <-receipt.done:
					joined = errors.Join(joined, e)
				default:
					joined = errors.Join(joined, ctx.Err())
				}
			}
		}
		// A ready nil receipt cannot publish success after the original caller
		// was canceled or its deadline expired. Keep any actual writer error too.
		if e := ctx.Err(); e != nil {
			joined = errors.Join(joined, e)
		}
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			joined = errors.Join(joined, context.DeadlineExceeded)
		}
		return n, joined
	}
}

func (s *UDPSession) admitRawWriteReceipt(ctx context.Context, req *sendRequest) error {
	for {
		if e := s.lockRawReceiptContext(ctx, &s.postProcessingAdmission); e != nil {
			return e
		}
		if e := ctx.Err(); e != nil {
			s.postProcessingAdmission.Unlock()
			return e
		}
		if s.postProcessingSealed || s.isClosed() {
			s.postProcessingAdmission.Unlock()
			return io.ErrClosedPipe
		}
		admitted := false
		select {
		case s.chPostProcessing <- req:
			admitted = true
		default:
		}
		s.postProcessingAdmission.Unlock()
		if admitted {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.die:
			return io.ErrClosedPipe
		case <-s.chPostProcessingAvailable:
		}
	}
}

// Lock acquisition is itself part of the original write budget. Both mutexes
// can be owned by another producer; waiting cannot renew or bypass that cap.
func (s *UDPSession) lockRawReceiptContext(ctx context.Context, mu *sync.Mutex) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if mu.TryLock() {
		return nil
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.die:
			return io.ErrClosedPipe
		case <-s.chWriteSealed:
			return net.ErrClosed
		case <-s.chSocketWriteError:
			return s.socketWriteError.Load().(error)
		case <-ticker.C:
		}
		if e := ctx.Err(); e != nil {
			return e
		}
		if mu.TryLock() {
			return nil
		}
	}
}
