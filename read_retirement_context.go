// SPDX-License-Identifier: MIT
package kcp

import (
	"context"
	"io"
	"sync/atomic"

	"github.com/pkg/errors"
)

var errUnboundedRetirementRead = errors.New("kcp: retirement read requires a bounded context")

// ReadRetirementContext consumes the original receive queue for an exclusively
// owned retirement reader. Its caller must supply the original close lifetime's
// absolute deadline after retiring application ownership. This method never
// changes or renews that deadline or the shared application read deadline. It
// consumes the same KCP queue through the existing receive/ACK implementation.
// The caller MUST join ordinary application readers before entry: both APIs
// share the data/deadline wakeup channel. Concurrent wakeup ownership must not
// be inferred from an unchanged deadline value. This API is not wired into Read.
func (s *UDPSession) ReadRetirementContext(ctx context.Context, b []byte) (int, error) {
	if ctx == nil {
		return 0, errUnboundedRetirementRead
	}
	if _, ok := ctx.Deadline(); !ok {
		return 0, errUnboundedRetirementRead
	}
	var boundScope context.Context
	var boundDone <-chan struct{}
	s.mu.Lock()
	s.readOwnershipStarted = true
	g := s.applicationReads
	if g != nil && (!g.sealed || g.active != 0) {
		s.mu.Unlock()
		return 0, errReadHandoffNotJoined
	}
	if g != nil {
		deadline, _ := ctx.Deadline()
		if !deadline.Equal(g.deadline) {
			s.mu.Unlock()
			return 0, errReadHandoffScope
		}
		boundScope = g.scope
		boundDone = boundScope.Done()
	}
	s.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		if boundScope != nil {
			if err := boundScope.Err(); err != nil {
				return 0, err
			}
		}
		s.mu.Lock()
		if boundScope != nil {
			if err := boundScope.Err(); err != nil {
				s.mu.Unlock()
				return 0, err
			}
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return 0, err
		}
		if len(s.bufptr) > 0 {
			n := copy(b, s.bufptr)
			s.bufptr = s.bufptr[n:]
			s.mu.Unlock()
			atomic.AddUint64(&DefaultSnmp.BytesReceived, uint64(n))
			return n, nil
		}
		if size := s.kcp.PeekSize(); size > 0 {
			if len(b) >= size {
				s.kcp.Recv(b)
				s.mu.Unlock()
				atomic.AddUint64(&DefaultSnmp.BytesReceived, uint64(size))
				return size, nil
			}
			if cap(s.recvbuf) < size {
				s.recvbuf = make([]byte, size)
			}
			s.recvbuf = s.recvbuf[:size]
			s.kcp.Recv(s.recvbuf)
			n := copy(b, s.recvbuf)
			s.bufptr = s.recvbuf[n:]
			s.mu.Unlock()
			atomic.AddUint64(&DefaultSnmp.BytesReceived, uint64(n))
			return n, nil
		}
		if s.readEOF.Load() {
			s.mu.Unlock()
			return 0, io.EOF
		}
		s.mu.Unlock()
		select {
		case <-boundDone:
			return 0, boundScope.Err()
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-s.chReadEOF:
		case <-s.chReadEvent:
		case <-s.chSocketReadError:
			return 0, s.socketReadError.Load().(error)
		case <-s.die:
			return 0, errors.WithStack(io.ErrClosedPipe)
		}
	}
}
