// SPDX-License-Identifier: MIT
package kcp

import (
	"context"
	"io"
	"time"

	"github.com/pkg/errors"
)

// ErrApplicationReadsRetired identifies only an explicitly sealed application
// reader. It must not be used to suppress socket, deadline or session errors.
var ErrApplicationReadsRetired = errors.New("kcp: application reads retired")
var errReadHandoffNotEnabled = errors.New("kcp: application read handoff not enabled")
var errReadHandoffStarted = errors.New("kcp: read ownership already started")
var errReadHandoffScope = errors.New("kcp: application read handoff requires original close scope")
var errReadHandoffNotJoined = errors.New("kcp: application reads not sealed and joined")

// All fields are protected by the owning session's mu. Both channels broadcast;
// neither competes for chReadEvent. Sealing and queue consumption use the same
// mutex, so no newly admitted application read can consume the retirement tail.
type applicationReadHandoff struct {
	scope    context.Context
	deadline time.Time
	sealed   bool
	active   int
	stop     chan struct{}
	joined   chan struct{}
	cause    error
}

// EnableApplicationReadHandoff must run before exposing the session to any
// receive API. It is an opt-in capability, not selected by any carrier here.
// Late activation is rejected even if an earlier ordinary read has returned.
func (s *UDPSession) EnableApplicationReadHandoff() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.applicationReads != nil {
		return nil
	}
	if s.readOwnershipStarted {
		return errReadHandoffStarted
	}
	select {
	case <-s.die:
		return errors.WithStack(io.ErrClosedPipe)
	default:
	}
	s.applicationReads = &applicationReadHandoff{stop: make(chan struct{}), joined: make(chan struct{})}
	return nil
}

func (s *UDPSession) beginApplicationRead() (*applicationReadHandoff, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readOwnershipStarted = true
	g := s.applicationReads
	if g != nil {
		if g.sealed {
			return nil, s.retiredApplicationReadErrorLocked(g)
		}
		g.active++
	}
	return g, nil
}
func (s *UDPSession) finishApplicationRead(g *applicationReadHandoff) {
	s.mu.Lock()
	defer s.mu.Unlock()
	g.active--
	if g.sealed && g.active == 0 {
		close(g.joined)
	}
}

// Real published errors retain precedence over the synthetic ownership signal.
// An already expired deadline is captured at seal and cannot be overwritten by
// a later application setter. The deadline atomic itself is never changed.
func (s *UDPSession) retiredApplicationReadErrorLocked(g *applicationReadHandoff) error {
	select {
	case <-s.chSocketReadError:
		return s.socketReadError.Load().(error)
	case <-s.die:
		return errors.WithStack(io.ErrClosedPipe)
	default:
	}
	if g.cause != nil {
		return g.cause
	}
	return ErrApplicationReadsRetired
}

// SealApplicationReadsContext seals new ordinary reads, wakes every in-flight
// ordinary reader and waits for their receive ownership to end. It requires the
// SAME bounded absolute close context later passed to ReadRetirementContext;
// it never creates a new timeout, clears an application deadline, or reopens a
// sealed session when waiting fails. First-seal deadline and cancellation are
// retained even if a later caller supplies another context. A different absolute
// deadline is rejected. Its success certifies ownership only, not
// successful transport retirement. Genuine errors still return through Read.
func (s *UDPSession) SealApplicationReadsContext(ctx context.Context) error {
	if ctx == nil {
		return errUnboundedRetirementRead
	}
	if _, ok := ctx.Deadline(); !ok {
		return errUnboundedRetirementRead
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	g := s.applicationReads
	if g == nil {
		s.mu.Unlock()
		return errReadHandoffNotEnabled
	}
	if g.sealed {
		deadline, _ := ctx.Deadline()
		if !deadline.Equal(g.deadline) {
			s.mu.Unlock()
			return errReadHandoffScope
		}
		if err := g.scope.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
	}
	if !g.sealed {
		g.scope = ctx
		g.deadline, _ = ctx.Deadline()
		g.sealed = true
		if d, ok := s.rd.Load().(time.Time); ok && !d.IsZero() && !time.Now().Before(d) {
			g.cause = errors.WithStack(errTimeout)
		}
		if g.cause == nil && s.readEOF.Load() && len(s.bufptr) == 0 && s.kcp.PeekSize() <= 0 {
			g.cause = io.EOF
		}
		close(g.stop)
		if g.active == 0 {
			close(g.joined)
		}
	}
	joined := g.joined
	original := g.scope
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-original.Done():
		return original.Err()
	case <-joined:
		if err := original.Err(); err != nil {
			return err
		}
		return ctx.Err()
	}
}
