package kcp

import (
	"context"
	"errors"
	"sync"
	"time"
)

var ErrSessionWorkersUnavailable = errors.New("kcp: original worker lifetime unavailable")

const (
	sessionReadWorker uint8 = 1 << iota
	sessionPostWorker
	sessionUpdateWorker
	sessionAllWorkers = sessionReadWorker | sessionPostWorker | sessionUpdateWorker
)

// These identities precede original worker creation. Only returned original
// readLoop/postProcess calls and the final scheduled update publish completion.
// No watcher, timer, network operation or replacement session is introduced.
type sessionWorkerLifetime struct {
	mu               sync.Mutex
	done             chan struct{}
	returned         uint8
	scheduledUpdates int
	nextUpdate       time.Time
	callback         func()
	callbackBound    bool
}

func newSessionWorkerLifetime(ownedReader bool) *sessionWorkerLifetime {
	w := &sessionWorkerLifetime{done: make(chan struct{})}
	// A listener owns its shared reader. It is not this session's worker.
	if !ownedReader {
		w.returned = sessionReadWorker
	}
	return w
}
func (s *UDPSession) runReadWorker() { defer s.workerReturned(sessionReadWorker); s.readLoop() }
func (s *UDPSession) runPostWorker() { defer s.workerReturned(sessionPostWorker); s.postProcess() }
func (s *UDPSession) scheduleUpdate(at time.Time) {
	if s.workers != nil {
		s.workers.mu.Lock()
		s.workers.scheduledUpdates++
		s.workers.nextUpdate = at
		s.workers.mu.Unlock()
	}
	SystemTimedSched.Put(s.runScheduledUpdate, at)
}
func (s *UDPSession) runScheduledUpdate() {
	defer func() {
		if s.workers == nil {
			return
		}
		w := s.workers
		w.mu.Lock()
		w.scheduledUpdates--
		if w.scheduledUpdates == 0 && s.isClosed() {
			w.returned |= sessionUpdateWorker
		}
		callback := w.publishLocked()
		w.mu.Unlock()
		if callback != nil {
			callback()
		}
	}()
	s.update()
}
func (s *UDPSession) workerReturned(worker uint8) {
	if s.workers == nil {
		return
	}
	w := s.workers
	w.mu.Lock()
	w.returned |= worker
	callback := w.publishLocked()
	w.mu.Unlock()
	if callback != nil {
		callback()
	}
}
func (w *sessionWorkerLifetime) publishLocked() func() {
	if w.returned != sessionAllWorkers {
		return nil
	}
	select {
	case <-w.done:
		return nil
	default:
	}
	callback := w.callback
	w.callback = nil
	close(w.done)
	return callback
}

// WorkersDone reports return of the original session-owned worker functions.
// It does not mean ACK drain, final-control receipt or listener shutdown.
func (s *UDPSession) WorkersDone() <-chan struct{} {
	if s == nil || s.workers == nil {
		return nil
	}
	return s.workers.done
}

// ObserveWorkersDone binds one original owner's completion callback. Its
// reference is removed before publication, including already-completed joins.
func (s *UDPSession) ObserveWorkersDone(callback func()) error {
	if s == nil || s.workers == nil {
		return ErrSessionWorkersUnavailable
	}
	if callback == nil {
		return errors.New("kcp: nil original worker observer")
	}
	w := s.workers
	w.mu.Lock()
	if w.callbackBound {
		w.mu.Unlock()
		return errors.New("kcp: original worker observer already bound")
	}
	w.callbackBound = true
	select {
	case <-w.done:
		w.mu.Unlock()
		callback()
	default:
		w.callback = callback
		w.mu.Unlock()
	}
	return nil
}
func (s *UDPSession) WaitWorkersContext(ctx context.Context) error {
	if s == nil || s.workers == nil {
		return ErrSessionWorkersUnavailable
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if e := sessionWorkerCallerError(ctx); e != nil {
		return e
	}
	select {
	case <-s.workers.done:
		return sessionWorkerCallerError(ctx)
	case <-ctx.Done():
		return sessionWorkerCallerError(ctx)
	}
}
func sessionWorkerCallerError(ctx context.Context) error {
	e := ctx.Err()
	if e != nil && context.Cause(ctx) != e {
		e = errors.Join(e, context.Cause(ctx))
	}
	if d, ok := ctx.Deadline(); ok && !time.Now().Before(d) {
		e = errors.Join(e, context.DeadlineExceeded)
	}
	return e
}
