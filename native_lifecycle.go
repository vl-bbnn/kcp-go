package kcp

import (
	"context"
	"errors"
	"time"
)

// Each scheduled callback has its own actual birth ticket, acquired before
// submission to the shared scheduler and released in its real epilogue.
// A queued callback stays outstanding after Close; no global scheduler changes.
func (s *UDPSession) scheduleOwnedUpdate(at time.Time) {
	ends, ok := s.nativeWork.acquire("kcp-scheduled-update")
	if !ok {
		return
	}
	SystemTimedSched.Put(func() { defer ends[0](); s.update() }, at)
}

// WaitClosed observes the original Close owner, postProcess, this session's
// own client readLoop (if any), and all its actual queued/executing callbacks.
// For caller-owned PacketConn, the caller must close/unblock that underlay.
// It never closes that underlay or shared listener/scheduler on its behalf.
func (s *UDPSession) WaitClosed(ctx context.Context) error {
	if err := waitCloseWorkers(ctx, s.nativeCloseReturned, s.nativeWork); err != nil {
		return err
	}
	return errors.Join(ctx.Err(), s.nativeCloseError, s.nativeWork.failure())
}

type NativeWorkerState struct {
	Generation      uint64
	Sealed          bool
	Started, Exited uint64
	Active          map[uint64]string
	ExitDigest      string
	ClientReadOwned bool
}

func (s *UDPSession) NativeWorkerSnapshot() NativeWorkerState {
	snap := s.nativeWork.snapshot()
	state := NativeWorkerState{Generation: snap.Generation, Sealed: snap.Sealed, Started: snap.Started, Exited: snap.Exited, Active: make(map[uint64]string), ExitDigest: snap.ExitDigest, ClientReadOwned: s.nativeReadOwned}
	for _, r := range snap.Workers {
		state.Active[r.ID] = r.Role
	}
	return state
}
