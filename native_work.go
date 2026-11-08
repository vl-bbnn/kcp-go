package kcp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
)

// A generation identifies one concrete in-process owner, never a global count
// of active peers. Retirement follows real registered worker epilogues.
var nextWorkGeneration atomic.Uint64

type workRecord struct {
	ID     uint64
	Role   string
	Exited bool
}

type workSnapshot struct {
	Generation   uint64
	Kind         string
	Sealed       bool
	Workers      []workRecord
	Started      uint64
	Exited       uint64
	ExitDigest   string
	FailureCount uint64
}

type ownedWork struct {
	mu           sync.Mutex
	generation   uint64
	kind         string
	sealed       bool
	next         uint64
	exited       uint64
	exitDigest   [32]byte
	records      map[uint64]workRecord
	joined       chan struct{}
	joinOnce     sync.Once
	firstFailure error
	failureCount uint64
}

func newOwnedWork(kind string) *ownedWork {
	return &ownedWork{generation: nextWorkGeneration.Add(1), kind: kind, records: make(map[uint64]workRecord), joined: make(chan struct{})}
}

// acquire registers the complete birth set before any corresponding go call.
// Sealing prevents late replacement workers from escaping the same join.
func (w *ownedWork) acquire(roles ...string) ([]func(), bool) {
	if w == nil {
		return nil, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.sealed {
		return nil, false
	}
	ends := make([]func(), len(roles))
	for i, role := range roles {
		w.next++
		id := w.next
		w.records[id] = workRecord{ID: id, Role: role}
		var once sync.Once
		ends[i] = func() {
			once.Do(func() {
				w.mu.Lock()
				defer w.mu.Unlock()
				r, ok := w.records[id]
				if !ok || r.Exited {
					panic("invalid owned worker terminal")
				}
				// Keep only active tickets. A bounded digest commits real terminals
				// without accumulating one retained object per lifetime restart.
				frame := strconv.FormatUint(w.generation, 10) + ":" + strconv.FormatUint(id, 10) + ":" + strconv.Quote(r.Role)
				w.exitDigest = sha256.Sum256(append(w.exitDigest[:], []byte(frame)...))
				delete(w.records, id)
				w.exited++
				if w.sealed && len(w.records) == 0 {
					w.joinOnce.Do(func() { close(w.joined) })
				}
			})
		}
	}
	return ends, true
}

func (w *ownedWork) seal() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sealed = true
	if len(w.records) == 0 {
		w.joinOnce.Do(func() { close(w.joined) })
	}
}

func (w *ownedWork) wait(ctx context.Context) error {
	if w == nil {
		return errors.New("owned worker lifetime is unregistered")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.joined:
		return ctx.Err()
	}
}

func (w *ownedWork) snapshot() workSnapshot {
	if w == nil {
		return workSnapshot{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	s := workSnapshot{Generation: w.generation, Kind: w.kind, Sealed: w.sealed, Started: w.next, Exited: w.exited, ExitDigest: hex.EncodeToString(w.exitDigest[:]), FailureCount: w.failureCount}
	for _, r := range w.records {
		s.Workers = append(s.Workers, r)
	}
	sort.Slice(s.Workers, func(i, j int) bool { return s.Workers[i].ID < s.Workers[j].ID })
	return s
}

func (w *ownedWork) recordFailure(err error) {
	if err == nil || w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.firstFailure == nil {
		w.firstFailure = err
	}
	w.failureCount++
}

func (w *ownedWork) failure() error {
	if w == nil {
		return errors.New("worker error lifetime is unregistered")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.firstFailure
}

func waitCloseWorkers(ctx context.Context, returned <-chan struct{}, workers *ownedWork) error {
	if ctx == nil {
		return errors.New("finite close wait context required")
	}
	if _, finite := ctx.Deadline(); !finite {
		return errors.New("finite close wait context required")
	}
	if returned == nil || workers == nil {
		return errors.New("close worker lifetime is unregistered")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-returned:
	}
	return workers.wait(ctx)
}
