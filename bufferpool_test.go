package kcp

import (
	"runtime"
	"testing"
)

func TestBufferPoolGetSize(t *testing.T) {
	bp := newBufferPool(mtuLimit)

	buf := bp.Get()

	// Check length
	if len(buf) != mtuLimit {
		t.Fatalf("expected len=%d, got %d", mtuLimit, len(buf))
		return
	}

	// Check capacity
	if cap(buf) != mtuLimit {
		t.Fatalf("expected cap=%d, got %d", mtuLimit, cap(buf))
		return
	}
}

// sync.Pool.Get may return a new buffer: Go also deliberately drops a
// fraction of Put values under race instrumentation. Validate safe borrowing
// and preserved bytes when reuse actually occurs, without demanding identity.
func TestBufferPoolPutAndReuse(t *testing.T) {
	bp := newBufferPool(mtuLimit)
	buf := bp.Get()
	buf[0] = 99
	if err := bp.Put(buf[:1]); err != nil {
		t.Fatal(err)
	}
	next := bp.Get()
	if len(next) != mtuLimit || cap(next) != mtuLimit {
		t.Fatalf("returned buffer shape len=%d cap=%d", len(next), cap(next))
	}
	if &next[0] == &buf[0] && next[0] != 99 {
		t.Fatal("reused buffer data changed")
	}
}
func TestBufferPoolAfterGCStillReturnsUsableFullBuffer(t *testing.T) {
	bp := newBufferPool(mtuLimit)
	buf := bp.Get()
	buf[0] = 99
	if err := bp.Put(buf[:1]); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	runtime.GC()
	next := bp.Get()
	if len(next) != mtuLimit || cap(next) != mtuLimit {
		t.Fatalf("buffer after allowed eviction len=%d cap=%d", len(next), cap(next))
	}
	next[mtuLimit-1] = 42
}
func TestBufferPoolOutstandingBorrowersDoNotAlias(t *testing.T) {
	bp := newBufferPool(mtuLimit)
	first := bp.Get()
	second := bp.Get()
	if &first[0] == &second[0] {
		t.Fatal("two outstanding borrowers share writable storage")
	}
	first[0] = 99
	second[0] = 42
	if first[0] != 99 {
		t.Fatal("second borrower changed first owned bytes")
	}
}

func TestBufferPoolPutWrongSizeIgnored(t *testing.T) {
	bp := newBufferPool(mtuLimit)

	// Make a buffer with wrong capacity
	wrongBuf := make([]byte, 100)

	bp.Put(wrongBuf)

	// Get should still return a buffer with mtuLimit capacity
	buf := bp.Get()

	if cap(buf) != mtuLimit {
		t.Fatalf("pool accepted wrong-sized buffer; expected cap=%d, got %d", mtuLimit, cap(buf))
		return
	}
}

func TestBufferPoolPutReturnsError(t *testing.T) {
	bp := newBufferPool(mtuLimit)

	// 1. Correct size
	buf := make([]byte, mtuLimit)
	if err := bp.Put(buf); err != nil {
		t.Fatalf("expected nil error for correct size, got %v", err)
	}

	// 2. Incorrect size
	wrongBuf := make([]byte, mtuLimit+1)
	if err := bp.Put(wrongBuf); err == nil {
		t.Fatalf("expected error for wrong size, got nil")
	}
}
