package kcp

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestSendRequestEnvelopeClearsPayload(t *testing.T) {
	req := acquireSendRequest([]byte{1, 2, 3}, true)
	releaseSendRequest(req)
	if req.buffer != nil || req.oob {
		t.Fatal("released envelope retains payload or OOB flag")
	}
	reused := acquireSendRequest([]byte{4}, false)
	defer releaseSendRequest(reused)
	if len(reused.buffer) != 1 || reused.buffer[0] != 4 || reused.oob {
		t.Fatal("stale envelope state")
	}
}

func TestPostProcessQueueFullAndClosedRemainNonblocking(t *testing.T) {
	s := &UDPSession{die: make(chan struct{}), chPostProcessing: make(chan *sendRequest, devBacklog)}
	for i := 0; i < devBacklog; i++ {
		payload := defaultBufferPool.Get()[:8]
		binary.LittleEndian.PutUint64(payload, uint64(i))
		if err := s.enqueuePostProcessing(payload, i%2 == 1); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() {
		s.mu.Lock() // The real KCP callback is invoked with this lock held.
		defer s.mu.Unlock()
		done <- s.enqueuePostProcessing(defaultBufferPool.Get()[:8], true)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("full queue blocked KCP flush")
	}
	if len(s.chPostProcessing) != devBacklog {
		t.Fatal("burst capacity changed")
	}
	for i := 0; i < devBacklog; i++ {
		req := <-s.chPostProcessing
		if binary.LittleEndian.Uint64(req.buffer) != uint64(i) || req.oob != (i%2 == 1) {
			t.Fatalf("queued envelope %d changed or reordered", i)
		}
		defaultBufferPool.Put(req.buffer)
		releaseSendRequest(req)
	}
	// A nil queue makes the closed branch deterministic, matching an exited
	// consumer: an owned payload must be released without waiting for a reader.
	s.chPostProcessing = nil
	close(s.die)
	if err := s.enqueuePostProcessing(defaultBufferPool.Get()[:8], true); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("closed queue error: %v", err)
	}
}

func TestPostProcessPooledConcurrentPayloadAndCloseDrain(t *testing.T) {
	conn := new(batchMemoryPacketConn)
	s := &UDPSession{conn: conn, remote: &net.UDPAddr{}, die: make(chan struct{}), chPostProcessing: make(chan *sendRequest, devBacklog), kcp: NewKCP(1, func([]byte, int) {})}
	done := make(chan struct{})
	go func() { s.postProcess(); close(done) }()
	const producers, perProducer = 4, 2000
	var wg sync.WaitGroup
	for id := 0; id < producers; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for seq := 0; seq < perProducer; seq++ {
				data := defaultBufferPool.Get()[:16]
				binary.LittleEndian.PutUint64(data, uint64(id))
				binary.LittleEndian.PutUint64(data[8:], uint64(seq))
				// Exercise pool transfer without intentional full-queue drops.
				s.chPostProcessing <- acquireSendRequest(data, seq%2 == 1)
			}
		}(id)
	}
	wg.Wait()
	close(s.die)
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("close did not drain queue")
	}
	if len(conn.packets) != producers*perProducer {
		t.Fatalf("packets=%d", len(conn.packets))
	}
	var next [producers]uint64
	for _, p := range conn.packets {
		id, seq := binary.LittleEndian.Uint64(p), binary.LittleEndian.Uint64(p[8:])
		if id >= producers || seq != next[id] {
			t.Fatalf("corrupt/reordered payload id=%d seq=%d", id, seq)
		}
		next[id]++
	}
	for _, count := range next {
		if count != perProducer {
			t.Fatalf("missing payloads: %d", count)
		}
	}
}

var postProcessBenchmarkSink any

func BenchmarkPostProcessQueueAllocation(b *testing.B) {
	b.Run("value_baseline", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postProcessBenchmarkSink = make(chan sendRequest, devBacklog)
		}
	})
	b.Run("pooled_pointer", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			postProcessBenchmarkSink = make(chan *sendRequest, devBacklog)
		}
	})
}

func BenchmarkPostProcessEnvelopeReuse(b *testing.B) {
	q := make(chan *sendRequest, devBacklog)
	buffer := make([]byte, 8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q <- acquireSendRequest(buffer, false)
		releaseSendRequest(<-q)
	}
}
