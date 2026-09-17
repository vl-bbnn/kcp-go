package kcp

import (
	"bytes"
	"io"
	"net"
	"strconv"
	"testing"
	"time"
)

type batchMemoryPacketConn struct{ packets [][]byte }

func (p *batchMemoryPacketConn) ReadFrom([]byte) (int, net.Addr, error) { return 0, nil, io.EOF }
func (p *batchMemoryPacketConn) WriteTo(data []byte, _ net.Addr) (int, error) {
	p.packets = append(p.packets, bytes.Clone(data))
	return len(data), nil
}
func (*batchMemoryPacketConn) Close() error                     { return nil }
func (*batchMemoryPacketConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (*batchMemoryPacketConn) SetDeadline(time.Time) error      { return nil }
func (*batchMemoryPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (*batchMemoryPacketConn) SetWriteDeadline(time.Time) error { return nil }

// Drive the real pipeline across several flush boundaries. Duplication makes
// the final batch exceed the normal flush size, so append growth stays covered.
func TestPostProcessBatchPayloadPreservation(t *testing.T) {
	for _, dup := range []int{0, 2, 70} {
		t.Run(strconv.Itoa(dup), func(t *testing.T) {
			conn := new(batchMemoryPacketConn)
			s := &UDPSession{conn: conn, remote: &net.UDPAddr{}, die: make(chan struct{}), chPostProcessing: make(chan sendRequest, devBacklog), kcp: NewKCP(1, func([]byte, int) {}), dup: dup}
			expected := make([][]byte, 0)
			for n := 0; n < 193; n++ {
				data := defaultBufferPool.Get()[:33]
				for i := range data {
					data[i] = byte(n + i)
				}
				s.chPostProcessing <- sendRequest{buffer: data}
				for i := 0; i <= dup; i++ {
					expected = append(expected, bytes.Clone(data))
				}
			}
			close(s.die)
			s.postProcess()
			if len(conn.packets) != len(expected) {
				t.Fatalf("packets: got %d want %d", len(conn.packets), len(expected))
			}
			for i := range expected {
				if !bytes.Equal(conn.packets[i], expected[i]) {
					t.Fatalf("packet %d changed", i)
				}
			}
		})
	}
}

// Measures the actual per-sender queue allocation independently of sockets,
// KCP setup, traffic payloads and background scheduling.
func BenchmarkPostProcessEmptyLifecycle(b *testing.B) {
	s := &UDPSession{die: make(chan struct{}), chPostProcessing: make(chan sendRequest, devBacklog)}
	close(s.die)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.postProcess()
	}
}
