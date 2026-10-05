package kcp

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"net"
	"sync"
	"testing"
	"time"
)

type watermarkTestPacketConn struct{}

type watermarkFECConn struct {
	watermarkTestPacketConn
	mu       sync.Mutex
	parity   [][]byte
	receipts [][]byte
}

func (c *watermarkFECConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	c.mu.Lock()
	c.parity = append(c.parity, append([]byte(nil), p...))
	c.mu.Unlock()
	return len(p), nil
}

func (c *watermarkFECConn) WriteToReceipt(_ context.Context, p []byte, _ net.Addr) error {
	c.mu.Lock()
	c.receipts = append(c.receipts, append([]byte(nil), p...))
	c.mu.Unlock()
	return nil
}

func (watermarkTestPacketConn) ReadFrom([]byte) (int, net.Addr, error)                 { return 0, nil, net.ErrClosed }
func (watermarkTestPacketConn) WriteTo([]byte, net.Addr) (int, error)                  { return 0, nil }
func (watermarkTestPacketConn) WriteToReceipt(context.Context, []byte, net.Addr) error { return nil }
func (watermarkTestPacketConn) Close() error                                           { return nil }
func (watermarkTestPacketConn) LocalAddr() net.Addr                                    { return &net.UDPAddr{} }
func (watermarkTestPacketConn) SetDeadline(time.Time) error                            { return nil }
func (watermarkTestPacketConn) SetReadDeadline(time.Time) error                        { return nil }
func (watermarkTestPacketConn) SetWriteDeadline(time.Time) error                       { return nil }

func watermarkPushFragment(conv, sn uint32, frg uint8, payload []byte) []byte {
	packet := make([]byte, IKCP_OVERHEAD+len(payload))
	seg := segment{conv: conv, cmd: IKCP_CMD_PUSH, sn: sn, frg: frg, wnd: 128, data: payload}
	copy(seg.encode(packet), payload)
	return packet
}

func watermarkPush(conv, sn uint32, payload []byte) []byte {
	return watermarkPushFragment(conv, sn, 0, payload)
}

func TestReceiveWatermarkAcknowledgesDroppedOriginalACKAndFragments(t *testing.T) {
	var dropped int
	receiver := NewKCP(7, func([]byte, int) { dropped++ })
	first := watermarkPushFragment(7, 0, 1, []byte("fragment-one"))
	second := watermarkPushFragment(7, 1, 0, []byte("fragment-two"))
	if ret := receiver.Input(first, IKCP_PACKET_REGULAR, true); ret != 0 {
		t.Fatalf("first input=%d", ret)
	}
	if ret := receiver.Input(second, IKCP_PACKET_REGULAR, true); ret != 0 {
		t.Fatalf("second input=%d", ret)
	}
	if dropped == 0 {
		t.Fatal("test did not drop original immediate ACK")
	}
	joined := make([]byte, 64)
	if n := receiver.Recv(joined); string(joined[:n]) != "fragment-onefragment-two" {
		t.Fatalf("fragmented payload = %q", joined[:n])
	}
	packet := receiver.receiveWatermark()
	got := binary.LittleEndian.Uint32(packet[16:20])
	if packet[4] != IKCP_CMD_WINS || got != 2 {
		t.Fatalf("WINS cmd=%d UNA=%d, want WINS UNA=2", packet[4], got)
	}
	sender := NewKCP(7, func([]byte, int) {})
	sender.NoDelay(1, 1, 2, 1)
	if ret := sender.Send([]byte("fragment-one")); ret != 0 {
		t.Fatalf("sender first send=%d", ret)
	}
	if ret := sender.Send([]byte("fragment-two")); ret != 0 {
		t.Fatalf("sender second send=%d", ret)
	}
	sender.flush(IKCP_FLUSH_FULL)
	if sender.WaitSnd() == 0 {
		t.Fatal("sender had no data awaiting ACK")
	}
	if ret := sender.Input(packet, IKCP_PACKET_REGULAR, true); ret != 0 {
		t.Fatalf("WINS input=%d", ret)
	}
	if sender.WaitSnd() != 0 {
		t.Fatalf("WINS did not cumulatively ACK sender: WaitSnd=%d", sender.WaitSnd())
	}
}

func TestReceiveWatermarkSequenceWrap(t *testing.T) {
	receiver := NewKCP(9, func([]byte, int) {})
	receiver.rcv_nxt = math.MaxUint32
	if ret := receiver.Input(watermarkPush(9, math.MaxUint32, []byte("final")), IKCP_PACKET_REGULAR, true); ret != 0 {
		t.Fatalf("wrap input=%d", ret)
	}
	if got := binary.LittleEndian.Uint32(receiver.receiveWatermark()[16:20]); got != 0 {
		t.Fatalf("wrapped WINS UNA=%d, want 0", got)
	}
}

func TestReceiveWatermarkFullPostProcessingQueueReturnsDeadline(t *testing.T) {
	s := &UDPSession{
		conn:             watermarkTestPacketConn{},
		kcp:              NewKCP(1, func([]byte, int) {}),
		chPostProcessing: make(chan sendRequest, 1),
		die:              make(chan struct{}),
	}
	s.chPostProcessing <- sendRequest{buffer: []byte("occupied")}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.FlushReceiveWatermark(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("full queue error=%v, want deadline", err)
	}
	if got := len(s.chPostProcessing); got != 1 {
		t.Fatalf("queue length=%d, want original occupied slot", got)
	}
}

func TestReceiveWatermarkCompletesFECGroupAndReusesEncoder(t *testing.T) {
	crypt, err := NewAESGCMCrypt([]byte("0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		block BlockCrypt
	}{
		{name: "plaintext"},
		{name: "aes_gcm", block: crypt},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &watermarkFECConn{}
			offset := 0
			if block, ok := tc.block.(*aeadCrypt); ok {
				offset = block.NonceSize()
			}
			s := &UDPSession{
				conn:             conn,
				remote:           &net.UDPAddr{},
				kcp:              NewKCP(11, func([]byte, int) {}),
				block:            tc.block,
				fecEncoder:       newFECEncoder(2, 1, offset),
				headerSize:       offset + fecHeaderSizePlus2,
				chPostProcessing: make(chan sendRequest, 4),
				die:              make(chan struct{}),
			}
			go s.postProcess()
			defer close(s.die)
			for i := 0; i < 4; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				err := s.FlushReceiveWatermark(ctx)
				cancel()
				if err != nil {
					t.Fatalf("watermark %d: %v", i, err)
				}
			}
			conn.mu.Lock()
			defer conn.mu.Unlock()
			if len(conn.receipts) != 4 || len(conn.parity) != 2 {
				t.Fatalf("wire packets: receipts=%d parity=%d, want 4 and 2", len(conn.receipts), len(conn.parity))
			}
			decode := func(packet []byte) []byte {
				if block, ok := tc.block.(*aeadCrypt); ok {
					plain, err := block.Open(nil, packet[:offset], packet[offset:], nil)
					if err != nil {
						t.Fatal(err)
					}
					return plain
				}
				return packet
			}
			for i, packet := range conn.receipts {
				plain := decode(packet)
				if len(plain) < fecHeaderSizePlus2+IKCP_OVERHEAD || plain[fecHeaderSizePlus2+4] != IKCP_CMD_WINS {
					t.Fatalf("receipt %d is not a FEC-wrapped WINS", i)
				}
			}
			for i, packet := range conn.parity {
				plain := decode(packet)
				if len(plain) <= fecHeaderSizePlus2 || binary.LittleEndian.Uint16(plain[4:6]) != typeParity {
					t.Fatalf("parity %d has invalid FEC frame", i)
				}
			}
		})
	}
}
