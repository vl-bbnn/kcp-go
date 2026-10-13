package kcp

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/crc32"
	"testing"
	"time"
)

func TestRawWriteReceiptOriginalPayloadThroughCryptoAndFEC(t *testing.T) {
	for _, kind := range []string{"plain", "aes-cfb", "aes-gcm"} {
		for _, fec := range []bool{false, true} {
			name := kind
			if fec {
				name += "/fec"
			}
			t.Run(name, func(t *testing.T) {
				wire := &watermarkFECConn{}
				s := rawWriteSession(&rawWriteTestConn{})
				s.conn = wire
				var err error
				switch kind {
				case "aes-cfb":
					s.block, err = NewAESBlockCrypt(make([]byte, 16))
					s.headerSize = cryptHeaderSize
				case "aes-gcm":
					s.block, err = NewAESGCMCrypt(make([]byte, 16))
					if err == nil {
						s.headerSize = s.block.(*aeadCrypt).NonceSize()
					}
				}
				if err != nil {
					t.Fatal(err)
				}
				if fec {
					s.fecEncoder = newFECEncoder(2, 1, s.headerSize)
					s.headerSize += fecHeaderSizePlus2
				}
				if !s.SetMtu(IKCP_MTU_DEF) {
					t.Fatal("production MTU rejected")
				}
				s.kcp.NoDelay(1, 1, 2, 1)
				rawWriteWorker(t, s)
				payloads := [][]byte{[]byte("first original payload"), []byte("second original payload")}
				for _, payload := range payloads {
					ctx, stop := context.WithTimeout(context.Background(), time.Second)
					n, err := s.WriteWithRawReceipt(ctx, payload)
					stop()
					if err != nil || n != len(payload) {
						t.Fatal(n, err)
					}
				}
				wire.mu.Lock()
				defer wire.mu.Unlock()
				if len(wire.receipts) != 2 {
					t.Fatal("original writer outcomes", len(wire.receipts))
				}
				for i, p := range wire.receipts {
					var decoded []byte
					switch block := s.block.(type) {
					case nil:
						decoded = p
					case *aeadCrypt:
						decoded, err = block.Open(nil, p[:block.NonceSize()], p[block.NonceSize():], nil)
						if err != nil {
							t.Fatal("actual AEAD original packet", err)
						}
					default:
						decoded = make([]byte, len(p))
						block.Decrypt(decoded, p)
						if binary.LittleEndian.Uint32(decoded[nonceSize:]) != crc32.ChecksumIEEE(decoded[cryptHeaderSize:]) {
							t.Fatal("actual CFB checksum")
						}
						decoded = decoded[cryptHeaderSize:]
					}
					if fec {
						if binary.LittleEndian.Uint16(decoded[4:]) != typeData {
							t.Fatal("receipt falsely bound to parity")
						}
						decoded = decoded[fecHeaderSizePlus2:]
					}
					if decoded[4] != IKCP_CMD_PUSH || !bytes.Equal(decoded[IKCP_OVERHEAD:], payloads[i]) {
						t.Fatal("original target bytes lost through packet pipeline")
					}
				}
				wantParity := 0
				if fec {
					wantParity = 1
				}
				if len(wire.parity) != wantParity {
					t.Fatal("original FEC parity pipeline", len(wire.parity), wantParity)
				}
				if s.WaitSnd() != 2 {
					t.Fatal("local original receipts fabricated remote ACKs")
				}
			})
		}
	}
}
