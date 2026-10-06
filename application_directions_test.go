package kcp

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func applicationDirectionSession(t *testing.T) *UDPSession {
	t.Helper()
	pc, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s, e := NewConn3(1, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9}, nil, 0, 0, pc)
	if e != nil {
		pc.Close()
		t.Fatal(e)
	}
	// Actual loopback sender exists, but no peer delivers ACKs to this session.
	t.Cleanup(func() { _ = s.Close(); _ = pc.Close() })
	return s
}
func TestApplicationEOFWakesEveryBlockedReader(t *testing.T) {
	s := applicationDirectionSession(t)
	done := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() { _, e := s.Read(make([]byte, 1)); done <- e }()
	}
	select {
	case e := <-done:
		t.Fatalf("read unexpectedly completed:%v", e)
	case <-time.After(30 * time.Millisecond):
	}
	s.SignalReadEOF()
	for i := 0; i < 4; i++ {
		select {
		case e := <-done:
			if !errors.Is(e, io.EOF) {
				t.Fatal(e)
			}
		case <-time.After(300 * time.Millisecond):
			t.Fatal("application EOF did not broadcast to every blocked reader")
		}
	}
}
func TestSealAdmissionWakesEveryBlockedWriterAndPreservesTail(t *testing.T) {
	s := applicationDirectionSession(t)
	s.SetWindowSize(1, 1)
	if _, e := s.Write(bytes.Repeat([]byte("accepted tail"), 100)); e != nil {
		t.Fatal(e)
	}
	pending := s.WaitSnd()
	if pending == 0 {
		t.Fatal("fixture lost pending sender")
	}
	done := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() { _, e := s.Write([]byte("blocked")); done <- e }()
	}
	select {
	case e := <-done:
		t.Fatalf("write not blocked:%v", e)
	case <-time.After(30 * time.Millisecond):
	}
	s.SealWriteAdmission()
	s.SealWriteAdmission()
	for i := 0; i < 4; i++ {
		select {
		case e := <-done:
			if !errors.Is(e, net.ErrClosed) {
				t.Fatal(e)
			}
		case <-time.After(300 * time.Millisecond):
			t.Fatal("seal did not broadcast to every blocked writer")
		}
	}
	if s.WaitSnd() != pending {
		t.Fatal("seal discarded accepted sender tail")
	}
	if s.readEOF.Load() {
		t.Fatal("seal stopped application receive direction")
	}
}
