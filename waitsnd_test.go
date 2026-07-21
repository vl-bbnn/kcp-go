package kcp

import "testing"

func TestUDPSessionWaitSnd(t *testing.T) {
	session := &UDPSession{kcp: NewKCP(1, func([]byte, int) {})}
	if got := session.WaitSnd(); got != 0 {
		t.Fatalf("new session WaitSnd() = %d, want 0", got)
	}

	payload := make([]byte, IKCP_MTU_DEF)
	if result := session.kcp.Send(payload); result != 0 {
		t.Fatalf("Send() = %d, want 0", result)
	}
	if got := session.WaitSnd(); got == 0 {
		t.Fatal("WaitSnd() = 0 with queued payload")
	}
}
