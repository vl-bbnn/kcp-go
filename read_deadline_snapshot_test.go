package kcp

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestReadDeadlineSnapshotUnsetAndReset(t *testing.T) {
	s := &UDPSession{}
	if deadline, observed := s.ReadDeadlineSnapshot(); observed || !deadline.IsZero() {
		t.Fatal("unconfigured deadline fabricated", deadline, observed)
	}
	for _, want := range []time.Time{time.Now().Add(time.Hour), time.Now().Add(-time.Hour), {}} {
		if err := s.SetReadDeadline(want); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if actual, observed := s.ReadDeadlineSnapshot(); !observed || !actual.Equal(want) {
				t.Fatal("snapshot changed the deadline", actual, observed, want)
			}
		}
	}
}

func TestReadDeadlineSnapshotDoesNotRelaxExpiredRead(t *testing.T) {
	s := &UDPSession{
		die: make(chan struct{}), chReadEOF: make(chan struct{}),
		chReadEvent: make(chan struct{}, 1), chSocketReadError: make(chan struct{}),
		kcp: NewKCP(1, func([]byte, int) {}),
	}
	want := time.Now().Add(-time.Second)
	if err := s.SetReadDeadline(want); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if actual, observed := s.ReadDeadlineSnapshot(); !observed || !actual.Equal(want) {
			t.Fatal("expired deadline lost", actual, observed)
		}
	}
	result := make(chan error, 1)
	go func() { _, err := s.Read(make([]byte, 1)); result <- err }()
	select {
	case err := <-result:
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatal("original read timeout changed", err)
		}
	case <-time.After(time.Second):
		close(s.die)
		<-result
		t.Fatal("snapshot extended original expired read")
	}
}

func TestReadDeadlineSnapshotConcurrentSetter(t *testing.T) {
	s := &UDPSession{}
	values := []time.Time{{}, time.Unix(100, 200), time.Unix(300, 400)}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if j%2 == 0 {
					_ = s.SetReadDeadline(values[j%len(values)])
				} else if value, observed := s.ReadDeadlineSnapshot(); observed && value != values[0] && value != values[1] && value != values[2] {
					t.Error("torn deadline snapshot", value)
				}
			}
		}()
	}
	wg.Wait()
}
