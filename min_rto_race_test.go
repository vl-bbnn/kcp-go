package kcp

import (
	"sync"
	"testing"
)

func TestMinRTOConcurrentConfiguration(t *testing.T) {
	s := &UDPSession{kcp: NewKCP(1, func([]byte, int) {})}
	s.SetNoDelay(1, 10, 2, 1)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				if !s.SetMinRTO(uint32(300 + i)) {
					t.Error("valid floor rejected")
				}
				if s.GetRTO() < 300 {
					t.Error("configured floor not visible")
				}
			}
		}(i)
	}
	wg.Wait()
}
