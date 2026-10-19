package kcp

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func pendingSession4543(t *testing.T) *UDPSession {
	t.Helper()
	pc, e := net.ListenPacket("udp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s, e := NewConn3(4543, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9}, nil, 0, 0, pc)
	if e != nil {
		_ = pc.Close()
		t.Fatal(e)
	}
	s.SetNoDelay(1, 10, 2, 1)
	t.Cleanup(func() {
		_ = s.Close()
		_ = pc.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if e := s.WaitWorkersContext(ctx); e != nil {
			t.Error("fixture workers not joined", e)
		}
	})
	return s
}
func pendingGoID4543() string {
	b := make([]byte, 128)
	n := runtime.Stack(b, false)
	return strings.Fields(string(b[:n]))[1]
}
func pendingSelect4543(t *testing.T, id, method string, extra func() bool) {
	t.Helper()
	end := time.Now().Add(time.Second)
	for time.Now().Before(end) {
		b := make([]byte, 2<<20)
		n := runtime.Stack(b, true)
		for _, s := range strings.Split(string(b[:n]), "\n\n") {
			if strings.HasPrefix(s, "goroutine "+id+" [select") && strings.Contains(s, ".(*UDPSession)."+method+"(") && (extra == nil || extra()) {
				return
			}
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("actual pending I/O select not observed", id, method)
}
func pendingTimeout4543(e error) bool { var n net.Error; return errors.As(e, &n) && n.Timeout() }
func pendingRecord4543(t *testing.T, name string, v any) {
	t.Helper()
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	root := os.Getenv("WLT_PENDING_DEADLINE_ROOT")
	if root == "" {
		t.Log(string(b))
		return
	}
	if !filepath.IsAbs(root) {
		t.Fatal("absolute evidence root required")
	}
	f, e := os.OpenFile(filepath.Join(root, name+".json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	_, e = f.Write(append(b, '\n'))
	if e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
}
func TestPendingCallerDeadline4543(t *testing.T) {
	for _, direction := range []string{"read", "write"} {
		scopes := []bool{false}
		if direction == "read" {
			scopes = []bool{false, true}
		}
		for _, handoff := range scopes {
			for _, setter := range []string{"direction", "both"} {
				for _, initial := range []string{"zero", "prearmed", "clear-rearm"} {
					name := direction + "/handoff-" + strconv.FormatBool(handoff) + "/" + setter + "/" + initial
					t.Run(name, func(t *testing.T) {
						s := pendingSession4543(t)
						if handoff {
							if e := s.EnableApplicationReadHandoff(); e != nil {
								t.Fatal(e)
							}
						}
						set := s.SetReadDeadline
						if direction == "write" {
							set = s.SetWriteDeadline
						}
						if setter == "both" {
							set = s.SetDeadline
						}
						if initial != "zero" {
							if e := set(time.Now().Add(time.Hour)); e != nil {
								t.Fatal(e)
							}
						}
						tail := 0
						if direction == "write" {
							s.SetWindowSize(1, 1)
							if _, e := s.Write([]byte("original accepted unacknowledged tail")); e != nil {
								t.Fatal(e)
							}
							tail = s.WaitSnd()
							if tail == 0 {
								t.Fatal("no pending original tail")
							}
						}
						ids := make(chan string, 1)
						done := make(chan error, 1)
						go func() {
							ids <- pendingGoID4543()
							var e error
							if direction == "read" {
								_, e = s.Read(make([]byte, 1))
							} else {
								_, e = s.Write([]byte("blocked caller write"))
							}
							done <- e
						}()
						id := <-ids
						method := "Read"
						if direction == "write" {
							method = "WriteBuffers"
						}
						pendingSelect4543(t, id, method, nil)
						if initial == "clear-rearm" {
							if e := set(time.Time{}); e != nil {
								t.Fatal(e)
							}
							pendingSelect4543(t, id, method, func() bool {
								if direction == "read" {
									return len(s.chReadEvent) == 0
								}
								return len(s.chWriteEvent) == 0
							})
						}
						deadline := time.Now().Add(20 * time.Millisecond)
						if e := set(deadline); e != nil {
							t.Fatal(e)
						}
						var original error
						select {
						case original = <-done:
						case <-time.After(100 * time.Millisecond):
							t.Fatal("pending explicit caller deadline was ignored")
						}
						if !pendingTimeout4543(original) {
							t.Fatal("genuine caller timeout replaced", original)
						}
						if direction == "write" && s.WaitSnd() != tail {
							t.Fatal("deadline changed accepted original tail")
						}
						if e := set(time.Now().Add(time.Hour)); e != nil {
							t.Fatal(e)
						}
						if !pendingTimeout4543(original) {
							t.Fatal("future setter changed returned raw timeout")
						}
						pendingRecord4543(t, strings.ReplaceAll(name, "/", "-"), map[string]any{"qualification": false, "direction": direction, "handoff_enabled": handoff, "setter": setter, "initial": initial, "actual_select_readiness": true, "explicit_deadline_unix_ns": deadline.UnixNano(), "caller_timeout_returned_within100ms": true, "original_timeout_after_later_setter_preserved": true, "accepted_write_tail_preserved": true})
					})
				}
			}
		}
	}
}
