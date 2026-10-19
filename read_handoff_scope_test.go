package kcp

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestApplicationReadHandoffFailedWaitCannotRenewScope(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	handoffEnable(t, s)
	g, e := s.beginApplicationRead()
	if e != nil {
		t.Fatal(e)
	}
	original, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if e := s.SealApplicationReadsContext(original); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
	s.finishApplicationRead(g)
	renewed, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if e := s.SealApplicationReadsContext(renewed); e == nil {
		t.Fatal("first expired handoff budget was renewed")
	}
	if _, e := s.ReadRetirementContext(renewed, make([]byte, 1)); !errors.Is(e, context.DeadlineExceeded) && e != errReadHandoffScope {
		t.Fatal("renewed read scope admitted", e)
	}
	if _, e := s.ReadRetirementContext(original, make([]byte, 1)); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal(e)
	}
}
func TestApplicationReadHandoffCanceledOwnerCannotBeReplaced(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	handoffEnable(t, s)
	original, cancel := context.WithTimeout(context.Background(), time.Second)
	deadline, _ := original.Deadline()
	if e := s.SealApplicationReadsContext(original); e != nil {
		t.Fatal(e)
	}
	cancel()
	replacement, stop := context.WithDeadline(context.Background(), deadline)
	defer stop()
	if e := s.SealApplicationReadsContext(replacement); !errors.Is(e, context.Canceled) {
		t.Fatal("original canceled owner replaced", e)
	}
	if _, e := s.ReadRetirementContext(replacement, make([]byte, 1)); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
}
func TestApplicationReadHandoffRejectsDifferentDeadlineAfterJoin(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	handoffEnable(t, s)
	original, cancel := handoffContext(t)
	defer cancel()
	if e := s.SealApplicationReadsContext(original); e != nil {
		t.Fatal(e)
	}
	deadline, _ := original.Deadline()
	for _, d := range []time.Time{deadline.Add(time.Second), deadline.Add(-10 * time.Millisecond)} {
		different, stop := context.WithDeadline(context.Background(), d)
		if e := s.SealApplicationReadsContext(different); e != errReadHandoffScope {
			stop()
			t.Fatal("different join scope admitted", e)
		}
		if _, e := s.ReadRetirementContext(different, make([]byte, 1)); e != errReadHandoffScope {
			stop()
			t.Fatal("different receive scope admitted", e)
		}
		stop()
	}
}
func TestApplicationReadHandoffOriginalCancellationWakesReplacementReader(t *testing.T) {
	s, _, _, _ := retirementScopePair(t)
	handoffEnable(t, s)
	original, cancel := handoffContext(t)
	defer cancel()
	deadline, _ := original.Deadline()
	if e := s.SealApplicationReadsContext(original); e != nil {
		t.Fatal(e)
	}
	replacement, stop := context.WithDeadline(context.Background(), deadline)
	defer stop()
	done := make(chan error, 1)
	go func() { _, e := s.ReadRetirementContext(replacement, make([]byte, 1)); done <- e }()
	cancel()
	select {
	case e := <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal("original cancellation lost", e)
		}
	case <-replacement.Done():
		t.Fatal("canceled owner not observed before deadline")
	}
}
