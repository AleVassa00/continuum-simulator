package partitioncompletion

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"continuum/internal/kafkautil"
	"continuum/internal/model"
)

func producerIDs(p, count int) []string {
	var ids []string
	for i := 0; len(ids) < count; i++ {
		id := fmt.Sprintf("edge-%d", i)
		if kafkautil.PartitionForEdge(id) == p {
			ids = append(ids, id)
		}
	}
	return ids
}

func receive(t *testing.T, ch <-chan int) int {
	t.Helper()
	select {
	case p := <-ch:
		return p
	case <-time.After(2 * time.Second):
		t.Fatal("partition EOS was not dispatched")
		return -1
	}
}

func TestIndependentPartitionsAndConcurrentIdempotentCompletion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	p0, p1 := producerIDs(0, 2), producerIDs(1, 1)
	ids := append(append([]string{}, p0...), p1...)
	published := make(chan int, 20)
	blocked := make(chan struct{})
	c, err := New(ctx, ids, func(ctx context.Context, p int) error {
		published <- p
		if p == 0 {
			select {
			case <-blocked:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); c.Wait() }()
	if err := c.Complete(p0[0]); err == nil {
		t.Fatal("completion before runner start accepted")
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	// Empty partitions are closed independently at startup, no data/Edge EOS needed.
	seen := map[int]bool{}
	for i := 2; i < model.SourcePartitionCount; i++ {
		p := receive(t, published)
		if p < 2 || seen[p] {
			t.Fatalf("unexpected empty EOS %d", p)
		}
		seen[p] = true
	}
	if err := c.Complete(p0[0]); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-published:
		t.Fatalf("premature EOS %d", p)
	default:
	}
	var callers sync.WaitGroup
	for i := 0; i < 20; i++ {
		callers.Add(1)
		go func() {
			defer callers.Done()
			if err := c.Complete(p0[1]); err != nil {
				t.Error(err)
			}
		}()
	}
	callers.Wait()
	if p := receive(t, published); p != 0 {
		t.Fatalf("wanted P0, got %d", p)
	}
	// P0's publishing call is still blocked, but P1 must dispatch immediately.
	if err := c.Complete(p1[0]); err != nil {
		t.Fatal(err)
	}
	if p := receive(t, published); p != 1 {
		t.Fatalf("wanted P1, got %d", p)
	}
	close(blocked)
	c.Wait()
	if done, failed := c.Status(); !done || failed {
		t.Fatalf("status complete=%v failed=%v", done, failed)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	if err := c.Complete(p0[0]); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-published:
		t.Fatalf("duplicate terminal %d", p)
	default:
	}
	if err := c.Complete("not-configured"); err == nil {
		t.Fatal("unknown producer accepted")
	}
}

func TestPublishFailureIsFatalAndNotComplete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ids := producerIDs(0, 1)
	c, err := New(ctx, ids, func(ctx context.Context, p int) error { return errors.New("broker unavailable") })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); c.Wait() }()
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Errors():
	case <-time.After(2 * time.Second):
		t.Fatal("failure not exposed")
	}
	if done, failed := c.Status(); done || !failed {
		t.Fatalf("status complete=%v failed=%v", done, failed)
	}
	if err := c.Complete(ids[0]); err == nil {
		t.Fatal("completion accepted after failure")
	}
}

func TestProducerConfigValidation(t *testing.T) {
	for _, ids := range [][]string{nil, {""}, {"edge-0", "edge-0"}, {" edge-0"}} {
		if _, err := New(context.Background(), ids, func(context.Context, int) error { return nil }); err == nil {
			t.Fatalf("accepted %v", ids)
		}
	}
}
