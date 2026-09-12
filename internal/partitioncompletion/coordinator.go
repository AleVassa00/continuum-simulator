// Package partitioncompletion is the orchestration control plane. It sees only
// successful producer completions, never EdgeAggregate payloads or event times.
package partitioncompletion

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"continuum/internal/kafkautil"
	"continuum/internal/model"
)

type Publisher func(context.Context, int) error

type Coordinator struct {
	mu        sync.Mutex
	edges     map[string]int
	completed map[string]bool
	remaining [model.SourcePartitionCount]int
	queued    [model.SourcePartitionCount]bool
	finished  [model.SourcePartitionCount]bool
	queues    [model.SourcePartitionCount]chan struct{}
	started   bool
	failed    bool
	errors    chan error
	workers   sync.WaitGroup
}

func New(ctx context.Context, edgeIDs []string, publish Publisher) (*Coordinator, error) {
	if publish == nil || len(edgeIDs) == 0 {
		return nil, fmt.Errorf("producer list and publisher are required")
	}
	c := &Coordinator{edges: make(map[string]int), completed: make(map[string]bool), errors: make(chan error, 1)}
	for _, id := range edgeIDs {
		if id == "" || strings.TrimSpace(id) != id {
			return nil, fmt.Errorf("invalid producer ID %q", id)
		}
		if _, exists := c.edges[id]; exists {
			return nil, fmt.Errorf("duplicate producer %q", id)
		}
		p := kafkautil.PartitionForEdge(id)
		c.edges[id] = p
		c.remaining[p]++
	}
	for p := 0; p < model.SourcePartitionCount; p++ {
		c.queues[p] = make(chan struct{}, 1)
		c.workers.Add(1)
		go func(partition int) {
			defer c.workers.Done()
			select {
			case <-ctx.Done():
				return
			case <-c.queues[partition]:
			}
			// Each partition has its own publisher path. A slow broker request for
			// P0 cannot delay terminal markers for P1..P5 or completion registration.
			err := publish(ctx, partition)
			c.mu.Lock()
			defer c.mu.Unlock()
			if err != nil {
				c.failed = true
				select {
				case c.errors <- fmt.Errorf("source partition %d EOS failed: %w", partition, err):
				default:
				}
				return
			}
			c.finished[partition] = true
		}(p)
	}
	return c, nil
}

// Start is called by the runner after the static consumer group is stable and
// before replay begins. It immediately closes partitions with zero producers.
// This is a startup readiness gate, NOT a barrier on producer completions.
func (c *Coordinator) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failed {
		return fmt.Errorf("coordinator failed")
	}
	c.started = true
	for p := range c.remaining {
		c.closeIfReady(p)
	}
	return nil
}

// Complete accepts only successful, fully drained producer completions.
// Repeated HTTP notifications cannot decrement a partition twice or emit two EOS.
func (c *Coordinator) Complete(edgeID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started || c.failed {
		return fmt.Errorf("coordinator is not accepting completions")
	}
	p, exists := c.edges[edgeID]
	if !exists {
		return fmt.Errorf("unknown producer %q", edgeID)
	}
	if c.completed[edgeID] {
		return nil
	}
	c.completed[edgeID] = true
	c.remaining[p]--
	c.closeIfReady(p)
	return nil
}

func (c *Coordinator) closeIfReady(p int) {
	if c.remaining[p] == 0 && !c.queued[p] {
		c.queued[p] = true
		c.queues[p] <- struct{}{} // one slot and exactly one enqueue per partition
	}
}

func (c *Coordinator) Status() (complete, failed bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	complete = true
	for _, done := range c.finished {
		complete = complete && done
	}
	return complete, c.failed
}

func (c *Coordinator) Errors() <-chan error { return c.errors }
func (c *Coordinator) Wait()                { c.workers.Wait() }
