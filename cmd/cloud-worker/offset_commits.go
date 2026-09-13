package main

import (
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

var errOffsetCommit = errors.New("Cloud offset commit failed")

// Owned by the generation's single processing loop, never shared by readers.
type offsetCommitBatch struct {
	topic     string
	batchSize int
	processed int
	pending   map[int]int64
	commit    func(map[string]map[int]int64) error
}

func newOffsetCommitBatch(topic string, batchSize int, commit func(map[string]map[int]int64) error) (*offsetCommitBatch, error) {
	if batchSize <= 0 {
		return nil, fmt.Errorf("consumer commit batch size must be positive")
	}
	return &offsetCommitBatch{topic: topic, batchSize: batchSize, pending: make(map[int]int64), commit: commit}, nil
}

// Called ONLY after Process (including every synchronous publish) succeeds.
func (b *offsetCommitBatch) add(message kafka.Message) error {
	next := message.Offset + 1
	if previous, ok := b.pending[message.Partition]; !ok || next > previous {
		b.pending[message.Partition] = next
	}
	b.processed++
	if b.processed >= b.batchSize {
		return b.flush()
	}
	return nil
}

func (b *offsetCommitBatch) flush() error {
	if len(b.pending) == 0 {
		return nil
	}
	if err := b.commit(map[string]map[int]int64{b.topic: b.pending}); err != nil {
		return fmt.Errorf("%w: %w", errOffsetCommit, err)
	}
	// Do not clear state on failure or mutate a map passed to the commit call.
	b.pending = make(map[int]int64)
	b.processed = 0
	return nil
}
