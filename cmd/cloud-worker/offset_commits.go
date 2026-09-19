package main

import (
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

var errOffsetCommit = errors.New("Cloud offset commit failed")

// Conserva il massimo next offset visto per ogni partizione
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

// L'offset viene aggiunto solo dopo Process
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

// L'EOS forza il commit degli offset pendenti
func (b *offsetCommitBatch) addAndFlush(message kafka.Message) error {
	if err := b.add(message); err != nil {
		return err
	}
	return b.flush()
}

// Invia insieme tutti gli offset elaborati e ancora pendenti
func (b *offsetCommitBatch) flush() error {
	if len(b.pending) == 0 {
		return nil
	}
	if err := b.commit(map[string]map[int]int64{b.topic: b.pending}); err != nil {
		return fmt.Errorf("%w: %w", errOffsetCommit, err)
	}
	// In caso di errore gli offset restano pendenti.
	b.pending = make(map[int]int64)
	b.processed = 0
	return nil
}
