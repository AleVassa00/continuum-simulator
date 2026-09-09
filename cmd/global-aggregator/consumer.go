package main

import (
	"context"
	"continuum/internal/kafkautil"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

const operationTimeout = 5 * time.Second

type KafkaMessageCommitter func(kafka.Message) error

func newKafkaReader(broker, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: topic, GroupID: groupID, StartOffset: kafka.FirstOffset, MinBytes: 1, MaxBytes: 10 * 1024 * 1024, MaxWait: 500 * time.Millisecond})
}
func consume(ctx context.Context, reader *kafka.Reader, p *GlobalMessageProcessor) (bool, error) {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return false, nil
			}
			return false, err
		}
		complete, err := processAndCommitMessage(ctx, message, p, func(m kafka.Message) error { return kafkautil.CommitMessage(reader, m, operationTimeout) })
		if err != nil {
			return false, fmt.Errorf("global protocol error offset=%d: %w", message.Offset, err)
		}
		if complete {
			return true, nil
		}
	}
}
func processAndCommitMessage(ctx context.Context, m kafka.Message, p *GlobalMessageProcessor, commit KafkaMessageCommitter) (bool, error) {
	complete, err := p.Process(ctx, m)
	if err != nil {
		return false, err
	}
	if err := commit(m); err != nil {
		return false, err
	}
	return complete, nil
}
