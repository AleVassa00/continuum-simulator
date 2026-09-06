package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"continuum/internal/kafkautil"

	"github.com/segmentio/kafka-go"
)

const (
	operationTimeout                 = 5 * time.Second
	maxWatermarkAdvanceCheckInterval = 1 * time.Second
)

type KafkaMessageCommitter func(kafka.Message) error

func newKafkaReader(broker string, topic string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker},
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: kafka.FirstOffset,
		MinBytes:    1,
		MaxBytes:    10 * 1024 * 1024,
		MaxWait:     500 * time.Millisecond,
	})
}

func consume(
	ctx context.Context,
	reader *kafka.Reader,
	processor *GlobalMessageProcessor,
	watermarkCheckInterval time.Duration,
) (bool, error) {
	for {
		fetchContext, cancelFetch := context.WithTimeout(
			ctx,
			watermarkCheckInterval,
		)
		message, err := reader.FetchMessage(fetchContext)
		cancelFetch()
		if err != nil {
			if ctx.Err() != nil {
				return false, nil
			}
			if errors.Is(err, context.DeadlineExceeded) {
				if err := processor.aggregator.AdvanceWatermark(ctx); err != nil {
					return false, fmt.Errorf(
						"avanzamento watermark globale fallito: %w",
						err,
					)
				}
				continue
			}
			return false, fmt.Errorf("lettura Kafka globale fallita: %w", err)
		}

		completed, err := processAndCommitMessage(
			ctx,
			message,
			processor,
			func(message kafka.Message) error {
				return kafkautil.CommitMessage(reader, message, operationTimeout)
			},
		)
		if err != nil {
			return false, fmt.Errorf(
				"global partition=%d offset=%d: %w",
				message.Partition,
				message.Offset,
				err,
			)
		}
		if completed {
			return true, nil
		}
	}
}

func processAndCommitMessage(
	ctx context.Context,
	message kafka.Message,
	processor *GlobalMessageProcessor,
	commit KafkaMessageCommitter,
) (bool, error) {
	completed, err := processor.Process(ctx, message)
	if err != nil {
		return false, err
	}
	if err := commit(message); err != nil {
		return false, fmt.Errorf("commit Kafka globale fallito: %w", err)
	}
	return completed, nil
}

func watermarkAdvanceCheckInterval(edgeIdleTimeout time.Duration) time.Duration {
	if edgeIdleTimeout < maxWatermarkAdvanceCheckInterval {
		return edgeIdleTimeout
	}
	return maxWatermarkAdvanceCheckInterval
}
