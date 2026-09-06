package main

import (
	"context"
	"fmt"

	"continuum/internal/kafkautil"

	"github.com/segmentio/kafka-go"
)

func consume(
	ctx context.Context,
	reader *kafka.Reader,
	processor *CloudMessageProcessor,
) error {

	for {
		message, err := reader.FetchMessage(
			ctx,
		)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf(
				"lettura Kafka fallita: %w",
				err,
			)
		}

		// Lo stato delle finestre e volatile. Gli esperimenti fissano il
		// numero di Worker prima del replay e non lo cambiano durante il run.
		if err := processAndCommitMessage(
			message,
			processor,
			func(message kafka.Message) error {
				return kafkautil.CommitMessage(reader, message, operationTimeout)
			},
		); err != nil {
			return fmt.Errorf(
				"worker=%s partition=%d offset=%d: %w",
				processor.workerID,
				message.Partition,
				message.Offset,
				err,
			)
		}
	}
}

func processAndCommitMessage(
	message kafka.Message,
	processor *CloudMessageProcessor,
	commit func(kafka.Message) error,
) error {
	if err := processor.Process(message); err != nil {
		return err
	}

	if err := commit(message); err != nil {
		return fmt.Errorf("commit Kafka fallito: %w", err)
	}

	return nil
}
