package main

import (
	"context"
	"fmt"

	"continuum/internal/kafkautil"

	"github.com/segmentio/kafka-go"
)

func consume(ctx context.Context, reader *kafka.Reader, processor *CloudMessageProcessor) error {

	for {
		message, err := reader.FetchMessage(ctx)

		if err != nil {
			//se sto spegnendo il processo
			if ctx.Err() != nil {
				return nil
			}
			//altrimenti se ctx ancora valido
			return fmt.Errorf("lettura Kafka fallita: %w", err)
		}

		// Lo stato delle finestre e volatile. Gli esperimenti fissano il numero di Worker prima del replay e non lo cambiano durante il run.
		if err := processAndCommitMessage(message, reader, processor); err != nil {
			return fmt.Errorf("worker=%s partition=%d offset=%d: %w",
				processor.workerID,
				message.Partition,
				message.Offset,
				err,
			)
		}
	}
}

func processAndCommitMessage(message kafka.Message, reader *kafka.Reader, processor *CloudMessageProcessor) error {
	if err := processor.Process(message); err != nil {
		return err
	}

	return kafkautil.CommitMessage(reader, message, operationTimeout)
}
