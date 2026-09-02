package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"continuum/internal/cloudworker"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

const operationTimeout = 5 * time.Second

func newKafkaReader(
	broker string,
	topic string,
	groupID string,
) *kafka.Reader {
	return kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{
				broker,
			},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
			MinBytes:    1,
			MaxBytes:    10 * 1024 * 1024,
			MaxWait:     500 * time.Millisecond,
		},
	)
}

func consume(
	ctx context.Context,
	reader *kafka.Reader,
	writer *kafka.Writer,
	aggregator *cloudworker.WindowAggregator,
	workerID string,
) error {
	processor := &CloudMessageProcessor{
		aggregator:  aggregator,
		outputTopic: writer.Topic,
		workerID:    workerID,
		publishMessage: func(
			ctx context.Context,
			message kafka.Message,
		) error {
			return writer.WriteMessages(ctx, message)
		},
		now:        time.Now,
		endedEdges: make(map[string]bool),
	}

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
				return commitMessage(reader, message)
			},
		); err != nil {
			return fmt.Errorf(
				"worker=%s partition=%d offset=%d: %w",
				workerID,
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

func kafkaRecordType(
	headers []kafka.Header,
) (string, error) {
	var recordType string
	found := false

	for _, header := range headers {
		if header.Key != model.RecordTypeHeader {
			continue
		}

		if found {
			return "", fmt.Errorf(
				"header Kafka %q duplicato",
				model.RecordTypeHeader,
			)
		}

		recordType = strings.TrimSpace(string(header.Value))
		found = true
	}

	if !found || recordType == "" {
		return "", fmt.Errorf(
			"header Kafka %q mancante o vuoto",
			model.RecordTypeHeader,
		)
	}

	return recordType, nil
}

func decodeEndOfReplay(
	payload []byte,
) (model.EndOfReplay, error) {
	var record model.EndOfReplay
	if err := json.Unmarshal(payload, &record); err != nil {
		return model.EndOfReplay{},
			fmt.Errorf(
				"EndOfReplay JSON non valido: %w",
				err,
			)
	}

	if err := model.ValidateEndOfReplay(record); err != nil {
		return model.EndOfReplay{}, err
	}

	return record, nil
}

func decodeEdgeAggregate(
	payload []byte,
) (model.EdgeAggregate, error) {
	var aggregate model.EdgeAggregate

	if err := json.Unmarshal(
		payload,
		&aggregate,
	); err != nil {
		return model.EdgeAggregate{},
			fmt.Errorf(
				"EdgeAggregate JSON non valido: %w",
				err,
			)
	}

	if err := cloudworker.ValidateEdgeAggregate(
		aggregate,
	); err != nil {
		return model.EdgeAggregate{}, err
	}

	return aggregate, nil
}

func publishCloudEdgeAggregate(
	writer *kafka.Writer,
	aggregate model.CloudEdgeAggregate,
) error {
	message, err := cloudEdgeAggregateMessage(aggregate)
	if err != nil {
		return err
	}

	if err := writeKafkaMessage(
		func(
			ctx context.Context,
			message kafka.Message,
		) error {
			return writer.WriteMessages(ctx, message)
		},
		message,
	); err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka aggregate_id=%s topic=%s fallita: %w",
			aggregate.AggregateID,
			writer.Topic,
			err,
		)
	}

	return nil
}

func cloudEdgeAggregateMessage(
	aggregate model.CloudEdgeAggregate,
) (kafka.Message, error) {
	if err := cloudworker.ValidateCloudEdgeAggregate(
		aggregate,
	); err != nil {
		return kafka.Message{}, fmt.Errorf(
			"CloudEdgeAggregate %q non valido: %w",
			aggregate.AggregateID,
			err,
		)
	}

	payload, err := json.Marshal(aggregate)
	if err != nil {
		return kafka.Message{}, fmt.Errorf(
			"serializzazione CloudEdgeAggregate %q fallita: %w",
			aggregate.AggregateID,
			err,
		)
	}

	return kafka.Message{
		Key:   []byte(aggregate.EdgeID),
		Value: payload,
		Time:  aggregate.EmittedAt,
		Headers: []kafka.Header{
			{
				Key:   model.RecordTypeHeader,
				Value: []byte(model.RecordTypeCloudEdgeAggregate),
			},
		},
	}, nil
}

func endOfReplayMessage(
	record model.EndOfReplay,
) (kafka.Message, error) {
	if err := model.ValidateEndOfReplay(record); err != nil {
		return kafka.Message{}, err
	}

	payload, err := json.Marshal(record)
	if err != nil {
		return kafka.Message{}, fmt.Errorf(
			"serializzazione EndOfReplay edge=%s fallita: %w",
			record.EdgeID,
			err,
		)
	}

	return kafka.Message{
		Key:   []byte(record.EdgeID),
		Value: payload,
		Time:  record.EmittedAt,
		Headers: []kafka.Header{
			{
				Key:   model.RecordTypeHeader,
				Value: []byte(model.RecordTypeEndOfReplay),
			},
		},
	}, nil
}

func writeKafkaMessage(
	publish KafkaMessagePublisher,
	message kafka.Message,
) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		operationTimeout,
	)
	defer cancel()

	return publish(ctx, message)
}

func flushWindows(
	writer *kafka.Writer,
	aggregator *cloudworker.WindowAggregator,
	workerID string,
) error {
	for _, output := range aggregator.Flush() {
		if err := publishCloudEdgeAggregate(
			writer,
			output,
		); err != nil {
			return fmt.Errorf(
				"flush finestra edge=%s fallito: %w",
				output.EdgeID,
				err,
			)
		}

		logPublishedWindow(
			workerID,
			writer.Topic,
			output,
			true,
		)
	}

	return nil
}

func commitMessage(
	reader *kafka.Reader,
	message kafka.Message,
) error {
	ctx, cancel := context.WithTimeout(
		context.Background(),
		operationTimeout,
	)
	defer cancel()

	return reader.CommitMessages(
		ctx,
		message,
	)
}

func logPublishedWindow(
	workerID string,
	topic string,
	aggregate model.CloudEdgeAggregate,
	partial bool,
) {
	fmt.Printf(
		"CLOUD_WINDOW_PUBLISHED worker=%s edge=%s aggregate_id=%s window=[%s,%s) inputs=%d events=%d partial=%t topic=%s\n",
		workerID,
		aggregate.EdgeID,
		aggregate.AggregateID,
		aggregate.WindowStart.Format(time.RFC3339),
		aggregate.WindowEnd.Format(time.RFC3339),
		aggregate.InputAggregates,
		aggregate.Events,
		partial,
		topic,
	)
}

func newKafkaWriter(
	broker string,
	topic string,
) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(
			broker,
		),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		MaxAttempts:  1,
		BatchSize:    1,
		WriteTimeout: operationTimeout,
		ReadTimeout:  operationTimeout,
		Async:        false,
	}
}
