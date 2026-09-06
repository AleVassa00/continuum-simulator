package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"continuum/internal/cloudworker"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

type KafkaMessagePublisher func(
	context.Context,
	kafka.Message,
) error

func (
	processor *CloudMessageProcessor,
) publishCloudAggregate(
	aggregate model.CloudEdgeAggregate,
	partial bool,
) error {
	message, err := cloudEdgeAggregateMessage(aggregate)
	if err != nil {
		return err
	}

	if err := writeKafkaMessage(
		processor.publishMessage,
		message,
	); err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka aggregate_id=%s topic=%s fallita: %w",
			aggregate.AggregateID,
			processor.outputTopic,
			err,
		)
	}

	logPublishedWindow(
		processor.workerID,
		processor.outputTopic,
		aggregate,
		partial,
	)

	return nil
}

func (
	processor *CloudMessageProcessor,
) publishEndOfReplay(
	edgeID string,
) error {
	message := kafka.Message{
		Key:   []byte(edgeID),
		Value: []byte{},
		Time:  time.Now().UTC(),
		Headers: []kafka.Header{
			{
				Key:   model.RecordTypeHeader,
				Value: []byte(model.RecordTypeEndOfReplay),
			},
		},
	}

	if err := writeKafkaMessage(
		processor.publishMessage,
		message,
	); err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka EndOfReplay edge=%s topic=%s fallita: %w",
			edgeID,
			processor.outputTopic,
			err,
		)
	}

	return nil
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
