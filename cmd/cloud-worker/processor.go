package main

import (
	"context"
	"fmt"
	"time"

	"continuum/internal/cloudworker"
	"continuum/internal/kafkautil"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

type KafkaMessagePublisher func(
	context.Context,
	kafka.Message,
) error

type CloudMessageProcessor struct {
	aggregator     *cloudworker.WindowAggregator
	outputTopic    string
	workerID       string
	publishMessage KafkaMessagePublisher
	now            func() time.Time
	endedEdges     map[string]bool
}

func (
	processor *CloudMessageProcessor,
) Process(
	message kafka.Message,
) error {
	recordType, err := kafkautil.ParseRecordType(message.Headers)
	if err != nil {
		return err
	}

	switch recordType {
	case model.RecordTypeEdgeAggregate:
		return processor.processEdgeAggregate(message)

	case model.RecordTypeEndOfReplay:
		return processor.processEndOfReplay(message)

	default:
		return fmt.Errorf(
			"record_type Kafka sconosciuto %q",
			recordType,
		)
	}
}

func (
	processor *CloudMessageProcessor,
) processEdgeAggregate(
	message kafka.Message,
) error {
	input, err := decodeEdgeAggregate(message.Value)
	if err != nil {
		return err
	}
	if string(message.Key) != input.EdgeID {
		return fmt.Errorf(
			"EdgeAggregate key Kafka=%q non coerente con edge_id=%q",
			message.Key,
			input.EdgeID,
		)
	}

	if processor.endedEdges == nil {
		processor.endedEdges = make(map[string]bool)
	}
	if processor.endedEdges[input.EdgeID] {
		return fmt.Errorf(
			"violazione invariant terminale: EdgeAggregate %s ricevuto dopo EndOfReplay edge=%s",
			input.AggregateID,
			input.EdgeID,
		)
	}

	output, err := processor.aggregator.Add(input)
	if err != nil {
		return fmt.Errorf(
			"elaborazione aggregate_id=%s fallita: %w",
			input.AggregateID,
			err,
		)
	}

	if output == nil {
		return nil
	}

	return processor.publishCloudAggregate(*output, false)
}

func (
	processor *CloudMessageProcessor,
) processEndOfReplay(
	message kafka.Message,
) error {
	record, err := kafkautil.DecodeEndOfReplay(message.Value)
	if err != nil {
		return err
	}

	if string(message.Key) != record.EdgeID {
		return fmt.Errorf(
			"EndOfReplay key Kafka=%q non coerente con edge_id=%q",
			message.Key,
			record.EdgeID,
		)
	}

	if processor.endedEdges == nil {
		processor.endedEdges = make(map[string]bool)
	}
	if processor.endedEdges[record.EdgeID] {
		fmt.Printf(
			"%s: EndOfReplay duplicato edge=%s ignorato\n",
			processor.workerID,
			record.EdgeID,
		)
		return nil
	}

	if output, found := processor.aggregator.FlushEdge(record.EdgeID); found {
		if err := processor.publishCloudAggregate(*output, true); err != nil {
			return fmt.Errorf(
				"flush finale Cloud edge=%s fallito: %w",
				record.EdgeID,
				err,
			)
		}
	}

	forwarded := record
	forwarded.EmittedAt = processor.now().UTC()
	if err := processor.publishEndOfReplay(forwarded); err != nil {
		return err
	}

	processor.endedEdges[record.EdgeID] = true

	return nil
}

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
	record model.EndOfReplay,
) error {
	message, err := endOfReplayMessage(record)
	if err != nil {
		return err
	}

	if err := writeKafkaMessage(
		processor.publishMessage,
		message,
	); err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka EndOfReplay edge=%s topic=%s fallita: %w",
			record.EdgeID,
			processor.outputTopic,
			err,
		)
	}

	return nil
}
