package main

import (
	"context"
	"continuum/internal/avrocodec"
	"continuum/internal/cloudworker"
	"continuum/internal/kafkautil"
	"continuum/internal/model"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type CloudMessageProcessor struct {
	aggregator     *cloudworker.PartitionAggregator
	outputTopic    string
	workerID       string
	publishMessage KafkaMessagePublisher
}

func (p *CloudMessageProcessor) Initialize(ctx context.Context) error {
	return p.publishOutput(ctx, p.aggregator.Initialize())
}

func (p *CloudMessageProcessor) Process(ctx context.Context, message kafka.Message) error {
	kind, err := kafkautil.ParseRecordType(message.Headers)
	if err != nil {
		return err
	}
	var out cloudworker.Output
	switch kind {
	case model.RecordTypeEdgeAggregate:
		input, decodeErr := avrocodec.DecodeEdgeAggregate(message.Value)
		if decodeErr != nil {
			return decodeErr
		}
		if string(message.Key) != input.EdgeID {
			return fmt.Errorf("Kafka key does not match EdgeAggregate edge_id")
		}
		out, err = p.aggregator.Add(message.Partition, input)
	case model.RecordTypeEndOfReplay:
		if len(message.Value) != 0 {
			return fmt.Errorf("Edge EOS must have empty payload")
		}
		out, err = p.aggregator.EndEdge(message.Partition, string(message.Key))
	default:
		return fmt.Errorf("unexpected Cloud record_type %q", kind)
	}
	if err != nil {
		return err
	}
	return p.publishOutput(ctx, out)
}
