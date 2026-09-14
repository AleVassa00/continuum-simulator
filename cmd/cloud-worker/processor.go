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
	case model.RecordTypeEdgeWatermark:
		watermark, decodeErr := avrocodec.DecodeEdgeWatermark(message.Value)
		if decodeErr != nil {
			return decodeErr
		}
		if string(message.Key) != watermark.EdgeID {
			return fmt.Errorf("Kafka key does not match EdgeWatermark edge_id")
		}
		out, err = p.aggregator.AdvanceEdgeWatermark(message.Partition, watermark)
	case model.RecordTypeEdgeEndOfInput:
		if len(message.Value) != 0 {
			return fmt.Errorf("Edge end-of-input must have empty payload")
		}
		edgeID := string(message.Key)
		if edgeID == "" {
			return fmt.Errorf("Edge end-of-input must have an Edge key")
		}
		out, err = p.aggregator.EndEdge(message.Partition, edgeID)
	default:
		return fmt.Errorf("unexpected Cloud record_type %q", kind)
	}
	if err != nil {
		return err
	}
	return p.publishOutput(ctx, out)
}
