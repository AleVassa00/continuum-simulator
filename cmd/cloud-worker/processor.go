package main

import (
	"context"
	"continuum/internal/avrocodec"
	"continuum/internal/cloudworker"
	"continuum/internal/kafkautil"
	"continuum/internal/model"
	"fmt"
	"time"

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
	if err := p.publishOutput(ctx, out); err != nil {
		return err
	}
	if out.Late != nil {
		late := out.Late
		fmt.Printf("CLOUD_LATE_RECORD_DROPPED worker=%s source_partition=%d offset=%d aggregate_id=%s edge_id=%s events=%d cloud_window_end=%s watermark=%s\n",
			p.workerID,
			message.Partition,
			message.Offset,
			late.Aggregate.AggregateID,
			late.Aggregate.EdgeID,
			late.Aggregate.Events,
			late.CloudWindowEnd.Format(time.RFC3339Nano),
			late.PartitionWatermark.Format(time.RFC3339Nano),
		)
	}
	return nil
}
