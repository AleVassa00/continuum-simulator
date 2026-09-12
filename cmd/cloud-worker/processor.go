package main

import (
	"context"
	"continuum/internal/avrocodec"
	"continuum/internal/cloudworker"
	"continuum/internal/kafkautil"
	"continuum/internal/model"
	"fmt"
	"log/slog"

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
	case model.RecordTypeSourcePartitionEndOfInput:
		if len(message.Value) != 0 {
			return fmt.Errorf("source partition EOS must have empty payload")
		}
		partition, keyErr := model.ParsePartitionKey(message.Key)
		if keyErr != nil {
			return keyErr
		}
		if partition != message.Partition || string(message.Key) != model.PartitionKey(partition) {
			return fmt.Errorf("source partition EOS key does not match actual partition %d", message.Partition)
		}
		out, err = p.aggregator.EndPartitionInput(message.Partition)
	default:
		return fmt.Errorf("unexpected Cloud record_type %q", kind)
	}
	if err != nil {
		return err
	}
	if late := out.Late; late != nil {
		slog.WarnContext(ctx, "CLOUD_LATE_RECORD_DROPPED",
			"worker", p.workerID, "source_partition", out.SourcePartition,
			"offset", message.Offset, "aggregate_id", late.AggregateID, "edge_id", late.EdgeID,
			"events", late.Events,
			"cloud_window_end", late.CloudWindowEnd, "watermark", late.Watermark)
	}
	return p.publishOutput(ctx, out)
}
