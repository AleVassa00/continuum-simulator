package main

import (
	"context"
	"continuum/internal/avrocodec"
	"continuum/internal/globalaggregator"
	"continuum/internal/kafkautil"
	"continuum/internal/model"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

type GlobalMessageProcessor struct{ aggregator *globalaggregator.Aggregator }

func (p *GlobalMessageProcessor) Process(ctx context.Context, message kafka.Message) (bool, error) {
	kind, err := kafkautil.ParseRecordType(message.Headers)
	if err != nil {
		return false, err
	}
	partition, err := model.ParsePartitionKey(message.Key)
	if err != nil {
		return false, err
	}
	switch kind {
	case model.RecordTypeCloudPartitionAggregate:
		a, err := avrocodec.DecodeCloudPartitionAggregate(message.Value)
		if err != nil {
			return false, err
		}
		if a.SourcePartition != partition {
			return false, fmt.Errorf("partial source partition differs from Kafka key")
		}
		late, err := p.aggregator.AddWithResult(ctx, a)
		if err != nil {
			return false, err
		}
		if late != nil {
			fmt.Printf("GLOBAL_LATE_PARTIAL_DROPPED source_partition=%d aggregate_id=%s events=%d window_end=%s watermark=%s\n",
				late.Aggregate.SourcePartition,
				late.Aggregate.AggregateID,
				late.Aggregate.Events,
				late.Aggregate.WindowEnd.Format(time.RFC3339Nano),
				late.GlobalWatermark.Format(time.RFC3339Nano),
			)
		}
		return false, nil
	case model.RecordTypePartitionEndOfReplay:
		if len(message.Value) != 0 {
			return false, fmt.Errorf("partition EOS must have empty payload")
		}
		return p.aggregator.EndPartition(ctx, partition)
	default:
		return false, fmt.Errorf("unexpected Global record_type %q", kind)
	}
}
