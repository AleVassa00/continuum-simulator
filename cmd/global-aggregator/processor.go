package main

import (
	"context"
	"continuum/internal/avrocodec"
	"continuum/internal/globalaggregator"
	"continuum/internal/kafkautil"
	"continuum/internal/model"
	"fmt"
	"github.com/segmentio/kafka-go"
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
		return false, p.aggregator.Add(ctx, a)
	case model.RecordTypePartitionProgress:
		progress, err := avrocodec.DecodePartitionProgress(message.Value)
		if err != nil {
			return false, err
		}
		if progress.SourcePartition != partition {
			return false, fmt.Errorf("progress source partition differs from Kafka key")
		}
		return false, p.aggregator.Progress(ctx, progress)
	case model.RecordTypePartitionEndOfReplay:
		if len(message.Value) != 0 {
			return false, fmt.Errorf("partition EOS must have empty payload")
		}
		return p.aggregator.EndPartition(ctx, partition)
	default:
		return false, fmt.Errorf("unexpected Global record_type %q", kind)
	}
}
