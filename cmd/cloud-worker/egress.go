package main

import (
	"context"
	"continuum/internal/avrocodec"
	"continuum/internal/cloudworker"
	"continuum/internal/model"
	"fmt"
	"github.com/segmentio/kafka-go"
	"time"
)

type KafkaMessagePublisher func(context.Context, kafka.Message) error

// Data precedes progress/EOS synchronously on the SAME source-partition key.
// A failed publish invalidates the run and prevents committing the input.
func (p *CloudMessageProcessor) publishOutput(ctx context.Context, out cloudworker.Output) error {
	for _, a := range out.Aggregates {
		if err := model.ValidateCloudPartitionAggregate(a); err != nil {
			return err
		}
		payload, err := avrocodec.EncodeCloudPartitionAggregate(a)
		if err != nil {
			return err
		}
		if err = p.publishRecord(ctx, out.SourcePartition, model.RecordTypeCloudPartitionAggregate, payload); err != nil {
			return err
		}
		fmt.Printf("CLOUD_WINDOW_PUBLISHED worker=%s source_partition=%d aggregate_id=%s inputs=%d events=%d topic=%s\n", p.workerID, a.SourcePartition, a.AggregateID, a.InputAggregates, a.Events, p.outputTopic)
	}
	if out.Progress != nil {
		payload, err := avrocodec.EncodePartitionProgress(*out.Progress)
		if err != nil {
			return err
		}
		if err = p.publishRecord(ctx, out.SourcePartition, model.RecordTypePartitionProgress, payload); err != nil {
			return err
		}
	}
	if out.End {
		return p.publishRecord(ctx, out.SourcePartition, model.RecordTypePartitionEndOfReplay, nil)
	}
	return nil
}
func (p *CloudMessageProcessor) publishRecord(ctx context.Context, partition int, kind string, payload []byte) error {
	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
	defer cancel()
	return p.publishMessage(ctx, kafka.Message{Key: []byte(model.PartitionKey(partition)), Value: payload, Time: time.Now().UTC(),
		Headers: []kafka.Header{{Key: model.RecordTypeHeader, Value: []byte(kind)}}})
}
