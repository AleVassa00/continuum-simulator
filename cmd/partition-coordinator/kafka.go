package main

import (
	"context"
	"fmt"
	"time"

	"continuum/internal/model"
	"continuum/internal/partitioncompletion"
	"github.com/segmentio/kafka-go"
)

const kafkaTimeout = 5 * time.Second

// Never hash a partition ID as a producer key: the EOS must reach that exact
// source partition, after the acknowledged writes of all its actual producers.
type fixedPartition int

func (p fixedPartition) Balance(_ kafka.Message, _ ...int) int { return int(p) }

func sourceEndPublisher(broker, topic string) (partitioncompletion.Publisher, func()) {
	var writers [model.SourcePartitionCount]*kafka.Writer
	for p := range writers {
		writers[p] = &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, Balancer: fixedPartition(p), RequiredAcks: kafka.RequireAll, BatchSize: 1, Async: false, WriteTimeout: kafkaTimeout, ReadTimeout: kafkaTimeout}
	}
	return func(ctx context.Context, p int) error {
		if err := model.ValidateSourcePartition(p); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(ctx, kafkaTimeout)
		defer cancel()
		err := writers[p].WriteMessages(ctx, kafka.Message{Key: []byte(model.PartitionKey(p)), Headers: []kafka.Header{{Key: model.RecordTypeHeader, Value: []byte(model.RecordTypeSourcePartitionEndOfInput)}}})
		if err == nil {
			fmt.Printf("SOURCE_PARTITION_INPUT_ENDED partition=%d\n", p)
		}
		return err
	}, func() {
		for _, writer := range writers {
			writer.Close()
		}
	}
}

func validateSourceTopic(ctx context.Context, broker, topic string) error {
	ctx, cancel := context.WithTimeout(ctx, kafkaTimeout)
	defer cancel()
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return err
	}
	if len(partitions) != model.SourcePartitionCount {
		return fmt.Errorf("source topic must have exactly %d partitions", model.SourcePartitionCount)
	}
	seen := make(map[int]bool)
	for _, p := range partitions {
		if p.Topic != topic || model.ValidateSourcePartition(p.ID) != nil || seen[p.ID] {
			return fmt.Errorf("unexpected source partition metadata")
		}
		seen[p.ID] = true
	}
	return nil
}
