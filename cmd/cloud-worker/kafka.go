package main

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

const operationTimeout = 5 * time.Second

func newKafkaWriter(broker string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    1,
		WriteTimeout: operationTimeout,
		ReadTimeout:  operationTimeout,
		Async:        false,
	}
}

func validateKafkaTopology(ctx context.Context, broker, topic string, count int) error {

	ctx, cancel := context.WithTimeout(ctx, operationTimeout)
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
	if len(partitions) != count {
		return fmt.Errorf("topic %s has %d partitions, requires %d; repartitioning during a run is unsupported", topic, len(partitions), count)
	}
	seen := make(map[int]bool)
	for _, p := range partitions {
		if p.Topic != topic || p.ID < 0 || p.ID >= count || seen[p.ID] {
			return fmt.Errorf("unexpected partition metadata for %s", topic)
		}
		seen[p.ID] = true
	}
	return nil
}
