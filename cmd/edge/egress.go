package main

import (
	"context"
	"fmt"
	"time"

	"continuum/internal/avrocodec"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

type EdgeOutputKind byte

const (
	EdgeOutputAggregate EdgeOutputKind = iota
)

type EdgeOutputRecord struct {
	Kind      EdgeOutputKind
	Aggregate model.EdgeAggregate
}
type KafkaEgress struct {
	edgeID string
	writer *kafka.Writer
	input  <-chan EdgeOutputRecord
	stats  *EdgeStats
}

func newKafkaWriter(broker string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    1,
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
		Async:        false,
	}
}

func (egress *KafkaEgress) Run() error {
	for record := range egress.input {
		switch record.Kind {
		case EdgeOutputAggregate:
			if err := egress.publishAggregate(record.Aggregate); err != nil {
				return err
			}

		default:
			return fmt.Errorf("tipo Edge output sconosciuto: %d", record.Kind)
		}
	}

	return nil
}

func (egress *KafkaEgress) publishAggregate(aggregate model.EdgeAggregate) error {
	payload, err := avrocodec.EncodeEdgeAggregate(aggregate)
	if err != nil {
		return fmt.Errorf("serializzazione EdgeAggregate fallita: %w", err)
	}

	headers := []kafka.Header{
		{
			Key:   model.RecordTypeHeader,
			Value: []byte(model.RecordTypeEdgeAggregate),
		},
	}

	message := kafka.Message{
		Key:     []byte(egress.edgeID),
		Value:   payload,
		Headers: headers,
		Time:    aggregate.EmittedAt,
	}
	if err := egress.writer.WriteMessages(context.Background(), message); err != nil {
		return fmt.Errorf("pubblicazione Kafka aggregate_id=%s fallita: %w",
			aggregate.AggregateID,
			err,
		)
	}

	fmt.Printf("KAFKA_PUBLISHED edge=%s aggregate_id=%s events=%d topic=%s\n",
		aggregate.EdgeID,
		aggregate.AggregateID,
		aggregate.Events,
		egress.writer.Topic,
	)

	egress.stats.aggregatesEmitted.Add(1)

	return nil
}
