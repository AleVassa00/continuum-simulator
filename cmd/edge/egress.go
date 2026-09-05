package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

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
		MaxAttempts:  1,
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
		case EdgeOutputEndOfReplay:
			if err := egress.publishEndOfReplay(record.EndOfReplay); err != nil {
				return err
			}
		default:
			return fmt.Errorf("tipo Edge output sconosciuto: %d", record.Kind)
		}
	}
	return nil
}

func (
	egress *KafkaEgress,
) publishAggregate(aggregate model.EdgeAggregate) error {
	payload, err := json.Marshal(
		aggregate,
	)
	if err != nil {
		return fmt.Errorf(
			"serializzazione EdgeAggregate fallita: %w",
			err,
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	err = egress.writer.WriteMessages(
		ctx,
		kafka.Message{
			Key: []byte(
				egress.edgeID,
			),

			Value: payload,

			Headers: []kafka.Header{
				{
					Key:   model.RecordTypeHeader,
					Value: []byte(model.RecordTypeEdgeAggregate),
				},
			},

			Time: aggregate.EmittedAt,
		},
	)
	if err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka aggregate_id=%s fallita: %w",
			aggregate.AggregateID,
			err,
		)
	}

	fmt.Printf(
		"KAFKA_PUBLISHED edge=%s aggregate_id=%s events=%d topic=%s\n",
		aggregate.EdgeID,
		aggregate.AggregateID,
		aggregate.Events,
		egress.writer.Topic,
	)
	egress.stats.aggregatesEmitted.Add(1)

	return nil
}

func (
	egress *KafkaEgress,
) publishEndOfReplay(record model.EndOfReplay) error {
	payload, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf(
			"serializzazione EndOfReplay Edge %s fallita: %w",
			egress.edgeID,
			err,
		)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := egress.writer.WriteMessages(
		ctx,
		kafka.Message{
			Key:   []byte(egress.edgeID),
			Value: payload,
			Headers: []kafka.Header{
				{
					Key:   model.RecordTypeHeader,
					Value: []byte(model.RecordTypeEndOfReplay),
				},
			},
			Time: record.EmittedAt,
		},
	); err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka EndOfReplay edge=%s fallita: %w",
			egress.edgeID,
			err,
		)
	}

	egress.stats.endOfReplayProcessed.Add(1)

	return nil
}
