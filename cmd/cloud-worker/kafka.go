package main

import (
	"time"

	"github.com/segmentio/kafka-go"
)

const operationTimeout = 5 * time.Second

func newKafkaReader(
	broker string,
	topic string,
	groupID string,
) *kafka.Reader {
	return kafka.NewReader(
		kafka.ReaderConfig{
			Brokers: []string{
				broker,
			},
			Topic:       topic,
			GroupID:     groupID,
			StartOffset: kafka.FirstOffset,
			MinBytes:    1,
			MaxBytes:    10 * 1024 * 1024,
			MaxWait:     500 * time.Millisecond,
		},
	)
}

func newKafkaWriter(
	broker string,
	topic string,
) *kafka.Writer {
	return &kafka.Writer{
		Addr: kafka.TCP(
			broker,
		),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    1,
		WriteTimeout: operationTimeout,
		ReadTimeout:  operationTimeout,
		Async:        false,
	}
}
