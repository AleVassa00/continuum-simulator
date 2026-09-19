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
	EdgeOutputEndOfInput
)

type EdgeOutputRecord struct {
	Kind      EdgeOutputKind
	Aggregate model.EdgeAggregate
}

type fixedPartitionBalancer struct {
	partition int
}

func (balancer fixedPartitionBalancer) Balance(_ kafka.Message, _ ...int) int {
	return balancer.partition
}

type pendingEdgeAggregate struct {
	aggregate model.EdgeAggregate
	message   kafka.Message
}

type KafkaEgress struct {
	edgeID        string
	topic         string
	writeMessages func(context.Context, ...kafka.Message) error
	input         <-chan EdgeOutputRecord
	stats         *EdgeStats
	batchSize     int
	batchMaxWait  time.Duration
}

func newKafkaWriter(broker string, topic string, partition int, batchSize int) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     fixedPartitionBalancer{partition: partition},
		RequiredAcks: kafka.RequireAll,
		BatchSize:    batchSize,
		BatchTimeout: time.Millisecond,
		WriteTimeout: 5 * time.Second,
		ReadTimeout:  5 * time.Second,
		Async:        false,
	}
}

func (egress *KafkaEgress) Run() error {
	pending := make([]pendingEdgeAggregate, 0, egress.batchSize)
	var timer *time.Timer
	var timerChannel <-chan time.Time

	stopTimer := func() {
		if timer != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer = nil
		timerChannel = nil
	}
	flush := func() error {
		stopTimer()
		if len(pending) == 0 {
			return nil
		}
		if err := egress.publishAggregates(pending); err != nil {
			return err
		}
		pending = pending[:0]
		return nil
	}

	for {
		select {
		case record, open := <-egress.input:
			if !open {
				return flush()
			}
			switch record.Kind {
			case EdgeOutputAggregate:
				prepared, err := egress.prepareAggregate(record.Aggregate)
				if err != nil {
					return err
				}
				pending = append(pending, prepared)
				if len(pending) == 1 && egress.batchSize > 1 {
					timer = time.NewTimer(egress.batchMaxWait)
					timerChannel = timer.C
				}
				if len(pending) >= egress.batchSize {
					if err := flush(); err != nil {
						return err
					}
				}
			case EdgeOutputEndOfInput:
				if err := flush(); err != nil {
					return err
				}
				return egress.publishEndOfInput()
			default:
				return fmt.Errorf("tipo Edge output sconosciuto: %d", record.Kind)
			}
		case <-timerChannel:
			if err := flush(); err != nil {
				return err
			}
		}
	}
}

// L'EOS viene scritto dopo l'ultimo batch confermato.
func (egress *KafkaEgress) publishEndOfInput() error {
	message := kafka.Message{
		Key:     []byte(egress.edgeID),
		Headers: []kafka.Header{{Key: model.RecordTypeHeader, Value: []byte(model.RecordTypeEdgeEndOfInput)}},
		Time:    time.Now().UTC(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := egress.writeMessages(ctx, message); err != nil {
		return fmt.Errorf("pubblicazione Kafka EdgeEndOfInput edge=%s fallita: %w", egress.edgeID, err)
	}
	fmt.Printf("EDGE_PRODUCTION_COMPLETED edge=%s topic=%s\n", egress.edgeID, egress.topic)
	return nil
}

func (egress *KafkaEgress) prepareAggregate(aggregate model.EdgeAggregate) (pendingEdgeAggregate, error) {
	payload, err := avrocodec.EncodeEdgeAggregate(aggregate)
	if err != nil {
		return pendingEdgeAggregate{}, fmt.Errorf("serializzazione EdgeAggregate fallita: %w", err)
	}

	headers := []kafka.Header{
		{
			Key:   model.RecordTypeHeader,
			Value: []byte(model.RecordTypeEdgeAggregate),
		},
	}

	return pendingEdgeAggregate{
		aggregate: aggregate,
		message: kafka.Message{
			Key:     []byte(egress.edgeID),
			Value:   payload,
			Headers: headers,
			Time:    aggregate.EmittedAt,
		},
	}, nil
}

func (egress *KafkaEgress) publishAggregates(pending []pendingEdgeAggregate) error {
	messages := make([]kafka.Message, len(pending))
	for index := range pending {
		messages[index] = pending[index].message
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := egress.writeMessages(ctx, messages...); err != nil {
		return fmt.Errorf(
			"pubblicazione Kafka batch edge=%s size=%d first_aggregate_id=%s last_aggregate_id=%s fallita: %w",
			egress.edgeID,
			len(pending),
			pending[0].aggregate.AggregateID,
			pending[len(pending)-1].aggregate.AggregateID,
			err,
		)
	}

	for _, item := range pending {
		fmt.Printf("KAFKA_PUBLISHED edge=%s aggregate_id=%s events=%d topic=%s\n",
			item.aggregate.EdgeID,
			item.aggregate.AggregateID,
			item.aggregate.Events,
			egress.topic,
		)
		egress.stats.aggregatesEmitted.Add(1)
	}

	return nil
}
