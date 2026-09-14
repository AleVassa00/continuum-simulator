package main

import (
	"context"
	"testing"
	"time"

	"continuum/internal/avrocodec"
	"continuum/internal/model"

	"github.com/segmentio/kafka-go"
)

func TestKafkaEgressBatchesAndFlushesBeforeEndOfInput(t *testing.T) {
	input := make(chan EdgeOutputRecord, 4)
	start := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for index := 0; index < 3; index++ {
		windowStart := start.Add(time.Duration(index) * 5 * time.Minute)
		input <- EdgeOutputRecord{Kind: EdgeOutputAggregate, Aggregate: model.EdgeAggregate{
			AggregateID:     "edge-0:" + windowStart.Format(time.RFC3339Nano),
			EdgeID:          "edge-0",
			WindowStart:     windowStart,
			WindowEnd:       windowStart.Add(5 * time.Minute),
			CompleteThrough: windowStart.Add(5 * time.Minute),
			Events:          1,
			EmittedAt:       start,
		}}
	}
	input <- EdgeOutputRecord{Kind: EdgeOutputEndOfInput}
	close(input)

	var calls [][]kafka.Message
	stats := &EdgeStats{}
	egress := &KafkaEgress{
		edgeID: "edge-0",
		topic:  "edge-aggregates",
		writeMessages: func(_ context.Context, messages ...kafka.Message) error {
			calls = append(calls, append([]kafka.Message(nil), messages...))
			return nil
		},
		input:        input,
		stats:        stats,
		batchSize:    2,
		batchMaxWait: time.Hour,
	}
	if err := egress.Run(); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 3 || len(calls[0]) != 2 || len(calls[1]) != 1 || len(calls[2]) != 1 {
		t.Fatalf("scritture Kafka inattese: %v", calls)
	}
	for index, message := range append(calls[0], calls[1]...) {
		aggregate, err := avrocodec.DecodeEdgeAggregate(message.Value)
		if err != nil || !aggregate.WindowStart.Equal(start.Add(time.Duration(index)*5*time.Minute)) {
			t.Fatalf("aggregato %d non valido: %+v %v", index, aggregate, err)
		}
	}
	if string(calls[2][0].Headers[0].Value) != model.RecordTypeEdgeEndOfInput {
		t.Fatal("EdgeEndOfInput non pubblicato dopo il flush dati")
	}
	if stats.aggregatesEmitted.Load() != 3 {
		t.Fatalf("aggregati confermati=%d", stats.aggregatesEmitted.Load())
	}
}
