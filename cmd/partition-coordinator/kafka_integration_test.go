package main

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"continuum/internal/kafkautil"
	"continuum/internal/model"
	"continuum/internal/partitioncompletion"
	"github.com/segmentio/kafka-go"
)

func TestKafkaCompletionFollowsAcknowledgedPartitionData(t *testing.T) {
	broker := os.Getenv("KAFKA_INTEGRATION_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_INTEGRATION_BROKER to a disposable broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	topic := fmt.Sprintf("completion-test-%d", time.Now().UnixNano())
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		t.Fatal(err)
	}
	err = conn.CreateTopics(kafka.TopicConfig{Topic: topic, NumPartitions: model.SourcePartitionCount, ReplicationFactor: 1})
	conn.Close()
	if err != nil {
		t.Fatal(err)
	}
	// CreateTopics returns before metadata is necessarily visible to producers.
	// Match the runner's readiness gate before starting any EOS publishers.
	for {
		if err := validateSourceTopic(ctx, broker, topic); err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("source topic did not become ready:", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
	var p0, p1 []string
	for n := 0; len(p0) < 2 || len(p1) < 1; n++ {
		id := fmt.Sprintf("edge-%d", n)
		p := kafkautil.PartitionForEdge(id)
		if p == 0 && len(p0) < 2 {
			p0 = append(p0, id)
		}
		if p == 1 && len(p1) < 1 {
			p1 = append(p1, id)
		}
	}
	ids := append(append([]string{}, p0...), p1...)
	publish, closeWriters := sourceEndPublisher(broker, topic)
	defer closeWriters()
	c, err := partitioncompletion.New(ctx, ids, publish)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { cancel(); c.Wait() }()
	writer := &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, Balancer: &kafka.Hash{}, RequiredAcks: kafka.RequireAll, BatchSize: 1, Async: false}
	defer writer.Close()
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	produce := func(id string) {
		// Same acknowledgement contract as the Edge egress; no orchestrator data relay.
		err := writer.WriteMessages(ctx, kafka.Message{Key: []byte(id), Value: []byte(id), Headers: []kafka.Header{{Key: model.RecordTypeHeader, Value: []byte(model.RecordTypeEdgeAggregate)}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Complete(id); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range p0 {
		produce(id)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	defer reader.Close()
	for i := 0; i < 3; i++ {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		kind, err := kafkautil.ParseRecordType(message.Headers)
		if err != nil {
			t.Fatal(err)
		}
		if i < 2 {
			if kind != model.RecordTypeEdgeAggregate || string(message.Key) != p0[i] {
				t.Fatalf("data order changed: %+v", message)
			}
		} else {
			if kind != model.RecordTypeSourcePartitionEndOfInput || string(message.Key) != "0" || len(message.Value) != 0 {
				t.Fatalf("bad source EOS: %+v", message)
			}
		}
	}
	// P0 EOS was already readable while P1 had not produced or completed anything.
	if done, failed := c.Status(); done || failed {
		t.Fatalf("premature/failing coordinator state %v %v", done, failed)
	}
	produce(p1[0])
	c.Wait()
	if done, failed := c.Status(); !done || failed {
		t.Fatalf("incomplete terminal dispatch %v %v", done, failed)
	}
}
