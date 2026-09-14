package main

import (
	"context"
	"continuum/internal/avrocodec"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func TestOffsetCommitBatch(t *testing.T) {
	var calls []map[string]map[int]int64
	b, err := newOffsetCommitBatch("input", 3, func(offsets map[string]map[int]int64) error { calls = append(calls, offsets); return nil })
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range []kafka.Message{{Partition: 0, Offset: 4}, {Partition: 1, Offset: 7}} {
		if err := b.add(m); err != nil {
			t.Fatal(err)
		}
	}
	if len(calls) != 0 {
		t.Fatal("early commit")
	}
	if err := b.add(kafka.Message{Partition: 0, Offset: 3}); err != nil {
		t.Fatal(err)
	}
	want := map[string]map[int]int64{"input": {0: 5, 1: 8}}
	if len(calls) != 1 || !reflect.DeepEqual(calls[0], want) {
		t.Fatalf("wrong maxima: %v", calls)
	}
	if err := b.add(kafka.Message{Partition: 1, Offset: 8}); err != nil {
		t.Fatal(err)
	}
	if err := b.addAndFlush(kafka.Message{Partition: 0, Offset: 9}); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 2 || !reflect.DeepEqual(calls[1], map[string]map[int]int64{"input": {0: 10, 1: 9}}) {
		t.Fatalf("EOS flush: %v", calls)
	}
	if err := b.flush(); err != nil || len(calls) != 2 {
		t.Fatal("empty flush committed")
	}
}

func TestOffsetCommitFailureAndProcessOrdering(t *testing.T) {
	failure := errors.New("broker unavailable")
	calls := 0
	b, _ := newOffsetCommitBatch("input", 1, func(map[string]map[int]int64) error { calls++; return failure })
	err := b.add(kafka.Message{Partition: 0, Offset: 10})
	if !errors.Is(err, errOffsetCommit) || !errors.Is(err, failure) || b.pending[0] != 11 || b.processed != 1 {
		t.Fatalf("failed commit lost state: %v %+v", err, b)
	}
	p, _, partition := processorFixture(t, "edge-0")
	input := edgeInput(t, "edge-0", 0, 1)
	input.Partition = partition
	aggregate, err := avrocodec.DecodeEdgeAggregate(input.Value)
	if err != nil {
		t.Fatal(err)
	}
	aggregate.CompleteThrough = testEpoch.Add(15 * time.Minute)
	input.Value, err = avrocodec.EncodeEdgeAggregate(aggregate)
	if err != nil {
		t.Fatal(err)
	}
	p.publishMessage = func(context.Context, kafka.Message) error { return failure }
	b, _ = newOffsetCommitBatch("input", 1, func(map[string]map[int]int64) error { t.Fatal("commit before successful publish"); return nil })
	if err := processAndCommitMessage(context.Background(), input, p, b.add); err == nil || len(b.pending) != 0 {
		t.Fatal("failed input entered batch")
	}
	if calls != 1 {
		t.Fatal("unexpected retry")
	}
}

func TestCommitBatchConfiguration(t *testing.T) {
	t.Setenv("KAFKA_BROKER", "unused:9092")
	t.Setenv("CLOUD_EDGE_PARTITIONS", "edge-0:0")
	for _, tc := range []struct {
		value string
		want  int
	}{{"", 1}, {"1", 1}, {"64", 64}, {"0", 0}, {"-1", 0}, {"1.5", 0}, {"invalid", 0}} {
		t.Setenv("CLOUD_CONSUMER_COMMIT_BATCH_SIZE", tc.value)
		cfg, err := loadCloudWorkerConfig()
		if tc.want == 0 {
			if err == nil {
				t.Fatalf("accepted %q", tc.value)
			}
			continue
		}
		if err != nil || cfg.ConsumerCommitBatchSize != tc.want {
			t.Fatalf("%q: %+v %v", tc.value, cfg, err)
		}
	}
}
