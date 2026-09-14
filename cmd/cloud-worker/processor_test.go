package main

import (
	"context"
	"continuum/internal/avrocodec"
	"continuum/internal/cloudworker"
	"continuum/internal/kafkautil"
	"continuum/internal/model"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
)

func processorFixture(t *testing.T, edgeIDs ...string) (*CloudMessageProcessor, *[]kafka.Message, int) {
	t.Helper()
	partition := 0
	a, err := cloudworker.NewPartitionAggregator(partition, model.DefaultSourcePartitionCount, edgeIDs, cloudworker.DefaultWindowSize, cloudworker.DefaultMaxEdgeWatermarkSkew)
	if err != nil {
		t.Fatal(err)
	}
	var wire []kafka.Message
	p := &CloudMessageProcessor{aggregator: a, workerID: "test-worker", publishMessage: func(_ context.Context, m kafka.Message) error { wire = append(wire, m); return nil }}
	return p, &wire, partition
}

func TestEmbeddedEdgeProgressFinalizesBeforePublishingProgress(t *testing.T) {
	p, wire, partition := processorFixture(t, "edge-0")
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
	if err := p.Process(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if len(*wire) != 2 {
		t.Fatalf("published %d records", len(*wire))
	}
	first, _ := kafkautil.ParseRecordType((*wire)[0].Headers)
	second, _ := kafkautil.ParseRecordType((*wire)[1].Headers)
	if first != model.RecordTypeCloudPartitionAggregate || second != model.RecordTypePartitionProgress {
		t.Fatalf("publication order: %s, %s", first, second)
	}
	progress, err := avrocodec.DecodePartitionProgress((*wire)[1].Value)
	if err != nil || progress.SourcePartition != partition || !progress.CompleteThrough.Equal(testEpoch.Add(15*time.Minute)) {
		t.Fatalf("wrong progress: %+v %v", progress, err)
	}
}

func TestEdgeEndValidationAndPartitionCompletion(t *testing.T) {
	edgeIDs := samePartitionEdgeIDs(t, 2)
	p, wire, partition := processorFixture(t, edgeIDs...)
	for _, id := range edgeIDs {
		input := edgeInput(t, id, 0, 1)
		input.Partition = partition
		if err := p.Process(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}
	*wire = nil
	firstEnd := edgeEndInput(edgeIDs[0])
	firstEnd.Partition = partition
	if err := p.Process(context.Background(), firstEnd); err != nil || len(*wire) != 0 {
		t.Fatalf("one Edge ended the partition: %v", err)
	}
	secondEnd := edgeEndInput(edgeIDs[1])
	secondEnd.Partition = partition
	if err := p.Process(context.Background(), secondEnd); err != nil {
		t.Fatal(err)
	}
	if len(*wire) != 2 {
		t.Fatalf("terminal output count=%d", len(*wire))
	}
	kind, _ := kafkautil.ParseRecordType((*wire)[1].Headers)
	if kind != model.RecordTypePartitionEndOfReplay || len((*wire)[1].Value) != 0 {
		t.Fatalf("bad partition terminal output: %+v", (*wire)[1])
	}
	if err := p.Process(context.Background(), secondEnd); err != nil || len(*wire) != 2 {
		t.Fatal("duplicate terminal marker changed output")
	}
}

func samePartitionEdgeIDs(t *testing.T, count int) []string {
	t.Helper()
	ids := make([]string, count)
	for index := range ids {
		ids[index] = fmt.Sprintf("edge-%d", index)
	}
	return ids
}

func TestEdgeControlWireValidation(t *testing.T) {
	p, _, partition := processorFixture(t, "edge-0")
	for name, message := range map[string]kafka.Message{
		"aggregate-key": func() kafka.Message {
			m := edgeInput(t, "edge-0", 0, 1)
			m.Partition, m.Key = partition, []byte("wrong")
			return m
		}(),
		"aggregate-payload": func() kafka.Message {
			m := edgeInput(t, "edge-0", 0, 1)
			m.Partition, m.Value = partition, []byte("bad")
			return m
		}(),
		"end-payload": func() kafka.Message {
			m := edgeEndInput("edge-0")
			m.Partition, m.Value = partition, []byte("bad")
			return m
		}(),
		"unknown-edge": func() kafka.Message {
			m := edgeEndInput("unknown")
			m.Partition = partition
			return m
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			if err := p.Process(context.Background(), message); err == nil {
				t.Fatal("invalid control accepted")
			}
		})
	}
}

func TestCloudConfigurationRequiresTopology(t *testing.T) {
	t.Setenv("KAFKA_BROKER", "unused:9092")
	t.Setenv("SOURCE_PARTITION_COUNT", "6")
	t.Setenv("CLOUD_EDGE_PARTITIONS", "")
	if _, err := loadCloudWorkerConfig(); err == nil {
		t.Fatal("missing Edge topology accepted")
	}
	t.Setenv("CLOUD_EDGE_PARTITIONS", "edge-0:0,edge-1:1")
	t.Setenv("CLOUD_WINDOW_SIZE", "30m")
	t.Setenv("CLOUD_MAX_EDGE_WATERMARK_SKEW", "45m")
	cfg, err := loadCloudWorkerConfig()
	if err != nil || cfg.WindowSize != 30*time.Minute || cfg.MaxEdgeWatermarkSkew != 45*time.Minute || len(cfg.Membership) != 6 {
		t.Fatalf("config: %+v %v", cfg, err)
	}
}
