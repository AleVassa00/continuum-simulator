package main

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"continuum/internal/cloudworker"
	"continuum/internal/globalaggregator"
	"continuum/internal/model"
	"github.com/segmentio/kafka-go"
)

func TestConfiguredPartitionPipeline(t *testing.T) {
	for _, count := range []int{1, 3, 6, 8} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var outputs []model.GlobalAggregate
			g, err := globalaggregator.New(count, func(_ context.Context, a model.GlobalAggregate) error {
				outputs = append(outputs, a)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, 13) // Producer count is independent of the source partition count.
			for e := range ids {
				ids[e] = fmt.Sprintf("edge-%d", e)
			}
			partitionPlan := testEdgePartitionPlan(ids, count)
			membership, err := cloudworker.BuildMembership(partitionPlan, count)
			if err != nil {
				t.Fatal(err)
			}
			processors := make([]*CloudMessageProcessor, count)
			for p := range processors {
				a, err := cloudworker.NewPartitionAggregator(p, count, membership[p], cloudworker.DefaultWindowSize, cloudworker.DefaultMaxEdgeWatermarkSkew)
				if err != nil {
					t.Fatal(err)
				}
				processors[p] = &CloudMessageProcessor{aggregator: a, publishMessage: func(ctx context.Context, m kafka.Message) error { return feedGlobal(ctx, g, m) }}
				if err := processors[p].publishOutput(ctx, a.Initialize()); err != nil {
					t.Fatal(err)
				}
			}
			for minute := 0; minute < 35; minute += 5 {
				for _, id := range ids {
					m := edgeInput(t, id, minute, 1)
					m.Partition = partitionPlan[id]
					if err := processors[m.Partition].Process(ctx, m); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, id := range ids {
				partition := partitionPlan[id]
				end := edgeEndInput(id)
				end.Partition = partition
				if err := processors[partition].Process(ctx, end); err != nil {
					t.Fatal(err)
				}
			}
			if !g.IsComplete() || len(outputs) != 3 {
				t.Fatalf("incomplete output: %+v", outputs)
			}
			var events uint64
			for _, a := range outputs {
				events += a.Events
				if a.ExpectedPartitions != uint64(count) || a.ContributingPartitions != uint64(count) {
					t.Fatalf("wrong partition counts: %+v", a)
				}
			}
			if events != 7*13 {
				t.Fatalf("event conservation: %d", events)
			}
			if _, err := g.EndPartition(ctx, count); err == nil {
				t.Fatal("out-of-range EOS accepted")
			}
			if err := g.Progress(ctx, model.PartitionProgress{SourcePartition: count, CompleteThrough: testEpoch}); err == nil {
				t.Fatal("out-of-range progress accepted")
			}
			if _, err := cloudworker.NewPartitionAggregator(count, count, nil, cloudworker.DefaultWindowSize, cloudworker.DefaultMaxEdgeWatermarkSkew); err == nil {
				t.Fatal("out-of-range owner accepted")
			}
		})
	}
}

func TestWorkerPartitionCountConfiguration(t *testing.T) {
	t.Setenv("KAFKA_BROKER", "unused:9092")
	t.Setenv("CLOUD_EDGE_PARTITIONS", "edge-0:0")
	for _, tc := range []struct {
		value string
		want  int
	}{{"", model.DefaultSourcePartitionCount}, {"1", 1}, {"8", 8}} {
		t.Setenv("SOURCE_PARTITION_COUNT", tc.value)
		cfg, err := loadCloudWorkerConfig()
		if err != nil || cfg.SourcePartitionCount != tc.want {
			t.Fatalf("value=%q config=%+v err=%v", tc.value, cfg, err)
		}
	}
}
